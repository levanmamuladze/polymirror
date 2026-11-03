import sqlite3

import pytest

from polymirror.broker import NoPosition, PaperBroker
from polymirror.storage import InsufficientCash, Store

META = {"condition_id": "0xc", "title": "T?", "outcome": "Yes"}
W = "0x" + "1" * 40


def test_buy_opens_position_and_debits_cash(store):
    broker = PaperBroker(store, slippage_bps=0, fee_bps=0)
    fill = broker.buy(W, "tok", notional=500, reference_price=0.50, meta=META)
    assert fill.kind == "OPEN"
    assert fill.size == pytest.approx(1000)
    assert store.account()["cash"] == pytest.approx(9_500)
    pos = store.open_position(W, "tok")
    assert pos["size"] == pytest.approx(1000)
    assert pos["avg_price"] == pytest.approx(0.50)


def test_add_averages_cost(store):
    broker = PaperBroker(store, 0, 0)
    broker.buy(W, "tok", 500, 0.50, META)          # 1000 @ .50
    fill = broker.buy(W, "tok", 600, 0.60, META)   # 1000 @ .60
    assert fill.kind == "ADD"
    pos = store.open_position(W, "tok")
    assert pos["size"] == pytest.approx(2000)
    assert pos["avg_price"] == pytest.approx(0.55)


def test_partial_and_full_exit_pnl(store):
    broker = PaperBroker(store, 0, 0)
    broker.buy(W, "tok", 500, 0.50, META)                    # 1000 @ .50
    f1 = broker.sell(W, "tok", 400, reference_price=0.70)
    assert f1.kind == "PARTIAL_EXIT"
    assert f1.realized_pnl == pytest.approx(0.20 * 400)
    f2 = broker.sell(W, "tok", 600, reference_price=0.40)
    assert f2.kind == "FULL_EXIT"
    assert f2.realized_pnl == pytest.approx(-0.10 * 600)
    assert store.open_position(W, "tok") is None
    # cash: 10000 - 500 + 280 + 240 = 10020, matching net pnl of +20
    assert store.account()["cash"] == pytest.approx(10_020)


def test_slippage_hurts_both_sides(store):
    broker = PaperBroker(store, slippage_bps=100, fee_bps=0)  # 1%
    buy = broker.buy(W, "tok", 500, 0.50, META)
    assert buy.price == pytest.approx(0.505)
    sell = broker.sell(W, "tok", buy.size, 0.50)
    assert sell.price == pytest.approx(0.495)


def test_fee_reduces_shares_and_realized(store):
    broker = PaperBroker(store, 0, fee_bps=100)  # 1%
    buy = broker.buy(W, "tok", 500, 0.50, META)
    assert buy.fee == pytest.approx(5.0)
    assert buy.size == pytest.approx(495 / 0.50)


def test_insufficient_cash_rejected(store):
    broker = PaperBroker(store, 0, 0)
    with pytest.raises(InsufficientCash):
        broker.buy(W, "tok", 999_999, 0.50, META)
    assert store.account()["cash"] == pytest.approx(10_000)


def test_sell_without_position_raises(store):
    broker = PaperBroker(store, 0, 0)
    with pytest.raises(NoPosition):
        broker.sell(W, "ghost", 10, 0.5)


def test_oversell_clamped_to_position(store):
    broker = PaperBroker(store, 0, 0)
    broker.buy(W, "tok", 500, 0.50, META)
    fill = broker.sell(W, "tok", 5_000, 0.50)
    assert fill.size == pytest.approx(1000)
    assert fill.kind == "FULL_EXIT"


def test_settlement_at_one_dollar(store):
    broker = PaperBroker(store, slippage_bps=500, fee_bps=100)
    broker.buy(W, "tok", 500, 0.50, META)
    fill = broker.sell(W, "tok", 10_000, 1.0, settlement=True)
    assert fill.price == 1.0        # no slippage on redemption
    assert fill.fee == 0.0          # no fee on redemption


def test_same_asset_can_open_and_fully_close_more_than_once(store):
    """Regression for a real incident: a blanket UNIQUE(wallet, asset,
    status) on sim_positions allowed only ONE closed position ever per
    (wallet, asset). Closing a SECOND position on the same asset violated
    it; the failure surfaced after cash had already been credited in a
    separate transaction, permanently orphaning the credit with no
    matching trade (repeating every time the stuck position was
    re-evaluated). Opening/closing the same asset twice must work cleanly,
    and cash must reconcile exactly against the two round trips."""
    broker = PaperBroker(store, 0, 0)
    broker.buy(W, "tok", 500, 0.50, META)                 # 1000 @ .50
    f1 = broker.sell(W, "tok", 1000, 0.60)                # +100, closes
    assert f1.kind == "FULL_EXIT"
    assert store.open_position(W, "tok") is None

    broker.buy(W, "tok", 500, 0.40, META)                 # new position, same asset
    f2 = broker.sell(W, "tok", 1250, 0.50)                # +125, closes again
    assert f2.kind == "FULL_EXIT"
    assert store.open_position(W, "tok") is None

    closed = [p for p in store.closed_positions() if p["wallet"] == W and p["asset"] == "tok"]
    assert len(closed) == 2
    # cash: 10000 - 500 + 600 - 500 + 625 = 10225, matching net pnl of +225
    assert store.account()["cash"] == pytest.approx(10_225)


def test_apply_fill_is_atomic_cash_and_position_together(store):
    """If the position half of a fill fails, the cash half must not be
    committed either — a failure partway through a fill must never leave
    cash and positions inconsistent with each other."""
    from polymirror.storage import InsufficientCash

    bad_position = {
        "wallet": W, "asset": "tok2", "condition_id": "0xc",
        "title": "T", "outcome": "Yes",
        "size": 100.0, "avg_price": 0.5, "mark_price": 0.5,
        "status": "open", "realized_pnl": 0.0,
    }
    with pytest.raises(InsufficientCash):
        store.apply_fill(-999_999, bad_position)
    assert store.account()["cash"] == pytest.approx(10_000)   # unchanged
    assert store.open_position(W, "tok2") is None              # never created


def test_migrates_databases_off_the_old_blanket_unique_constraint(tmp_path):
    """A database created before this fix has UNIQUE(wallet, asset, status)
    on sim_positions. Opening a Store against it must migrate the table —
    preserving existing rows — so a second close on the same asset, which
    used to violate that constraint, now succeeds."""
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE sim_positions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet       TEXT NOT NULL,
            asset        TEXT NOT NULL,
            condition_id TEXT NOT NULL DEFAULT '',
            title        TEXT NOT NULL DEFAULT '',
            outcome      TEXT NOT NULL DEFAULT '',
            size         REAL NOT NULL,
            avg_price    REAL NOT NULL,
            mark_price   REAL,
            opened_at    INTEGER NOT NULL,
            updated_at   INTEGER NOT NULL,
            status       TEXT NOT NULL DEFAULT 'open',
            closed_at    INTEGER,
            realized_pnl REAL NOT NULL DEFAULT 0.0,
            UNIQUE (wallet, asset, status)
        );
        INSERT INTO sim_positions
            (wallet, asset, size, avg_price, mark_price, opened_at, updated_at,
             status, closed_at, realized_pnl)
            VALUES ('0xabc', 'tok', 0, 0.5, 0.6, 1000, 1000, 'closed', 1000, 42.0);
    """)
    conn.commit()
    conn.close()

    store = Store(str(db_path))
    try:
        closed = store.closed_positions()
        assert len(closed) == 1
        assert closed[0]["realized_pnl"] == pytest.approx(42.0)   # old data preserved

        store.init_account(10_000.0)
        broker = PaperBroker(store, 0, 0)
        broker.buy("0xabc", "tok", 500, 0.50, META)
        fill = broker.sell("0xabc", "tok", 1000, 0.60)            # the exact failure mode
        assert fill.kind == "FULL_EXIT"
    finally:
        store.close()
