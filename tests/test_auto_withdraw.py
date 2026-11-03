import asyncio

import pytest

from polymirror.broker import PaperBroker

META = {"condition_id": "0xc", "title": "T?", "outcome": "Yes"}
W = "0x" + "1" * 40


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def realize_pnl(store, amount: float) -> None:
    """Open and fully close a position for an exact realized PnL of
    `amount`: entry at 0.50, exit at 0.60 (profit) or 0.40 (loss), with
    size scaled so (exit - entry) * size == amount. Fixed +/-0.10 move
    (rather than deriving the exit price from a large `amount`) avoids
    PaperBroker's fill-price clamp to (0.001, 0.999), which a naive
    price-based approach would hit for amount >= 500."""
    price_move = 0.10 if amount >= 0 else -0.10
    size = abs(amount) / abs(price_move)
    broker = PaperBroker(store, slippage_bps=0, fee_bps=0)
    broker.buy(W, "tok", notional=size * 0.50, reference_price=0.50, meta=META)
    broker.sell(W, "tok", size=size, reference_price=0.50 + price_move)


def test_disabled_by_default_never_sweeps(engine, store, cfg):
    realize_pnl(store, 1000.0)
    cash_before = store.account()["cash"]
    run(engine.check_auto_withdraw())
    assert store.account()["cash"] == pytest.approx(cash_before)


def test_no_sweep_below_one_increment(engine, store, cfg):
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.profit_increment_usd = 500.0
    realize_pnl(store, 300.0)
    cash_before = store.account()["cash"]
    run(engine.check_auto_withdraw())
    assert store.account()["cash"] == pytest.approx(cash_before)
    assert store.get_meta("auto_withdraw_swept_usd") is None


def test_sweeps_exact_increment_at_threshold(engine, store, cfg):
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.profit_increment_usd = 500.0
    realize_pnl(store, 500.0)
    cash_before = store.account()["cash"]
    run(engine.check_auto_withdraw())
    assert store.account()["cash"] == pytest.approx(cash_before - 500.0)
    assert store.account()["total_withdrawals"] == pytest.approx(500.0)
    assert float(store.get_meta("auto_withdraw_swept_usd")) == pytest.approx(500.0)


def test_sweeps_multiple_whole_increments_in_one_shot(engine, store, cfg):
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.profit_increment_usd = 500.0
    realize_pnl(store, 1300.0)   # 2 whole increments owed, 300 left uncollected
    cash_before = store.account()["cash"]
    run(engine.check_auto_withdraw())
    assert store.account()["cash"] == pytest.approx(cash_before - 1000.0)
    assert float(store.get_meta("auto_withdraw_swept_usd")) == pytest.approx(1000.0)


def test_does_not_resweep_same_increment_next_cycle(engine, store, cfg):
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.profit_increment_usd = 500.0
    realize_pnl(store, 500.0)
    run(engine.check_auto_withdraw())
    cash_after_first = store.account()["cash"]
    run(engine.check_auto_withdraw())   # no new profit since — must be a no-op
    assert store.account()["cash"] == pytest.approx(cash_after_first)


def test_one_way_a_later_loss_never_claws_back_a_sweep(engine, store, cfg):
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.profit_increment_usd = 500.0
    realize_pnl(store, 500.0)
    run(engine.check_auto_withdraw())
    swept_after_first = float(store.get_meta("auto_withdraw_swept_usd"))
    realize_pnl(store, -300.0)  # net realized PnL now 200 — below the already-swept baseline
    cash_before = store.account()["cash"]
    run(engine.check_auto_withdraw())
    assert store.account()["cash"] == pytest.approx(cash_before)     # no extra withdrawal
    assert float(store.get_meta("auto_withdraw_swept_usd")) == pytest.approx(swept_after_first)

    realize_pnl(store, 800.0)  # cumulative realized now 200 + 800 = 1000 -> one more increment owed
    run(engine.check_auto_withdraw())
    assert float(store.get_meta("auto_withdraw_swept_usd")) == pytest.approx(1000.0)


def test_insufficient_free_cash_skips_without_erroring(engine, store, cfg):
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.profit_increment_usd = 500.0
    broker = PaperBroker(store, slippage_bps=0, fee_bps=0)
    broker.buy(W, "tok", notional=500, reference_price=0.50, meta=META)
    broker.sell(W, "tok", size=1000, reference_price=1.00)  # realized +500
    # Tie up nearly all cash in a new position, leaving too little free cash
    # to actually fund the sweep.
    acct = store.account()
    broker.buy(W, "tok2", notional=acct["cash"] - 10, reference_price=0.50, meta=META)
    cash_before = store.account()["cash"]

    run(engine.check_auto_withdraw())

    assert store.account()["cash"] == pytest.approx(cash_before)   # skipped, not partial
    assert store.get_meta("auto_withdraw_swept_usd") is None       # retry allowed next cycle


# ---------------------------------------------------- percent_starting_balance mode
def test_percent_mode_sweeps_matching_pct_of_starting_balance(engine, store, cfg):
    # cfg fixture sets starting_balance=10_000, so 5% == the same $500
    # increment the fixed_usd tests already exercise above.
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.mode = "percent_starting_balance"
    cfg.auto_withdraw.profit_increment_pct = 5.0
    realize_pnl(store, 500.0)
    cash_before = store.account()["cash"]
    run(engine.check_auto_withdraw())
    assert store.account()["cash"] == pytest.approx(cash_before - 500.0)
    assert float(store.get_meta("auto_withdraw_swept_usd")) == pytest.approx(500.0)


def test_percent_mode_scales_with_a_different_starting_balance(engine, store, cfg):
    # A $1,000 account at 5% should sweep $50 increments, not $500.
    store.reset_account(1_000.0)
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.mode = "percent_starting_balance"
    cfg.auto_withdraw.profit_increment_pct = 5.0
    realize_pnl(store, 50.0)
    cash_before = store.account()["cash"]
    run(engine.check_auto_withdraw())
    assert store.account()["cash"] == pytest.approx(cash_before - 50.0)


def test_percent_mode_below_threshold_does_not_sweep(engine, store, cfg):
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.mode = "percent_starting_balance"
    cfg.auto_withdraw.profit_increment_pct = 5.0   # $500 on a 10k account
    realize_pnl(store, 300.0)
    cash_before = store.account()["cash"]
    run(engine.check_auto_withdraw())
    assert store.account()["cash"] == pytest.approx(cash_before)


# --------------------------------------------------------------------- config
def test_bad_auto_withdraw_mode_rejected():
    from polymirror.config import Config
    cfg = Config()
    cfg.auto_withdraw.mode = "percent_balance"   # not a valid auto_withdraw mode
    with pytest.raises(ValueError, match="auto_withdraw.mode"):
        cfg.validate()


def test_percent_mode_requires_positive_pct_when_enabled():
    from polymirror.config import Config
    cfg = Config()
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.mode = "percent_starting_balance"
    cfg.auto_withdraw.profit_increment_pct = 0.0
    with pytest.raises(ValueError, match="profit_increment_pct"):
        cfg.validate()


def test_fixed_usd_mode_requires_positive_usd_when_enabled():
    from polymirror.config import Config
    cfg = Config()
    cfg.auto_withdraw.enabled = True
    cfg.auto_withdraw.profit_increment_usd = 0.0
    with pytest.raises(ValueError, match="profit_increment_usd"):
        cfg.validate()
