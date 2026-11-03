"""Settlement of resolved markets — the accounting invariant that decides
whether the paper record means anything.

Background: the only path to settlement used to be the source wallet's
on-chain REDEEM event, and `settlement_price` ended in `return 1.0`.
Traders only redeem *winners* (a losing outcome token is worthless, so
nobody pays gas to claim $0), so losers never settled at all — they stayed
open forever, marked at their entry-era price because a resolved market
returns no CLOB midpoint for mark-to-market to pick up. Every winner booked
$1, no loser ever booked $0, and the equity curve could only rise.

These tests pin both halves of the fix: losers must settle at $0 off the
engine's own resolution sweep, and an *unknown* resolution must never be
mistaken for a win.
"""
from __future__ import annotations

import asyncio
import json
import time

import pytest

from polymirror.clients import parse_resolution
from tests.conftest import T0, resolved_market, trade

W = "0x" + "a" * 40


def follow(store, mode="fixed_usd", value=100.0, mult=1.0):
    store.add_wallet(W, "tester", mode, value, mult)
    return store.wallet(W)


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def buy(engine, store, data, gamma, *, asset, condition, price=0.50, size=1000, ts=100):
    """Copy one source buy so the engine holds an open position."""
    data.activity_events = [trade(W, "BUY", size, price, ts=ts, tx=f"0x{asset}",
                                  asset=asset, conditionId=condition)]
    run(engine.poll_all())
    return store.open_position(W, asset)


# --------------------------------------------------------------- parsing

def test_winner_pays_one_and_loser_pays_zero():
    market = resolved_market(["tokA", "tokB"], winner_index=0)
    assert parse_resolution(market, "tokA") == 1.0
    assert parse_resolution(market, "tokB") == 0.0


def test_payout_follows_the_token_index_not_position_order():
    """The losing side must not be settled at the winner's payout. Getting
    this backwards would be worse than not settling at all."""
    market = resolved_market(["tokA", "tokB"], winner_index=1)
    assert parse_resolution(market, "tokA") == 0.0
    assert parse_resolution(market, "tokB") == 1.0


def test_open_market_never_settles():
    market = resolved_market(["tokA", "tokB"], winner_index=None, closed=False)
    assert parse_resolution(market, "tokA") is None


def test_closed_but_indecisive_prices_do_not_settle():
    """A market quoting 0.62 is trading, not resolved — settling it would
    book a fictitious payout."""
    market = resolved_market(["tokA", "tokB"], winner_index=None, closed=True)
    assert parse_resolution(market, "tokA") is None


def test_near_certain_price_is_not_a_resolution():
    """0.98 is a near-certainty, not a payout. Only 0/1 within tolerance."""
    market = {"closed": True, "clobTokenIds": json.dumps(["tokA", "tokB"]),
              "outcomePrices": json.dumps(["0.98", "0.02"])}
    assert parse_resolution(market, "tokA") is None


def test_unknown_token_is_unknown_not_a_win():
    market = resolved_market(["tokA", "tokB"], winner_index=0)
    assert parse_resolution(market, "tokZ") is None


@pytest.mark.parametrize("market", [
    None,
    {},
    {"closed": True},
    {"closed": True, "clobTokenIds": "not json", "outcomePrices": "[]"},
    # tokens and prices disagree in length — index would be meaningless
    {"closed": True, "clobTokenIds": json.dumps(["tokA", "tokB"]),
     "outcomePrices": json.dumps(["1"])},
])
def test_malformed_payloads_return_unknown(market):
    assert parse_resolution(market, "tokA") is None


def test_accepts_real_arrays_as_well_as_json_strings():
    """Gamma returns these fields both ways depending on deployment."""
    market = resolved_market(["tokA", "tokB"], winner_index=0, as_json_strings=False)
    assert parse_resolution(market, "tokA") == 1.0


def test_uma_resolved_status_counts_as_closed():
    market = {"umaResolutionStatus": "resolved",
              "clobTokenIds": json.dumps(["tokA", "tokB"]),
              "outcomePrices": json.dumps(["0", "1"])}
    assert parse_resolution(market, "tokB") == 1.0


# ------------------------------------------------------- the sweep itself

def test_losing_position_settles_at_zero(engine, store, stubs):
    """The regression that invalidated the whole paper record: no source
    REDEEM ever arrives for a loser, so only the sweep can book it."""
    data, _, gamma = stubs
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd", price=0.50)
    cash_before = store.account()["cash"]
    assert cash_before == pytest.approx(9_900.0)

    gamma.markets["0xc0nd"] = resolved_market(["111", "222"], winner_index=1)
    run(engine.settle_resolved_positions())

    settles = [t for t in store.trades() if t["kind"] == "SETTLE"]
    assert len(settles) == 1
    assert settles[0]["price"] == pytest.approx(0.0)
    assert settles[0]["realized_pnl"] == pytest.approx(-100.0)
    assert store.open_position(W, "111") is None
    # Position was a total loss: no cash comes back.
    assert store.account()["cash"] == pytest.approx(9_900.0)


def test_winning_position_settles_without_a_source_redeem(engine, store, stubs):
    data, _, gamma = stubs
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd", price=0.50)

    gamma.markets["0xc0nd"] = resolved_market(["111", "222"], winner_index=0)
    run(engine.settle_resolved_positions())

    settles = [t for t in store.trades() if t["kind"] == "SETTLE"]
    assert len(settles) == 1
    assert settles[0]["price"] == pytest.approx(1.0)
    # $100 bought 200 shares at 0.50; they pay $1 each.
    assert settles[0]["realized_pnl"] == pytest.approx(100.0)
    assert store.account()["cash"] == pytest.approx(10_100.0)


def test_unresolved_position_is_left_open(engine, store, stubs):
    data, _, gamma = stubs
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd")
    gamma.markets["0xc0nd"] = resolved_market(["111", "222"], winner_index=None,
                                              closed=False)
    run(engine.settle_resolved_positions())

    assert [t for t in store.trades() if t["kind"] == "SETTLE"] == []
    assert store.open_position(W, "111") is not None


def test_sweep_is_idempotent(engine, store, stubs):
    """Exactly-once: the key is claimed before the fill, so a repeated sweep
    cannot double-book a settlement."""
    data, _, gamma = stubs
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd")
    gamma.markets["0xc0nd"] = resolved_market(["111", "222"], winner_index=0)

    run(engine.settle_resolved_positions())
    run(engine.settle_resolved_positions())
    run(engine.settle_resolved_positions())

    assert len([t for t in store.trades() if t["kind"] == "SETTLE"]) == 1
    assert store.account()["cash"] == pytest.approx(10_100.0)


def test_sweep_disabled_does_nothing(engine, store, stubs, cfg):
    data, _, gamma = stubs
    cfg.settlement.enabled = False
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd")
    gamma.markets["0xc0nd"] = resolved_market(["111", "222"], winner_index=1)

    run(engine.settle_resolved_positions())

    assert [t for t in store.trades() if t["kind"] == "SETTLE"] == []
    assert store.open_position(W, "111") is not None


def test_mixed_book_nets_winners_against_losers(engine, store, stubs):
    """The aggregate property the old code could not express: a book of one
    winner and one loser at even odds is a wash, not a doubling."""
    data, _, gamma = stubs
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xcond1", price=0.50)
    buy(engine, store, data, gamma, asset="333", condition="0xcond2", price=0.50, ts=200)
    assert store.account()["cash"] == pytest.approx(9_800.0)

    gamma.markets["0xcond1"] = resolved_market(["111", "222"], winner_index=0)  # win
    gamma.markets["0xcond2"] = resolved_market(["333", "444"], winner_index=1)  # loss
    run(engine.settle_resolved_positions())

    assert len([t for t in store.trades() if t["kind"] == "SETTLE"]) == 2
    assert store.open_positions() == []
    # +$100 on the winner, -$100 on the loser: back to the starting balance.
    assert store.account()["cash"] == pytest.approx(10_000.0)
    assert store.realized_pnl_total() == pytest.approx(0.0)


def test_unresolved_alert_fires_once_when_position_is_stale(engine, store, stubs, cfg):
    data, _, gamma = stubs
    cfg.settlement.unresolved_alert_hours = 24.0
    follow(store)
    pos = buy(engine, store, data, gamma, asset="111", condition="0xc0nd")
    # Backdate the position well past the alert threshold.
    store._exec("UPDATE sim_positions SET opened_at = ? WHERE id = ?",
                (int(time.time()) - 72 * 3600, pos["id"]))

    run(engine.settle_resolved_positions())
    run(engine.settle_resolved_positions())

    warnings = [m for lvl, kind, m in engine.notifier.sent
                if lvl == "warning" and "no readable resolution" in m]
    assert len(warnings) == 1


def test_fresh_unresolved_position_does_not_alert(engine, store, stubs, cfg):
    data, _, gamma = stubs
    cfg.settlement.unresolved_alert_hours = 24.0
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd")

    run(engine.settle_resolved_positions())

    assert [m for lvl, kind, m in engine.notifier.sent
            if "no readable resolution" in m] == []


# ------------------------------------------------------------------ config

def test_settlement_is_on_by_default():
    """Off by default would silently reintroduce the winners-only bias."""
    from polymirror.config import Config
    assert Config().settlement.enabled is True


@pytest.mark.parametrize("field,bad", [
    ("interval_seconds", 5.0),          # would hammer Gamma
    ("unresolved_alert_hours", -1.0),
])
def test_invalid_settlement_config_is_rejected(field, bad):
    from polymirror.config import Config
    cfg = Config()
    setattr(cfg.settlement, field, bad)
    with pytest.raises(ValueError):
        cfg.validate()


# -------------------------------------------------- redeem-path regression

def _redeem_event(asset="111", condition="0xc0nd", ts=200):
    return {"proxyWallet": W, "type": "REDEEM", "timestamp": T0 + ts, "asset": asset,
            "conditionId": condition, "size": 1000, "usdcSize": 1000, "price": 1,
            "side": "", "transactionHash": "0xr", "title": "Test market?",
            "outcome": "Yes"}


def test_redeem_with_unknown_resolution_does_not_book_a_win(engine, store, stubs):
    """The old `return 1.0` fallback: an unpriceable position was booked as
    a full $1 payout. Unknown must leave it open instead."""
    data, _, gamma = stubs
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd")
    cash_before = store.account()["cash"]

    # Source redeemed, but Gamma tells us nothing about the outcome.
    data.activity_events = [_redeem_event()]
    run(engine.poll_all())

    assert [t for t in store.trades() if t["kind"] == "REDEEM"] == []
    assert store.open_position(W, "111") is not None
    assert store.account()["cash"] == pytest.approx(cash_before)


def test_redeem_settles_a_losing_side_at_zero(engine, store, stubs):
    """A source redeeming one side of a condition must not pay out *our*
    side when ours is the loser."""
    data, _, gamma = stubs
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd")
    gamma.markets["0xc0nd"] = resolved_market(["111", "222"], winner_index=1)

    data.activity_events = [_redeem_event(asset="222")]
    run(engine.poll_all())

    redeems = [t for t in store.trades() if t["kind"] == "REDEEM"]
    assert len(redeems) == 1
    assert redeems[0]["price"] == pytest.approx(0.0)
    assert store.account()["cash"] == pytest.approx(9_900.0)


def test_position_unpriced_at_redeem_is_recovered_by_the_sweep(engine, store, stubs):
    """Unknown-at-redeem is a deferral, not a loss of the position: once
    Gamma reports the resolution, the sweep settles it."""
    data, _, gamma = stubs
    follow(store)
    buy(engine, store, data, gamma, asset="111", condition="0xc0nd")

    data.activity_events = [_redeem_event()]
    run(engine.poll_all())
    assert store.open_position(W, "111") is not None

    gamma.markets["0xc0nd"] = resolved_market(["111", "222"], winner_index=0)
    run(engine.settle_resolved_positions())

    assert store.open_position(W, "111") is None
    assert store.account()["cash"] == pytest.approx(10_100.0)
