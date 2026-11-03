"""End-to-end engine tests against stubbed Polymarket APIs."""
import asyncio
import time

import pytest

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


def test_buy_is_copied_once_and_deduped(engine, store, stubs):
    data, _, _ = stubs
    follow(store)
    data.activity_events = [trade(W, "BUY", 1000, 0.50, ts=100)]
    run(engine.poll_all())
    run(engine.poll_all())  # same event again -> must not duplicate

    trades = store.trades()
    assert len(trades) == 1
    t = trades[0]
    assert t["kind"] == "OPEN" and t["side"] == "BUY"
    assert t["notional"] == pytest.approx(100.0)          # fixed_usd sizing
    assert store.account()["cash"] == pytest.approx(9_900)


def test_sells_mirror_source_fraction(engine, store, stubs):
    data, _, _ = stubs
    follow(store)
    data.activity_events = [trade(W, "BUY", 1000, 0.50, ts=100, tx="0x1")]
    run(engine.poll_all())
    sim = store.open_position(W, "111")
    original_size = sim["size"]

    # Source sells 40% of their 1000 shares -> we must sell 40% of ours.
    data.activity_events.append(trade(W, "SELL", 400, 0.70, ts=200, tx="0x2"))
    run(engine.poll_all())
    sim = store.open_position(W, "111")
    assert sim["size"] == pytest.approx(original_size * 0.6)

    # Source exits the rest -> our position closes fully.
    data.activity_events.append(trade(W, "SELL", 600, 0.80, ts=300, tx="0x3"))
    run(engine.poll_all())
    assert store.open_position(W, "111") is None
    kinds = [t["kind"] for t in store.trades()]
    assert "PARTIAL_EXIT" in kinds and "FULL_EXIT" in kinds


def test_adds_accumulate(engine, store, stubs):
    data, _, _ = stubs
    follow(store, mode="fixed_usd", value=100)
    data.activity_events = [
        trade(W, "BUY", 1000, 0.50, ts=100, tx="0x1"),
        trade(W, "BUY", 500, 0.60, ts=150, tx="0x2"),
    ]
    run(engine.poll_all())
    kinds = sorted(t["kind"] for t in store.trades())
    assert kinds == ["ADD", "OPEN"]


def test_sell_of_uncopied_position_is_skipped_gracefully(engine, store, stubs):
    data, _, _ = stubs
    follow(store)
    # Source had this position before we followed; we never bought it.
    data.positions_by_wallet[W] = [
        {"asset": "999", "conditionId": "0xc9", "size": 500, "avgPrice": 0.3,
         "title": "Old market", "outcome": "No"}
    ]
    data.activity_events = [trade(W, "SELL", 250, 0.5, ts=100, asset="999")]
    run(engine.poll_all())
    assert store.trades() == []
    assert store.account()["cash"] == pytest.approx(10_000)
    # Shadow model updated: 250 remaining
    shadow = store.source_position(W, "999")
    assert shadow["size"] == pytest.approx(250)


def test_proportional_sizing_uses_source_portfolio_value(engine, store, stubs):
    data, _, _ = stubs
    follow(store, mode="proportional", value=0)
    data.values[W] = 500_000.0
    # Source bets $5,000 (1% of book); our equity 10k -> $100 copy.
    data.activity_events = [trade(W, "BUY", 10_000, 0.50, ts=100)]
    run(engine.poll_all())
    assert store.trades()[0]["notional"] == pytest.approx(100.0)


def test_redeem_settles_open_copy(engine, store, stubs):
    data, clob, gamma = stubs
    follow(store)
    data.activity_events = [trade(W, "BUY", 1000, 0.50, ts=100, tx="0x1")]
    run(engine.poll_all())
    # Market resolved YES; our token "111" is the winning outcome.
    gamma.markets["0xc0nd"] = resolved_market(["111", "222"], winner_index=0)
    data.activity_events.append({
        "proxyWallet": W, "type": "REDEEM", "timestamp": T0 + 200, "asset": "111",
        "conditionId": "0xc0nd", "size": 1000, "usdcSize": 1000, "price": 1,
        "side": "", "transactionHash": "0xr", "title": "Test market?", "outcome": "Yes",
    })
    run(engine.poll_all())
    redeems = [t for t in store.trades() if t["kind"] == "REDEEM"]
    assert len(redeems) == 1
    assert redeems[0]["price"] == pytest.approx(1.0)
    assert store.open_position(W, "111") is None
    # 10k - 100 + 200 shares * $1 = 10_100
    assert store.account()["cash"] == pytest.approx(10_100)


def test_cursor_advances_and_survives_restart(engine, store, stubs, cfg):
    data, clob, gamma = stubs
    follow(store)
    data.activity_events = [trade(W, "BUY", 1000, 0.50, ts=100)]
    run(engine.poll_all())
    assert store.get_cursor(W) == T0 + 100

    # Simulate a restart: new engine, same store. The old event is still in
    # the API window (overlap) but must not re-execute.
    from polymirror.tracker import Engine
    from tests.conftest import SilentNotifier
    engine2 = Engine(cfg, store, data, clob, gamma, SilentNotifier(store))
    run(engine2.poll_all())
    assert len(store.trades()) == 1


def test_skips_when_sized_below_minimum(engine, store, stubs, cfg):
    data, _, _ = stubs
    cfg.execution.min_trade_usd = 50.0
    follow(store, mode="fixed_usd", value=10.0)
    data.activity_events = [trade(W, "BUY", 100, 0.5, ts=100)]
    run(engine.poll_all())
    assert store.trades() == []
    assert any(k == "trade_skipped" for _, k, _ in engine.notifier.sent)


def test_reconciliation_corrects_shadow_drift(engine, store, stubs):
    data, _, _ = stubs
    follow(store)
    run(engine.poll_all())  # seeds
    # Shadow says nothing; the source actually holds 5,000 shares (we missed
    # events while offline).
    data.positions_by_wallet[W] = [
        {"asset": "777", "conditionId": "0xc7", "size": 5000, "avgPrice": 0.2,
         "title": "Missed market", "outcome": "Yes"}
    ]
    run(engine.reconcile_all())
    shadow = store.source_position(W, "777")
    assert shadow is not None and shadow["size"] == pytest.approx(5000)
    assert any(k == "sync" for _, k, _ in engine.notifier.sent)


def test_market_quote_price_mode(engine, store, stubs, cfg):
    data, clob, _ = stubs
    cfg.execution.price_mode = "market_quote"
    follow(store)
    clob.quotes[("111", "BUY")] = 0.55  # market moved since the source filled at .50
    data.activity_events = [trade(W, "BUY", 1000, 0.50, ts=100)]
    run(engine.poll_all())
    assert store.trades()[0]["price"] == pytest.approx(0.55)


def test_take_profit_auto_exits_position(engine, store, stubs, cfg):
    _, clob, _ = stubs
    follow(store)
    engine.broker.buy(W, "111", 100.0, 0.40, {"condition_id": "0xc", "title": "T", "outcome": "Yes"})
    assert store.open_position(W, "111") is not None

    # room = 1 - 0.40 = 0.60; default 50% of room -> target 0.70. 0.75 clears it.
    clob.mids["111"] = 0.75
    run(engine.mark_to_market())
    run(engine.check_position_risk())

    assert store.open_position(W, "111") is None
    trades = store.trades()
    assert len(trades) == 1
    assert trades[0]["kind"] == "POSITION_RISK" and trades[0]["side"] == "SELL"
    assert trades[0]["realized_pnl"] > 0
    assert any(k == "risk" and "take-profit" in m for _, k, m in engine.notifier.sent)


def test_take_profit_is_reachable_from_expensive_entries(engine, store, stubs, cfg):
    """A flat-percentage take-profit can never fire from a 0.90 entry (max
    possible gain is ~11%, far under a 50% threshold) — the room-based
    formula must still be reachable near the price ceiling."""
    _, clob, _ = stubs
    follow(store)
    engine.broker.buy(W, "555", 100.0, 0.90, {"condition_id": "0xc", "title": "T", "outcome": "Yes"})

    # room = 1 - 0.90 = 0.10; 50% of room -> target 0.95.
    clob.mids["555"] = 0.95
    run(engine.mark_to_market())
    run(engine.check_position_risk())

    assert store.open_position(W, "555") is None
    trades = store.trades()
    assert trades[0]["kind"] == "POSITION_RISK"
    assert trades[0]["realized_pnl"] > 0


def test_stop_loss_auto_exits_position(engine, store, stubs, cfg):
    _, clob, _ = stubs
    follow(store)
    engine.broker.buy(W, "222", 100.0, 0.40, {"condition_id": "0xc", "title": "T", "outcome": "Yes"})

    clob.mids["222"] = 0.25  # -37.5% — below the default 25% stop-loss
    run(engine.mark_to_market())
    run(engine.check_position_risk())

    assert store.open_position(W, "222") is None
    trades = store.trades()
    assert trades[0]["kind"] == "POSITION_RISK"
    assert trades[0]["realized_pnl"] < 0
    assert any(k == "risk" and "stop-loss" in m for _, k, m in engine.notifier.sent)


def test_position_within_thresholds_stays_open(engine, store, stubs, cfg):
    _, clob, _ = stubs
    follow(store)
    engine.broker.buy(W, "333", 100.0, 0.40, {"condition_id": "0xc", "title": "T", "outcome": "Yes"})

    clob.mids["333"] = 0.45  # +12.5% — within both thresholds
    run(engine.mark_to_market())
    run(engine.check_position_risk())

    assert store.open_position(W, "333") is not None
    assert store.trades() == []


def test_position_risk_disabled_does_nothing(engine, store, stubs, cfg):
    _, clob, _ = stubs
    cfg.position_risk.enabled = False
    follow(store)
    engine.broker.buy(W, "444", 100.0, 0.40, {"condition_id": "0xc", "title": "T", "outcome": "Yes"})

    clob.mids["444"] = 0.90  # would massively exceed take-profit if enabled
    run(engine.mark_to_market())
    run(engine.check_position_risk())

    assert store.open_position(W, "444") is not None
    assert store.trades() == []


def test_close_position_sells_at_live_quote(engine, store, stubs):
    _, clob, _ = stubs
    follow(store)
    engine.broker.buy(W, "666", 100.0, 0.40, {"condition_id": "0xc", "title": "T", "outcome": "Yes"})
    pos = store.open_position(W, "666")
    clob.mids["666"] = 0.55

    result = run(engine.close_position(pos["id"]))

    assert result["status"] == "closed"
    assert store.open_position(W, "666") is None
    trades = store.trades()
    assert trades[0]["kind"] == "MANUAL_CLOSE" and trades[0]["price"] == pytest.approx(0.55, abs=0.01)
    assert trades[0]["realized_pnl"] > 0


def test_close_position_falls_back_to_avg_price_without_a_quote(engine, store, stubs):
    _, clob, _ = stubs
    follow(store)
    engine.broker.buy(W, "777", 100.0, 0.40, {"condition_id": "0xc", "title": "T", "outcome": "Yes"})
    pos = store.open_position(W, "777")
    # no clob.mids entry for "777" -> midpoint() returns None

    result = run(engine.close_position(pos["id"]))
    assert result["status"] == "closed"
    assert store.trades()[0]["price"] == pytest.approx(0.40, abs=0.01)


def test_close_position_returns_none_for_unknown_or_closed(engine, store):
    assert run(engine.close_position(999_999)) is None


def test_poll_all_polls_wallets_concurrently(engine, store, stubs):
    """A slow/retrying wallet must not add its full delay in front of every
    other wallet — poll_all used to await wallets one at a time, so N slow
    wallets cost N * delay; concurrent polling costs ~1 * delay regardless
    of N."""
    data, _, _ = stubs
    wallets = ["0x" + str(i) * 40 for i in range(4)]
    for addr in wallets:
        store.add_wallet(addr, "", "fixed_usd", 100.0, 1.0)

    delay = 0.15

    async def slow_activity_since(wallet, start, types="TRADE,REDEEM"):
        await asyncio.sleep(delay)
        return []

    data.activity_since = slow_activity_since

    start = time.monotonic()
    run(engine.poll_all())
    elapsed = time.monotonic() - start

    # Sequential would take ~4 * delay = 0.6s; concurrent should stay close to 1 * delay.
    assert elapsed < delay * 2.5, f"poll_all took {elapsed:.3f}s for {len(wallets)} wallets — looks sequential"


def test_cursor_reinit_survives_a_reset_even_if_already_seeded(engine, store, stubs):
    """Regression: a DB-level reset (e.g. POST /api/reset) clears the
    cursors table, but an already-running engine's in-memory `_seeded` set
    survives it. Cursor initialization used to live inside seed_wallet(),
    gated by `_seeded` — so a wallet seeded before the reset would never
    get its cursor reinitialized, and the next poll would fetch with
    start=0: the wallet's entire history, replayed as brand-new trades."""
    follow(store)
    data, _, _ = stubs
    engine._seeded.add(W)                 # simulate: already seeded this session
    assert store.get_cursor(W) == 0        # simulate: reset just cleared the cursor

    year_old_ts = -365 * 86400
    data.activity_events = [trade(W, "BUY", 1000, 0.50, ts=year_old_ts)]

    run(engine.poll_wallet(store.wallet(W)))

    assert store.trades() == []            # the year-old event must NOT be copied
    assert store.get_cursor(W) > T0 + year_old_ts
