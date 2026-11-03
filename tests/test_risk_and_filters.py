"""Stop-trading controls, portfolio risk limits, copy filters, sizing
semantics and copy analytics."""
import asyncio
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from polymirror.api import create_app
from polymirror.config import load_config
from polymirror.metrics import copy_analytics
from polymirror.storage import Store
from tests.conftest import trade

W = "0x" + "c" * 40
META = {"condition_id": "0xc", "title": "T?", "outcome": "Yes"}


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def follow(store, mode="fixed_usd", value=100.0):
    store.add_wallet(W, "tester", mode, value, 1.0)


# ---------------------------------------------------------- sizing semantics
def test_percent_balance_compounds_on_remaining_cash(engine, store, stubs, cfg):
    """The user's spec: 50% on a $10k wallet invests $5k; the next 50%
    invests $2.5k (50% of the remaining cash)."""
    data, _, _ = stubs
    cfg.portfolio.max_single_market_pct = 100.0
    follow(store, mode="percent_balance", value=0.5)
    data.activity_events = [
        trade(W, "BUY", 1000, 0.50, ts=100, tx="0x1", asset="111", conditionId="0xa"),
        trade(W, "BUY", 1000, 0.50, ts=200, tx="0x2", asset="222", conditionId="0xb"),
    ]
    run(engine.poll_all())
    notionals = [t["notional"] for t in reversed(store.trades())]
    assert notionals == [pytest.approx(5_000.0), pytest.approx(2_500.0)]
    assert store.account()["cash"] == pytest.approx(2_500.0)


def test_percent_starting_balance_is_constant_sizing(engine, store, stubs, cfg):
    data, _, _ = stubs
    cfg.portfolio.max_single_market_pct = 100.0
    follow(store, mode="percent_starting_balance", value=0.5)
    data.activity_events = [
        trade(W, "BUY", 1000, 0.50, ts=100, tx="0x1", asset="111", conditionId="0xa"),
        trade(W, "BUY", 1000, 0.50, ts=200, tx="0x2", asset="222", conditionId="0xb"),
    ]
    run(engine.poll_all())
    notionals = [t["notional"] for t in reversed(store.trades())]
    # Always 50% of the $10k starting balance (second capped only by cash).
    assert notionals == [pytest.approx(5_000.0), pytest.approx(5_000.0)]


# ------------------------------------------------------------- halt control
def test_halt_blocks_buys_but_exits_still_mirror(engine, store, stubs):
    data, _, _ = stubs
    follow(store)
    data.activity_events = [trade(W, "BUY", 1000, 0.50, ts=100, tx="0x1")]
    run(engine.poll_all())
    assert len(store.trades()) == 1

    store.set_meta("copy_halted", "manual")
    data.activity_events += [
        trade(W, "SELL", 1000, 0.70, ts=200, tx="0x2"),          # exit -> must mirror
        trade(W, "BUY", 1000, 0.50, ts=300, tx="0x3", asset="999",
              conditionId="0x9"),                                # new buy -> blocked
    ]
    run(engine.poll_all())
    kinds = [t["kind"] for t in store.trades()]
    assert "FULL_EXIT" in kinds
    assert kinds.count("OPEN") == 1                              # no second entry
    assert store.open_position(W, "999") is None


def test_manual_halt_via_api(client):
    r = client.post("/api/trading", json={"copying": False})
    assert r.json() == {"copying": False, "halt_reason": "manual"}
    assert client.get("/api/trading").json()["copying"] is False
    assert client.get("/api/summary").json()["trading"]["halt_reason"] == "manual"
    r = client.post("/api/trading", json={"copying": True})
    assert r.json()["copying"] is True


def test_daily_loss_halts_and_rehalts_only_on_new_losses(engine, store, cfg):
    cfg.portfolio.max_daily_loss_pct = 5.0                           # test the mechanism at a fixed cap
    store.snapshot_equity(int(time.time()), 10_000, 10_000, 0, 0)   # today's baseline
    store.adjust_cash(-600)                                          # -6% > 5% cap
    run(engine.check_portfolio_risk())
    assert (store.get_meta("copy_halted") or "").startswith("auto: daily loss")

    store.set_meta("copy_halted", "")                                # user resumes
    run(engine.check_portfolio_risk())
    assert store.get_meta("copy_halted") == ""                       # no new losses -> honored

    store.adjust_cash(-100)                                          # losses deepen
    run(engine.check_portfolio_risk())
    assert (store.get_meta("copy_halted") or "").startswith("auto:")


def test_daily_rule_refires_on_a_new_day(engine, store, cfg):
    """An overridden halt must not suppress a NEW day's daily-loss breach."""
    cfg.portfolio.max_daily_loss_pct = 5.0                           # test the mechanism at a fixed cap
    store.snapshot_equity(int(time.time()), 10_000, 10_000, 0, 0)
    store.adjust_cash(-600)
    run(engine.check_portfolio_risk())
    assert (store.get_meta("copy_halted") or "").startswith("auto: daily loss")

    store.set_meta("copy_halted", "")                                # user resumes
    store.set_meta("halt_day", str(int(time.time()) // 86_400 - 1))  # halt was "yesterday"
    run(engine.check_portfolio_risk())                               # same equity, new day
    assert (store.get_meta("copy_halted") or "").startswith("auto: daily loss")


def test_reset_clears_halt_and_risk_state(store):
    store.set_meta("copy_halted", "manual")
    store.set_meta("halt_equity_mark", "9400")
    store.set_meta(f"autopause:{W}", "5")
    store.reset_account(10_000)
    assert store.get_meta("copy_halted") is None
    assert store.get_meta("halt_equity_mark") is None
    assert store.get_meta(f"autopause:{W}") is None


def test_user_pause_is_never_auto_resumed(engine, store, cfg):
    """A stale cool-off marker from an old auto-pause must not override a
    pause the user made manually via the API."""
    store.add_wallet(W, "manual-pause", "fixed_usd", 100.0, 1.0)
    store.set_meta(f"autopause_at:{W}", str(int(time.time()) - 100 * 3600))  # old auto-pause
    app = create_app(cfg, store=store)
    with TestClient(app) as c:
        r = c.patch(f"/api/wallets/{W}", json={"active": False})    # user pauses
        assert r.status_code == 200
    run(engine.auto_manage())                                        # cool-off long elapsed
    assert store.wallet(W)["active"] == 0                            # user pause stands


def test_default_daily_loss_cap_is_tighter_than_old_20pct():
    """Regression guard for the tightened default (20% -> 10%) — a single
    bad day should trip well before a third of the way to ruin."""
    from polymirror.config import Config
    assert Config().portfolio.max_daily_loss_pct == pytest.approx(10.0)


def test_total_drawdown_halts(engine, store, cfg):
    cfg.portfolio.max_daily_loss_pct = 100.0                         # isolate the drawdown rule
    store.snapshot_equity(int(time.time()) - 60, 12_000, 12_000, 0, 0)
    store.adjust_cash(-600)                                          # equity 9.4k vs 12k peak = -21.7%
    run(engine.check_portfolio_risk())
    assert "drawdown" in (store.get_meta("copy_halted") or "")


def test_daily_loss_is_not_triggered_by_a_withdrawal(engine, store, cfg):
    """A withdrawal reduces cash/equity directly but is not a trading
    loss — it must never trip the daily-loss auto-halt."""
    cfg.portfolio.max_daily_loss_pct = 5.0
    store.snapshot_equity(int(time.time()), 10_000, 10_000, 0, 0)   # today's baseline, no transfers yet
    store.withdraw(2_000.0)                                          # naive: -20% of starting balance
    run(engine.check_portfolio_risk())
    assert not (store.get_meta("copy_halted") or "")


def test_daily_loss_still_triggers_after_a_deposit(engine, store, cfg):
    """A same-day deposit must not mask a genuine trading loss."""
    cfg.portfolio.max_daily_loss_pct = 5.0
    store.snapshot_equity(int(time.time()), 10_000, 10_000, 0, 0)   # today's baseline, no transfers yet
    store.adjust_cash(-600)                                          # a real -6% trading loss
    store.deposit(5_000.0)                                          # raw equity now looks fine (14.4k)
    run(engine.check_portfolio_risk())
    assert (store.get_meta("copy_halted") or "").startswith("auto: daily loss")


def test_drawdown_is_not_triggered_by_a_withdrawal(engine, store, cfg):
    """A withdrawal off a real peak is not a trading drawdown."""
    cfg.portfolio.max_daily_loss_pct = 100.0                         # isolate the drawdown rule
    store.adjust_cash(2_000.0)                                       # pure trading gain -> 12k
    store.snapshot_equity(int(time.time()) - 60, 12_000, 12_000, 0, 0)   # peak, no transfers yet
    store.withdraw(3_000.0)                                          # naive: -25% from peak
    run(engine.check_portfolio_risk())
    assert not (store.get_meta("copy_halted") or "")


def test_drawdown_still_triggers_after_a_deposit(engine, store, cfg):
    """A deposit must not mask a genuine drawdown from a real peak."""
    cfg.portfolio.max_daily_loss_pct = 100.0                         # isolate the drawdown rule
    store.snapshot_equity(int(time.time()) - 60, 12_000, 12_000, 0, 0)   # peak, no transfers yet
    store.adjust_cash(-2_600.0)                                      # a real trading loss
    store.deposit(5_000.0)                                          # raw equity now looks fine
    run(engine.check_portfolio_risk())
    assert "drawdown" in (store.get_meta("copy_halted") or "")


# ------------------------------------------------------------ buy-side gates
def buy_event(price, asset="111", cond="0xa", tx="0x1", ts=100):
    return trade(W, "BUY", 1000, price, ts=ts, tx=tx, asset=asset, conditionId=cond)


def test_entry_price_band_filters(engine, store, stubs, cfg):
    data, _, _ = stubs
    cfg.copy_filters.ignore_markets_near_resolution = False
    follow(store)
    data.activity_events = [buy_event(0.93, tx="0x1", asset="1", cond="0xa"),
                            buy_event(0.05, tx="0x2", asset="2", cond="0xb", ts=200)]
    run(engine.poll_all())
    assert store.trades() == []                                      # both outside 0.10-0.90


def test_near_resolution_filter(engine, store, stubs, cfg):
    data, _, _ = stubs
    cfg.copy_filters.max_entry_price = 0.99
    follow(store)
    data.activity_events = [buy_event(0.96)]
    run(engine.poll_all())
    assert store.trades() == []
    assert any("near resolution" in m for _, _, m in engine.notifier.sent)


def test_copy_buys_and_sells_toggles(engine, store, stubs, cfg):
    data, _, _ = stubs
    follow(store)
    data.activity_events = [buy_event(0.50, tx="0x1")]
    run(engine.poll_all())
    assert len(store.trades()) == 1

    cfg.copy_filters.copy_sells = False
    data.activity_events += [trade(W, "SELL", 1000, 0.70, ts=200, tx="0x2",
                                   asset="111", conditionId="0xa")]
    run(engine.poll_all())
    assert store.open_position(W, "111") is not None                 # sell not mirrored

    cfg.copy_filters.copy_buys = False
    data.activity_events += [buy_event(0.50, tx="0x3", asset="333", cond="0xc", ts=300)]
    run(engine.poll_all())
    assert store.open_position(W, "333") is None


def test_max_open_positions_cap(engine, store, stubs, cfg):
    data, _, _ = stubs
    cfg.portfolio.max_open_positions = 1
    follow(store)
    data.activity_events = [buy_event(0.50, tx="0x1", asset="1", cond="0xa"),
                            buy_event(0.50, tx="0x2", asset="2", cond="0xb", ts=200)]
    run(engine.poll_all())
    assert len(store.open_positions()) == 1
    assert any("max_open_positions" in m for _, _, m in engine.notifier.sent)


def test_max_single_market_exposure_cap(engine, store, stubs, cfg):
    data, _, _ = stubs
    follow(store, mode="fixed_usd", value=3_000.0)   # 30% of equity > 20% cap
    data.activity_events = [buy_event(0.50)]
    run(engine.poll_all())
    assert store.trades() == []
    assert any("exposure" in m for _, _, m in engine.notifier.sent)


def test_illiquid_market_filter(engine, store, stubs, cfg):
    data, _, gamma = stubs
    gamma.stats["0xa"] = {"volume": 50_000.0, "liquidity": 500.0}    # thin book
    follow(store)
    data.activity_events = [buy_event(0.50)]
    run(engine.poll_all())
    assert store.trades() == []
    assert any("liquidity" in m for _, _, m in engine.notifier.sent)


# ---------------------------------------------------------- auto-resume
def test_auto_resume_after_cooloff(engine, store, cfg):
    cfg.auto_manage.min_closed_trades = 2
    store.add_wallet(W, "bleeder", "fixed_usd", 100.0, 1.0)
    for asset in ("a1", "a2"):
        engine.broker.buy(W, asset, 100, 0.50, META)
        engine.broker.sell(W, asset, 1e9, 0.25)
    run(engine.auto_manage())
    assert store.wallet(W)["active"] == 0

    # Simulate the cool-off having elapsed.
    store.set_meta(f"autopause_at:{W}", str(int(time.time()) - 73 * 3600))
    run(engine.auto_manage())
    w = store.wallet(W)
    assert w["active"] == 1                          # resumed…
    assert any("Auto-resumed" in m for _, _, m in engine.notifier.sent)
    # …and the stale-evidence guard prevents an instant re-pause.
    run(engine.auto_manage())
    assert store.wallet(W)["active"] == 1


def test_auto_manage_drawdown_trigger(engine, store, cfg):
    cfg.auto_manage.min_closed_trades = 2
    cfg.auto_manage.pause_profit_factor = 0.0        # isolate the drawdown rule
    cfg.auto_manage.pause_loss_pct = 100.0
    cfg.auto_manage.pause_drawdown_pct = 12.0        # $1,200 on the $10k account
    store.add_wallet(W, "swinger", "fixed_usd", 100.0, 1.0)
    engine.broker.buy(W, "a1", 3_000, 0.50, META)
    engine.broker.sell(W, "a1", 1e9, 0.75)           # +$1,500 peak
    engine.broker.buy(W, "a2", 3_000, 0.50, META)
    engine.broker.sell(W, "a2", 1e9, 0.25)           # -$1,500 -> $3,000 drawdown from peak
    # Record the trades so the drawdown series exists (broker alone doesn't log).
    now = int(time.time())
    for i, pnl in enumerate((1_500.0, -1_500.0)):
        store.record_trade({"event_key": f"k{i}", "wallet": W, "asset": f"a{i+1}",
                            "side": "SELL", "kind": "FULL_EXIT", "size": 6_000,
                            "price": 0.75 if pnl > 0 else 0.25, "source_price": 0.5,
                            "notional": 3_000, "realized_pnl": pnl, "source_ts": now,
                            "tx_hash": ""})
    run(engine.auto_manage())
    assert store.wallet(W)["active"] == 0
    assert any("drawdown" in m for _, _, m in engine.notifier.sent)


# -------------------------------------------------------------- analytics
def test_copy_analytics_numbers(store, cfg):
    now = int(time.time())
    store.record_trade({"event_key": "k1", "wallet": W, "asset": "a", "side": "BUY",
                        "kind": "OPEN", "size": 100, "price": 0.55, "source_price": 0.50,
                        "notional": 55.0, "realized_pnl": 0.0, "source_ts": now - 10,
                        "tx_hash": ""})
    a = copy_analytics(store, cfg)
    assert 5 <= a["copy_latency"]["median_s"] <= 30
    assert a["price_difference_bps"]["avg"] == pytest.approx(1_000.0)   # 0.50 -> 0.55
    assert a["missed_edge_usd"] == pytest.approx(5.0)                   # $0.05 x 100
    assert isinstance(a["alpha_decay"], list)


# ------------------------------------------------------------------ config
def test_example_config_loads_and_validates():
    example = Path(__file__).resolve().parents[1] / "config.example.yaml"
    cfg = load_config(example)
    assert cfg.requirements.min_closed_trades == 30
    assert cfg.requirements.min_win_rate_confidence_pct == 30
    assert cfg.scoring.minimum_score == 75
    assert cfg.portfolio.stop_copy_after_drawdown is True
    assert cfg.portfolio.max_daily_loss_pct == 10
    assert cfg.live.order_type == "FOK"
    assert cfg.auto_withdraw.mode == "percent_starting_balance"


def test_dedup_cannot_be_disabled(cfg):
    cfg.copy_filters.ignore_duplicate_events = False
    with pytest.raises(ValueError, match="exactly-once"):
        cfg.validate()


# --------------------------------------------------------------- api extras
@pytest.fixture
def client(cfg, tmp_path):
    cfg.db_path = str(tmp_path / "test.db")
    app = create_app(cfg, store=Store(cfg.db_path))
    with TestClient(app) as c:
        yield c


def test_new_endpoints_respond(client):
    for path in ("/api/trading", "/api/analytics", "/api/health"):
        assert client.get(path).status_code == 200
    health = client.get("/api/health").json()
    assert health["mode"] == "paper"
    assert "copying" in health
