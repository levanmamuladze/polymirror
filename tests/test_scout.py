"""Wallet scout: candidate evaluation, scoring, auto-follow and auto-pause."""
import asyncio
import json

import pytest

from polymirror.metrics import wallet_metrics
from polymirror.scout import (
    candidate_stats,
    category_breakdown,
    dominant_category,
    judge,
    wilson_lower_bound,
)
from tests.conftest import T0, trade

GOOD = "0x" + "1" * 40
BAD = "0x" + "2" * 40
META = {"condition_id": "0xc", "title": "T?", "outcome": "Yes"}


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def history(wallet, trips, entry=0.50, exit_=0.60, size=100, start_ts=-80_000):
    """`trips` completed round-trips, one market each (low concentration)."""
    events = []
    for i in range(trips):
        base = start_ts + i * 200
        kw = dict(asset=str(1000 + i), conditionId=f"0xc{i}")
        events.append(trade(wallet, "BUY", size, entry, ts=base, tx=f"0xb{i}", **kw))
        events.append(trade(wallet, "SELL", size, exit_, ts=base + 100, tx=f"0xs{i}", **kw))
    return events


# ------------------------------------------------------------ candidate_stats
def test_stats_round_trips_and_profit_factor():
    events = history(GOOD, 3, entry=0.50, exit_=0.60)          # 3 wins of $10
    events += history(GOOD, 1, entry=0.50, exit_=0.45,          # 1 loss of $5
                      start_ts=-70_000)
    # regenerate the losing trip on distinct markets to avoid asset collision
    events[-2]["asset"] = events[-1]["asset"] = "9999"
    events[-2]["conditionId"] = events[-1]["conditionId"] = "0xc999"

    s = candidate_stats(events)
    assert s["closed_trades"] == 4
    assert s["wins"] == 3 and s["losses"] == 1
    assert s["realized_pnl"] == pytest.approx(25.0)
    assert s["profit_factor"] == pytest.approx(30.0 / 5.0)
    assert s["win_rate"] == pytest.approx(0.75)


def test_stats_skips_sells_without_basis():
    # A sell of a position opened before the window must not fabricate PnL.
    events = [trade(GOOD, "SELL", 100, 0.70, ts=-500, asset="777")]
    s = candidate_stats(events)
    assert s["closed_trades"] == 0
    assert s["realized_pnl"] == 0.0
    assert s["untracked_sells"] == 1


def test_stats_redeem_settles_at_payout():
    events = [
        trade(GOOD, "BUY", 100, 0.60, ts=-1000, asset="42", conditionId="0xr"),
        {"proxyWallet": GOOD, "type": "REDEEM", "timestamp": T0 - 500, "asset": "42",
         "conditionId": "0xr", "size": 100, "usdcSize": 100, "price": 0, "side": "",
         "transactionHash": "0xr", "title": "T?", "outcome": "Yes"},
    ]
    s = candidate_stats(events)
    assert s["closed_trades"] == 1 and s["wins"] == 1
    assert s["realized_pnl"] == pytest.approx(40.0)   # (1.00 - 0.60) * 100


def test_stats_extreme_entry_share_and_concentration():
    events = [
        trade(GOOD, "BUY", 100, 0.97, ts=-900, asset="1", conditionId="0xa", tx="0x1"),
        trade(GOOD, "BUY", 100, 0.50, ts=-800, asset="2", conditionId="0xb", tx="0x2"),
    ]
    s = candidate_stats(events, extreme_band=0.05)
    assert s["extreme_entry_share"] == pytest.approx(97.0 / 147.0)
    assert s["concentration"] == pytest.approx(97.0 / 147.0)


def test_stats_drawdown_vs_peak_working_capital():
    # One $50 position (100 @ 0.5); win +$20 then lose -$30 from the peak.
    kw1 = dict(asset="1", conditionId="0xa")
    kw2 = dict(asset="2", conditionId="0xb")
    events = [
        trade(GOOD, "BUY", 100, 0.50, ts=-1000, tx="0x1", **kw1),
        trade(GOOD, "SELL", 100, 0.70, ts=-900, tx="0x2", **kw1),   # +20 (peak)
        trade(GOOD, "BUY", 100, 0.50, ts=-800, tx="0x3", **kw2),
        trade(GOOD, "SELL", 100, 0.20, ts=-700, tx="0x4", **kw2),   # -30
    ]
    s = candidate_stats(events)
    assert s["max_drawdown_pct"] == pytest.approx(100.0 * 30.0 / 50.0)


# ------------------------------------------------------------ wilson_lower_bound
def test_wilson_bound_zero_trials_is_zero():
    assert wilson_lower_bound(0, 0) == 0.0


def test_wilson_bound_widens_the_less_evidence_there_is():
    # Same 60% observed win rate, very different sample sizes: more
    # evidence must produce a higher (tighter) confidence lower bound.
    small = wilson_lower_bound(3, 5)
    large = wilson_lower_bound(120, 200)
    assert small == pytest.approx(0.2307, abs=1e-3)
    assert large > small
    assert large == pytest.approx(0.5308, abs=1e-3)


def test_wilson_bound_perfect_record_large_sample_is_high_confidence():
    assert wilson_lower_bound(60, 60) == pytest.approx(0.9398, abs=1e-3)


# --------------------------------------------------- category_breakdown / dominant
def test_category_breakdown_weights_by_notional():
    buy_by_condition = {"0xa": 800.0, "0xb": 200.0}
    cats = {"0xa": "Sports", "0xb": "Politics"}
    mix = category_breakdown(buy_by_condition, cats)
    assert mix == pytest.approx({"Sports": 0.8, "Politics": 0.2})


def test_category_breakdown_excludes_unresolved_conditions():
    buy_by_condition = {"0xa": 800.0, "0xb": 200.0}
    cats = {"0xa": "Sports"}   # 0xb never resolved
    mix = category_breakdown(buy_by_condition, cats)
    assert mix == pytest.approx({"Sports": 1.0})   # 0xb's notional excluded from the total


def test_category_breakdown_empty_when_nothing_resolved():
    assert category_breakdown({"0xa": 100.0}, {}) == {}


def test_dominant_category_picks_largest_share():
    assert dominant_category({"Sports": 0.6, "Politics": 0.4}) == "Sports"


def test_dominant_category_none_when_empty():
    assert dominant_category({}) is None


# ---------------------------------------------------------------------- judge
def test_judge_rejects_with_reasons(cfg):
    s = candidate_stats(history(BAD, 5, entry=0.50, exit_=0.40))   # 5 losses, too few trades
    score, reasons = judge(s, cfg)
    joined = " ".join(reasons)
    assert "closed_trades" in joined
    assert "realized_pnl" in joined
    assert 0.0 <= score <= 100.0


def test_judge_passes_good_wallet(cfg):
    s = candidate_stats(history(GOOD, 60, entry=0.50, exit_=0.60))
    score, reasons = judge(s, cfg)
    assert reasons == []
    assert score >= cfg.scoring.minimum_score


def test_judge_enforces_minimum_score(cfg):
    cfg.scoring.minimum_score = 99.0
    s = candidate_stats(history(GOOD, 60, entry=0.50, exit_=0.60))
    score, reasons = judge(s, cfg)
    assert any("score" in r for r in reasons)


def test_judge_rejects_win_rate_farming(cfg):
    # Plenty of profitable trades, but everything entered above 0.95.
    events = history(GOOD, 60, entry=0.96, exit_=0.99)
    s = candidate_stats(events)
    score, reasons = judge(s, cfg)
    assert any("extreme" in r for r in reasons)


def test_judge_enforces_win_rate_and_roi(cfg):
    # 60 trips, 26 small wins / 34 losses -> win rate ~43% < 55% requirement.
    events = history(GOOD, 26, entry=0.50, exit_=0.60)
    losers = history(GOOD, 34, entry=0.50, exit_=0.45, start_ts=-60_000)
    for i, ev in enumerate(losers):        # keep assets distinct from the winners
        ev["asset"] = str(5000 + i // 2)
        ev["conditionId"] = f"0xl{i // 2}"
    s = candidate_stats(events + losers)
    assert s["closed_trades"] == 60
    score, reasons = judge(s, cfg)
    assert any("win_rate" in r for r in reasons)


def test_judge_rejects_low_confidence_despite_good_point_estimate(cfg):
    """A 60% win rate over just 5 trades clears min_win_rate_pct (55%) but
    is thin evidence — the Wilson 95% lower bound (~23%) must fail the
    confidence requirement even though the raw win-rate point estimate
    passes. Isolates the new rule by relaxing sample-size/pnl/roi gates
    that would otherwise also fire on such a tiny fixture."""
    cfg.requirements.min_closed_trades = 5
    cfg.requirements.min_realized_pnl_usd = 0.0
    cfg.requirements.min_roi_pct = 0.0
    winners = history(GOOD, 3, entry=0.50, exit_=0.60)
    losers = history(GOOD, 2, entry=0.50, exit_=0.40, start_ts=-60_000)
    for i, ev in enumerate(losers):        # keep assets distinct from the winners
        ev["asset"] = str(9000 + i // 2)
        ev["conditionId"] = f"0xw{i // 2}"
    s = candidate_stats(winners + losers)
    assert s["closed_trades"] == 5
    assert s["win_rate"] == pytest.approx(0.6)

    score, reasons = judge(s, cfg)
    assert not any(r.startswith("win_rate ") for r in reasons)   # point estimate passes (60% > 55%)
    assert any("confidence" in r for r in reasons)               # but confidence check fails


def test_judge_confidence_check_passes_with_enough_evidence(cfg):
    """The same 60%-ish win rate, but with enough round-trips behind it,
    must clear the confidence bar (the sample-size/PnL gates are the ones
    doing the rejecting in the thin-evidence test above, not this rule)."""
    events = history(GOOD, 60, entry=0.50, exit_=0.60)   # 100% win rate, n=60
    s = candidate_stats(events)
    score, reasons = judge(s, cfg)
    assert not any("confidence" in r for r in reasons)


# ------------------------------------------------------------------ the scout
def seed_history(data, wallet, good=True):
    if good:
        data.activity_events += history(wallet, 60, entry=0.50, exit_=0.60)
    else:
        data.activity_events += history(wallet, 60, entry=0.50, exit_=0.40)


def test_scout_cycle_follows_good_rejects_bad(scout, store, stubs):
    data, _, _ = stubs
    scout.lb.rows = [{"wallet": GOOD, "name": "ace", "amount": 1000.0},
                     {"wallet": BAD, "name": "dud", "amount": 900.0}]
    seed_history(data, GOOD, good=True)
    seed_history(data, BAD, good=False)

    run(scout.cycle())

    followed = store.wallet(GOOD)
    assert followed is not None and followed["auto"] == 1 and followed["active"] == 1
    assert store.wallet(BAD) is None
    cands = {c["wallet"]: c for c in store.candidates()}
    assert cands[GOOD]["passed"] == 1 and cands[GOOD]["followed"] == 1
    assert cands[BAD]["passed"] == 0 and cands[BAD]["reasons"]
    assert any(k == "scout" for _, k, _ in scout.notifier.sent)


def test_scout_respects_auto_slot_cap(scout, store, stubs, cfg):
    data, _, _ = stubs
    cfg.scout.max_auto_wallets = 1
    other = "0x" + "3" * 40
    scout.lb.rows = [{"wallet": GOOD, "name": "", "amount": 1000.0},
                     {"wallet": other, "name": "", "amount": 900.0}]
    seed_history(data, GOOD)
    seed_history(data, other)

    run(scout.cycle())
    auto_active = [w for w in store.wallets(active_only=True) if w["auto"]]
    assert len(auto_active) == 1


def test_scout_never_readds_previously_followed(scout, store, stubs):
    data, _, _ = stubs
    store.add_wallet(GOOD, "manual", "fixed_usd", 50.0, 1.0)
    store.remove_wallet(GOOD)                       # user removed it (real delete)
    assert store.wallet(GOOD) is None               # confirm it's actually gone from the list
    scout.lb.rows = [{"wallet": GOOD, "name": "", "amount": 1000.0}]
    seed_history(data, GOOD)

    run(scout.cycle())
    # Removed wallets are permanently excluded via `ever_followed`, even
    # though the followed_wallets row itself no longer exists to check.
    assert store.wallet(GOOD) is None


def test_remove_wallet_deletes_row_but_ever_followed_persists(store):
    store.add_wallet(GOOD, "manual", "fixed_usd", 50.0, 1.0)
    assert store.was_ever_followed(GOOD)

    store.remove_wallet(GOOD)
    assert store.wallet(GOOD) is None                       # gone from the visible list
    assert store.was_ever_followed(GOOD)                     # but permanently remembered
    assert GOOD in store.ever_followed_addresses()


def test_scout_wont_readd_via_stale_candidate_row_after_hard_delete(scout, store, stubs):
    """A wallet auto-followed, then removed, can leave a stale `passed=True`
    row in scout_candidates. follow_best() must not re-add it just because
    the followed_wallets row is gone — it has to check ever_followed too."""
    data, _, _ = stubs
    store.add_wallet(GOOD, "", "fixed_usd", 50.0, 1.0, auto=True)
    store.upsert_candidate({
        "wallet": GOOD, "source": "leaderboard:1w", "name": "",
        "evaluated_at": T0, "closed_trades": 60, "win_rate": 0.6,
        "profit_factor": 2.0, "realized_pnl": 1000.0, "max_drawdown_pct": 5.0,
        "extreme_entry_share": 0.0, "concentration": 0.1, "roi_pct": 30.0,
        "positive_month_ratio": 1.0, "score": 90.0, "passed": True,
        "reasons": "", "followed": True,
    })
    store.remove_wallet(GOOD)

    run(scout.follow_best())
    assert store.wallet(GOOD) is None


def test_scout_diversification_blocks_overlapping_candidate(scout, store, stubs, cfg):
    data, _, _ = stubs
    # Already following W1, whose shadow book is 100% in market 0xshared.
    w1 = "0x" + "9" * 40
    store.add_wallet(w1, "incumbent", "fixed_usd", 50.0, 1.0)
    store.upsert_source_position(w1, "888", "0xshared", 1000, 0.5, "Shared?", "Yes")
    # GOOD passes every requirement but holds the exact same market.
    scout.lb.rows = [{"wallet": GOOD, "name": "clone", "amount": 1000.0}]
    seed_history(data, GOOD)
    data.positions_by_wallet[GOOD] = [
        {"asset": "777", "conditionId": "0xshared", "size": 500, "avgPrice": 0.5}
    ]

    run(scout.cycle())
    cands = {c["wallet"]: c for c in store.candidates()}
    assert cands[GOOD]["passed"] == 1            # requirements were fine
    assert store.wallet(GOOD) is None            # but diversification blocked the follow
    assert any("overlap" in m or "correlation" in m
               for _, k, m in scout.notifier.sent if k == "scout")


def test_scout_discover_iterates_configured_categories(scout, store, stubs, cfg):
    cfg.scout.auto_follow = False
    cfg.scout.leaderboard_windows = ["1m"]           # isolate: one window
    cfg.scout.categories = ["SPORTS", "POLITICS"]
    scout.lb.rows = []

    run(scout.cycle())

    assert scout.lb.categories_called == ["SPORTS", "POLITICS"]


def test_evaluate_stores_category_mix(scout, store, stubs):
    data, _, gamma = stubs
    data.activity_events = history(GOOD, 4, entry=0.50, exit_=0.60)
    gamma.categories["0xc0"] = "Sports"
    gamma.categories["0xc1"] = "Sports"
    gamma.categories["0xc2"] = "Politics"
    gamma.categories["0xc3"] = "Politics"

    run(scout.evaluate(GOOD, "leaderboard:OVERALL:1m", "ace"))

    mix = json.loads(store.candidate(GOOD)["category_mix"])
    assert mix == pytest.approx({"Sports": 0.5, "Politics": 0.5})


def _followed_sports_incumbent(store, address, label):
    store.add_wallet(address, label, "fixed_usd", 50.0, 1.0, auto=True)
    store.upsert_candidate({
        "wallet": address, "source": "leaderboard:SPORTS:1m", "name": "",
        "evaluated_at": T0, "closed_trades": 60, "win_rate": 0.6,
        "profit_factor": 2.0, "realized_pnl": 1000.0, "max_drawdown_pct": 5.0,
        "extreme_entry_share": 0.0, "concentration": 0.1, "roi_pct": 30.0,
        "positive_month_ratio": 1.0, "score": 90.0, "passed": True,
        "reasons": "", "followed": True, "category_mix": {"Sports": 1.0},
    })


def test_scout_diversification_blocks_category_concentration(scout, store, stubs, cfg):
    cfg.diversification.max_category_share_pct = 60.0
    cfg.scout.max_auto_wallets = 3     # one free slot after the two incumbents below
    _followed_sports_incumbent(store, "0x" + "8" * 40, "incumbent0")
    _followed_sports_incumbent(store, "0x" + "9" * 40, "incumbent1")
    store.upsert_candidate({
        "wallet": GOOD, "source": "leaderboard:SPORTS:1m", "name": "clone",
        "evaluated_at": T0, "closed_trades": 60, "win_rate": 1.0,
        "profit_factor": None, "realized_pnl": 600.0, "max_drawdown_pct": 0.0,
        "extreme_entry_share": 0.0, "concentration": 0.02, "roi_pct": 40.0,
        "positive_month_ratio": 1.0, "score": 95.0, "passed": True,
        "reasons": "", "followed": False, "category_mix": {"Sports": 1.0},
    })

    run(scout.follow_best())

    assert store.wallet(GOOD) is None       # a third Sports wallet would hit 100% > 60%
    assert any("category" in m for _, k, m in scout.notifier.sent if k == "scout")


def test_scout_diversification_allows_category_diversifying_candidate(scout, store, stubs, cfg):
    cfg.diversification.max_category_share_pct = 60.0
    cfg.scout.max_auto_wallets = 3
    _followed_sports_incumbent(store, "0x" + "8" * 40, "incumbent0")
    _followed_sports_incumbent(store, "0x" + "9" * 40, "incumbent1")
    store.upsert_candidate({
        "wallet": GOOD, "source": "leaderboard:POLITICS:1m", "name": "diversifier",
        "evaluated_at": T0, "closed_trades": 60, "win_rate": 1.0,
        "profit_factor": None, "realized_pnl": 600.0, "max_drawdown_pct": 0.0,
        "extreme_entry_share": 0.0, "concentration": 0.02, "roi_pct": 40.0,
        "positive_month_ratio": 1.0, "score": 95.0, "passed": True,
        "reasons": "", "followed": False, "category_mix": {"Politics": 1.0},
    })

    run(scout.follow_best())

    assert store.wallet(GOOD) is not None   # a Politics wallet only reaches 33% share


def test_scout_reeval_cache_skips_fresh_candidates(scout, store, stubs, cfg):
    data, _, _ = stubs
    cfg.scout.auto_follow = False                   # keep the candidate un-followed
    scout.lb.rows = [{"wallet": BAD, "name": "", "amount": 100.0}]
    seed_history(data, BAD, good=False)

    run(scout.cycle())
    calls_after_first = data.activity_calls
    run(scout.cycle())                              # within reeval_hours
    assert data.activity_calls == calls_after_first


# ------------------------------------------------------------------ auto-pause
def losing_round_trip(engine, wallet, asset, notional=100.0):
    engine.broker.buy(wallet, asset, notional, 0.50, META)
    engine.broker.sell(wallet, asset, 1e9, 0.25)    # sell everything at a loss


def test_auto_manage_pauses_bleeding_wallet(engine, store, cfg):
    cfg.auto_manage.min_closed_trades = 2
    store.add_wallet(GOOD, "bleeder", "fixed_usd", 100.0, 1.0)
    losing_round_trip(engine, GOOD, "a1")
    losing_round_trip(engine, GOOD, "a2")

    run(engine.auto_manage())
    assert store.wallet(GOOD)["active"] == 0
    assert any(k == "risk" for _, k, _ in engine.notifier.sent)


def test_auto_manage_respects_user_resume_until_new_evidence(engine, store, cfg):
    cfg.auto_manage.min_closed_trades = 2
    store.add_wallet(GOOD, "bleeder", "fixed_usd", 100.0, 1.0)
    losing_round_trip(engine, GOOD, "a1")
    losing_round_trip(engine, GOOD, "a2")
    run(engine.auto_manage())
    assert store.wallet(GOOD)["active"] == 0

    store.update_wallet(GOOD, active=1)             # user override
    run(engine.auto_manage())
    assert store.wallet(GOOD)["active"] == 1        # stale evidence — stays active

    losing_round_trip(engine, GOOD, "a3")           # new closed trade, still bleeding
    run(engine.auto_manage())
    assert store.wallet(GOOD)["active"] == 0


def test_auto_manage_loss_cap_trigger(engine, store, cfg):
    cfg.auto_manage.min_closed_trades = 1
    cfg.auto_manage.pause_profit_factor = 0.0       # isolate the loss-cap rule
    cfg.auto_manage.pause_loss_pct = 5.0            # $500 of the 10k account
    store.add_wallet(GOOD, "whale", "fixed_usd", 100.0, 1.0)
    losing_round_trip(engine, GOOD, "a1", notional=2_000.0)   # loses $1,000

    run(engine.auto_manage())
    assert store.wallet(GOOD)["active"] == 0


def test_auto_manage_leaves_small_samples_alone(engine, store, cfg):
    cfg.auto_manage.min_closed_trades = 8
    store.add_wallet(GOOD, "new", "fixed_usd", 100.0, 1.0)
    losing_round_trip(engine, GOOD, "a1")
    run(engine.auto_manage())
    assert store.wallet(GOOD)["active"] == 1        # n=1 — too few to judge


# -------------------------------------------------------------- wallet metrics
def test_wallet_metrics_per_wallet_attribution(engine, store):
    store.add_wallet(GOOD, "winner", "fixed_usd", 100.0, 1.0)
    store.add_wallet(BAD, "loser", "fixed_usd", 100.0, 1.0)
    engine.broker.buy(GOOD, "g1", 500, 0.50, META)
    engine.broker.sell(GOOD, "g1", 1e9, 0.60)       # +100
    losing_round_trip(engine, BAD, "b1")            # -50

    wm = {m["wallet"]: m for m in wallet_metrics(store)}
    assert wm[GOOD]["realized_pnl"] == pytest.approx(100.0)
    assert wm[GOOD]["win_rate_pct"] == pytest.approx(100.0)
    assert wm[GOOD]["profit_factor"] is None        # no losses yet
    assert wm[BAD]["realized_pnl"] == pytest.approx(-50.0)
    assert wm[BAD]["closed_trades"] == 1 and wm[BAD]["losses"] == 1


# -------------------------------------------------------------------- config
def test_config_rejects_unknown_scout_key(tmp_path):
    from polymirror.config import load_config
    p = tmp_path / "c.yaml"
    p.write_text("scout:\n  bogus_knob: 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="bogus_knob"):
        load_config(p)


def test_config_rejects_empty_scout_categories():
    from polymirror.config import Config
    cfg = Config()
    cfg.scout.categories = []
    with pytest.raises(ValueError, match="scout.categories"):
        cfg.validate()


def test_config_rejects_bad_max_category_share_pct():
    from polymirror.config import Config
    cfg = Config()
    cfg.diversification.max_category_share_pct = 0.0
    with pytest.raises(ValueError, match="max_category_share_pct"):
        cfg.validate()


def test_config_default_categories_preserves_overall_only_behavior():
    from polymirror.config import Config
    assert Config().scout.categories == ["OVERALL"]
