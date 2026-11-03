"""Wallet scout: automatic discovery, evaluation and following of wallets.

Pipeline (runs on its own cadence, fully unattended):

  1. DISCOVER — pull top wallets from the public leaderboard (plus any
     `scout.candidate_wallets` from config), excluding wallets already in
     the follow list.
  2. EVALUATE — fetch each candidate's public trade history over
     `scout.lookback_days` and reconstruct completed round-trips with
     avg-cost accounting (same convention as the broker). This yields the
     stats the `requirements` section gates on: closed trades, ROI, win
     rate, profit factor, realized PnL, drawdown, positive-month
     consistency, near-resolution entry share, market concentration, and
     (via cached Gamma metadata) the volume/liquidity of the markets the
     candidate trades. All computed from the *source's own fills*, i.e.
     the trader's skill before copy latency.
  3. REQUIREMENTS + SCORE — every `requirements` criterion must pass, and
     the weighted composite score (`scoring.weights`, 0-100) must clear
     `scoring.minimum_score`. Failing candidates are stored with readable
     reasons — the scout's judgment is auditable, not a black box.
  4. FOLLOW — the best passing candidates fill free auto slots
     (`scout.max_auto_wallets` active auto wallets), subject to the
     `diversification` caps against wallets already followed: market
     overlap, exposure correlation, and category concentration (don't let
     the auto-followed roster be secretly all one market vertical).
     Wallets ever followed before — including ones auto-paused by risk
     management or removed by the user — are never re-added; a paused
     wallet's freed slot goes to the next-best NEW candidate. That is the
     rotation loop.

Caveats the numbers carry (also surfaced in docs):
  * Leaderboard presence is survivorship-biased; the requirements exist to
    reject lucky streaks, not to bless winners.
  * Sells of positions opened before the lookback window have no cost basis
    and are excluded from round-trip stats.
  * `activity_since` caps pagination (~5.5k fills); hyperactive wallets are
    evaluated on the earliest slice of the window.
  * Market volume/liquidity comes from undocumented Gamma fields — unknown
    values are treated as "no data", never as a failure. Category (see
    `category_breakdown`) carries the identical caveat.
  * A wallet's requirements/score are computed from its ENTIRE trade
    history blended across every market category — a strong category-
    specific edge can be diluted by mediocre results elsewhere, and vice
    versa. `category_mix` surfaces the blend (and diversification caps
    the roster's exposure to it) but does not re-score per category —
    doing that properly needs per-trade category resolution and a
    separate round-trip reconstruction per bucket, a larger change than
    this pass makes.

Read-only by construction: this module talks to the network exclusively
through the injected clients (public GETs).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from typing import Any

from .clients import ApiError, DataApiClient, GammaClient, LeaderboardClient
from .config import Config
from .notify import Notifier
from .storage import Store

log = logging.getLogger("polymirror.scout")

DUST = 1e-9
MAX_MARKET_LOOKUPS = 25   # per candidate: Gamma lookups for the largest buys only


# --------------------------------------------------------------- evaluation
def candidate_stats(events: list[dict[str, Any]], extreme_band: float = 0.05) -> dict[str, Any]:
    """Reconstruct a trader's performance from their raw activity feed.

    Pure function: takes Data API activity rows (TRADE/REDEEM), returns
    stats. Positions are tracked per asset with average-cost basis; a
    round-trip completes when a position returns to zero. Sells without a
    tracked basis (opened before the window) are counted but excluded.
    """
    events = sorted(events, key=lambda e: int(e.get("timestamp", 0)))

    pos: dict[str, dict[str, float]] = {}   # asset -> {size, avg, trip_pnl}
    buy_notional = 0.0
    extreme_notional = 0.0
    buy_by_condition: dict[str, float] = {}
    month_pnl: dict[str, float] = {}        # "YYYY-MM" (UTC) -> realized PnL
    open_cost = 0.0        # current cost basis deployed across all tracked positions
    peak_open_cost = 0.0   # working-capital proxy for drawdown/ROI normalization
    cum_pnl = 0.0
    peak_pnl = 0.0
    max_dd_usd = 0.0
    wins = losses = 0
    gross_profit = gross_loss = 0.0
    untracked_sells = 0
    first_ts = last_ts = 0

    def realize(asset: str, sell_size: float, price: float, ts: int) -> None:
        nonlocal open_cost, cum_pnl, peak_pnl, max_dd_usd
        nonlocal wins, losses, gross_profit, gross_loss
        p = pos[asset]
        sell_size = min(sell_size, p["size"])
        if sell_size <= DUST:
            return
        pnl = (price - p["avg"]) * sell_size
        p["size"] -= sell_size
        p["trip_pnl"] += pnl
        open_cost = max(0.0, open_cost - p["avg"] * sell_size)
        cum_pnl += pnl
        peak_pnl = max(peak_pnl, cum_pnl)
        max_dd_usd = max(max_dd_usd, peak_pnl - cum_pnl)
        month = time.strftime("%Y-%m", time.gmtime(ts))
        month_pnl[month] = month_pnl.get(month, 0.0) + pnl
        if p["size"] <= DUST:                      # round-trip complete
            if p["trip_pnl"] > 0:
                wins += 1
                gross_profit += p["trip_pnl"]
            else:
                losses += 1
                gross_loss += -p["trip_pnl"]
            del pos[asset]

    for ev in events:
        etype = str(ev.get("type", "")).upper()
        asset = str(ev.get("asset", ""))
        size = float(ev.get("size", 0.0) or 0.0)
        price = float(ev.get("price", 0.0) or 0.0)
        ts = int(ev.get("timestamp", 0))
        if not asset or size <= 0:
            continue
        first_ts = first_ts or ts
        last_ts = max(last_ts, ts)

        if etype == "TRADE":
            side = str(ev.get("side", "")).upper()
            if side == "BUY" and price > 0:
                p = pos.setdefault(asset, {"size": 0.0, "avg": 0.0, "trip_pnl": 0.0})
                notional = price * size
                p["avg"] = (p["avg"] * p["size"] + notional) / (p["size"] + size)
                p["size"] += size
                open_cost += notional
                peak_open_cost = max(peak_open_cost, open_cost)
                buy_notional += notional
                cond = str(ev.get("conditionId", "")) or asset
                buy_by_condition[cond] = buy_by_condition.get(cond, 0.0) + notional
                if price >= 1 - extreme_band or price <= extreme_band:
                    extreme_notional += notional
            elif side == "SELL" and price > 0:
                if asset not in pos or pos[asset]["size"] <= DUST:
                    untracked_sells += 1
                    continue
                realize(asset, size, price, ts)
        elif etype == "REDEEM":
            if asset not in pos or pos[asset]["size"] <= DUST:
                continue
            usdc = float(ev.get("usdcSize", 0.0) or 0.0)
            payout = min(1.0, max(0.0, usdc / size)) if size > 0 else 0.0
            realize(asset, pos[asset]["size"], payout, ts)

    closed = wins + losses
    months = len(month_pnl)
    positive_months = sum(1 for v in month_pnl.values() if v > 0)
    profit_factor = (gross_profit / gross_loss) if gross_loss > DUST else None
    return {
        "closed_trades": closed,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / closed if closed else None,
        "profit_factor": profit_factor,
        "realized_pnl": cum_pnl,
        "roi_pct": 100.0 * cum_pnl / peak_open_cost if peak_open_cost > 0 else None,
        "max_drawdown_pct": 100.0 * max_dd_usd / peak_open_cost if peak_open_cost > 0 else 0.0,
        "months_observed": months,
        "positive_month_ratio": positive_months / months if months else None,
        "extreme_entry_share": extreme_notional / buy_notional if buy_notional > 0 else 0.0,
        "concentration": (max(buy_by_condition.values()) / buy_notional
                          if buy_notional > 0 else 0.0),
        "buy_notional": buy_notional,
        "buy_by_condition": buy_by_condition,
        "untracked_sells": untracked_sells,
        "first_ts": first_ts,
        "last_ts": last_ts,
    }


def wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
    """Lower bound of the Wilson score confidence interval (default 95%,
    z=1.96) on a true win rate, given `wins` observed out of `n` trials.

    Unlike a raw win-rate threshold, this naturally penalizes small
    samples: a 60% win rate over 10 trades produces a much lower bound
    than 60% over 200 trades, because the interval itself is wider when
    there's less evidence. Used alongside (not instead of)
    `requirements.min_win_rate_pct` — the point estimate answers "is this
    trader good," this answers "do we have enough evidence to believe
    that." Returns 0.0 for n=0 (no evidence at all)."""
    if n <= 0:
        return 0.0
    phat = wins / n
    denom = 1 + z * z / n
    center = phat + z * z / (2 * n)
    margin = z * math.sqrt(phat * (1 - phat) / n + z * z / (4 * n * n))
    return (center - margin) / denom


def judge(stats: dict[str, Any], cfg: Config) -> tuple[float, list[str]]:
    """Apply `requirements`, then the weighted `scoring` model.

    Returns (score 0-100, fail_reasons). `stats` may additionally carry
    `avg_market_volume`, `avg_market_liquidity` and `liquidity_share`
    (buy-notional-weighted Gamma market quality; None = no data, which
    never fails a requirement and scores neutrally).
    """
    req = cfg.requirements
    reasons: list[str] = []
    pf = stats["profit_factor"]
    closed = stats["closed_trades"]
    win_rate = stats["win_rate"]
    roi = stats.get("roi_pct")
    pmr = stats.get("positive_month_ratio")

    if closed < req.min_closed_trades:
        reasons.append(f"closed_trades {closed} < {req.min_closed_trades}")
    if roi is not None and roi < req.min_roi_pct:
        reasons.append(f"roi {roi:.1f}% < {req.min_roi_pct}%")
    if pf is not None and pf < req.min_profit_factor:
        reasons.append(f"profit_factor {pf:.2f} < {req.min_profit_factor}")
    if pf is None and stats["wins"] == 0 and closed > 0:
        reasons.append("no winning round-trips")
    if stats["realized_pnl"] < req.min_realized_pnl_usd:
        reasons.append(
            f"realized_pnl ${stats['realized_pnl']:,.0f} < ${req.min_realized_pnl_usd:,.0f}")
    if win_rate is not None and win_rate * 100 < req.min_win_rate_pct:
        reasons.append(f"win_rate {win_rate:.0%} < {req.min_win_rate_pct}%")
    if closed > 0:
        lb = wilson_lower_bound(stats["wins"], closed)
        if lb * 100 < req.min_win_rate_confidence_pct:
            reasons.append(
                f"win-rate confidence too low: 95% lower bound {lb:.0%} < "
                f"{req.min_win_rate_confidence_pct:.0f}% (n={closed})")
    if stats["max_drawdown_pct"] > req.max_drawdown_pct:
        reasons.append(f"drawdown {stats['max_drawdown_pct']:.1f}% > {req.max_drawdown_pct}%")
    if (pmr is not None and stats.get("months_observed", 0) >= 2
            and pmr < req.min_positive_month_ratio):
        reasons.append(f"positive months {pmr:.0%} < {req.min_positive_month_ratio:.0%}")
    if stats["extreme_entry_share"] > req.max_extreme_entry_share:
        reasons.append(
            f"extreme entries {stats['extreme_entry_share']:.0%} > {req.max_extreme_entry_share:.0%}")
    if stats["concentration"] > req.max_market_concentration:
        reasons.append(
            f"concentration {stats['concentration']:.0%} > {req.max_market_concentration:.0%}")
    vol = stats.get("avg_market_volume")
    liq = stats.get("avg_market_liquidity")
    if vol is not None and vol < req.min_market_volume_usd:
        reasons.append(f"avg market volume ${vol:,.0f} < ${req.min_market_volume_usd:,.0f}")
    if liq is not None and liq < req.min_market_liquidity_usd:
        reasons.append(f"avg market liquidity ${liq:,.0f} < ${req.min_market_liquidity_usd:,.0f}")

    # ---- composite score (each component in [0, 1]) -----------------------
    w = cfg.scoring.weights
    liquidity_share = stats.get("liquidity_share")
    components = {
        "roi": min(max(roi or 0.0, 0.0) / 50.0, 1.0),                 # saturates at 50% ROI
        "profit_factor": 1.0 if (pf is None and stats["wins"] > 0) else min(pf or 0.0, 4.0) / 4.0,
        "consistency": pmr if pmr is not None else 0.5,               # neutral when unknown
        "drawdown": max(0.0, 1.0 - stats["max_drawdown_pct"] / 50.0), # zero credit at 50% dd
        "pnl": min(max(stats["realized_pnl"], 0.0) / 5_000.0, 1.0),   # saturates at $5k
        "diversification": 1.0 - stats["concentration"],
        "win_rate": win_rate or 0.0,
        "liquidity": liquidity_share if liquidity_share is not None else 0.5,
    }
    total_w = sum(getattr(w, k) for k in components)
    score = 100.0 * sum(getattr(w, k) * v for k, v in components.items()) / total_w
    if score < cfg.scoring.minimum_score:
        reasons.append(f"score {score:.0f} < {cfg.scoring.minimum_score:.0f}")
    return round(score, 1), reasons


def exposure_vector(positions: list[dict[str, Any]],
                    cond_key: str = "conditionId", size_key: str = "size",
                    price_key: str = "avgPrice") -> dict[str, float]:
    """Value-weighted exposure by market (condition) from a positions list."""
    vec: dict[str, float] = {}
    for p in positions:
        cond = str(p.get(cond_key, "") or "")
        if not cond:
            continue
        value = p.get("currentValue")
        if value is None:
            value = float(p.get(size_key, 0.0) or 0.0) * float(p.get(price_key, 0.0) or 0.0)
        value = float(value or 0.0)
        if value > 0:
            vec[cond] = vec.get(cond, 0.0) + value
    return vec


def cosine_similarity(a: dict[str, float], b: dict[str, float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(v * b.get(k, 0.0) for k, v in a.items())
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    return dot / (na * nb) if na > 0 and nb > 0 else 0.0


def category_breakdown(buy_by_condition: dict[str, float],
                       category_by_condition: dict[str, str]) -> dict[str, float]:
    """Buy-notional-weighted category mix, e.g. {"Sports": 0.82,
    "Politics": 0.18}, given a precomputed condition -> category map.

    Pure and synchronous by design, like exposure_vector/cosine_similarity
    above — network resolution of each condition's category happens
    separately (see WalletScout.resolve_categories) so this stays directly
    unit-testable. Conditions with no resolved category are simply
    excluded from the notional total, not counted as a 0% bucket."""
    by_category: dict[str, float] = {}
    total = 0.0
    for cond, notional in buy_by_condition.items():
        cat = category_by_condition.get(cond)
        if cat:
            by_category[cat] = by_category.get(cat, 0.0) + notional
            total += notional
    if total <= 0:
        return {}
    return {cat: v / total for cat, v in by_category.items()}


def dominant_category(mix: dict[str, float]) -> str | None:
    """The single largest-share category in a category_breakdown() mix, or
    None if the mix is empty (nothing resolved)."""
    return max(mix.items(), key=lambda kv: kv[1])[0] if mix else None


# --------------------------------------------------------------------- scout
class WalletScout:
    def __init__(self, cfg: Config, store: Store, data: DataApiClient,
                 leaderboard: LeaderboardClient, notifier: Notifier,
                 gamma: GammaClient | None = None):
        self.cfg = cfg
        self.store = store
        self.data = data
        self.lb = leaderboard
        self.gamma = gamma
        self.notifier = notifier

    async def run(self, stop: asyncio.Event) -> None:
        """Own loop, spawned by the engine; a failed sweep never kills it."""
        while not stop.is_set():
            try:
                await self.cycle()
            except Exception as err:  # noqa: BLE001 — same survival rule as the engine
                log.exception("scout cycle failed")
                await self.notifier.send("error", "scout", f"Scout cycle failed: {err}")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.cfg.scout.interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def cycle(self) -> None:
        candidates = await self.discover()
        fresh_cutoff = int(time.time()) - int(self.cfg.scout.reeval_hours * 3600)
        evaluated = skipped = 0
        for wallet, source, name in candidates:
            existing = self.store.candidate(wallet)
            if existing and existing["evaluated_at"] > fresh_cutoff:
                skipped += 1
                continue
            try:
                await self.evaluate(wallet, source, name)
                evaluated += 1
            except ApiError as err:
                log.warning("could not evaluate %s: %s", wallet, err)
        if evaluated:
            await self.notifier.send(
                "info", "scout",
                f"Scout sweep: {evaluated} candidates evaluated, {skipped} fresh (cached)",
            )
        if self.cfg.scout.auto_follow:
            await self.follow_best()

    async def discover(self) -> list[tuple[str, str, str]]:
        """Candidate (wallet, source, name) triples, deduped, follow list
        excluded. `ever_followed` also excludes addresses the user has
        explicitly removed — a real delete drops a wallet from the
        followed list but must never make it eligible for re-discovery.

        Loops `scout.categories` (default just ["OVERALL"], preserving the
        historical single-leaderboard behavior) outside the existing
        window loop, so opting into more categories is what feeds
        candidates for `diversification.max_category_share_pct` to
        actually balance — see ScoutConfig's docstring."""
        followed = {w["address"] for w in self.store.wallets()} | self.store.ever_followed_addresses()
        seen: set[str] = set()
        out: list[tuple[str, str, str]] = []
        for category in self.cfg.scout.categories:
            for window in self.cfg.scout.leaderboard_windows:
                try:
                    rows = await self.lb.top_wallets(window, self.cfg.scout.leaderboard_limit,
                                                     category=category)
                except ApiError as err:
                    await self.notifier.send(
                        "warning", "scout",
                        f"Leaderboard unavailable ({category}/{window}): {err}")
                    continue
                if not rows:
                    await self.notifier.send(
                        "warning", "scout",
                        f"Leaderboard ({category}/{window}) returned no parseable wallets — "
                        "check scripts/probe_leaderboard.py against the endpoint",
                    )
                for r in rows:
                    if r["wallet"] not in seen and r["wallet"] not in followed:
                        seen.add(r["wallet"])
                        out.append((r["wallet"], f"leaderboard:{category}:{window}", r["name"]))
        for addr in self.cfg.scout.candidate_wallets:
            addr = addr.lower()
            if addr not in seen and addr not in followed:
                seen.add(addr)
                out.append((addr, "config", ""))
        return out

    async def evaluate(self, wallet: str, source: str, name: str) -> None:
        start = int(time.time()) - self.cfg.scout.lookback_days * 86_400
        events = await self.data.activity_since(wallet, start=start)
        stats = candidate_stats(events, self.cfg.requirements.extreme_price_band)
        stats.update(await self.market_quality(stats["buy_by_condition"]))
        category_by_condition = await self.resolve_categories(stats["buy_by_condition"])
        category_mix = category_breakdown(stats["buy_by_condition"], category_by_condition)
        score, reasons = judge(stats, self.cfg)
        self.store.upsert_candidate({
            "wallet": wallet, "source": source, "name": name,
            "evaluated_at": int(time.time()),
            "closed_trades": stats["closed_trades"],
            "win_rate": stats["win_rate"],
            "profit_factor": stats["profit_factor"],
            "realized_pnl": stats["realized_pnl"],
            "max_drawdown_pct": stats["max_drawdown_pct"],
            "extreme_entry_share": stats["extreme_entry_share"],
            "concentration": stats["concentration"],
            "roi_pct": stats["roi_pct"],
            "positive_month_ratio": stats["positive_month_ratio"],
            "score": score, "passed": not reasons,
            "reasons": "; ".join(reasons),
            "followed": self.store.wallet(wallet) is not None,
            "category_mix": category_mix,
        })

    async def market_quality(self, buy_by_condition: dict[str, float]) -> dict[str, Any]:
        """Buy-notional-weighted market volume/liquidity via cached Gamma
        metadata. Bounded to the candidate's largest markets; unknowns are
        excluded (no data is never treated as bad data)."""
        empty = {"avg_market_volume": None, "avg_market_liquidity": None,
                 "liquidity_share": None}
        if self.gamma is None or not buy_by_condition:
            return empty
        top = sorted(buy_by_condition.items(), key=lambda kv: -kv[1])[:MAX_MARKET_LOOKUPS]
        vol_sum = vol_w = liq_sum = liq_w = liquid_notional = 0.0
        for cond, notional in top:
            try:
                ms = await self.gamma.market_stats(cond)
            except Exception:  # noqa: BLE001 — enrichment must never fail an evaluation
                continue
            if ms["volume"] is not None:
                vol_sum += ms["volume"] * notional
                vol_w += notional
            if ms["liquidity"] is not None:
                liq_sum += ms["liquidity"] * notional
                liq_w += notional
                if ms["liquidity"] >= self.cfg.requirements.min_market_liquidity_usd:
                    liquid_notional += notional
        return {
            "avg_market_volume": vol_sum / vol_w if vol_w > 0 else None,
            "avg_market_liquidity": liq_sum / liq_w if liq_w > 0 else None,
            "liquidity_share": liquid_notional / liq_w if liq_w > 0 else None,
        }

    async def resolve_categories(self, buy_by_condition: dict[str, float]) -> dict[str, str]:
        """Best-effort condition -> category map for a candidate's largest
        markets, via cached Gamma metadata (same MAX_MARKET_LOOKUPS cap and
        "unknown is just omitted" posture as market_quality — Gamma's
        category field isn't a documented stable contract; verify the live
        shape with scripts/probe_market_category.py if this looks wrong,
        e.g. empty or everything landing in one bucket)."""
        out: dict[str, str] = {}
        if self.gamma is None or not buy_by_condition:
            return out
        top = sorted(buy_by_condition.items(), key=lambda kv: -kv[1])[:MAX_MARKET_LOOKUPS]
        for cond, _ in top:
            try:
                cat = await self.gamma.market_category(cond)
            except Exception:  # noqa: BLE001 — enrichment must never fail an evaluation
                cat = None
            if cat:
                out[cond] = cat
        return out

    async def follow_best(self) -> None:
        """Fill free auto-follow slots with the highest-scoring passing
        candidates, subject to diversification caps. Anything ever followed
        before (paused, removed, manual) is off-limits — the scout only
        ever adds brand-new wallets."""
        active_auto = [w for w in self.store.wallets(active_only=True) if w["auto"]]
        slots = self.cfg.scout.max_auto_wallets - len(active_auto)
        if slots <= 0:
            return
        ever_followed = self.store.ever_followed_addresses()
        picks = [c for c in self.store.candidates()
                 if c["passed"] and self.store.wallet(c["wallet"]) is None
                 and c["wallet"] not in ever_followed]
        followed_vectors = await self.followed_exposure_vectors()
        followed_dominant = self.followed_dominant_categories()
        added = 0
        for c in picks:
            if added >= slots:
                break
            ok, why = await self.diversification_ok(c["wallet"], followed_vectors, followed_dominant)
            if not ok:
                await self.notifier.send(
                    "info", "scout",
                    f"Skipped {c['wallet'][:6]}…{c['wallet'][-4:]} despite passing — {why}",
                    {"wallet": c["wallet"]},
                )
                continue
            ds = self.cfg.default_sizing
            label = c["name"] or f"scout {c['wallet'][:6]}…{c['wallet'][-4:]}"
            self.store.add_wallet(c["wallet"], label, ds.mode, ds.value, ds.multiplier,
                                  auto=True)
            self.store.set_candidate_followed(c["wallet"])
            added += 1
            pf = c["profit_factor"]
            pf_txt = f"PF {pf:.2f}" if pf is not None else "no losses in window"
            await self.notifier.send(
                "info", "scout",
                f"Auto-following {label} ({c['wallet'][:6]}…{c['wallet'][-4:]}) — "
                f"score {c['score']:.0f}, {pf_txt}, "
                f"{c['closed_trades']} round-trips, dd {c['max_drawdown_pct']:.1f}%",
                {"wallet": c["wallet"], "score": c["score"]},
            )

    async def followed_exposure_vectors(self) -> dict[str, dict[str, float]]:
        """Exposure vectors of currently active followed wallets, from the
        locally maintained shadow model (no network)."""
        out: dict[str, dict[str, float]] = {}
        for w in self.store.wallets(active_only=True):
            vec = exposure_vector(self.store.source_positions(w["address"]),
                                  cond_key="condition_id", price_key="avg_price")
            if vec:
                out[w["address"]] = vec
        return out

    def _stored_category_mix(self, wallet: str) -> dict[str, float]:
        cand = self.store.candidate(wallet)
        if not cand or not cand.get("category_mix"):
            return {}
        try:
            return json.loads(cand["category_mix"])
        except (TypeError, ValueError):
            return {}

    def followed_dominant_categories(self) -> list[str]:
        """Dominant category (see dominant_category()) of every currently
        active followed wallet that has a stored scout evaluation with a
        resolved category_mix. Manually-added wallets (never evaluated)
        contribute nothing — unknown category never blocks, matching this
        module's usual philosophy for missing metadata.

        Purely local (SQLite via Store), no network — like
        followed_exposure_vectors. Computed once per follow_best() cycle
        and, like that sibling method, not updated as wallets are added
        within the same cycle (a pre-existing characteristic of the
        market-overlap/correlation checks too, not a new limitation)."""
        out: list[str] = []
        for w in self.store.wallets(active_only=True):
            dom = dominant_category(self._stored_category_mix(w["address"]))
            if dom:
                out.append(dom)
        return out

    async def diversification_ok(self, wallet: str,
                                 followed: dict[str, dict[str, float]],
                                 followed_dominant: list[str] | None = None,
                                 ) -> tuple[bool, str]:
        dv = self.cfg.diversification
        if not dv.enabled:
            return True, ""

        if followed:
            try:
                positions = await self.data.positions(wallet)
            except ApiError:
                positions = None   # can't judge — don't block on missing data
            vec = exposure_vector(positions) if positions is not None else {}
            if vec:
                total = sum(vec.values())
                held = set().union(*[set(v) for v in followed.values()])
                overlap = 100.0 * sum(v for k, v in vec.items() if k in held) / total
                if overlap > dv.max_market_overlap_pct:
                    return False, (f"market overlap {overlap:.0f}% > "
                                   f"{dv.max_market_overlap_pct:.0f}% with already-followed wallets")
                worst = max((cosine_similarity(vec, fv) for fv in followed.values()), default=0.0)
                if worst > dv.max_wallet_correlation:
                    return False, (f"exposure correlation {worst:.2f} > "
                                   f"{dv.max_wallet_correlation:.2f} with an already-followed wallet")

        if followed_dominant:
            cand_dom = dominant_category(self._stored_category_mix(wallet))
            if cand_dom:
                projected = followed_dominant + [cand_dom]
                share = 100.0 * projected.count(cand_dom) / len(projected)
                if share > dv.max_category_share_pct:
                    return False, (
                        f"category '{cand_dom}' would reach {share:.0f}% of the followed "
                        f"roster's dominant categories (cap {dv.max_category_share_pct:.0f}%)")

        return True, ""
