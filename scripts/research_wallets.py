"""Widen wallet discovery beyond the scout's default single-category sweep
and rank everyone evaluated so far by score, for manual curation.

Reuses the exact same vetting pipeline the automatic scout uses
(candidate_stats -> judge -> Store.upsert_candidate), just fed from more
leaderboard category/window combinations than discover() covers on its
own. Results land in the same scout_candidates table the dashboard reads,
so anything found here is immediately visible there too.

Read-only against Polymarket (public GETs only, same as everywhere else in
this project) and uses a deliberately gentler rate limit than the live
engine's, since this runs alongside it.

Usage:
    python scripts/research_wallets.py [--categories OVERALL,SPORTS,...] [--top N]
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from polymirror.clients import DataApiClient, GammaClient, LeaderboardClient, RateLimiter  # noqa: E402
from polymirror.config import load_config  # noqa: E402
from polymirror.notify import Notifier  # noqa: E402
from polymirror.scout import WalletScout  # noqa: E402
from polymirror.storage import Store  # noqa: E402

DEFAULT_CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO"]
WINDOWS = ["1w", "1m", "all"]
RESEARCH_RATE_LIMIT = 2.0  # req/s — gentler than the live engine's, since they run concurrently


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--categories", default=",".join(DEFAULT_CATEGORIES))
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()
    categories = [c.strip().upper() for c in args.categories.split(",") if c.strip()]

    cfg = load_config()
    store = Store(cfg.db_path)
    limiter = RateLimiter(RESEARCH_RATE_LIMIT)
    data = DataApiClient(cfg.data_api_base, limiter, cfg.request_timeout_seconds)
    gamma = GammaClient(cfg.gamma_api_base, limiter, cfg.request_timeout_seconds)
    lb = LeaderboardClient(cfg.scout.leaderboard_base, limiter, cfg.request_timeout_seconds)
    notifier = Notifier(cfg.notifications, store)
    scout = WalletScout(cfg, store, data, lb, notifier, gamma=gamma)

    followed = {w["address"] for w in store.wallets()} | store.ever_followed_addresses()
    fresh_cutoff = int(time.time()) - int(cfg.scout.reeval_hours * 3600)

    seen: dict[str, tuple[str, str]] = {}  # wallet -> (source, name)
    for category in categories:
        for window in WINDOWS:
            try:
                rows = await lb.top_wallets(window, cfg.scout.leaderboard_limit, category=category)
            except Exception as err:  # noqa: BLE001
                print(f"  ! {category}/{window} leaderboard failed: {err}")
                continue
            new_here = 0
            for r in rows:
                w = r["wallet"]
                if w in followed or w in seen:
                    continue
                seen[w] = (f"leaderboard:{category}:{window}", r["name"])
                new_here += 1
            print(f"{category:10} {window:4}  {len(rows):3} returned, {new_here:3} new unique candidates")

    print(f"\nTotal unique new candidates to consider: {len(seen)}")

    to_evaluate = []
    for wallet, (source, name) in seen.items():
        existing = store.candidate(wallet)
        if existing and existing["evaluated_at"] > fresh_cutoff:
            continue  # already fresh from a recent scout/research pass
        to_evaluate.append((wallet, source, name))
    print(f"Need evaluation (not already fresh-cached): {len(to_evaluate)}\n")

    for i, (wallet, source, name) in enumerate(to_evaluate, 1):
        try:
            await scout.evaluate(wallet, source, name)
        except Exception as err:  # noqa: BLE001
            print(f"  ! {wallet[:10]} evaluation failed: {err}")
            continue
        if i % 10 == 0:
            print(f"  ...evaluated {i}/{len(to_evaluate)}")

    all_candidates = [c for c in store.candidates(limit=1000) if c["wallet"] not in followed]
    all_candidates.sort(key=lambda c: -(c["score"] or 0))

    print(f"\n{'='*100}")
    print(f"TOP {args.top} CANDIDATES BY SCORE (not currently followed)")
    print(f"{'='*100}")
    header = (f"{'Wallet':<12} {'Name':<20} {'Score':>6} {'Closed':>7} {'Win%':>6} "
              f"{'PF':>6} {'ROI%':>7} {'DD%':>6} {'Realized':>12} {'Pass':>6}")
    print(header)
    print("-" * len(header))
    for c in all_candidates[:args.top]:
        pf = f"{c['profit_factor']:.2f}" if c["profit_factor"] is not None else "n/a"
        wr = f"{c['win_rate']*100:.0f}" if c["win_rate"] is not None else "n/a"
        roi = f"{c['roi_pct']:.1f}" if c.get("roi_pct") is not None else "n/a"
        dd = f"{c['max_drawdown_pct']:.1f}" if c["max_drawdown_pct"] is not None else "n/a"
        print(f"{c['wallet'][:10]:<12} {(c['name'] or '')[:20]:<20} {c['score']:>6.0f} "
              f"{c['closed_trades']:>7} {wr:>6} {pf:>6} {roi:>7} {dd:>6} "
              f"${c['realized_pnl']:>10,.0f} {'YES' if c['passed'] else 'no':>6}")

    store.close()


if __name__ == "__main__":
    asyncio.run(main())
