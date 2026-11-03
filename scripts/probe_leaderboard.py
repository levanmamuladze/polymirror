"""Probe the Polymarket leaderboard endpoint and print its raw shape.

Officially documented at
docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings, but
field names in the response aren't part of that contract, so the wallet
scout still parses defensively. Run this after upgrading if scout discovery
goes quiet, to confirm the live response still matches what
clients.LeaderboardClient expects:

    python scripts/probe_leaderboard.py [--base https://data-api.polymarket.com]

Read-only: issues a single unauthenticated GET.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polymirror.clients import LeaderboardClient, RateLimiter, WINDOW_TO_TIME_PERIOD  # noqa: E402


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default="https://data-api.polymarket.com")
    ap.add_argument("--window", default="1m", help="1d | 1w | 1m | all")
    ap.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    client = LeaderboardClient(args.base, RateLimiter(2.0))
    try:
        raw = await client.get_json(
            "/v1/leaderboard",
            {"category": "OVERALL", "timePeriod": WINDOW_TO_TIME_PERIOD.get(args.window, "ALL"),
             "orderBy": "PNL", "limit": args.limit},
        )
        print("=== raw response (first 3 rows) ===")
        rows = raw if isinstance(raw, list) else raw
        print(json.dumps(rows[:3] if isinstance(rows, list) else rows, indent=2)[:3000])

        parsed = await client.top_wallets(args.window, args.limit)
        print(f"\n=== parsed by LeaderboardClient: {len(parsed)} wallets ===")
        for p in parsed:
            print(f"  {p['wallet']}  pnl~${p['amount']:>12,.0f}  {p['name']}")
        if not parsed:
            print("  !! Endpoint responded but no rows parsed - the response shape")
            print("  !! has likely changed. Compare the raw dump above with the key")
            print("  !! names in clients.LeaderboardClient.top_wallets().")
            return 1
        return 0
    finally:
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
