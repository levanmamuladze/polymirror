"""Probe Gamma's market metadata for a condition_id and show what
GammaClient.market_category() would parse out of it.

Gamma's category/vertical field (used by the wallet scout's category-aware
discovery and diversification, see scout.py) isn't a documented stable
contract — it has shifted across deployments (sometimes a top-level
"category" field, sometimes nested under the market's "events[0].category"
or "events[0].tags"). Run this after upgrading, or whenever candidates'
category_mix looks wrong (empty, or everything landing in one bucket), to
confirm the live response still matches what clients.GammaClient expects:

    python scripts/probe_market_category.py <condition_id> [--base https://gamma-api.polymarket.com]

A condition_id can be copied from any market's URL/API response, or from
a row already in the local `scout_candidates` table (see db_check.py).

Read-only: issues a single unauthenticated GET.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polymirror.clients import GammaClient, RateLimiter  # noqa: E402


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("condition_id", help="Market condition_id to probe")
    ap.add_argument("--base", default="https://gamma-api.polymarket.com")
    args = ap.parse_args()

    client = GammaClient(args.base, RateLimiter(2.0))
    try:
        market = await client.market_by_condition(args.condition_id)
        if market is None:
            print(f"!! No market found for condition_id={args.condition_id}")
            return 1

        print("=== raw market payload (truncated) ===")
        print(json.dumps(market, indent=2)[:3000])

        category = await client.market_category(args.condition_id)
        print(f"\n=== parsed by GammaClient.market_category(): {category!r} ===")
        if category is None:
            print("  !! Nothing parsed — the response shape has likely changed.")
            print("  !! Compare the raw dump above with clients.GammaClient.market_category().")
            return 1
        return 0
    finally:
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
