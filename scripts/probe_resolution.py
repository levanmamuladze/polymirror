"""Probe Gamma's resolution payload for a market and show what
clients.parse_resolution() would settle each outcome token at.

Settlement correctness is the single most load-bearing assumption in the
paper record: if resolved markets can't be priced, losing positions never
settle, they sit open marked at their entry price, and the equity curve
only ever ratchets upward on redeemed winners (see SettlementConfig).

Gamma's schema is not a documented stable contract — `clobTokenIds` and
`outcomePrices` come back sometimes as JSON arrays and sometimes as JSON
*strings*, and the closed/resolved flag has moved between `closed`,
`archived`, and `umaResolutionStatus`. Run this whenever:

  * the engine warns "Position open Nh with no readable resolution",
  * positions stay open long after their market ended, or
  * you've upgraded and want to confirm the live shape still parses.

    python scripts/probe_resolution.py <condition_id> [--base URL]

A condition_id can be copied from any market URL/API response, or from a
row in the local sim_positions table (see db_check.py).

Read-only: issues a single unauthenticated GET.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polymirror.clients import (  # noqa: E402
    GammaClient, RateLimiter, _as_list, _market_is_closed, parse_resolution,
)


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
            print("   Settlement cannot price any position in this market.")
            return 1

        print("=== fields settlement depends on ===")
        for key in ("closed", "archived", "umaResolutionStatus", "endDate",
                    "outcomes", "outcomePrices", "clobTokenIds"):
            raw = market.get(key)
            shown = raw if not isinstance(raw, str) or len(raw) < 120 else raw[:117] + "..."
            print(f"  {key:<20} {type(raw).__name__:<6} {shown!r}")

        closed = _market_is_closed(market)
        print(f"\n=== parsed ===\n  _market_is_closed -> {closed}")
        if not closed:
            print("  Market is still trading; nothing should settle yet.")

        tokens = [str(t) for t in _as_list(market.get("clobTokenIds"))]
        prices = _as_list(market.get("outcomePrices"))
        outcomes = _as_list(market.get("outcomes"))
        if not tokens:
            print("  !! clobTokenIds did not parse into a list — settlement will")
            print("     return None for every position in this market. This is the")
            print("     failure that silently hides losing positions.")
            return 1
        if len(prices) != len(tokens):
            print(f"  !! length mismatch: {len(tokens)} token ids vs {len(prices)} "
                  f"outcome prices — indexes are meaningless, settlement returns None.")
            return 1

        print(f"\n  {'outcome':<24} {'token_id':<22} {'price':<8} settles at")
        for i, token in enumerate(tokens):
            name = str(outcomes[i]) if i < len(outcomes) else f"#{i}"
            payout = parse_resolution(market, token)
            verdict = "unresolved (stays open)" if payout is None else f"${payout:.2f}"
            print(f"  {name[:24]:<24} {token[:20]:<22} {str(prices[i])[:8]:<8} {verdict}")

        if closed and all(parse_resolution(market, t) is None for t in tokens):
            print("\n  !! Market reads as closed but no outcome is decisively 0/1.")
            print("     Either it is mid-resolution/disputed, or the payload shape")
            print("     changed. Positions here will stay open and be flagged.")
        return 0
    finally:
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
