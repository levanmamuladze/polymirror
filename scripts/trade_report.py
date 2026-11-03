"""Rerunnable performance report against the live SQLite DB.

Usage:
    python scripts/trade_report.py [--config config.yaml]

Prints portfolio summary, per-wallet attribution (always broken down before
aggregating - a portfolio can look fine while one wallet bleeds), copy-quality
analytics (what copying costs vs the source's own fills), and risk flags
(thin sample sizes, near-resolution entries that inflate win rate).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from polymirror.config import load_config  # noqa: E402
from polymirror.metrics import compute_metrics, copy_analytics, wallet_metrics  # noqa: E402
from polymirror.storage import Store  # noqa: E402

MIN_SAMPLE = 15  # closed trades below this are flagged as too few to conclude edge
NEAR_RES_BAND = 0.05


def usd(x: float | None) -> str:
    return f"${x:,.2f}" if x is not None else "n/a"


def pct(x: float | None, sign: bool = False) -> str:
    if x is None:
        return "n/a"
    return f"{x:+.1f}%" if sign else f"{x:.1f}%"


def num(x: float | None, unit: str = "") -> str:
    return f"{x:.1f}{unit}" if x is not None else "n/a"


def section(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymirror performance report")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    args = parser.parse_args()
    cfg = load_config(args.config)
    store = Store(cfg.db_path)

    m = compute_metrics(store)
    section("PORTFOLIO SUMMARY")
    print(f"Starting balance    {usd(m['starting_balance'])}")
    print(f"Equity              {usd(m['equity'])}  ({pct(m['total_return_pct'], sign=True)})")
    print(f"  cash              {usd(m['cash'])}")
    print(f"  position value    {usd(m['position_value'])}")
    print(f"Realized PnL        {usd(m['realized_pnl'])}")
    print(f"Unrealized PnL      {usd(m['unrealized_pnl'])}")
    print(f"Open positions      {m['open_positions']}")
    print(f"Closed round-trips  {m['closed_positions']}  (wins {m['wins']}, losses {m['losses']})")
    print(f"Win rate            {pct(m['win_rate_pct'])}")
    print(f"Profit factor       {num(m['profit_factor'])}")
    print(f"Max drawdown        {pct(m['max_drawdown_pct'])}")
    if m["closed_positions"] < 30:
        print(f"\n  NOTE: n={m['closed_positions']} closed trades - too few to conclude edge.")

    section("PER-WALLET ATTRIBUTION (drawdown/PF ranked above raw return)")
    wm = wallet_metrics(store)
    header = (f"{'Wallet':<14} {'Label':<10} {'Status':<7} {'Closed':>7} "
              f"{'Win%':>6} {'PF':>6} {'Realized':>12} {'Unrealized':>12}")
    print(header)
    print("-" * len(header))
    for w in wm:
        addr = w["wallet"][:6] + ".." + w["wallet"][-4:]
        status = "AUTO" if w["auto"] else ("on" if w["active"] else "PAUSED")
        flag = "  <- n too small" if 0 < w["closed_trades"] < MIN_SAMPLE else ""
        print(f"{addr:<14} {w['label'][:10]:<10} {status:<7} {w['closed_trades']:>7} "
              f"{pct(w['win_rate_pct']):>6} {num(w['profit_factor']):>6} "
              f"{usd(w['realized_pnl']):>12} {usd(w['unrealized_pnl']):>12}{flag}")

    section("COPY QUALITY (what you captured vs the source's own fills)")
    ca = copy_analytics(store, cfg)
    cl = ca.get("copy_latency", {})
    print(f"Copy latency        avg {num(cl.get('avg_s'), 's')}  median {num(cl.get('median_s'), 's')}  "
          f"(n={cl.get('count', 0)})")
    pd = ca.get("price_difference_bps", {})
    print(f"Price difference    avg {num(pd.get('avg'), ' bps')}  median {num(pd.get('median'), ' bps')}  "
          f"(positive = cost to you)")
    print(f"Missed edge         {usd(ca.get('missed_edge_usd', 0.0))}")
    print("Alpha decay by entry latency:")
    for b in ca.get("alpha_decay", []):
        print(f"  {b['bucket']:<8}  n={b['closed']:>4}  avg realized PnL {usd(b['avg_realized_pnl'])}")

    section("RISK FLAGS")
    buys = [t for t in store.all_trades() if t["side"] == "BUY"]
    near_res = [t for t in buys
                if t["source_price"] >= 1 - NEAR_RES_BAND or t["source_price"] <= NEAR_RES_BAND]
    if buys:
        share = 100.0 * len(near_res) / len(buys)
        flag = "  <- inflates win rate, poor risk/reward at entry" if share > 10 else ""
        print(f"Entries near resolution (>{1-NEAR_RES_BAND:.2f} or <{NEAR_RES_BAND:.2f}): "
              f"{len(near_res)}/{len(buys)} ({share:.0f}%){flag}")
    else:
        print("No buy trades yet.")
    print(f"Active price_mode:  {cfg.execution.price_mode}  "
          "(source_price = trader's own skill; market_quote = what a real copier captures "
          "after polling latency - only market_quote results say whether a wallet is copyable)")

    store.close()


if __name__ == "__main__":
    main()
