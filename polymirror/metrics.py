"""Portfolio performance metrics derived from stored trades and snapshots."""
from __future__ import annotations

import math
import statistics
from typing import Any

from .storage import Store

# Trades that fill at a resolved market's payout rather than at a quoted
# price: REDEEM (settled off the source's on-chain redemption) and SETTLE
# (settled by the engine's own resolution sweep — see
# tracker.Engine.settle_resolved_positions). Copy-quality analytics compare
# our fill against the source's fill, which is meaningless for both.
SETTLEMENT_KINDS = frozenset({"REDEEM", "SETTLE"})


def compute_metrics(store: Store) -> dict[str, Any]:
    acct = store.account()
    starting = float(acct["starting_balance"])
    cash = float(acct["cash"])
    total_deposits = float(acct.get("total_deposits", 0.0))
    total_withdrawals = float(acct.get("total_withdrawals", 0.0))
    # Deposits/withdrawals are capital moves, not trading P&L — return% must
    # be measured against net capital ever contributed, or a deposit would
    # look like profit and a withdrawal would look like a loss.
    net_contributed = starting + total_deposits - total_withdrawals

    open_positions = store.open_positions()
    position_value = 0.0
    unrealized = 0.0
    for p in open_positions:
        mark = p["mark_price"] if p["mark_price"] is not None else p["avg_price"]
        position_value += mark * p["size"]
        unrealized += (mark - p["avg_price"]) * p["size"]

    equity = cash + position_value
    realized = store.realized_pnl_total()

    # Win rate over completed round-trips (closed positions).
    closed = store.closed_positions(limit=10_000)
    wins = sum(1 for p in closed if p["realized_pnl"] > 0)
    losses = sum(1 for p in closed if p["realized_pnl"] <= 0)
    round_trips = wins + losses
    win_rate = wins / round_trips if round_trips else None

    curve = store.equity_curve()
    # Trading-adjusted (deposits/withdrawals backed out) — same reasoning
    # as net_contributed above: a withdrawal must never look like a
    # drawdown and a deposit must never mask a real one.
    max_drawdown = _max_drawdown(
        [row["equity"] - row.get("net_transfers", 0.0) for row in curve]) if curve else 0.0

    gross_profit = sum(p["realized_pnl"] for p in closed if p["realized_pnl"] > 0)
    gross_loss = -sum(p["realized_pnl"] for p in closed if p["realized_pnl"] < 0)
    profit_factor = (gross_profit / gross_loss) if gross_loss > 1e-9 else None

    return {
        "starting_balance": starting,
        "equity": equity,
        "cash": cash,
        "position_value": position_value,
        "unrealized_pnl": unrealized,
        "realized_pnl": realized,
        "total_deposits": total_deposits,
        "total_withdrawals": total_withdrawals,
        "net_contributed": net_contributed,
        "total_return_pct": (equity / net_contributed - 1) * 100 if net_contributed else 0.0,
        "open_positions": len(open_positions),
        "closed_positions": round_trips,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": win_rate * 100 if win_rate is not None else None,
        "profit_factor": profit_factor,
        "max_drawdown_pct": max_drawdown * 100,
    }


def _max_drawdown(equity: list[float]) -> float:
    peak = -math.inf
    mdd = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak > 0:
            mdd = max(mdd, (peak - value) / peak)
    return mdd


def wallet_metrics(store: Store) -> list[dict[str, Any]]:
    """Per-followed-wallet attribution of the *copied* results.

    Judged on simulated positions (what the copier captured after sizing,
    slippage and latency), the numbers auto-manage and the dashboard use to
    tell which followed wallet is earning its slot."""
    out: list[dict[str, Any]] = []
    for w in store.wallets():
        positions = store.positions_for_wallet(w["address"])
        closed = [p for p in positions if p["status"] == "closed"]
        open_ = [p for p in positions if p["status"] == "open"]
        wins = sum(1 for p in closed if p["realized_pnl"] > 0)
        losses = len(closed) - wins
        gross_profit = sum(p["realized_pnl"] for p in closed if p["realized_pnl"] > 0)
        gross_loss = -sum(p["realized_pnl"] for p in closed if p["realized_pnl"] < 0)
        realized = sum(p["realized_pnl"] for p in positions)  # incl. partial exits still open
        open_value = unrealized = 0.0
        for p in open_:
            mark = p["mark_price"] if p["mark_price"] is not None else p["avg_price"]
            open_value += mark * p["size"]
            unrealized += (mark - p["avg_price"]) * p["size"]
        out.append({
            "wallet": w["address"],
            "label": w["label"],
            "active": bool(w["active"]),
            "auto": bool(w["auto"]),
            "closed_trades": len(closed),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": 100.0 * wins / len(closed) if closed else None,
            "profit_factor": (gross_profit / gross_loss) if gross_loss > 1e-9 else None,
            "realized_pnl": realized,
            "open_positions": len(open_),
            "open_value": open_value,
            "unrealized_pnl": unrealized,
            "last_trade_ts": store.last_trade_ts(w["address"]),
        })
    out.sort(key=lambda r: -(r["realized_pnl"] + r["unrealized_pnl"]))
    return out


def copy_analytics(store: Store, cfg: Any) -> dict[str, Any]:
    """Copy-quality analytics: how much of the source's edge the copier
    actually captures after polling latency and slippage.

    * copy_latency        — source trade timestamp -> simulated execution
    * price_difference    — fill vs source price in bps (positive = cost)
    * missed_edge_usd     — total USD given up vs the source's own fills
    * alpha_decay         — realized PnL per closed round-trip, bucketed by
                            how stale the entry copy was (approximate: first
                            OPEN fill per wallet+asset)
    REDEEM and SETTLE rows are excluded (both fill at the resolved payout
    by definition, so their "slippage vs source" is meaningless).
    """
    trades = [t for t in store.all_trades() if t["kind"] not in SETTLEMENT_KINDS]
    ac = cfg.analytics
    out: dict[str, Any] = {}

    if ac.track_copy_latency:
        lats = [t["executed_at"] - t["source_ts"] for t in trades if t["source_ts"] > 0]
        out["copy_latency"] = {
            "count": len(lats),
            "avg_s": statistics.fmean(lats) if lats else None,
            "median_s": statistics.median(lats) if lats else None,
            "max_s": max(lats) if lats else None,
        }

    if ac.track_price_difference:
        diffs = []
        for t in trades:
            if t["source_price"] > 0:
                bps = (t["price"] - t["source_price"]) / t["source_price"] * 10_000
                diffs.append(bps if t["side"] == "BUY" else -bps)   # positive = against us
        out["price_difference_bps"] = {
            "count": len(diffs),
            "avg": statistics.fmean(diffs) if diffs else None,
            "median": statistics.median(diffs) if diffs else None,
        }

    if ac.track_missed_edge:
        out["missed_edge_usd"] = sum(
            (t["price"] - t["source_price"]) * t["size"] if t["side"] == "BUY"
            else (t["source_price"] - t["price"]) * t["size"]
            for t in trades
        )

    if ac.track_alpha_decay:
        entry_latency: dict[tuple[str, str], int] = {}
        for t in trades:
            if t["side"] == "BUY" and t["source_ts"] > 0:
                entry_latency.setdefault((t["wallet"], t["asset"]),
                                         t["executed_at"] - t["source_ts"])
        buckets: dict[str, list[float]] = {"<10s": [], "10-60s": [], ">60s": []}
        for p in store.closed_positions(limit=10_000):
            lat = entry_latency.get((p["wallet"], p["asset"]))
            if lat is None:
                continue
            key = "<10s" if lat < 10 else ("10-60s" if lat <= 60 else ">60s")
            buckets[key].append(float(p["realized_pnl"]))
        out["alpha_decay"] = [
            {"bucket": k, "closed": len(v),
             "avg_realized_pnl": statistics.fmean(v) if v else None}
            for k, v in buckets.items()
        ]

    return out


def allocation(store: Store) -> list[dict[str, Any]]:
    """Portfolio allocation by open position (market value)."""
    rows = []
    for p in store.open_positions():
        mark = p["mark_price"] if p["mark_price"] is not None else p["avg_price"]
        rows.append({
            "title": p["title"] or p["asset"][:12],
            "outcome": p["outcome"],
            "value": mark * p["size"],
        })
    rows.sort(key=lambda r: -r["value"])
    return rows
