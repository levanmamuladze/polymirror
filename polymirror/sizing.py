"""Position sizing for copied BUY trades.

All modes return a target notional (USD) for the copy, before the broker
converts it to shares at the simulated fill price. The per-wallet
`multiplier` acts as a risk dial on top of any mode (0.5 = half risk,
2.0 = double).

Modes
-----
proportional     Copy the same *fraction of portfolio* the source used:
                 notional = source_usdc * (my_equity / source_portfolio_value) * multiplier
                 A trader with $1M who bets $10k risks 1%; on a $10k sim
                 account that maps to a $100 copy.
fixed_usd        notional = value * multiplier, regardless of source size.
percent_balance  notional = available_cash * value * multiplier
                 (value expressed as a fraction, e.g. 0.05 = 5%).

SELL trades are *not* sized here: exits always mirror the fraction of the
source's position being sold (see tracker.py), which keeps the simulated
book structurally synchronized with the source regardless of entry mode.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SizingContext:
    equity: float                  # simulated account equity (cash + positions)
    available_cash: float
    source_trade_usdc: float       # notional of the source trader's buy
    source_portfolio_value: float  # source trader's open-position value (from /value)
    starting_balance: float = 0.0  # configured starting balance (for percent-starting modes)


def target_notional(mode: str, value: float, multiplier: float, ctx: SizingContext) -> float:
    if mode == "proportional":
        if ctx.source_portfolio_value <= 0:
            return 0.0
        scale = ctx.equity / ctx.source_portfolio_value
        notional = ctx.source_trade_usdc * scale
    elif mode == "fixed_usd":
        notional = value
    elif mode == "percent_balance":
        notional = ctx.available_cash * value
    elif mode == "percent_starting_balance":
        notional = ctx.starting_balance * value
    else:
        raise ValueError(f"unknown sizing mode: {mode}")

    notional *= multiplier
    # Never exceed available cash.
    return max(0.0, min(notional, ctx.available_cash))
