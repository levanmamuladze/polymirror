"""Execution engines: PaperBroker (simulation) and LiveBroker (real orders).

Both expose the identical `buy`/`sell` -> `Fill` interface so that
`tracker.Engine` never has to know which one it's holding (execution
abstraction, see CLAUDE.md) — only `run.py` decides which to build, based
on `execution.mode`.

PaperBroker
-----------
Simulation-only by construction: it receives a reference price and produces
bookkeeping entries. It cannot place orders — it holds no client, no keys,
no network access.

Fill model
* price_mode = source_price  -> fill at the source trader's own fill price,
  degraded by `slippage_bps` against the copier (buys fill higher, sells
  lower). Honest baseline: "could I have gotten roughly the same fill?"
* price_mode = market_quote  -> fill at the current CLOB best quote for the
  taker side (plus slippage). More realistic for slow polling intervals,
  since the market may have moved since the source traded.

Fees: Polymarket currently charges no trading fee on most markets, so the
default fee_bps is 0, but the field exists so results can be stress-tested.

Accounting: prediction-market outcome tokens are long-only assets priced in
(0, 1). Positions use average-cost basis; realized PnL is computed on sells
as (sell_price - avg_price) * size. Redemption settles at $1 (winning
outcome) or $0.

LiveBroker
----------
Optional, off by default (see config.py's LiveConfig / Config.validate).
Submits real orders through an `execution.ExecutionClient` and books
whatever the exchange actually reports — never the naive reference price
used to size the order — into the same Store schema PaperBroker uses, so
tracking/analytics/the dashboard behave identically regardless of mode.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from .execution import ExecutionClient, LiveExecutionError, OrderResult
from .storage import InsufficientCash, Store


@dataclass
class Fill:
    side: str            # BUY | SELL
    kind: str            # OPEN | ADD | PARTIAL_EXIT | FULL_EXIT | REDEEM
    size: float          # shares
    price: float         # simulated fill price after slippage
    source_price: float  # reference price before slippage
    fee: float
    notional: float
    realized_pnl: float
    position_id: int
    cash_after: float


class PaperBroker:
    def __init__(self, store: Store, slippage_bps: float = 0.0, fee_bps: float = 0.0):
        self.store = store
        self.slippage = slippage_bps / 10_000.0
        self.fee_rate = fee_bps / 10_000.0

    # ------------------------------------------------------------------ fills
    def _fill_price(self, reference: float, side: str) -> float:
        adj = 1 + self.slippage if side == "BUY" else 1 - self.slippage
        return min(0.999, max(0.001, reference * adj))

    def buy(self, wallet: str, asset: str, notional: float, reference_price: float,
            meta: dict) -> Fill:
        """Open or add to a position, spending `notional` USD of cash."""
        price = self._fill_price(reference_price, "BUY")
        fee = notional * self.fee_rate
        size = (notional - fee) / price
        if size <= 0:
            raise ValueError("buy size resolved to zero")

        pos = self.store.open_position(wallet, asset)
        if pos is None:
            kind = "OPEN"
            pos = {
                "wallet": wallet, "asset": asset,
                "condition_id": meta.get("condition_id", ""),
                "title": meta.get("title", ""), "outcome": meta.get("outcome", ""),
                "size": size, "avg_price": price, "mark_price": reference_price,
                "status": "open", "realized_pnl": 0.0,
            }
        else:
            kind = "ADD"
            total_cost = pos["avg_price"] * pos["size"] + price * size
            pos["size"] += size
            pos["avg_price"] = total_cost / pos["size"]
            pos["mark_price"] = reference_price
            pos["status"] = "open"

        # Cash and position move together in one transaction — see
        # Store.apply_fill for why a two-step adjust_cash()+save_position()
        # is dangerous (a failure between the two can orphan a cash credit).
        cash_after, pos_id = self.store.apply_fill(-notional, pos)  # raises InsufficientCash

        return Fill("BUY", kind, size, price, reference_price, fee,
                    notional, 0.0, pos_id, cash_after)

    def sell(self, wallet: str, asset: str, size: float, reference_price: float,
             settlement: bool = False) -> Fill:
        """Sell `size` shares of an open position (or settle at redemption)."""
        pos = self.store.open_position(wallet, asset)
        if pos is None or pos["size"] <= 0:
            raise NoPosition(f"no open simulated position for {asset}")

        size = min(size, pos["size"])
        price = reference_price if settlement else self._fill_price(reference_price, "SELL")
        notional = size * price
        fee = 0.0 if settlement else notional * self.fee_rate
        realized = (price - pos["avg_price"]) * size

        pos["size"] -= size
        pos["realized_pnl"] = pos.get("realized_pnl", 0.0) + realized - fee
        full_exit = pos["size"] <= 1e-9
        if full_exit:
            pos["size"] = 0.0
            pos["status"] = "closed"
            pos["closed_at"] = int(time.time())
        pos["mark_price"] = reference_price

        cash_after, pos_id = self.store.apply_fill(notional - fee, pos)

        kind = "REDEEM" if settlement else ("FULL_EXIT" if full_exit else "PARTIAL_EXIT")
        return Fill("SELL", kind, size, price, reference_price, fee,
                    notional, realized, pos_id, cash_after)


class NoPosition(Exception):
    pass


class LiveBroker:
    """Real-money counterpart to PaperBroker. Same `buy`/`sell` -> `Fill`
    interface, same Store bookkeeping (`open_position` / `apply_fill`), so
    `tracker.Engine` calls it identically to PaperBroker.

    `max_live_notional_usd` is a hard per-order cap enforced here, in
    addition to (not instead of) `execution.max_trade_usd` — a second,
    independent ceiling specifically for real funds (see LiveConfig).
    `slippage_bps` sets a marketable-limit cushion around the reference
    price (buy above it, sell below it) so the order actually crosses the
    book instead of resting unfilled; it reuses execution.slippage_bps so
    paper and live share one "how much am I willing to concede to get
    filled" knob. `order_type` defaults to FOK (see LiveConfig) precisely
    so this cushion either fills the whole order immediately or the order
    is killed — no partial/resting fill for reconciliation to miss.

    Note on blocking: `submit_order` on the real client is a synchronous
    network call (py-clob-client has no async API). `tracker.Engine` calls
    `broker.buy`/`sell` synchronously today (matching PaperBroker, which is
    pure/local), so this briefly blocks the asyncio event loop during a
    live order — acceptable given copy events are infrequent relative to
    the poll interval, but worth knowing if latency ever needs tightening.
    """

    def __init__(self, store: Store, client: ExecutionClient,
                 max_live_notional_usd: float, slippage_bps: float = 0.0,
                 order_type: str = "FOK", funder_address: str = ""):
        self.store = store
        self.client = client
        self.max_live_notional_usd = max_live_notional_usd
        self.slippage = slippage_bps / 10_000.0
        self.order_type = order_type
        # Public wallet address, used only by tracker.Engine's own-account
        # reconciliation (duck-typed attribute access — see
        # Engine.reconcile_own_live_account). Empty means "skip that check"
        # (e.g. in tests that don't care about reconciliation).
        self.funder_address = funder_address

    def _check_cap(self, notional: float) -> None:
        if self.max_live_notional_usd <= 0 or notional > self.max_live_notional_usd:
            raise LiveExecutionError(
                f"live order ${notional:,.2f} exceeds live.max_live_notional_usd cap "
                f"(${self.max_live_notional_usd:,.2f})"
            )

    def _limit_price(self, reference: float, side: str) -> float:
        adj = 1 + self.slippage if side == "BUY" else 1 - self.slippage
        return min(0.999, max(0.001, reference * adj))

    def buy(self, wallet: str, asset: str, notional: float, reference_price: float,
            meta: dict) -> Fill:
        """Open or add to a position by submitting a real marketable-limit
        buy order for `notional` USD of exposure at `reference_price`."""
        self._check_cap(notional)
        limit_price = self._limit_price(reference_price, "BUY")
        size = notional / limit_price
        result = self.client.submit_order(
            token_id=asset, side="BUY", size=size, price=limit_price,
            order_type=self.order_type,
        )
        if result.filled_size <= 0:
            raise ValueError("live buy resolved to zero filled size")

        pos = self.store.open_position(wallet, asset)
        if pos is None:
            kind = "OPEN"
            pos = {
                "wallet": wallet, "asset": asset,
                "condition_id": meta.get("condition_id", ""),
                "title": meta.get("title", ""), "outcome": meta.get("outcome", ""),
                "size": result.filled_size, "avg_price": result.avg_price,
                "mark_price": reference_price, "status": "open", "realized_pnl": 0.0,
            }
        else:
            kind = "ADD"
            total_cost = pos["avg_price"] * pos["size"] + result.avg_price * result.filled_size
            pos["size"] += result.filled_size
            pos["avg_price"] = total_cost / pos["size"]
            pos["mark_price"] = reference_price
            pos["status"] = "open"

        cash_after, pos_id = self.store.apply_fill(-(result.notional + result.fee), pos)
        return Fill("BUY", kind, result.filled_size, result.avg_price, reference_price,
                    result.fee, result.notional, 0.0, pos_id, cash_after)

    def sell(self, wallet: str, asset: str, size: float, reference_price: float,
             settlement: bool = False) -> Fill:
        """Sell `size` shares of an open position via a real marketable-limit
        sell order, or settle a redemption at `reference_price` (a
        redemption is an on-chain claim already made by the source wallet,
        not a CLOB order — nothing left to sign or submit here)."""
        pos = self.store.open_position(wallet, asset)
        if pos is None or pos["size"] <= 0:
            raise NoPosition(f"no open live position for {asset}")
        size = min(size, pos["size"])

        if settlement:
            result = OrderResult(
                order_id="redeem", status="settled", side="SELL",
                filled_size=size, avg_price=reference_price, fee=0.0,
                notional=size * reference_price, raw={},
            )
        else:
            limit_price = self._limit_price(reference_price, "SELL")
            result = self.client.submit_order(
                token_id=asset, side="SELL", size=size, price=limit_price,
                order_type=self.order_type,
            )

        realized = (result.avg_price - pos["avg_price"]) * result.filled_size
        pos["size"] -= result.filled_size
        pos["realized_pnl"] = pos.get("realized_pnl", 0.0) + realized - result.fee
        full_exit = pos["size"] <= 1e-9
        if full_exit:
            pos["size"] = 0.0
            pos["status"] = "closed"
            pos["closed_at"] = int(time.time())
        pos["mark_price"] = reference_price

        cash_after, pos_id = self.store.apply_fill(result.notional - result.fee, pos)
        kind = "REDEEM" if settlement else ("FULL_EXIT" if full_exit else "PARTIAL_EXIT")
        return Fill("SELL", kind, result.filled_size, result.avg_price, reference_price,
                    result.fee, result.notional, realized, pos_id, cash_after)


def sync_live_cash(store: Store, client: ExecutionClient) -> str | None:
    """Bridge Store's cash ledger to the real on-chain USDC balance for
    live mode. Called once at engine startup (run.py's `build_broker`,
    before any order can be placed) and again on every reconciliation
    cycle (`tracker.Engine.reconcile_own_live_account`).

    First call ever for this Store (no `live_cash_synced_at` meta):
    bootstraps the account from the real balance — see
    `Store.bootstrap_live_account`. There is no other ground truth for
    "starting balance" once real money is involved, unlike paper mode
    where the config value IS the definition.

    Every subsequent call: compares booked cash to the real balance and
    returns a description of any drift found. Never auto-corrects after
    that first bootstrap — per CLAUDE.md's shadow-model invariant, our own
    recorded state is not force-adjusted to match an external number after
    the fact; a human resolves real drift. Returns None when there's
    nothing to report.
    """
    real_cash = client.get_usdc_balance()
    if store.get_meta("live_cash_synced_at") is None:
        store.bootstrap_live_account(real_cash)
        store.set_meta("live_cash_synced_at", str(int(time.time())))
        return f"Live mode initialised: account reset to on-chain USDC balance ${real_cash:,.2f}"
    booked = float(store.account()["cash"])
    if abs(booked - real_cash) > max(1.0, 0.02 * max(booked, real_cash, 1.0)):
        return (f"LIVE CASH DRIFT — book shows ${booked:,.2f}, on-chain USDC balance is "
                f"${real_cash:,.2f}. NOT auto-corrected; investigate before trading further.")
    return None


__all__ = ["PaperBroker", "LiveBroker", "Fill", "NoPosition", "InsufficientCash", "sync_live_cash"]
