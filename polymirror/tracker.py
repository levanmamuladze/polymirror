"""Copy-trading engine.

Responsibilities
----------------
1. Poll the Data API `/activity` feed for every active followed wallet.
2. Deduplicate events (idempotency keys survive restarts).
3. Maintain a *shadow model* of each source trader's positions so that
   sells can be classified as partial vs. full exits and mirrored
   proportionally.
4. Size and execute copies through `self.broker` — PaperBroker (simulated,
   default) or LiveBroker (real orders, opt-in only) — see broker.py.
5. Mark open positions to market via CLOB midpoints and snapshot equity.
6. Periodically reconcile the shadow model against `/positions` and flag
   drift (missed events, splits/merges we don't model, etc.).

Event ordering & restart safety
-------------------------------
Each wallet has a persisted cursor (`last_ts`). Every cycle re-fetches
activity from `last_ts - OVERLAP` to tolerate indexer lag and out-of-order
delivery; the processed-events table suppresses anything already handled.
A crash between "mark processed" steps can at worst skip one event, never
double-execute one — the idempotency insert happens *before* execution and
is rolled into the same logical flow.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from typing import Any

from .broker import InsufficientCash, NoPosition, PaperBroker, sync_live_cash
from .clients import ApiError, ClobClient, DataApiClient, GammaClient
from .config import Config
from .metrics import wallet_metrics
from .notify import Notifier
from .scout import WalletScout
from .sizing import SizingContext, target_notional
from .storage import Store

log = logging.getLogger("polymirror.tracker")

CURSOR_OVERLAP_SECONDS = 120   # re-scan window to catch late-indexed events
DUST = 1e-9


def event_key(ev: dict[str, Any]) -> str:
    """Stable idempotency key for one activity row.

    A single transaction can contain several fills (different assets or
    sides), so the key includes everything that distinguishes a fill.
    """
    raw = "|".join(str(ev.get(k, "")) for k in
                   ("transactionHash", "asset", "side", "size", "price", "timestamp", "type"))
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


class Engine:
    def __init__(self, cfg: Config, store: Store, data: DataApiClient,
                 clob: ClobClient, gamma: GammaClient, notifier: Notifier,
                 scout: WalletScout | None = None, broker: object | None = None):
        self.cfg = cfg
        self.store = store
        self.data = data
        self.clob = clob
        self.gamma = gamma
        self.notifier = notifier
        self.scout = scout
        # `broker` is the execution abstraction: PaperBroker (default,
        # simulation-only) or LiveBroker (real orders — only ever
        # constructed by run.py's composition root when execution.mode is
        # explicitly "live"). The engine never builds a LiveBroker itself
        # and never imports execution.py, so it cannot be the thing that
        # turns live trading on.
        self.broker = broker if broker is not None else PaperBroker(
            store, cfg.execution.slippage_bps, cfg.execution.fee_bps)
        self._source_value_cache: dict[str, tuple[float, float]] = {}  # wallet -> (value, fetched_at)
        self._stop = asyncio.Event()
        self._seeded: set[str] = set()
        self._last_polled: dict[str, float] = {}   # wallet -> last successful poll (monotonic-free wall time)
        self._stale_notified: set[str] = set()
        self._unresolved_notified: set[str] = set()  # position ids already warned about

    # ------------------------------------------------------------------ loops
    async def run(self) -> None:
        """Main loop; individual cycles are exception-isolated so a bad API
        response or network outage never kills the process."""
        mode_msg = ("LIVE TRADING ENABLED — real funds at risk"
                    if self.cfg.execution.mode == "live" else "paper trading only")
        await self.notifier.send("info", "system", f"Engine started ({mode_msg})")
        scout_task = None
        if self.scout is not None and self.cfg.scout.enabled:
            # The scout runs as its own task: a full discovery sweep can take
            # minutes at the shared rate limit and must not stall polling.
            scout_task = asyncio.create_task(self.scout.run(self._stop))
        last_mark = 0.0
        last_reconcile = 0.0
        last_manage = 0.0
        last_settle = 0.0
        # Snapshot cadence: honor whichever of the two knobs is tighter.
        mark_every = min(self.cfg.mark_interval_seconds,
                         self.cfg.analytics.equity_snapshot_seconds)
        try:
            while not self._stop.is_set():
                started = time.monotonic()
                self.store.set_meta("heartbeat_ts", str(int(time.time())))
                try:
                    await self.poll_all()
                except Exception as err:  # noqa: BLE001 — engine must survive anything
                    log.exception("poll cycle failed")
                    await self.notifier.send("error", "system", f"Poll cycle failed: {err}")

                now = time.monotonic()
                # Settle resolved markets BEFORE marking and risk checks:
                # a resolved position has no CLOB midpoint, so leaving it
                # open means equity keeps carrying it at its entry-era mark
                # and stop-loss/take-profit never see the real outcome.
                if (self.cfg.settlement.enabled
                        and now - last_settle >= self.cfg.settlement.interval_seconds):
                    last_settle = now
                    try:
                        await self.settle_resolved_positions()
                    except Exception as err:  # noqa: BLE001
                        log.exception("settlement sweep failed")
                        await self.notifier.send("error", "sync",
                                                 f"Settlement sweep failed: {err}")

                if now - last_mark >= mark_every:
                    last_mark = now
                    try:
                        await self.mark_to_market()
                        await self.check_position_risk()
                        await self.check_portfolio_risk()
                        await self.check_auto_withdraw()
                    except Exception as err:  # noqa: BLE001
                        log.exception("mark-to-market failed")
                        await self.notifier.send("error", "system", f"Mark-to-market failed: {err}")

                if now - last_reconcile >= self.cfg.reconcile_interval_seconds:
                    last_reconcile = now
                    try:
                        await self.reconcile_all()
                    except Exception as err:  # noqa: BLE001
                        log.exception("reconciliation failed")
                        await self.notifier.send("error", "sync", f"Reconciliation failed: {err}")

                if (self.cfg.auto_manage.enabled
                        and now - last_manage >= self.cfg.auto_manage.interval_seconds):
                    last_manage = now
                    try:
                        await self.auto_manage()
                    except Exception as err:  # noqa: BLE001
                        log.exception("auto-manage failed")
                        await self.notifier.send("error", "risk", f"Auto-manage failed: {err}")

                elapsed = time.monotonic() - started
                await self._sleep(max(0.5, self.cfg.poll_interval_seconds - elapsed))
        finally:
            if scout_task is not None:
                scout_task.cancel()

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------- poll
    async def poll_all(self) -> None:
        """Poll every active wallet concurrently.

        Each wallet's HTTP round trips (and any retry backoff) used to run
        strictly one-after-another, so a single slow or failing wallet added
        its full retry delay (worst case ~15s) as dead time in front of
        every wallet queued behind it. Concurrency removes that: the shared
        RateLimiter still paces actual request throughput identically, but
        one wallet's wait no longer blocks another's progress."""
        wallets = self.store.wallets(active_only=True)
        await asyncio.gather(*(self._poll_one(w) for w in wallets))
        await self.check_stale_wallets()

    async def _poll_one(self, w: dict[str, Any]) -> None:
        wallet = w["address"]
        if wallet not in self._seeded:
            await self.seed_wallet(wallet)
        try:
            await self.poll_wallet(w)
            self._last_polled[wallet] = time.time()
            self._stale_notified.discard(wallet)
        except ApiError as err:
            if self.cfg.health.alert_on_api_failure:
                await self.notifier.send("warning", "sync",
                                         f"Data API unavailable for {short(wallet)}: {err}")
            else:
                log.warning("Data API unavailable for %s: %s", short(wallet), err)

    async def check_stale_wallets(self) -> None:
        """One warning per staleness episode when an active wallet hasn't
        been polled successfully for `health.stale_wallet_minutes`."""
        stale_after = self.cfg.health.stale_wallet_minutes * 60
        if stale_after <= 0:
            return
        now = time.time()
        for w in self.store.wallets(active_only=True):
            wallet = w["address"]
            last = self._last_polled.get(wallet)
            if last is None or wallet in self._stale_notified:
                continue
            if now - last > stale_after:
                self._stale_notified.add(wallet)
                await self.notifier.send(
                    "warning", "sync",
                    f"{w['label'] or short(wallet)} has not been polled successfully for "
                    f"{(now - last) / 60:.0f} min — data may be stale",
                )

    async def seed_wallet(self, wallet: str) -> None:
        """First contact with a wallet *this session*: snapshot its current
        positions into the shadow model so later sells can be interpreted,
        and send the one-time "now following" notice. Cursor initialization
        deliberately does NOT live here — it's guarded by `_seeded`, an
        in-memory set that a DB-level reset (clearing the persisted cursor)
        cannot see, so gating cursor init on it can desync: a reset wallet
        already in `_seeded` would never get its cursor reinitialized and
        the next poll would fetch the wallet's *entire* history with
        start=0, replaying years-old trades as brand new. See
        ensure_cursor_initialized(), called unconditionally on every poll."""
        try:
            positions = await self.data.positions(wallet)
        except ApiError as err:
            log.warning("could not seed %s yet: %s", wallet, err)
            return
        for p in positions:
            self.store.upsert_source_position(
                wallet, str(p.get("asset", "")), str(p.get("conditionId", "")),
                float(p.get("size", 0.0)), float(p.get("avgPrice", 0.0)),
                str(p.get("title", "")), str(p.get("outcome", "")),
            )
        self._seeded.add(wallet)
        await self.notifier.send(
            "info", "system",
            f"Now following {short(wallet)} ({len(positions)} existing source positions snapshotted)",
        )

    def ensure_cursor_initialized(self, wallet: str) -> None:
        """If this wallet has no cursor yet — never followed, or one just
        cleared by a reset — set it to "now" (or the configured backfill
        window) before any poll can fetch its full history. Pure, cheap,
        idempotent DB check; safe to call on every single poll cycle,
        unlike the one-time seeding above."""
        if self.store.get_cursor(wallet) == 0:
            start = int(time.time()) - int(self.cfg.backfill_seconds)
            self.store.set_cursor(wallet, start)

    async def poll_wallet(self, w: dict[str, Any]) -> None:
        wallet = w["address"]
        self.ensure_cursor_initialized(wallet)
        cursor = self.store.get_cursor(wallet)
        start = max(0, cursor - CURSOR_OVERLAP_SECONDS)
        events = await self.data.activity_since(wallet, start=start)
        events.sort(key=lambda e: (int(e.get("timestamp", 0)), str(e.get("transactionHash", ""))))

        max_ts = cursor
        for ev in events:
            ts = int(ev.get("timestamp", 0))
            key = event_key(ev)
            if self.store.is_processed(key):
                max_ts = max(max_ts, ts)
                continue
            # Claim the event first: guarantees at-most-once execution even
            # if we crash mid-handling.
            if not self.store.mark_processed(key, wallet, ts):
                continue
            try:
                await self.handle_event(w, ev, key)
            except Exception as err:  # noqa: BLE001
                log.exception("event handling failed")
                await self.notifier.send("error", "system",
                                         f"Failed to process event {key[:8]}: {err}")
            max_ts = max(max_ts, ts)

        if max_ts > cursor:
            self.store.set_cursor(wallet, max_ts)

    # ------------------------------------------------------------ event logic
    async def handle_event(self, w: dict[str, Any], ev: dict[str, Any], key: str) -> None:
        etype = str(ev.get("type", "")).upper()
        if etype == "TRADE":
            await self.handle_trade(w, ev, key)
        elif etype == "REDEEM":
            await self.handle_redeem(w, ev, key)
        # SPLIT / MERGE / CONVERSION are intentionally not mirrored; they are
        # caught by reconciliation and reported as sync notices.

    async def handle_trade(self, w: dict[str, Any], ev: dict[str, Any], key: str) -> None:
        wallet = w["address"]
        asset = str(ev.get("asset", ""))
        side = str(ev.get("side", "")).upper()
        size = float(ev.get("size", 0.0))
        price = float(ev.get("price", 0.0))
        usdc = float(ev.get("usdcSize", size * price))
        meta = {
            "condition_id": str(ev.get("conditionId", "")),
            "title": str(ev.get("title", "")),
            "outcome": str(ev.get("outcome", "")),
        }
        if not asset or size <= 0 or price <= 0 or side not in ("BUY", "SELL"):
            return

        # --- update the shadow model of the source trader -------------------
        shadow = self.store.source_position(wallet, asset)
        prev_size = float(shadow["size"]) if shadow else 0.0
        if side == "BUY":
            new_size = prev_size + size
            prev_cost = (float(shadow["avg_price"]) * prev_size) if shadow else 0.0
            new_avg = (prev_cost + price * size) / new_size
            src_kind = "OPEN" if prev_size <= DUST else "ADD"
        else:
            new_size = max(0.0, prev_size - size)
            new_avg = float(shadow["avg_price"]) if shadow else price
            src_kind = "FULL_EXIT" if new_size <= DUST else "PARTIAL_EXIT"
        self.store.upsert_source_position(
            wallet, asset, meta["condition_id"], new_size, new_avg,
            meta["title"], meta["outcome"],
        )

        label = w["label"] or short(wallet)
        await self.notifier.send(
            "info", "trade_detected",
            f"{label} {side} {size:,.1f} '{meta['outcome']}' @ {price:.3f} — "
            f"{meta['title'][:70]} ({src_kind.replace('_', ' ').lower()})",
            {"wallet": wallet, "asset": asset, "side": side, "size": size, "price": price},
        )

        # --- mirror it -------------------------------------------------------
        if side == "BUY":
            await self.copy_buy(w, ev, key, usdc, price, asset, meta, src_kind)
        else:
            await self.copy_sell(w, ev, key, size, prev_size, price, asset, meta)

    async def copy_buy(self, w: dict[str, Any], ev: dict[str, Any], key: str,
                       src_usdc: float, src_price: float, asset: str,
                       meta: dict, src_kind: str) -> None:
        wallet = w["address"]

        # --- pre-trade gates: halt state, copy filters, portfolio limits ----
        skip = await self.buy_gate(src_price, meta)
        if skip:
            await self.notifier.send("info", "trade_skipped",
                                     f"Skipped buy ({meta['title'][:50]}) — {skip}",
                                     {"event": key})
            return

        equity, cash, _, _ = await self.portfolio_value(refresh_marks=False)
        acct = self.store.account()
        starting = float(acct.get("starting_balance", 0.0))
        ctx = SizingContext(
            equity=equity, available_cash=cash, source_trade_usdc=src_usdc,
            source_portfolio_value=await self.source_value(wallet),
            starting_balance=starting,
        )
        notional = target_notional(w["sizing_mode"], w["sizing_value"], w["multiplier"], ctx)
        cap = self.cfg.execution.max_trade_usd
        if cap:
            notional = min(notional, cap)
        if notional < self.cfg.execution.min_trade_usd:
            await self.notifier.send(
                "warning", "trade_skipped",
                f"Skipped copy of {short(wallet)} buy — sized at ${notional:.2f} "
                f"(below minimum / insufficient cash)",
                {"event": key},
            )
            return

        # Market exposure cap needs the sized notional, so it runs last.
        pc = self.cfg.portfolio
        cond = meta["condition_id"]
        if cond and equity > 0:
            exposure = sum(
                (p["mark_price"] if p["mark_price"] is not None else p["avg_price"]) * p["size"]
                for p in self.store.open_positions() if p["condition_id"] == cond
            )
            projected = 100.0 * (exposure + notional) / equity
            if projected > pc.max_single_market_pct:
                await self.notifier.send(
                    "info", "trade_skipped",
                    f"Skipped buy ({meta['title'][:50]}) — market exposure would reach "
                    f"{projected:.1f}% of equity (cap {pc.max_single_market_pct}%)",
                    {"event": key})
                return

        reference = await self.reference_price(asset, "BUY", src_price)
        try:
            fill = self.broker.buy(wallet, asset, notional, reference, meta)
        except InsufficientCash:
            await self.notifier.send("warning", "trade_skipped",
                                     f"Insufficient simulated cash for ${notional:.2f} buy")
            return

        self.store.record_trade({
            "event_key": key, "wallet": wallet, "asset": asset, **meta,
            "side": "BUY", "kind": fill.kind, "size": fill.size, "price": fill.price,
            "source_price": src_price, "fee": fill.fee, "notional": fill.notional,
            "realized_pnl": 0.0, "source_ts": int(ev.get("timestamp", 0)),
            "tx_hash": str(ev.get("transactionHash", "")),
        })
        await self.notifier.send(
            "info", "trade_copied",
            f"SIM {fill.kind}: bought {fill.size:,.1f} '{meta['outcome']}' @ {fill.price:.3f} "
            f"(${fill.notional:,.2f}) — {meta['title'][:60]} | cash ${fill.cash_after:,.2f}",
        )

    async def buy_gate(self, src_price: float, meta: dict) -> str | None:
        """Reasons NOT to open/add a position right now; None means clear.
        A halt only blocks new buys — exits keep mirroring so risk can
        still be taken off."""
        halted = self.store.get_meta("copy_halted")
        if halted:
            return f"copying halted ({halted}) — resume from the dashboard"
        cf = self.cfg.copy_filters
        if not cf.copy_buys:
            return "buy copying disabled (copy_filters.copy_buys)"
        if src_price > cf.max_entry_price:
            return f"entry {src_price:.2f} above max_entry_price {cf.max_entry_price}"
        if src_price < cf.min_entry_price:
            return f"entry {src_price:.2f} below min_entry_price {cf.min_entry_price}"
        if cf.ignore_markets_near_resolution and (src_price >= 0.95 or src_price <= 0.05):
            return f"market near resolution (entry {src_price:.2f})"
        if len(self.store.open_positions()) >= self.cfg.portfolio.max_open_positions:
            return f"portfolio at max_open_positions ({self.cfg.portfolio.max_open_positions})"
        cond = meta.get("condition_id", "")
        if cf.min_market_liquidity_usd > 0 and cond:
            try:
                ms = await self.gamma.market_stats(cond)
            except Exception:  # noqa: BLE001 — metadata must never block trading logic
                ms = {"liquidity": None}
            liq = ms.get("liquidity")
            if liq is not None and liq < cf.min_market_liquidity_usd:
                return (f"market liquidity ${liq:,.0f} below "
                        f"${cf.min_market_liquidity_usd:,.0f} floor")
        return None

    async def copy_sell(self, w: dict[str, Any], ev: dict[str, Any], key: str,
                        src_size: float, src_prev_size: float, src_price: float,
                        asset: str, meta: dict) -> None:
        """Mirror the *fraction* of the source position being sold."""
        wallet = w["address"]
        if not self.cfg.copy_filters.copy_sells:
            await self.notifier.send(
                "info", "trade_skipped",
                f"Sell of {meta['title'][:50]} not mirrored (copy_filters.copy_sells off)")
            return
        pos = self.store.open_position(wallet, asset)
        if pos is None:
            await self.notifier.send(
                "info", "trade_skipped",
                f"{short(wallet)} sold a position we never copied "
                f"({meta['title'][:50]}) — nothing to mirror",
            )
            return

        fraction = 1.0 if src_prev_size <= DUST else min(1.0, src_size / src_prev_size)
        sell_size = pos["size"] * fraction
        if sell_size <= DUST:
            return
        reference = await self.reference_price(asset, "SELL", src_price)
        try:
            fill = self.broker.sell(wallet, asset, sell_size, reference)
        except NoPosition:
            return

        self.store.record_trade({
            "event_key": key, "wallet": wallet, "asset": asset, **meta,
            "side": "SELL", "kind": fill.kind, "size": fill.size, "price": fill.price,
            "source_price": src_price, "fee": fill.fee, "notional": fill.notional,
            "realized_pnl": fill.realized_pnl, "source_ts": int(ev.get("timestamp", 0)),
            "tx_hash": str(ev.get("transactionHash", "")),
        })
        await self.notifier.send(
            "info", "trade_copied",
            f"SIM {fill.kind.replace('_', ' ')}: sold {fill.size:,.1f} '{meta['outcome']}' "
            f"@ {fill.price:.3f} (PnL {fill.realized_pnl:+,.2f}) — {meta['title'][:60]}",
        )

    async def handle_redeem(self, w: dict[str, Any], ev: dict[str, Any], key: str) -> None:
        """The source redeemed a resolved market. Settle our copy at the
        market's actual payout — $1 or $0 — never at an assumed win: the
        source redeeming tells us *they* held a winner, not which side we
        hold. When the payout can't be established the position is left
        open for settle_resolved_positions() to retry.

        Note this path only ever fires for markets a source bothered to
        redeem, i.e. their winners; the sweep is what settles everything
        else. See SettlementConfig for why that distinction matters."""
        wallet = w["address"]
        asset = str(ev.get("asset", ""))
        # Redeem events sometimes carry the conditionId but not the asset of
        # our side; settle every open copied position on that condition.
        condition = str(ev.get("conditionId", ""))
        targets = [p for p in self.store.open_positions()
                   if p["wallet"] == wallet and
                   (p["asset"] == asset or (condition and p["condition_id"] == condition))]
        if not targets:
            return
        for pos in targets:
            payout = await self.settlement_price(pos)
            if payout is None:
                # Unknown resolution is NOT a win. Leave the position open;
                # settle_resolved_positions() retries every sweep and will
                # flag it if it stays unresolvable.
                log.warning("redeem for %s: resolution unknown, leaving position %s open",
                            short(wallet), pos["id"])
                continue
            await self._settle_position(pos, payout, kind="REDEEM",
                                        event_key=f"{key}:{pos['asset'][:8]}",
                                        source_ts=int(ev.get("timestamp", 0)),
                                        tx_hash=str(ev.get("transactionHash", "")))
        # Clear the shadow entry too — the source really has redeemed, even
        # if we could not price our own copy yet.
        self.store.upsert_source_position(wallet, asset, condition, 0.0, 0.0, "", "")

    async def settlement_price(self, pos: dict[str, Any]) -> float | None:
        """Final payout for a resolved position: 1.0 (won), 0.0 (lost), or
        None when the market's resolution cannot be established.

        None is *not* a win. This used to end in `return 1.0`, reasoning
        that a source only redeems winners. That is true of the source, but
        it made every position we couldn't price book a full $1 payout —
        and because losing tokens are never redeemed at all, losers never
        even reached this code. Unknown now means "leave it open and flag
        it" (see SettlementConfig)."""
        return await self.gamma.resolution(pos["condition_id"], pos["asset"])

    async def _settle_position(self, pos: dict[str, Any], payout: float, *, kind: str,
                               event_key: str, source_ts: int, tx_hash: str = "") -> bool:
        """Book one resolved position at its real payout ($1 or $0).

        The idempotency key is claimed BEFORE the fill mutates portfolio
        state (CLAUDE.md exactly-once invariant), so a crash mid-settlement
        can at worst skip a position — which the next sweep re-settles —
        never double-book one. Returns True if a fill was recorded."""
        if not self.store.mark_processed(event_key, pos["wallet"], source_ts):
            return False
        try:
            fill = self.broker.sell(pos["wallet"], pos["asset"], pos["size"], payout,
                                    settlement=True)
        except NoPosition:
            return False
        self.store.record_trade({
            "event_key": event_key, "wallet": pos["wallet"], "asset": pos["asset"],
            "condition_id": pos["condition_id"], "title": pos["title"],
            "outcome": pos["outcome"], "side": "SELL", "kind": kind,
            "size": fill.size, "price": payout, "source_price": payout,
            "fee": 0.0, "notional": fill.notional, "realized_pnl": fill.realized_pnl,
            "source_ts": source_ts, "tx_hash": tx_hash,
        })
        verdict = "WON" if payout >= 0.5 else "LOST"
        await self.notifier.send(
            "info", "trade_copied",
            f"SIM {kind}: {verdict} — settled {fill.size:,.1f} '{pos['outcome']}' at "
            f"${payout:.2f} (PnL {fill.realized_pnl:+,.2f}) — {pos['title'][:60]}",
            {"wallet": pos["wallet"], "asset": pos["asset"]},
        )
        return True

    # ---------------------------------------------------------- settlement
    async def settle_resolved_positions(self) -> None:
        """Settle every open position whose market has resolved, at the real
        payout, independent of whether the source wallet redeemed.

        This is the counterweight to handle_redeem's structural bias: a
        source only ever redeems winners (losing outcome tokens are
        worthless, so nobody claims them), which meant losers stayed open
        forever, marked at their entry-era price because a resolved market
        returns no CLOB midpoint for mark_to_market to pick up. Winners
        booked $1, losers booked nothing, and the equity curve could only
        rise. Asking Gamma directly is the only way the book sees a loss.
        """
        sc = self.cfg.settlement
        if not sc.enabled:
            return
        settled = 0
        unresolved: list[dict[str, Any]] = []
        now = int(time.time())
        for pos in self.store.open_positions():
            payout = await self.settlement_price(pos)
            if payout is None:
                unresolved.append(pos)
                continue
            if await self._settle_position(
                pos, payout, kind="SETTLE",
                event_key=f"settle:{pos['id']}", source_ts=now,
            ):
                settled += 1
        if settled:
            log.info("settlement sweep: closed %d resolved position(s)", settled)
        await self._alert_unresolved(unresolved, now)

    async def _alert_unresolved(self, unresolved: list[dict[str, Any]], now: int) -> None:
        """Warn once per position that has stayed open well past the point
        where its market should have resolved. In practice this means the
        condition_id or outcome-token id isn't matching Gamma's payload —
        the exact failure that hid every loser before. Diagnose the live
        response shape with scripts/probe_resolution.py."""
        max_age = self.cfg.settlement.unresolved_alert_hours * 3600
        if max_age <= 0:
            return
        for pos in unresolved:
            pid = str(pos["id"])
            if pid in self._unresolved_notified:
                continue
            age = now - int(pos["opened_at"] or now)
            if age < max_age:
                continue
            self._unresolved_notified.add(pid)
            await self.notifier.send(
                "warning", "sync",
                f"Position open {age / 3600:.0f}h with no readable resolution — "
                f"'{pos['outcome']}' in {pos['title'][:60]}. If its market has ended, "
                f"settlement cannot price it (check condition_id/token matching with "
                f"scripts/probe_resolution.py); equity is carrying it at "
                f"${(pos['mark_price'] if pos['mark_price'] is not None else pos['avg_price']):.3f}.",
                {"wallet": pos["wallet"], "asset": pos["asset"], "position_id": pos["id"]},
            )

    # --------------------------------------------------------------- pricing
    async def reference_price(self, asset: str, side: str, source_price: float) -> float:
        if self.cfg.execution.price_mode == "market_quote":
            # Optional extra reaction delay before quoting: the quote fetched
            # after the sleep is genuinely staler relative to detection. The
            # poll interval still dominates total copy latency.
            delay_ms = self.cfg.execution.simulated_latency_ms
            if delay_ms > 0:
                if self.cfg.execution.randomize_latency and self.cfg.execution.latency_jitter_ms > 0:
                    delay_ms += random.uniform(-self.cfg.execution.latency_jitter_ms,
                                               self.cfg.execution.latency_jitter_ms)
                await asyncio.sleep(max(0.0, delay_ms) / 1000.0)
            quote = await self.clob.quote(asset, side)
            if quote and 0 < quote < 1:
                return quote
        return source_price

    async def source_value(self, wallet: str) -> float:
        cached = self._source_value_cache.get(wallet)
        if cached and time.time() - cached[1] < 300:
            return cached[0]
        try:
            value = await self.data.portfolio_value(wallet)
        except ApiError:
            value = cached[0] if cached else 0.0
        self._source_value_cache[wallet] = (value, time.time())
        return value

    # -------------------------------------------------------- marks & equity
    async def mark_to_market(self) -> None:
        positions = self.store.open_positions()
        await asyncio.gather(*(self._mark_one(pos) for pos in positions))
        equity, cash, unrealized, realized = await self.portfolio_value(refresh_marks=False)
        self.store.snapshot_equity(int(time.time()), equity, cash, unrealized, realized)

    async def _mark_one(self, pos: dict[str, Any]) -> None:
        mid = await self.clob.midpoint(pos["asset"])
        if mid is not None:
            self.store.set_mark(int(pos["id"]), mid)

    async def portfolio_value(self, refresh_marks: bool = False) -> tuple[float, float, float, float]:
        if refresh_marks:
            await self.mark_to_market()
        acct = self.store.account()
        cash = float(acct["cash"])
        unrealized = 0.0
        for pos in self.store.open_positions():
            mark = pos["mark_price"] if pos["mark_price"] is not None else pos["avg_price"]
            unrealized += (mark - pos["avg_price"]) * pos["size"]
        position_value = sum(
            (p["mark_price"] if p["mark_price"] is not None else p["avg_price"]) * p["size"]
            for p in self.store.open_positions()
        )
        realized = self.store.realized_pnl_total()
        return cash + position_value, cash, unrealized, realized

    # -------------------------------------------------------- position risk
    async def check_position_risk(self) -> None:
        """Auto-exit individual copied positions that have moved far enough
        in our favor (take-profit) or against us (stop-loss), independent
        of whether the source trader has sold — a per-position safety net
        on top of whatever the copied wallet does. Uses the mark set by the
        most recent mark_to_market() cycle; paper simulation only, no real
        funds move.

        Take-profit is measured as a fraction of the *remaining room to
        $1* (certainty), not a flat percentage of entry price. Outcome
        tokens are bounded in (0, 1): a position entered at 0.90 can never
        gain 50% of its entry price (the ceiling only leaves ~11% of
        possible upside), so a flat-percentage rule is structurally dead
        for expensive entries — silently leaving them with stop-loss
        protection but no profit-taking at all. Scaling by remaining room
        keeps the rule meaningful across the whole entry-price range."""
        pr = self.cfg.position_risk
        if not pr.enabled:
            return
        for pos in self.store.open_positions():
            avg = pos["avg_price"]
            if avg <= 0:
                continue
            mark = pos["mark_price"] if pos["mark_price"] is not None else avg
            room = 1.0 - avg
            target = avg + (pr.take_profit_room_pct / 100.0) * room if room > DUST else None
            loss_pct = 100.0 * (avg - mark) / avg
            if target is not None and mark >= target:
                room_captured_pct = 100.0 * (mark - avg) / room
                reason = (f"take-profit: captured {room_captured_pct:.0f}% of remaining room to $1 "
                          f"(mark {mark:.3f} >= target {target:.3f})")
            elif loss_pct >= pr.stop_loss_pct:
                reason = f"stop-loss -{loss_pct:.1f}% <= -{pr.stop_loss_pct:.1f}%"
            else:
                continue
            try:
                fill = self.broker.sell(pos["wallet"], pos["asset"], pos["size"], mark)
            except NoPosition:
                continue
            self.store.record_trade({
                "event_key": f"position-risk:{pos['id']}:{int(time.time())}",
                "wallet": pos["wallet"], "asset": pos["asset"],
                "condition_id": pos["condition_id"], "title": pos["title"],
                "outcome": pos["outcome"], "side": "SELL", "kind": "POSITION_RISK",
                "size": fill.size, "price": fill.price, "source_price": mark,
                "fee": fill.fee, "notional": fill.notional,
                "realized_pnl": fill.realized_pnl, "source_ts": int(time.time()),
                "tx_hash": "",
            })
            await self.notifier.send(
                "info", "risk",
                f"Auto-exit ({reason}): sold {fill.size:,.1f} '{pos['outcome']}' @ {fill.price:.3f} "
                f"(PnL {fill.realized_pnl:+,.2f}) — {pos['title'][:60]}",
                {"wallet": pos["wallet"], "asset": pos["asset"]},
            )

    async def close_position(self, position_id: int) -> dict[str, Any] | None:
        """Manually close one simulated position right now, at the current
        mark (or last-known mark/avg if no live quote), independent of the
        automatic take-profit/stop-loss thresholds. User-triggered only —
        returns the closed position row, or None if it wasn't open."""
        pos = self.store.position(position_id)
        if pos is None or pos["status"] != "open":
            return None
        mid = await self.clob.midpoint(pos["asset"])
        reference = mid if mid is not None else (
            pos["mark_price"] if pos["mark_price"] is not None else pos["avg_price"])
        try:
            fill = self.broker.sell(pos["wallet"], pos["asset"], pos["size"], reference)
        except NoPosition:
            return None
        self.store.record_trade({
            "event_key": f"manual-close:{pos['id']}:{int(time.time())}",
            "wallet": pos["wallet"], "asset": pos["asset"],
            "condition_id": pos["condition_id"], "title": pos["title"],
            "outcome": pos["outcome"], "side": "SELL", "kind": "MANUAL_CLOSE",
            "size": fill.size, "price": fill.price, "source_price": reference,
            "fee": fill.fee, "notional": fill.notional,
            "realized_pnl": fill.realized_pnl, "source_ts": int(time.time()),
            "tx_hash": "",
        })
        await self.notifier.send(
            "info", "trade_copied",
            f"SIM manual close: sold {fill.size:,.1f} '{pos['outcome']}' @ {fill.price:.3f} "
            f"(PnL {fill.realized_pnl:+,.2f}) — {pos['title'][:60]}",
            {"wallet": pos["wallet"], "asset": pos["asset"]},
        )
        return self.store.position(position_id)

    # ----------------------------------------------------------- reconcile
    async def reconcile_all(self) -> None:
        """Compare the shadow model with the authoritative /positions feed.

        Drift means we missed something (downtime, splits/merges, transfers).
        The shadow model is corrected in place; material drift is reported so
        it is visible on the dashboard. Simulated positions are *not* force-
        adjusted — the trade log stays an honest record of copied fills."""
        wallets = self.store.wallets(active_only=True)
        await asyncio.gather(*(self._reconcile_one(w) for w in wallets))
        await self.reconcile_own_live_account()

    async def reconcile_own_live_account(self) -> None:
        """Live-mode-only: compare Store's own bookkeeping (cash, open
        positions) against the real wallet. `PaperBroker` exposes neither
        `client` nor `funder_address`, so this is a no-op in paper mode by
        construction — plain attribute access via `getattr`, no import of
        `execution.py` or isinstance check needed here (see
        `broker.LiveBroker`).

        Unlike `_reconcile_one` above (which corrects the *source trader's*
        shadow model — that's a model of someone else's account, safe to
        overwrite from their own authoritative feed), this NEVER force-
        adjusts our own cash or positions. Per CLAUDE.md's shadow-model
        invariant, our own recorded state must stay an honest record of
        trades we actually executed — any drift here means something
        real happened (a missed fill, a crash mid-order, a resting order
        that filled late) and is surfaced loudly for a human to
        investigate, never silently patched."""
        client = getattr(self.broker, "client", None)
        if client is None:
            return

        try:
            msg = sync_live_cash(self.store, client)
        except Exception as err:  # noqa: BLE001 — a balance check must never crash the loop
            log.warning("live balance check failed: %s", err)
            msg = None
        if msg:
            level = "warning" if "initialised" in msg else "error"
            await self.notifier.send(level, "risk", msg)

        funder = getattr(self.broker, "funder_address", "") or ""
        if not funder:
            return
        try:
            actual = await self.data.positions(funder)
        except ApiError:
            return
        real_by_asset: dict[str, float] = {}
        for p in actual:
            asset = str(p.get("asset", ""))
            real_by_asset[asset] = real_by_asset.get(asset, 0.0) + float(p.get("size", 0.0) or 0.0)
        ours_by_asset: dict[str, float] = {}
        for p in self.store.open_positions():
            ours_by_asset[p["asset"]] = ours_by_asset.get(p["asset"], 0.0) + p["size"]

        drift = [
            f"{asset[:10]}…: book {ours_by_asset.get(asset, 0.0):,.1f} vs on-chain "
            f"{real_by_asset.get(asset, 0.0):,.1f}"
            for asset in set(real_by_asset) | set(ours_by_asset)
            if abs(real_by_asset.get(asset, 0.0) - ours_by_asset.get(asset, 0.0))
            > max(1.0, 0.02 * max(real_by_asset.get(asset, 0.0), ours_by_asset.get(asset, 0.0), 1.0))
        ]
        if drift:
            await self.notifier.send(
                "error", "risk",
                "LIVE POSITION DRIFT vs on-chain (NOT auto-corrected — investigate before "
                "trading further): " + "; ".join(drift[:5]),
            )

    async def _reconcile_one(self, w: dict[str, Any]) -> None:
        wallet = w["address"]
        try:
            actual = await self.data.positions(wallet)
        except ApiError:
            return
        actual_by_asset = {str(p.get("asset", "")): p for p in actual}
        shadow_by_asset = {p["asset"]: p for p in self.store.source_positions(wallet)}

        drift: list[str] = []
        for asset, p in actual_by_asset.items():
            size = float(p.get("size", 0.0))
            shadow = shadow_by_asset.get(asset)
            shadow_size = float(shadow["size"]) if shadow else 0.0
            if abs(size - shadow_size) > max(1.0, 0.02 * max(size, shadow_size)):
                drift.append(f"{p.get('title', asset)[:40]}: shadow {shadow_size:,.0f} vs actual {size:,.0f}")
            self.store.upsert_source_position(
                wallet, asset, str(p.get("conditionId", "")), size,
                float(p.get("avgPrice", 0.0)), str(p.get("title", "")),
                str(p.get("outcome", "")),
            )
        for asset, shadow in shadow_by_asset.items():
            if asset not in actual_by_asset:
                self.store.upsert_source_position(wallet, asset, "", 0.0, 0.0, "", "")

        if drift:
            await self.notifier.send(
                "warning", "sync",
                f"Shadow drift detected for {short(wallet)} (corrected): " + "; ".join(drift[:3]),
            )

    # ------------------------------------------------------- portfolio risk
    async def check_portfolio_risk(self) -> None:
        """Halt copying (new buys) when portfolio-level limits are breached:
        today's loss beyond `max_daily_loss_pct`, or peak-to-trough equity
        drawdown beyond `max_total_drawdown_pct`. These are deliberately
        independent rules with different reference points, not two views
        of one thing — see `PortfolioConfig`'s docstring for the worked
        example of how they interact.

        Every comparison here uses *trading-adjusted* equity (deposits and
        withdrawals backed out) — a withdrawal must never look like a
        trading loss and trigger a false halt, and a deposit must never
        mask a real one. `baseline`/`peak` from storage are already
        adjusted the same way (see Store.snapshot_equity).

        A manual resume is honored per rule, on fresh evidence only:
        the daily-loss rule won't re-fire the SAME day unless equity falls
        below the level at which the last halt fired, and the drawdown rule
        won't re-fire at all until equity falls below that level (resuming
        means the user accepted that drawdown). A new day's loss always
        counts as new evidence."""
        if self.store.get_meta("copy_halted"):
            return
        equity, _, _, _ = await self.portfolio_value(refresh_marks=False)
        pc = self.cfg.portfolio
        acct = self.store.account()
        starting = float(acct["starting_balance"])
        net_transfers = float(acct.get("total_deposits", 0.0)) - float(acct.get("total_withdrawals", 0.0))
        adjusted_equity = equity - net_transfers
        try:
            mark_raw = self.store.get_meta("halt_equity_mark")
            mark = float(mark_raw) if mark_raw else None
        except ValueError:
            mark = None
        today = int(time.time()) // 86_400
        halt_day_raw = self.store.get_meta("halt_day")
        halt_day = int(halt_day_raw) if halt_day_raw and halt_day_raw.isdigit() else None
        overridden_today = mark is not None and halt_day == today and adjusted_equity >= mark
        overridden_dd = mark is not None and adjusted_equity >= mark

        reason = None
        baseline = self.store.first_equity_since(today * 86_400)   # first snapshot since UTC midnight
        if baseline is not None and starting > 0 and not overridden_today:
            daily_loss_pct = 100.0 * (baseline - adjusted_equity) / starting
            if daily_loss_pct > pc.max_daily_loss_pct:
                reason = (f"daily loss {daily_loss_pct:.1f}% > "
                          f"{pc.max_daily_loss_pct}% of starting balance")

        peak = self.store.peak_equity()
        if (reason is None and pc.stop_copy_after_drawdown and peak and peak > 0
                and not overridden_dd):
            dd_pct = 100.0 * (peak - adjusted_equity) / peak
            if dd_pct > pc.max_total_drawdown_pct:
                reason = f"equity drawdown {dd_pct:.1f}% > {pc.max_total_drawdown_pct}% from peak"

        if reason:
            self.store.set_meta("copy_halted", f"auto: {reason}")
            self.store.set_meta("halt_equity_mark", str(adjusted_equity))
            self.store.set_meta("halt_day", str(today))
            await self.notifier.send(
                "warning", "risk",
                f"Copying HALTED — {reason}. Exits still mirror; new buys are blocked. "
                "Resume from the dashboard.",
                {"equity": equity},
            )

    # ------------------------------------------------------ auto-withdraw
    async def check_auto_withdraw(self) -> None:
        """Sweep a fixed increment of realized profit out of the tradeable
        cash balance every time cumulative *realized* PnL crosses another
        multiple of that increment — "take chips off the table" as the
        strategy actually banks money, without touching open positions or
        reacting to unrealized swings.

        The increment itself is either a flat dollar figure
        (`profit_increment_usd`) or a percentage of `starting_balance`
        (`profit_increment_pct`), per `auto_withdraw.mode` — recomputed
        from the account's *current* starting_balance on every call, so a
        percent-mode increment stays correctly scaled even if
        starting_balance is edited later.

        One-way and idempotent across restarts: `auto_withdraw_swept_usd`
        in `meta` records the cumulative amount already swept, so a crash
        between the withdraw and the meta update would at worst re-sweep
        the same increment once more — acceptable, and no worse than the
        existing halt-state persistence pattern — but a later trading loss
        never reverses a sweep already recorded, and the next sweep never
        fires early against a stale high-water mark. If realized profit
        has grown by more than one increment since the last check (e.g. a
        big redemption), every whole increment owed is swept in one
        withdrawal rather than one per cycle."""
        aw = self.cfg.auto_withdraw
        if not aw.enabled:
            return
        if aw.mode == "percent_starting_balance":
            starting = float(self.store.account()["starting_balance"])
            increment = starting * aw.profit_increment_pct / 100.0
        else:
            increment = aw.profit_increment_usd
        if increment <= 0:
            return
        realized = self.store.realized_pnl_total()
        swept = float(self.store.get_meta("auto_withdraw_swept_usd") or 0.0)
        # Float accumulation over many fills can land a hair under an exact
        # multiple (e.g. 499.9999999999999) — DUST absorbs that without
        # letting a real shortfall through.
        owed = realized - swept + DUST
        if owed < increment:
            return
        increments = int(owed // increment)
        amount = increments * increment
        if self.store.account()["cash"] < amount:
            # Profit is realized but currently tied up in new positions —
            # try again once cash frees up rather than partially sweep.
            return
        try:
            new_cash = self.store.withdraw(amount, "auto-profit-sweep")
        except InsufficientCash:
            return
        self.store.set_meta("auto_withdraw_swept_usd", str(swept + amount))
        await self.notifier.send(
            "info", "system",
            f"Auto-withdrew ${amount:,.2f} in realized profit (cumulative realized "
            f"${realized:,.2f}) — cash now ${new_cash:,.2f}",
            {"amount": amount, "realized_pnl": realized},
        )

    # ---------------------------------------------------------- auto-manage
    async def auto_manage(self) -> None:
        """Pause wallets whose *copied* performance breaches the configured
        risk thresholds. Judged on simulated results (what the copier
        actually captured), not the source's own fills.

        A pause records the wallet's closed-trade count; if the user
        manually resumes the wallet, it is only re-paused after NEW closed
        trades keep it in breach — auto-manage never fights a user override
        on stale evidence. Purely local (SQLite), no network."""
        amc = self.cfg.auto_manage
        starting = float(self.store.account()["starting_balance"])
        loss_cap = amc.pause_loss_pct / 100.0 * starting
        dd_cap = amc.pause_drawdown_pct / 100.0 * starting

        # Cool-off resume: an auto-paused wallet gets a second chance after
        # `resume_after_hours`; the closed-trade guard below still prevents
        # an instant re-pause on the same stale evidence.
        if amc.resume_after_hours > 0:
            for w in self.store.wallets():
                if w["active"]:
                    continue
                paused_at = self.store.get_meta(f"autopause_at:{w['address']}")
                if paused_at and time.time() - float(paused_at) >= amc.resume_after_hours * 3600:
                    self.store.update_wallet(w["address"], active=1)
                    self.store.set_meta(f"autopause_at:{w['address']}", "")
                    await self.notifier.send(
                        "info", "risk",
                        f"Auto-resumed {w['label'] or short(w['address'])} after "
                        f"{amc.resume_after_hours:.0f}h cool-off — re-pauses only on new losses",
                        {"wallet": w["address"]},
                    )

        for wm in wallet_metrics(self.store):
            if not wm["active"] or wm["closed_trades"] < amc.min_closed_trades:
                continue
            reasons = []
            pf = wm["profit_factor"]
            if pf is not None and pf < amc.pause_profit_factor:
                reasons.append(f"profit factor {pf:.2f} < {amc.pause_profit_factor}")
            if wm["realized_pnl"] < -loss_cap:
                reasons.append(
                    f"realized loss ${-wm['realized_pnl']:,.2f} > "
                    f"{amc.pause_loss_pct}% of starting balance")
            cum = peak = dd_usd = 0.0
            for t in self.store.trades_for_wallet(wm["wallet"]):
                cum += float(t["realized_pnl"])
                peak = max(peak, cum)
                dd_usd = max(dd_usd, peak - cum)
            if dd_usd > dd_cap:
                reasons.append(
                    f"realized-PnL drawdown ${dd_usd:,.2f} > "
                    f"{amc.pause_drawdown_pct}% of starting balance")
            if not reasons:
                continue
            guard = self.store.get_meta(f"autopause:{wm['wallet']}")
            if guard is not None and wm["closed_trades"] <= int(guard):
                continue  # user resumed; wait for new evidence before re-pausing
            self.store.update_wallet(wm["wallet"], active=0)
            self.store.set_meta(f"autopause:{wm['wallet']}", str(wm["closed_trades"]))
            self.store.set_meta(f"autopause_at:{wm['wallet']}", str(int(time.time())))
            await self.notifier.send(
                "warning", "risk",
                f"Auto-paused {wm['label'] or short(wm['wallet'])} — " + "; ".join(reasons) +
                f" ({wm['closed_trades']} closed copies). Open positions kept; "
                "resume from the dashboard to keep copying.",
                {"wallet": wm["wallet"], "closed_trades": wm["closed_trades"]},
            )


def short(wallet: str) -> str:
    return wallet[:6] + "…" + wallet[-4:] if len(wallet) > 12 else wallet
