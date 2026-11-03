"""Read-only clients for Polymarket's public APIs.

Platform analysis (why these endpoints):

  Data API   https://data-api.polymarket.com   (no auth)
      GET /activity?user=&type=TRADE,REDEEM  -> per-wallet trade & redemption
          stream with transactionHash, asset (outcome token id), conditionId,
          side, size, price, usdcSize, timestamp. This is the primary feed.
      GET /positions?user=                   -> authoritative current holdings,
          used to seed the shadow model and to reconcile drift.
      GET /value?user=                       -> total open-position value,
          used for proportional sizing.

  CLOB API   https://clob.polymarket.com      (read endpoints are public)
      GET /midpoint?token_id=                -> mark-to-market price.
      GET /price?token_id=&side=             -> best bid/ask, used when
          price_mode = market_quote to fill at a realistic quote.

  Gamma API  https://gamma-api.polymarket.com (no auth)
      Market metadata / resolution status when needed.

  Leaderboard  https://data-api.polymarket.com  (no auth)
      GET /v1/leaderboard?category=&timePeriod=&orderBy=&limit=&offset= -> top
          wallets by PnL/volume, used by the wallet scout as a candidate
          source for auto-discovery. Officially documented at
          docs.polymarket.com/api-reference/core/get-trader-leaderboard-rankings
          (the old lb-api.polymarket.com/leaderboard host+path is retired and
          now 404s). category is an enum (OVERALL, POLITICS, SPORTS, ...);
          timePeriod is DAY|WEEK|MONTH|ALL; orderBy is PNL|VOL; limit is
          capped at 50 and offset at 1000 server-side.

Alternatives considered:
  * CLOB user websocket: real-time, but authenticated — it only streams the
    credential owner's own orders, so it cannot observe third-party wallets.
  * On-chain / subgraph (Goldsky, OrderFilled events): fully verifiable but
    requires decoding ERC-1155 fills and maintaining indexer infra; the Data
    API already exposes the same fills in normalized form.
  Polling the Data API is therefore the most reliable *official* method for
  observing another wallet's activity. The trade-off is latency: detection
  lag = poll interval + indexing delay (typically seconds).

Safety: every method in this module issues HTTP GETs only. There is no
order-placement, signing, or key-handling code anywhere in the project.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any

import httpx

log = logging.getLogger("polymirror.clients")

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}

# How close a resolved outcome price must be to $1 / $0 before we accept it
# as a final payout. Resolved markets report exactly "1"/"0"; anything in
# between means "still trading" or "disputed" and must NOT be settled — a
# market quoting 0.98 is a near-certainty, not a resolution.
RESOLVED_TOLERANCE = 0.01


class RateLimiter:
    """Simple client-side token pacing shared across all API calls."""

    def __init__(self, max_per_second: float):
        self._interval = 1.0 / max(max_per_second, 0.1)
        self._next_at = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_at:
                await asyncio.sleep(self._next_at - now)
            self._next_at = max(now, self._next_at) + self._interval


class ApiError(Exception):
    pass


class BaseClient:
    def __init__(self, base_url: str, limiter: RateLimiter, timeout: float = 15.0,
                 max_retries: int = 5):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={"User-Agent": "polymirror/1.0 (paper-trading simulator; read-only)"},
        )
        self._limiter = limiter
        self._max_retries = max_retries

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        last_err: Exception | None = None
        for attempt in range(self._max_retries):
            await self._limiter.wait()
            try:
                resp = await self._client.get(path, params=params)
                if resp.status_code in RETRYABLE_STATUS:
                    raise ApiError(f"HTTP {resp.status_code} on {path}")
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as err:
                # Non-retryable status (not in RETRYABLE_STATUS) — e.g. 404/400/403.
                # Retrying won't help, but callers throughout the codebase only
                # catch ApiError, so this must never escape as a raw httpx error.
                raise ApiError(f"HTTP {err.response.status_code} on {path}: {err}") from err
            except (httpx.TransportError, httpx.TimeoutException, ApiError) as err:
                last_err = err
                # Exponential backoff with jitter: 0.5s, 1s, 2s, 4s, 8s (±25%)
                delay = min(0.5 * 2 ** attempt, 10.0) * random.uniform(0.75, 1.25)
                log.warning("GET %s failed (%s); retry %d/%d in %.1fs",
                            path, err, attempt + 1, self._max_retries, delay)
                await asyncio.sleep(delay)
        raise ApiError(f"GET {path} failed after {self._max_retries} retries: {last_err}")

    async def aclose(self) -> None:
        await self._client.aclose()


class DataApiClient(BaseClient):
    """data-api.polymarket.com — per-user public activity."""

    async def activity(self, wallet: str, start: int = 0, types: str = "TRADE,REDEEM",
                       limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "user": wallet, "type": types, "limit": limit, "offset": offset,
            "sortBy": "TIMESTAMP", "sortDirection": "ASC",
        }
        if start > 0:
            params["start"] = start
        data = await self.get_json("/activity", params)
        return data if isinstance(data, list) else []

    async def activity_since(self, wallet: str, start: int,
                             types: str = "TRADE,REDEEM") -> list[dict[str, Any]]:
        """Fetch all activity since `start`, paginating until exhausted.

        Bounded to 11 pages (~5.5k fills) per call to keep one poll/eval
        cycle finite; hyperactive wallets get the earliest slice now and
        the rest over subsequent cycles (the cursor only advances past
        what was actually processed, so nothing is skipped). If the
        upstream API rejects a page outright — some deployments cap total
        offset below what we'd otherwise request — that's treated as "no
        more data available" rather than failing everything already
        fetched this call.
        """
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            try:
                page = await self.activity(wallet, start=start, types=types, offset=offset)
            except ApiError as err:
                log.warning("activity pagination stopped for %s at offset %d: %s",
                            wallet, offset, err)
                break
            out.extend(page)
            if len(page) < 500 or offset >= 5000:  # hard stop to bound one cycle
                break
            offset += 500
        return out

    async def positions(self, wallet: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = await self.get_json(
                "/positions", {"user": wallet, "limit": 500, "offset": offset}
            )
            page = page if isinstance(page, list) else []
            out.extend(page)
            if len(page) < 500:
                break
            offset += 500
        return out

    async def portfolio_value(self, wallet: str) -> float:
        data = await self.get_json("/value", {"user": wallet})
        # Response shape: [{"user": "0x..", "value": 1234.5}] or {"value": ...}
        if isinstance(data, list) and data:
            return float(data[0].get("value", 0.0))
        if isinstance(data, dict):
            return float(data.get("value", 0.0))
        return 0.0


WINDOW_TO_TIME_PERIOD = {"1d": "DAY", "1w": "WEEK", "1m": "MONTH", "all": "ALL"}


class LeaderboardClient(BaseClient):
    """data-api.polymarket.com/v1/leaderboard — public profit/volume leaderboard.

    Used by the wallet scout as a candidate source. Officially documented
    (see module docstring above) but response field names aren't part of
    that contract, so parsing stays defensive: the wallet address and PnL
    amount are looked up under several known key names and rows that don't
    yield a plausible address are dropped. Verify the live shape with
    `python scripts/probe_leaderboard.py`.
    """

    async def top_wallets(self, window: str = "1m", limit: int = 50,
                          rank_type: str = "pnl", category: str = "OVERALL"
                          ) -> list[dict[str, Any]]:
        time_period = WINDOW_TO_TIME_PERIOD.get(window, "ALL")
        order_by = "VOL" if rank_type.lower() == "volume" else "PNL"
        data = await self.get_json(
            "/v1/leaderboard",
            {"category": category, "timePeriod": time_period, "orderBy": order_by,
             "limit": min(max(limit, 1), 50)},
        )
        if isinstance(data, dict):
            data = data.get("leaderboard") or data.get("data") or data.get("results") or []
        if not isinstance(data, list):
            return []
        out: list[dict[str, Any]] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            addr = row.get("proxyWallet") or row.get("wallet") or row.get("user") \
                or row.get("address") or ""
            addr = str(addr).lower()
            if not (addr.startswith("0x") and len(addr) == 42):
                continue
            try:
                amount = float(row.get("pnl") if row.get("pnl") is not None
                               else row.get("amount") or row.get("value") or 0.0)
            except (TypeError, ValueError):
                amount = 0.0
            out.append({
                "wallet": addr,
                "name": str(row.get("userName") or row.get("name") or row.get("pseudonym") or ""),
                "amount": amount,
            })
        return out


class ClobClient(BaseClient):
    """clob.polymarket.com — public read-only price endpoints."""

    async def midpoint(self, token_id: str) -> float | None:
        try:
            data = await self.get_json("/midpoint", {"token_id": token_id})
            mid = data.get("mid") if isinstance(data, dict) else None
            return float(mid) if mid is not None else None
        except (ApiError, ValueError, TypeError):
            return None

    async def quote(self, token_id: str, side: str) -> float | None:
        """Best price to trade `side` (BUY hits the ask, SELL hits the bid)."""
        try:
            data = await self.get_json("/price", {"token_id": token_id, "side": side.upper()})
            price = data.get("price") if isinstance(data, dict) else None
            return float(price) if price is not None else None
        except (ApiError, ValueError, TypeError):
            return None


def _as_list(value: Any) -> list[Any]:
    """Gamma returns several array-ish fields (`outcomes`, `outcomePrices`,
    `clobTokenIds`) sometimes as real JSON arrays and sometimes as JSON
    *strings* containing an array, depending on endpoint and deployment.
    Normalize both; anything else is an empty list (unknown, never guessed)."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _market_is_closed(market: dict[str, Any] | None) -> bool:
    """Whether Gamma considers this market finished trading. Several flags
    are checked because the schema isn't a documented stable contract (same
    defensive posture as market_category); resolution still additionally
    requires decisive 0/1 outcome prices before anything is settled."""
    if not market:
        return False
    if market.get("closed") is True or market.get("archived") is True:
        return True
    status = str(market.get("umaResolutionStatus") or "").lower()
    return status == "resolved"


def parse_resolution(market: dict[str, Any] | None, token_id: str) -> float | None:
    """Final payout for one outcome token given a Gamma market payload:
    1.0 (won), 0.0 (lost), or None when the market has not resolved or the
    payout cannot be read.

    Pure and payload-shaped on purpose: test doubles parse real Gamma
    payloads through this same function instead of reimplementing the rules,
    so a stub cannot quietly disagree with production about what "resolved"
    means.

    None means "don't know" and must never be treated as a win — that
    conflation is what made every unsettled loser invisible in the paper
    book. Two independent conditions are both required:

      1. Gamma reports the market closed/resolved (`_market_is_closed`).
      2. The outcome price for *our* token is decisively 0 or 1 within
         RESOLVED_TOLERANCE.

    The token is located by index in `clobTokenIds`; if our token isn't in
    that list the result is None rather than a guess — matching the wrong
    index would settle a position at the *opposite* outcome's payout, which
    is worse than not settling at all.
    """
    if not token_id or not _market_is_closed(market):
        return None

    token_ids = [str(t) for t in _as_list((market or {}).get("clobTokenIds"))]
    prices = _as_list((market or {}).get("outcomePrices"))
    if not token_ids or len(prices) != len(token_ids):
        return None
    try:
        idx = token_ids.index(str(token_id))
        payout = float(prices[idx])
    except (ValueError, TypeError):
        return None

    if payout >= 1.0 - RESOLVED_TOLERANCE:
        return 1.0
    if payout <= RESOLVED_TOLERANCE:
        return 0.0
    return None  # closed but not decisively priced (disputed / mid-resolution)


class GammaClient(BaseClient):
    """gamma-api.polymarket.com — market metadata (optional enrichment).

    Lookups are cached for an hour: market volume/liquidity move slowly and
    the same conditions recur across candidates and copied trades."""

    _CACHE_TTL = 3600.0

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._market_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}

    async def market_by_condition(self, condition_id: str, *,
                                  refresh: bool = False) -> dict[str, Any] | None:
        """`refresh=True` bypasses the hour-long TTL — used by the settlement
        sweep, which would otherwise not notice a resolution for up to an
        hour. A market that has already resolved can never un-resolve, so a
        cached *closed* market is still served from cache even when
        refreshing; only still-open markets cost a request."""
        cached = self._market_cache.get(condition_id)
        if cached and _market_is_closed(cached[1]):
            return cached[1]
        if cached and not refresh and time.monotonic() - cached[0] < self._CACHE_TTL:
            return cached[1]
        market: dict[str, Any] | None = None
        try:
            data = await self.get_json("/markets", {"condition_ids": condition_id})
            if isinstance(data, list) and data:
                market = data[0]
        except ApiError:
            return cached[1] if cached else None  # stale beats nothing; don't cache failures
        self._market_cache[condition_id] = (time.monotonic(), market)
        return market

    async def resolution(self, condition_id: str, token_id: str) -> float | None:
        """Final payout for one outcome token — see `parse_resolution` for
        the rules. Returns None (never a guess) when unresolvable."""
        if not condition_id or not token_id:
            return None
        market = await self.market_by_condition(condition_id, refresh=True)
        return parse_resolution(market, token_id)

    async def market_stats(self, condition_id: str) -> dict[str, float | None]:
        """Best-effort volume/liquidity for a market. Gamma's field names have
        shifted over time, so several are tried; None means unknown."""
        market = await self.market_by_condition(condition_id) if condition_id else None

        def pick(*keys: str) -> float | None:
            for k in keys:
                v = (market or {}).get(k)
                if v is not None:
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        continue
            return None

        return {
            "volume": pick("volumeNum", "volume", "volume24hr"),
            "liquidity": pick("liquidityNum", "liquidity"),
        }

    async def market_category(self, condition_id: str) -> str | None:
        """Best-effort market category/vertical (e.g. "Politics", "Sports",
        "Crypto") for the wallet scout's category-aware diversification
        (see scout.py). Gamma's schema for this isn't a documented stable
        contract and has shifted across deployments — several plausible
        shapes are tried defensively, same posture as market_stats():
        unknown is None, never guessed, and never treated as a failure.
        Verify the live shape with `scripts/probe_market_category.py` if
        categorization ever looks wrong (empty, or everything landing in
        one bucket)."""
        market = await self.market_by_condition(condition_id) if condition_id else None
        if not market:
            return None
        cat = market.get("category")
        if cat:
            return str(cat)
        events = market.get("events")
        if isinstance(events, list) and events and isinstance(events[0], dict):
            ev_cat = events[0].get("category")
            if ev_cat:
                return str(ev_cat)
            tags = events[0].get("tags")
            if isinstance(tags, list) and tags:
                first = tags[0]
                if isinstance(first, dict):
                    name = first.get("label") or first.get("name")
                    if name:
                        return str(name)
                elif isinstance(first, str):
                    return first
        return None


def build_clients(cfg) -> tuple[DataApiClient, ClobClient, GammaClient, LeaderboardClient]:
    limiter = RateLimiter(cfg.max_requests_per_second)
    return (
        DataApiClient(cfg.data_api_base, limiter, cfg.request_timeout_seconds),
        ClobClient(cfg.clob_api_base, limiter, cfg.request_timeout_seconds),
        GammaClient(cfg.gamma_api_base, limiter, cfg.request_timeout_seconds),
        LeaderboardClient(cfg.scout.leaderboard_base, limiter, cfg.request_timeout_seconds),
    )
