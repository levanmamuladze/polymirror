from __future__ import annotations

import asyncio
import json

import pytest

from polymirror.clients import parse_resolution
from polymirror.config import Config
from polymirror.notify import Notifier
from polymirror.storage import Store
from polymirror.tracker import Engine


class StubDataApi:
    """Scriptable replacement for DataApiClient."""

    def __init__(self):
        self.activity_events: list[dict] = []
        self.positions_by_wallet: dict[str, list[dict]] = {}
        self.values: dict[str, float] = {}
        self.activity_calls = 0

    async def activity_since(self, wallet, start, types="TRADE,REDEEM"):
        self.activity_calls += 1
        return [e for e in self.activity_events
                if e.get("proxyWallet", wallet) == wallet and int(e["timestamp"]) >= start]

    async def positions(self, wallet):
        return self.positions_by_wallet.get(wallet, [])

    async def portfolio_value(self, wallet):
        return self.values.get(wallet, 0.0)


class StubLeaderboard:
    """Scriptable replacement for LeaderboardClient."""

    def __init__(self):
        self.rows: list[dict] = []       # {"wallet","name","amount"}
        self.calls = 0
        self.categories_called: list[str] = []

    async def top_wallets(self, window="1m", limit=50, rank_type="pnl", category="OVERALL"):
        self.calls += 1
        self.categories_called.append(category)
        return self.rows[:limit]


class StubClob:
    def __init__(self):
        self.mids: dict[str, float] = {}
        self.quotes: dict[tuple[str, str], float] = {}

    async def midpoint(self, token_id):
        return self.mids.get(token_id)

    async def quote(self, token_id, side):
        return self.quotes.get((token_id, side.upper()))


class StubGamma:
    def __init__(self):
        self.markets: dict[str, dict] = {}       # condition_id -> gamma market payload
        self.stats: dict[str, dict] = {}         # condition_id -> {"volume","liquidity"}
        self.categories: dict[str, str] = {}     # condition_id -> category name

    async def market_by_condition(self, condition_id, *, refresh=False):
        return self.markets.get(condition_id)

    async def market_stats(self, condition_id):
        return self.stats.get(condition_id, {"volume": None, "liquidity": None})

    async def market_category(self, condition_id):
        return self.categories.get(condition_id)

    async def resolution(self, condition_id, token_id):
        """Deliberately delegates to the production parser rather than
        reimplementing it: the stub stores real-shaped Gamma payloads, so
        it cannot drift from what GammaClient actually accepts as
        'resolved'. Tests script outcomes with `resolved_market()`."""
        return parse_resolution(self.markets.get(condition_id), token_id)


def resolved_market(token_ids: list[str], winner_index: int | None, *,
                    closed: bool = True, as_json_strings: bool = True) -> dict:
    """Build a Gamma market payload in the shape the live API returns.

    `winner_index=None` produces a still-trading market (indecisive prices),
    which must never settle. `as_json_strings` mirrors Gamma's habit of
    returning these arrays as JSON *strings* rather than real arrays.
    """
    if winner_index is None:
        prices = ["0.62", "0.38"][:len(token_ids)]
    else:
        prices = ["1" if i == winner_index else "0" for i in range(len(token_ids))]
    if as_json_strings:
        return {"closed": closed, "clobTokenIds": json.dumps(token_ids),
                "outcomePrices": json.dumps(prices)}
    return {"closed": closed, "clobTokenIds": token_ids, "outcomePrices": prices}


class SilentNotifier(Notifier):
    """Logs to the store but never touches the network."""

    def __init__(self, store):
        self.store = store
        self.sent: list[tuple[str, str, str]] = []

    async def send(self, level, kind, message, data=None):
        self.store.log_event(level, kind, message, data)
        self.sent.append((level, kind, message))


@pytest.fixture
def cfg():
    c = Config()
    c.starting_balance = 10_000.0
    c.execution.slippage_bps = 0.0
    c.execution.fee_bps = 0.0
    c.db_path = ":memory:"
    return c


@pytest.fixture
def store(cfg):
    s = Store(":memory:")
    s.init_account(cfg.starting_balance)
    yield s
    s.close()


@pytest.fixture
def stubs():
    return StubDataApi(), StubClob(), StubGamma()


@pytest.fixture
def engine(cfg, store, stubs):
    data, clob, gamma = stubs
    notifier = SilentNotifier(store)
    return Engine(cfg, store, data, clob, gamma, notifier)


@pytest.fixture
def scout(cfg, store, stubs):
    from polymirror.scout import WalletScout
    data, _, gamma = stubs
    return WalletScout(cfg, store, data, StubLeaderboard(), SilentNotifier(store),
                       gamma=gamma)


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


TRADE_DEFAULTS = dict(
    type="TRADE", conditionId="0xc0nd", title="Test market?", outcome="Yes",
    transactionHash="0xabc", asset="111", outcomeIndex=0,
)

# When a wallet is first followed, the engine sets its cursor to "now" so
# only *new* trades are copied. Test events therefore anchor to T0 = now.
import time  # noqa: E402

T0 = int(time.time())


def trade(wallet, side, size, price, ts, tx="0xabc", asset="111", **kw):
    ev = dict(TRADE_DEFAULTS)
    ev.update(proxyWallet=wallet, side=side, size=size, price=price,
              usdcSize=size * price, timestamp=T0 + ts, transactionHash=tx, asset=asset)
    ev.update(kw)
    return ev
