import asyncio

import pytest

from polymirror.broker import LiveBroker, NoPosition, sync_live_cash
from polymirror.config import Config
from polymirror.execution import (
    LiveExecutionError,
    OrderResult,
    PolymarketExecutionClient,
    build_execution_client,
)
from polymirror.storage import Store


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

META = {"condition_id": "0xc", "title": "T?", "outcome": "Yes"}
W = "0x" + "1" * 40


# --------------------------------------------------------------------- config
def test_paper_mode_needs_no_live_gates():
    cfg = Config()
    cfg.validate()  # must not raise — paper is the safe default


def test_live_mode_refused_without_confirm():
    cfg = Config()
    cfg.execution.mode = "live"
    cfg.live.max_live_notional_usd = 50.0
    with pytest.raises(ValueError, match="confirm"):
        cfg.validate()


def test_live_mode_refused_without_notional_cap():
    cfg = Config()
    cfg.execution.mode = "live"
    cfg.live.confirm = True
    with pytest.raises(ValueError, match="max_live_notional_usd"):
        cfg.validate()


def test_live_mode_refused_with_bad_signature_type():
    cfg = Config()
    cfg.execution.mode = "live"
    cfg.live.confirm = True
    cfg.live.max_live_notional_usd = 50.0
    cfg.live.signature_type = 7
    with pytest.raises(ValueError, match="signature_type"):
        cfg.validate()


def test_live_mode_passes_validation_when_fully_configured():
    cfg = Config()
    cfg.execution.mode = "live"
    cfg.live.confirm = True
    cfg.live.max_live_notional_usd = 50.0
    cfg.validate()  # must not raise


def test_default_order_type_is_fok_not_gtc():
    """FOK either fills completely now or does nothing — no resting/partial
    fill for reconciliation to miss. GTC/GTD are supported but must be an
    explicit, informed choice, not the silent default."""
    assert Config().live.order_type == "FOK"


def test_bad_order_type_rejected():
    cfg = Config()
    cfg.execution.mode = "live"
    cfg.live.confirm = True
    cfg.live.max_live_notional_usd = 50.0
    cfg.live.order_type = "MARKET"
    with pytest.raises(ValueError, match="order_type"):
        cfg.validate()


def test_bad_execution_mode_rejected():
    cfg = Config()
    cfg.execution.mode = "turbo"
    with pytest.raises(ValueError, match="execution.mode"):
        cfg.validate()


# ---------------------------------------------------------- execution client
def test_build_execution_client_refuses_when_mode_is_paper():
    cfg = Config()
    with pytest.raises(LiveExecutionError, match="execution.mode"):
        build_execution_client(cfg)


class FakeClobClient:
    """Stand-in for py_clob_client's ClobClient — enough surface for
    PolymarketExecutionClient to drive without the real (optional,
    uninstalled-in-CI) dependency."""

    def __init__(self):
        self.orders: list[dict] = []
        self.balance = 12_345.0

    def get_balance_allowance(self):
        return {"balance": self.balance * 1_000_000}  # 6-decimal USDC units

    def create_order(self, args):
        return {"args": args}

    def post_order(self, signed, order_type):
        self.orders.append({"signed": signed, "order_type": order_type})
        return {"success": True, "orderID": "abc123", "status": "matched",
                "price": signed["args"].price, "makingAmount": signed["args"].size}


def _live_cfg(**overrides):
    cfg = Config()
    cfg.live.dry_run = overrides.pop("dry_run", True)
    for k, v in overrides.items():
        setattr(cfg.live, k, v)
    return cfg.live


def test_dry_run_never_calls_the_real_client():
    fake = FakeClobClient()
    client = PolymarketExecutionClient(_live_cfg(dry_run=True), "https://clob.example", fake)
    result = client.submit_order(token_id="tok", side="BUY", size=100, price=0.5, order_type="GTC")
    assert result.status == "dry_run"
    assert result.filled_size == pytest.approx(100)
    assert result.avg_price == pytest.approx(0.5)
    assert fake.orders == []  # nothing was actually submitted


def test_live_submit_order_uses_injected_client():
    fake = FakeClobClient()
    client = PolymarketExecutionClient(_live_cfg(dry_run=False), "https://clob.example", fake)
    result = client.submit_order(token_id="tok", side="BUY", size=100, price=0.55, order_type="GTC")
    assert result.status == "matched"
    assert result.order_id == "abc123"
    assert len(fake.orders) == 1


def test_get_usdc_balance_converts_from_6_decimals():
    fake = FakeClobClient()
    client = PolymarketExecutionClient(_live_cfg(), "https://clob.example", fake)
    assert client.get_usdc_balance() == pytest.approx(12_345.0)


def test_submit_order_rejects_nonpositive_size_or_price():
    fake = FakeClobClient()
    client = PolymarketExecutionClient(_live_cfg(dry_run=False), "https://clob.example", fake)
    with pytest.raises(LiveExecutionError):
        client.submit_order(token_id="tok", side="BUY", size=0, price=0.5, order_type="GTC")


class RejectingClobClient(FakeClobClient):
    def post_order(self, signed, order_type):
        return {"success": False, "error": "insufficient allowance"}


def test_rejected_order_raises_live_execution_error():
    fake = RejectingClobClient()
    client = PolymarketExecutionClient(_live_cfg(dry_run=False), "https://clob.example", fake)
    with pytest.raises(LiveExecutionError, match="rejected"):
        client.submit_order(token_id="tok", side="BUY", size=100, price=0.5, order_type="GTC")


# --------------------------------------------------------------- live broker
class ScriptableExecutionClient:
    """Directly implements the ExecutionClient protocol for LiveBroker
    tests — no py-clob-client involved at all."""

    def __init__(self, fills: list[OrderResult]):
        self._fills = list(fills)
        self.calls: list[dict] = []

    def submit_order(self, *, token_id, side, size, price, order_type):
        self.calls.append({"token_id": token_id, "side": side, "size": size,
                           "price": price, "order_type": order_type})
        return self._fills.pop(0)

    def get_usdc_balance(self):
        return 0.0


def test_live_broker_buy_books_actual_reported_fill(store):
    fill = OrderResult(order_id="1", status="matched", side="BUY", filled_size=980.0,
                       avg_price=0.51, fee=1.5, notional=499.8, raw={})
    client = ScriptableExecutionClient([fill])
    broker = LiveBroker(store, client, max_live_notional_usd=1_000.0)
    result = broker.buy(W, "tok", notional=500, reference_price=0.50, meta=META)

    assert result.kind == "OPEN"
    assert result.size == pytest.approx(980.0)
    assert result.price == pytest.approx(0.51)
    pos = store.open_position(W, "tok")
    assert pos["size"] == pytest.approx(980.0)
    assert pos["avg_price"] == pytest.approx(0.51)
    # cash debited by the REAL notional+fee, not the naive $500 requested
    assert store.account()["cash"] == pytest.approx(10_000 - 499.8 - 1.5)


def test_live_broker_enforces_max_notional_cap(store):
    client = ScriptableExecutionClient([])
    broker = LiveBroker(store, client, max_live_notional_usd=100.0)
    with pytest.raises(LiveExecutionError, match="cap"):
        broker.buy(W, "tok", notional=500, reference_price=0.50, meta=META)
    assert client.calls == []  # never reached the exchange


def test_live_broker_zero_cap_blocks_everything(store):
    client = ScriptableExecutionClient([])
    broker = LiveBroker(store, client, max_live_notional_usd=0.0)
    with pytest.raises(LiveExecutionError):
        broker.buy(W, "tok", notional=1, reference_price=0.50, meta=META)


def test_live_broker_sell_realizes_pnl_from_actual_fill(store):
    buy_fill = OrderResult(order_id="1", status="matched", side="BUY", filled_size=1000.0,
                           avg_price=0.50, fee=0.0, notional=500.0, raw={})
    sell_fill = OrderResult(order_id="2", status="matched", side="SELL", filled_size=1000.0,
                            avg_price=0.70, fee=2.0, notional=700.0, raw={})
    client = ScriptableExecutionClient([buy_fill, sell_fill])
    broker = LiveBroker(store, client, max_live_notional_usd=1_000.0)
    broker.buy(W, "tok", notional=500, reference_price=0.50, meta=META)
    result = broker.sell(W, "tok", size=1000, reference_price=0.70)

    assert result.kind == "FULL_EXIT"
    # Fill.realized_pnl is pre-fee (matching PaperBroker's convention — the
    # stored position's realized_pnl is the fee-adjusted figure).
    assert result.realized_pnl == pytest.approx((0.70 - 0.50) * 1000)
    assert store.open_position(W, "tok") is None


# ------------------------------------------------------------- live cash sync
class FakeLiveClient:
    """Minimal ExecutionClient stand-in exposing only get_usdc_balance() —
    enough for sync_live_cash / reconcile_own_live_account tests."""

    def __init__(self, balance: float):
        self.balance = balance

    def get_usdc_balance(self) -> float:
        return self.balance


class FakeLiveBrokerStub:
    """Duck-typed stand-in for LiveBroker: exposes exactly the two
    attributes tracker.Engine.reconcile_own_live_account looks for via
    getattr, nothing else — confirms the check is genuinely broker-agnostic
    rather than accidentally depending on real LiveBroker internals."""

    def __init__(self, client, funder_address: str = "0x" + "f" * 40):
        self.client = client
        self.funder_address = funder_address


def test_sync_live_cash_bootstraps_fresh_account(store):
    client = FakeLiveClient(2_500.0)
    msg = sync_live_cash(store, client)
    assert msg is not None and "initialised" in msg
    acct = store.account()
    assert acct["cash"] == pytest.approx(2_500.0)
    assert acct["starting_balance"] == pytest.approx(2_500.0)
    assert acct["total_deposits"] == 0.0 and acct["total_withdrawals"] == 0.0
    assert store.get_meta("live_cash_synced_at") is not None


def test_sync_live_cash_overrides_stale_paper_balance(store):
    # `store` fixture already ran init_account(10_000) — simulating a DB
    # previously used in paper mode. First live sync must still win.
    assert store.account()["cash"] == pytest.approx(10_000.0)
    sync_live_cash(store, FakeLiveClient(777.0))
    assert store.account()["cash"] == pytest.approx(777.0)


def test_sync_live_cash_reports_drift_without_correcting(store):
    client = FakeLiveClient(2_500.0)
    sync_live_cash(store, client)          # bootstrap
    client.balance = 2_000.0               # real balance moved since
    msg = sync_live_cash(store, client)
    assert msg is not None and "DRIFT" in msg
    assert store.account()["cash"] == pytest.approx(2_500.0)   # NOT auto-corrected


def test_sync_live_cash_quiet_when_matching(store):
    client = FakeLiveClient(2_500.0)
    sync_live_cash(store, client)
    assert sync_live_cash(store, client) is None


# ------------------------------------------ Engine.reconcile_own_live_account
def test_reconcile_own_live_account_noop_for_paper_broker(engine):
    run(engine.reconcile_own_live_account())
    assert engine.notifier.sent == []


def test_reconcile_own_live_account_bootstraps_on_first_call(engine, store):
    engine.broker = FakeLiveBrokerStub(FakeLiveClient(3_400.0))
    run(engine.reconcile_own_live_account())
    assert store.account()["cash"] == pytest.approx(3_400.0)
    assert any("initialised" in m for _, k, m in engine.notifier.sent if k == "risk")


def test_reconcile_own_live_account_flags_cash_drift(engine, store):
    client = FakeLiveClient(1_000.0)
    engine.broker = FakeLiveBrokerStub(client)
    run(engine.reconcile_own_live_account())          # bootstrap to 1000
    client.balance = 800.0                            # unexplained drop
    run(engine.reconcile_own_live_account())
    assert store.account()["cash"] == pytest.approx(1_000.0)   # not auto-corrected
    assert any("DRIFT" in m for _, k, m in engine.notifier.sent if k == "risk")


def test_reconcile_own_live_account_flags_position_drift(engine, store, stubs):
    data, _, _ = stubs
    funder = "0x" + "f" * 40
    engine.broker = FakeLiveBrokerStub(FakeLiveClient(1_000.0), funder_address=funder)
    # on-chain shows a real position our book has no record of
    data.positions_by_wallet[funder] = [{"asset": "tok1", "size": 500.0}]
    run(engine.reconcile_own_live_account())
    assert any("tok1" in m and "DRIFT" in m
              for _, k, m in engine.notifier.sent if k == "risk")


def test_reconcile_own_live_account_quiet_when_everything_matches(engine, store, stubs):
    data, _, _ = stubs
    funder = "0x" + "f" * 40
    engine.broker = FakeLiveBrokerStub(FakeLiveClient(1_000.0), funder_address=funder)
    run(engine.reconcile_own_live_account())          # bootstrap, no positions either side
    engine.notifier.sent.clear()
    run(engine.reconcile_own_live_account())
    assert engine.notifier.sent == []


def test_live_broker_sell_without_position_raises(store):
    client = ScriptableExecutionClient([])
    broker = LiveBroker(store, client, max_live_notional_usd=1_000.0)
    with pytest.raises(NoPosition):
        broker.sell(W, "ghost", size=10, reference_price=0.5)


def test_live_broker_settlement_never_calls_execution_client(store):
    buy_fill = OrderResult(order_id="1", status="matched", side="BUY", filled_size=1000.0,
                           avg_price=0.50, fee=0.0, notional=500.0, raw={})
    client = ScriptableExecutionClient([buy_fill])
    broker = LiveBroker(store, client, max_live_notional_usd=1_000.0)
    broker.buy(W, "tok", notional=500, reference_price=0.50, meta=META)
    calls_before_settlement = len(client.calls)
    result = broker.sell(W, "tok", size=1000, reference_price=1.0, settlement=True)
    assert result.kind == "REDEEM"
    assert result.price == 1.0
    assert len(client.calls) == calls_before_settlement  # settlement never hits the exchange


def test_live_broker_applies_slippage_as_marketable_limit_cushion(store):
    fill = OrderResult(order_id="1", status="matched", side="BUY", filled_size=100.0,
                       avg_price=0.505, fee=0.0, notional=50.5, raw={})
    client = ScriptableExecutionClient([fill])
    broker = LiveBroker(store, client, max_live_notional_usd=1_000.0, slippage_bps=100)  # 1%
    broker.buy(W, "tok", notional=50.5, reference_price=0.50, meta=META)
    assert client.calls[0]["price"] == pytest.approx(0.505)
