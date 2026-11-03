import pytest
from fastapi.testclient import TestClient

from polymirror.api import create_app
from polymirror.broker import PaperBroker
from polymirror.metrics import compute_metrics
from polymirror.storage import Store

W = "0x" + "b" * 40
META = {"condition_id": "0xc", "title": "T?", "outcome": "Yes"}


def test_metrics_after_round_trips(store):
    broker = PaperBroker(store, 0, 0)
    broker.buy(W, "a", 500, 0.50, META)
    broker.sell(W, "a", 10_000, 0.60)            # win  +100
    broker.buy(W, "b", 500, 0.50, META)
    broker.sell(W, "b", 10_000, 0.45)            # loss  -50
    broker.buy(W, "c", 500, 0.50, META)          # still open
    store.set_mark(store.open_position(W, "c")["id"], 0.55)

    m = compute_metrics(store)
    assert m["realized_pnl"] == pytest.approx(50.0)
    assert m["unrealized_pnl"] == pytest.approx(50.0)
    assert m["equity"] == pytest.approx(10_100.0)
    assert m["win_rate_pct"] == pytest.approx(50.0)
    assert m["open_positions"] == 1 and m["closed_positions"] == 2
    assert m["total_return_pct"] == pytest.approx(1.0)


def test_equity_snapshots_and_drawdown(store):
    for ts, eq in [(1, 10_000), (2, 11_000), (3, 9_900), (4, 10_500)]:
        store.snapshot_equity(ts, eq, eq, 0, 0)
    m = compute_metrics(store)
    assert m["max_drawdown_pct"] == pytest.approx(10.0)  # 11000 -> 9900


def test_deposit_and_withdraw_adjust_cash(store):
    assert store.deposit(2_000.0, "0xfake111") == pytest.approx(12_000.0)
    assert store.account()["total_deposits"] == pytest.approx(2_000.0)
    assert store.withdraw(500.0, "0xfake222") == pytest.approx(11_500.0)
    assert store.account()["total_withdrawals"] == pytest.approx(500.0)

    transfers = store.transfers()
    assert len(transfers) == 2
    assert transfers[0]["kind"] == "withdraw" and transfers[0]["amount"] == pytest.approx(500.0)
    assert transfers[1]["kind"] == "deposit" and transfers[1]["address"] == "0xfake111"


def test_withdraw_beyond_cash_raises(store):
    from polymirror.storage import InsufficientCash
    with pytest.raises(InsufficientCash):
        store.withdraw(999_999.0)
    assert store.account()["cash"] == pytest.approx(10_000.0)  # unchanged


def test_deposit_withdraw_rejects_non_positive_amounts(store):
    with pytest.raises(ValueError):
        store.deposit(0.0)
    with pytest.raises(ValueError):
        store.withdraw(-10.0)


def test_return_pct_is_unaffected_by_deposits_and_withdrawals(store):
    """A deposit is not profit and a withdrawal is not a loss — return%
    must be measured against net contributed capital, not just the
    original starting balance, or funding moves would masquerade as
    trading performance."""
    baseline = compute_metrics(store)["total_return_pct"]
    assert baseline == pytest.approx(0.0)

    store.deposit(5_000.0)
    assert compute_metrics(store)["total_return_pct"] == pytest.approx(0.0)

    store.withdraw(2_000.0)
    assert compute_metrics(store)["total_return_pct"] == pytest.approx(0.0)

    # Actual trading P&L still moves it, on top of net-contributed capital.
    broker = PaperBroker(store, 0, 0)
    broker.buy(W, "tok", 500, 0.50, META)
    broker.sell(W, "tok", 1000, 0.60)   # +100 realized
    m = compute_metrics(store)
    net_contributed = 10_000.0 + 5_000.0 - 2_000.0
    assert m["total_return_pct"] == pytest.approx(100.0 / net_contributed * 100)


def test_reset_clears_deposit_withdrawal_totals(store):
    store.deposit(1_000.0)
    store.withdraw(200.0)
    store.reset_account(10_000.0)
    acct = store.account()
    assert acct["total_deposits"] == pytest.approx(0.0)
    assert acct["total_withdrawals"] == pytest.approx(0.0)
    assert store.transfers() == []


def test_max_drawdown_pct_is_not_inflated_by_a_withdrawal(store):
    """A withdrawal drops raw equity but isn't a trading drawdown — the
    reported max_drawdown_pct stat must stay deposit/withdrawal-neutral,
    same principle as total_return_pct."""
    store.snapshot_equity(1, 10_000, 10_000, 0, 0)
    store.snapshot_equity(2, 12_000, 12_000, 0, 0)   # peak, no transfers yet
    store.withdraw(3_000.0)
    store.snapshot_equity(3, 9_000, 9_000, 0, 0)     # raw equity down 25% from peak
    assert compute_metrics(store)["max_drawdown_pct"] == pytest.approx(0.0)


@pytest.fixture
def client(cfg, tmp_path):
    cfg.db_path = str(tmp_path / "test.db")
    app = create_app(cfg, store=Store(cfg.db_path))
    with TestClient(app) as c:
        yield c


def test_wallet_crud(client):
    r = client.post("/api/wallets", json={"address": W, "label": "whale",
                                          "sizing_mode": "fixed_usd", "sizing_value": 50})
    assert r.status_code == 201
    assert client.get("/api/wallets").json()[0]["label"] == "whale"

    r = client.patch(f"/api/wallets/{W}", json={"multiplier": 2.0, "active": False})
    assert r.status_code == 200
    assert r.json()["multiplier"] == 2.0
    assert r.json()["active"] == 0

    # DELETE is a real removal, not a soft-pause: the wallet disappears from
    # the list entirely (trade history, kept separately, is unaffected).
    assert client.delete(f"/api/wallets/{W}").status_code == 204
    assert client.get("/api/wallets").json() == []
    assert client.delete(f"/api/wallets/{W}").status_code == 404  # already gone


def test_wallet_validation(client):
    assert client.post("/api/wallets", json={"address": "not-a-wallet"}).status_code == 422
    assert client.post("/api/wallets", json={"address": W, "sizing_mode": "yolo"}).status_code == 422


def test_percent_sizing_rejects_whole_percentage_on_add(client):
    # 20.0 for percent_balance would size every copy at min(20x cash, cash) —
    # i.e. the entire account — which then always trips the portfolio's
    # market-exposure cap. The API must reject it before it's ever stored.
    r = client.post("/api/wallets", json={
        "address": W, "sizing_mode": "percent_balance", "sizing_value": 20.0})
    assert r.status_code == 422
    assert client.get("/api/wallets").json() == []

    r = client.post("/api/wallets", json={
        "address": W, "sizing_mode": "percent_balance", "sizing_value": 0.20})
    assert r.status_code == 201


def test_percent_sizing_rejects_whole_percentage_on_patch(client):
    client.post("/api/wallets", json={
        "address": W, "sizing_mode": "fixed_usd", "sizing_value": 100})

    # Switching mode to percent_balance without fixing the value must be caught.
    r = client.patch(f"/api/wallets/{W}", json={"sizing_mode": "percent_balance"})
    assert r.status_code == 422
    assert client.get("/api/wallets").json()[0]["sizing_mode"] == "fixed_usd"

    # Valid pairing goes through.
    r = client.patch(f"/api/wallets/{W}",
                     json={"sizing_mode": "percent_balance", "sizing_value": 0.30})
    assert r.status_code == 200
    assert r.json()["sizing_mode"] == "percent_balance"


def test_summary_positions_trades_endpoints(client):
    for path in ("/api/summary", "/api/positions?status=open", "/api/trades",
                 "/api/equity", "/api/events", "/api/health", "/api/scout"):
        assert client.get(path).status_code == 200
    summary = client.get("/api/summary").json()
    assert summary["metrics"]["equity"] == 10_000
    assert "wallet_metrics" in summary
    scout = client.get("/api/scout").json()
    assert set(scout) >= {"enabled", "auto_follow", "candidates"}


def test_scout_surfaces_last_failure(client, cfg):
    scout_store = Store(cfg.db_path)  # same DB file the app's store writes to

    # No scout activity yet: nothing to report.
    assert client.get("/api/scout").json()["last_failure"] is None

    # A successful sweep summary must not be reported as a failure.
    scout_store.log_event("info", "scout", "Scout sweep: 3 candidates evaluated, 0 fresh (cached)")
    assert client.get("/api/scout").json()["last_failure"] is None

    # A failed leaderboard fetch must be surfaced, not silently swallowed —
    # the dashboard previously said "scanning automatically" even when every
    # sweep had failed, which reads as healthy when it isn't.
    scout_store.log_event("warning", "scout", "Leaderboard unavailable (1w): HTTP 404 on /leaderboard")
    failure = client.get("/api/scout").json()["last_failure"]
    assert failure is not None
    assert "404" in failure["message"]
    assert failure["level"] == "warning"
    scout_store.close()


def test_reset(client):
    r = client.post("/api/reset")
    assert r.json()["ok"] is True


def test_deposit_and_withdraw_endpoints(client):
    r = client.post("/api/deposit", json={"amount": 500.0, "address": "0xfakeref"})
    assert r.status_code == 200
    assert r.json()["cash"] == pytest.approx(10_500.0)

    r = client.post("/api/withdraw", json={"amount": 200.0})
    assert r.status_code == 200
    assert r.json()["cash"] == pytest.approx(10_300.0)

    r = client.get("/api/transfers")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 2
    assert body[0]["kind"] == "withdraw"

    r = client.post("/api/withdraw", json={"amount": 999_999.0})
    assert r.status_code == 422

    r = client.post("/api/deposit", json={"amount": -5.0})
    assert r.status_code == 422  # pydantic gt=0 rejects it before it reaches the store


def test_close_position_503s_without_a_running_engine(client):
    # `client` builds no engine (no network in tests), so app.state.engine
    # is never set — closing a position needs the engine's broker/clob.
    r = client.post("/api/positions/1/close")
    assert r.status_code == 503


def test_close_position_delegates_to_engine(client):
    class FakeEngine:
        async def close_position(self, position_id):
            assert position_id == 42
            return {"id": 42, "status": "closed"}

    client.app.state.engine = FakeEngine()
    r = client.post("/api/positions/42/close")
    assert r.status_code == 200
    assert r.json() == {"id": 42, "status": "closed"}


def test_close_position_404s_when_not_open(client):
    class FakeEngine:
        async def close_position(self, position_id):
            return None

    client.app.state.engine = FakeEngine()
    r = client.post("/api/positions/1/close")
    assert r.status_code == 404


def test_dashboard_served(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "PAPER TRADING" in r.text
