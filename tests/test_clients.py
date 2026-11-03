"""BaseClient.get_json error handling against a mocked transport."""
import asyncio

import httpx
import pytest

from polymirror.clients import ApiError, BaseClient, DataApiClient, LeaderboardClient, RateLimiter


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def make_client(handler, max_retries=5, cls=BaseClient):
    client = cls("https://example.com", RateLimiter(1000.0), max_retries=max_retries)
    client._client = httpx.AsyncClient(
        base_url="https://example.com",
        transport=httpx.MockTransport(handler),
    )
    return client


def test_non_retryable_status_raises_api_error_without_retrying():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(404, json={"error": "not found"})

    client = make_client(handler)
    with pytest.raises(ApiError):
        run(client.get_json("/leaderboard"))
    assert calls["n"] == 1  # non-retryable: exactly one attempt, no backoff loop


def test_retryable_status_retries_then_raises_api_error(monkeypatch):
    real_sleep = asyncio.sleep
    monkeypatch.setattr("polymirror.clients.asyncio.sleep", lambda _: real_sleep(0))
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(503, json={"error": "unavailable"})

    client = make_client(handler, max_retries=3)
    with pytest.raises(ApiError):
        run(client.get_json("/leaderboard"))
    assert calls["n"] == 3  # retried up to max_retries


def test_successful_response_returns_json():
    def handler(request):
        return httpx.Response(200, json={"ok": True})

    client = make_client(handler)
    result = run(client.get_json("/activity"))
    assert result == {"ok": True}


def page_of(n):
    return [{"timestamp": i, "asset": str(i)} for i in range(n)]


def test_activity_since_stops_at_pagination_cap_without_extra_request():
    calls = {"n": 0, "offsets": []}

    def handler(request):
        calls["n"] += 1
        offset = int(dict(request.url.params)["offset"])
        calls["offsets"].append(offset)
        return httpx.Response(200, json=page_of(500))  # always a full page

    client = make_client(handler, cls=DataApiClient)
    result = run(client.activity_since("0xabc", start=0))
    # 11 pages (offsets 0..5000 step 500) = 5,500 fills; must never request 5500.
    assert calls["offsets"] == list(range(0, 5001, 500))
    assert 5500 not in calls["offsets"]
    assert len(result) == 5500


def test_activity_since_stops_gracefully_on_error_mid_pagination():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(200, json=page_of(500))
        return httpx.Response(400, json={"error": "bad request"})

    client = make_client(handler, cls=DataApiClient)
    result = run(client.activity_since("0xabc", start=0))
    assert len(result) == 500       # first page kept, no exception propagated
    assert calls["n"] == 2


def test_activity_since_stops_on_short_page():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(200, json=page_of(500) if calls["n"] == 1 else page_of(200))

    client = make_client(handler, cls=DataApiClient)
    result = run(client.activity_since("0xabc", start=0))
    assert len(result) == 700
    assert calls["n"] == 2


def test_leaderboard_hits_v1_endpoint_with_mapped_params():
    seen = {}

    def handler(request):
        seen["path"] = request.url.path
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json=[])

    client = make_client(handler, cls=LeaderboardClient)
    run(client.top_wallets(window="1w", limit=200, rank_type="volume"))
    assert seen["path"] == "/v1/leaderboard"
    assert seen["params"]["category"] == "OVERALL"
    assert seen["params"]["timePeriod"] == "WEEK"
    assert seen["params"]["orderBy"] == "VOL"
    assert seen["params"]["limit"] == "50"   # clamped to the API's own max


def test_leaderboard_parses_real_response_shape():
    def handler(request):
        return httpx.Response(200, json=[
            {"rank": "1", "proxyWallet": "0x" + "a" * 40, "userName": "whale",
             "xUsername": "", "verifiedBadge": False, "vol": 100.0, "pnl": 5000.0,
             "profileImage": ""},
            {"rank": "2", "proxyWallet": "not-an-address", "userName": "bad-row",
             "pnl": 999.0},
        ])

    client = make_client(handler, cls=LeaderboardClient)
    result = run(client.top_wallets())
    assert len(result) == 1   # the malformed-address row is dropped
    assert result[0] == {"wallet": "0x" + "a" * 40, "name": "whale", "amount": 5000.0}
