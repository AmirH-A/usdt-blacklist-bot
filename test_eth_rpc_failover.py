"""Self-check for ETH JSON-RPC endpoint failover.

Both of this project's Chainstack keys expired within two days of each other. The
ETH one answered HTTP 403 on every call, rpc_call returned None, and the block
watcher quietly fell back to the 3s Etherscan path. Nothing alerted. These checks
pin the behaviour that stops that happening again.

Run: python3 test_eth_rpc_failover.py
"""

import asyncio
import os

os.environ.setdefault("BOT_TOKEN", "123456789:AAHfake-TokenForTests_000000000000000")
os.environ["ETH_RPC_URL"] = "https://dead.example/primary"
os.environ["ETH_RPC_URL_FALLBACK"] = "https://live.example/backup"

import src.config as cfg  # noqa: E402
from src.eth import rpc  # noqa: E402

cfg.ETH_RPC_URLS = cfg._eth_rpc_urls()
DEAD, LIVE = "https://dead.example/primary", "https://live.example/backup"


class FakeResp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}

    def json(self):
        return self._payload


def install(behaviour):
    """Point rpc at a two-endpoint list with a fake transport.

    behaviour(url) -> FakeResp, or raises to simulate a connect error.
    Returns the list of URLs actually hit.
    """
    rpc._urls = [DEAD, LIVE]
    rpc._url_i = 0
    rpc.rpc_failovers = 0
    hits = []

    class FakeClient:
        async def post(self, url, json=None, headers=None):
            hits.append(url)
            return behaviour(url)

    async def fake_client():
        return FakeClient()

    rpc._client = fake_client
    return hits


def run(coro):
    # asyncio.run, not get_event_loop(): Python 3.14 removed the implicit loop.
    return asyncio.run(coro)


def test_two_urls_configured_primary_first():
    assert cfg.ETH_RPC_URLS == [DEAD, LIVE], cfg.ETH_RPC_URLS


def test_403_on_primary_fails_over():
    """The real symptom: expired key answers 403 on everything."""
    def behaviour(url):
        if url == DEAD:
            return FakeResp(403)
        return FakeResp(200, {"result": "0x18e2f07"})

    hits = install(behaviour)
    got = run(rpc.rpc_call("eth_blockNumber", []))
    assert got == "0x18e2f07", got
    assert hits == [DEAD, LIVE], hits
    assert rpc.rpc_failovers == 1
    assert rpc.active_rpc_url() == LIVE, "must stay on the working endpoint"


def test_stays_on_working_endpoint_for_next_call():
    def behaviour(url):
        if url == DEAD:
            return FakeResp(403)
        return FakeResp(200, {"result": "0x1"})

    install(behaviour)
    run(rpc.rpc_call("eth_blockNumber", []))
    before = rpc.rpc_failovers
    run(rpc.rpc_call("eth_blockNumber", []))
    assert rpc.rpc_failovers == before, "second call must not re-failover"


def test_transport_exception_fails_over_instead_of_raising():
    """It used to propagate connect errors to callers; now it rotates."""
    def behaviour(url):
        if url == DEAD:
            raise OSError("connection refused")
        return FakeResp(200, {"result": "0x2"})

    install(behaviour)
    got = run(rpc.rpc_call("eth_blockNumber", []))
    assert got == "0x2", got
    assert rpc.rpc_failovers == 1


def test_jsonrpc_error_does_not_rotate():
    """A bad request fails the same way everywhere; rotating just adds latency."""
    def behaviour(url):
        return FakeResp(200, {"error": {"code": -32602, "message": "invalid params"}})

    hits = install(behaviour)
    got = run(rpc.rpc_call("eth_getBlockByNumber", ["bogus"]))
    assert got is None
    assert rpc.rpc_failovers == 0, "JSON-RPC error must not rotate the endpoint"
    assert hits == [DEAD], "and must not retry elsewhere"


def test_limit_error_in_200_body_rotates():
    """1rpc answers HTTP 200 with -32001 "usage limit": the key is spent."""
    def behaviour(url):
        if url == DEAD:
            return FakeResp(200, {"error": {"code": -32001, "message": "You've reached the usage limit"}})
        return FakeResp(200, {"result": "0x1"})

    hits = install(behaviour)
    assert run(rpc.rpc_call("eth_blockNumber", [])) == "0x1"
    assert hits == [DEAD, LIVE], hits
    assert rpc.active_rpc_url() == LIVE


def test_all_endpoints_dead_returns_none_once_each():
    hits = install(lambda url: FakeResp(403))
    got = run(rpc.rpc_call("eth_blockNumber", []))
    assert got is None
    assert hits == [DEAD, LIVE], f"one attempt per endpoint, got {hits}"


def test_rpc_enabled_true_when_only_fallback_present():
    saved = list(rpc._urls)
    try:
        rpc._urls = [LIVE]
        rpc._url_i = 0
        assert rpc.rpc_enabled() is True
    finally:
        rpc._urls = saved
        rpc._url_i = 0


def test_duplicate_urls_are_collapsed():
    """Same value in both env vars must not look like two endpoints."""
    saved = (cfg.ETH_RPC_URL, cfg.ETH_RPC_URL_FALLBACK)
    try:
        cfg.ETH_RPC_URL = "https://same/x"
        cfg.ETH_RPC_URL_FALLBACK = "https://same/x"
        assert cfg._eth_rpc_urls() == ["https://same/x"]
    finally:
        cfg.ETH_RPC_URL, cfg.ETH_RPC_URL_FALLBACK = saved


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("\nall checks passed")
