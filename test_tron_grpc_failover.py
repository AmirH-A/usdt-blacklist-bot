"""Self-check for TRON gRPC endpoint failover.

Why this exists: the production Chainstack token expired and every call came
back PERMISSION_DENIED / http2 403. get_now_block() returned None, the watcher
silently degraded to the slower TronGrid HTTP path, and nothing alerted. These
checks pin the behaviour that stops that recurring.

Run: python3 test_tron_grpc_failover.py
"""

import os

os.environ.setdefault("BOT_TOKEN", "123456789:AAHfake-TokenForTests_000000000000000")
os.environ["CHAINSTACK_API_TOKEN"] = "dead-primary"
os.environ["CHAINSTACK_API_TOKEN_FALLBACK"] = "live-backup"

import src.config as cfg  # noqa: E402
from src.tron import grpc_client as gc  # noqa: E402

# config is read at import time; rebuild the list for this process
cfg.TRON_GRPC_ENDPOINTS = cfg._tron_grpc_endpoints()
gc.TRON_GRPC_ENDPOINTS = cfg.TRON_GRPC_ENDPOINTS

HEAD = {"block_number": 86666941, "transactions": []}


class FakeRpcError(Exception):
    """Stands in for grpc.RpcError."""

    def code(self):
        return "StatusCode.PERMISSION_DENIED"

    def details(self):
        return "Received http2 header with status: 403"


def _client(behaviour):
    """A client whose transport is `behaviour`: token -> result or raises."""
    c = gc.TronGrpcClient()
    c._init_channel = lambda: None          # no real network
    calls = []

    def fake_invoke_transport(path, payload, timeout):
        calls.append((c.api_token, path))
        return behaviour(c.api_token)

    class Chan:
        def unary_unary(self, path, request_serializer=None, response_deserializer=None):
            return lambda payload, timeout=5: fake_invoke_transport(path, payload, timeout)

        def close(self):
            pass

    c._channel = Chan()
    # _switch_endpoint nulls the channel; keep handing back the fake
    orig_switch = c._switch_endpoint

    def switch():
        ok = orig_switch()
        c._channel = Chan()
        return ok

    c._switch_endpoint = switch
    return c, calls


def test_two_endpoints_are_configured():
    hosts = [(h, t) for h, _, t in cfg.TRON_GRPC_ENDPOINTS]
    assert len(hosts) == 2, hosts
    assert hosts[0][1] == "dead-primary", "primary from the existing env must stay first"
    assert hosts[1][1] == "live-backup", "fallback second"


def test_dead_primary_fails_over_and_returns_data():
    """The real production symptom: primary 403s, backup serves the block."""
    def behaviour(token):
        if token == "dead-primary":
            raise FakeRpcError()
        return HEAD

    c, calls = _client(behaviour)
    got = c._invoke("/protocol.Wallet/GetNowBlock2", b"",
                    lambda r: r, empty_is_failure=True)
    assert got == HEAD, got
    assert [t for t, _ in calls] == ["dead-primary", "live-backup"], calls
    assert c.api_token == "live-backup", "should be parked on the working endpoint"
    assert c.failovers == 1


def test_silent_no_data_also_fails_over():
    """A token that answers but yields nothing must not be treated as healthy."""
    def behaviour(token):
        return None if token == "dead-primary" else HEAD

    c, calls = _client(behaviour)
    got = c._invoke("/protocol.Wallet/GetNowBlock2", b"",
                    lambda r: r, empty_is_failure=True)
    assert got == HEAD, "empty result on the chain head must trigger failover"
    assert c.failovers == 1


def test_not_found_does_not_burn_a_failover():
    """get_transaction_by_id legitimately returns None; that is not a fault."""
    c, calls = _client(lambda token: None)
    got = c._invoke("/protocol.Wallet/GetTransactionById", b"\x00",
                    lambda r: r, empty_is_failure=False)
    assert got is None
    assert c.failovers == 0, "a missing tx must not rotate the endpoint"
    assert len(calls) == 1, "and must not retry on the other endpoint"


def test_all_endpoints_down_returns_none_without_looping():
    def behaviour(token):
        raise FakeRpcError()

    c, calls = _client(behaviour)
    got = c._invoke("/protocol.Wallet/GetNowBlock2", b"",
                    lambda r: r, empty_is_failure=True)
    assert got is None
    assert len(calls) == 2, f"one attempt per endpoint, got {len(calls)}"


def test_explicit_endpoint_has_no_failover():
    """Probes and tests that pass host/token explicitly stay on that endpoint."""
    c = gc.TronGrpcClient(host="h", port=443, api_token="explicit")
    assert len(c._endpoints) == 1
    assert c._switch_endpoint() is False


def test_grpc_available_true_when_only_fallback_is_set():
    saved_token = cfg.CHAINSTACK_API_TOKEN
    saved_eps = list(gc.TRON_GRPC_ENDPOINTS)   # restoring this matters: leaving a
    try:                                       # 1-endpoint list here silently
        cfg.CHAINSTACK_API_TOKEN = ""          # disabled failover in later tests
        gc.CHAINSTACK_API_TOKEN = ""
        gc.TRON_GRPC_ENDPOINTS = [("h", 443, "live-backup")]
        gc.USE_GRPC = True
        if gc.grpc is None:
            return  # grpcio absent locally; nothing to assert
        assert gc.grpc_available() is True
    finally:
        cfg.CHAINSTACK_API_TOKEN = saved_token
        gc.CHAINSTACK_API_TOKEN = saved_token
        gc.TRON_GRPC_ENDPOINTS = saved_eps


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("\nall checks passed")
