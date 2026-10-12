"""Node endpoint parsing. Run: python3 test_nodes.py

The backward-compatibility cases matter most: a deployment that sets neither
TRON_NODES nor ETH_NODES must produce exactly the endpoints the previous
release used, in the same order.
"""

from src.nodes import eth_endpoints, parse_node, parse_nodes, tron_endpoints

LEGACY_GRPC = [
    ("tron-mainnet.core.chainstack.com", 443, "deadkey"),
    ("tron-mainnet.core.chainstack.com", 443, "livekey"),
]
LEGACY_HTTP = "https://api.trongrid.io"
LEGACY_HTTP_KEY = "tgkey"
LEGACY_ETH = [
    "https://ethereum-mainnet.core.chainstack.com/aaa",
    "https://ethereum-mainnet.core.chainstack.com/bbb",
]


def test_bare_host_port_is_grpc():
    ep = parse_node("tron-mainnet.core.chainstack.com:443")
    assert ep.kind == "grpc", ep
    assert ep.host == "tron-mainnet.core.chainstack.com"
    assert ep.port == 443


def test_grpc_scheme_with_token():
    ep = parse_node("grpc://tron.example.com:50051?token=abc123")
    assert (ep.kind, ep.host, ep.port) == ("grpc", "tron.example.com", 50051)
    assert ep.token == "abc123"
    assert ep.headers == {"x-token": "abc123"}


def test_http_chainstack_style_uses_x_token():
    ep = parse_node("https://tron-mainnet.core.chainstack.com?token=hex")
    assert ep.kind == "http"
    assert ep.headers == {"x-token": "hex"}
    # The secret must not survive into the request URL.
    assert "hex" not in ep.host
    assert "?" not in ep.host


def test_http_trongrid_style_uses_pro_api_key():
    ep = parse_node("https://api.trongrid.io?api_key=KEY")
    assert ep.headers == {"TRON-PRO-API-KEY": "KEY"}
    assert ep.host == "https://api.trongrid.io"


def test_self_hosted_node_needs_no_auth():
    ep = parse_node("http://10.0.0.5:8090")
    assert ep.kind == "http"
    assert ep.headers == {}
    assert ep.token == ""
    assert ep.host == "http://10.0.0.5:8090"


def test_explicit_header_escape_hatch():
    ep = parse_node("https://node.example.com?header=Authorization:Bearer%20xyz")
    assert ep.headers.get("Authorization") == "Bearer xyz", ep.headers


def test_order_preserved_and_duplicates_dropped():
    eps = parse_nodes(
        "http://a:8090, http://b:8090, http://a:8090, grpc://c:443?token=t"
    )
    assert [e.label for e in eps] == ["a", "b", "c"], [e.label for e in eps]


def test_multiline_and_junk_tolerated():
    eps = parse_nodes("\n  http://a:8090 ,\n\n  ,  not a url  ,\n http://b:8090\n")
    labels = [e.label for e in eps]
    # "not a url" has no scheme, so it parses as a grpc host -- acceptable, but
    # it must not break the list or reorder the real entries.
    assert labels[0] == "a" and labels[-1] == "b", labels


# --- backward compatibility -------------------------------------------------

def test_tron_falls_back_to_legacy_config_exactly():
    eps = tron_endpoints("", LEGACY_GRPC, LEGACY_HTTP, LEGACY_HTTP_KEY)
    assert [e.kind for e in eps] == ["grpc", "grpc", "http"], eps
    assert eps[0].token == "deadkey"       # primary first, as before
    assert eps[1].token == "livekey"
    assert eps[2].host == "https://api.trongrid.io"
    assert eps[2].headers == {"TRON-PRO-API-KEY": "tgkey"}


def test_tron_skips_grpc_endpoints_with_no_token():
    eps = tron_endpoints("", [("h", 443, "")], LEGACY_HTTP, "")
    assert [e.kind for e in eps] == ["http"], eps


def test_tron_nodes_overrides_legacy():
    eps = tron_endpoints(
        "http://10.0.0.5:8090", LEGACY_GRPC, LEGACY_HTTP, LEGACY_HTTP_KEY
    )
    assert [e.host for e in eps] == ["http://10.0.0.5:8090"], eps


def test_eth_falls_back_to_legacy_pair_in_order():
    eps = eth_endpoints("", LEGACY_ETH)
    assert [e.host for e in eps] == LEGACY_ETH, eps


def test_eth_dedupes_legacy_pair():
    eps = eth_endpoints("", [LEGACY_ETH[0], LEGACY_ETH[0]])
    assert len(eps) == 1


def test_eth_nodes_overrides_and_ignores_grpc_entries():
    eps = eth_endpoints("grpc://x:443?token=t, http://10.0.0.6:8545", LEGACY_ETH)
    assert [e.host for e in eps] == ["http://10.0.0.6:8545"], eps


def test_empty_everywhere_is_empty_not_a_crash():
    assert tron_endpoints("", [], "", "") == []
    assert eth_endpoints("", []) == []


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_"):
            fn()
            print(f"ok  {name}")
    print("all passed")
