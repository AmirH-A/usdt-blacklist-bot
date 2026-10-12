"""Node endpoints as configuration, so a URL is all it takes to add a provider.

Before this, the chains were wired to specific vendors: TRON could only reach a
Chainstack gRPC endpoint plus one TronGrid-shaped HTTP base, and ETH was capped
at the two ETH_RPC_URL* variables. Pointing either at a self-hosted node meant
editing code.

Now each chain takes an ordered, comma-separated list:

    TRON_NODES=grpc://tron-mainnet.core.chainstack.com:443?token=<hex>,
               https://api.trongrid.io?api_key=<key>,
               http://10.0.0.5:8090
    ETH_NODES=https://eth-mainnet.example.com/<key>,
              http://10.0.0.6:8545

Order is priority: the first entry is tried first, the rest are failover. The
scheme picks the transport, and the query string carries auth:

    grpc://host:port?token=X   TRON gRPC, sends `x-token: X`
    http(s)://host?token=X     HTTP, sends `x-token: X`        (Chainstack REST)
    http(s)://host?api_key=X   HTTP, sends `TRON-PRO-API-KEY`  (TronGrid)
    http(s)://host?header=K:V  HTTP, sends a literal header    (escape hatch)
    http(s)://host             HTTP, no auth                   (self-hosted node)

Backward compatibility is the point: when TRON_NODES / ETH_NODES are unset,
the lists are built from the existing CHAINSTACK_* and ETH_RPC_URL* variables
in the same order as before, so a deployment that sets neither behaves
identically to the previous release.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import parse_qsl, urlsplit, urlunsplit


@dataclass(frozen=True)
class NodeEndpoint:
    """One place a chain can be read from."""

    kind: str  # "grpc" | "http"
    host: str  # hostname only (grpc) or full base URL (http)
    port: int = 0
    token: str = ""  # auth value, whatever header it belongs in
    headers: Dict[str, str] = field(default_factory=dict)
    label: str = ""  # for logs; never contains the secret

    @property
    def target(self) -> str:
        return f"{self.host}:{self.port}" if self.kind == "grpc" else self.host


def _split_list(raw: str) -> List[str]:
    """Comma- or whitespace-separated, so a multi-line .env value works."""
    out = []
    for chunk in (raw or "").replace("\n", ",").split(","):
        chunk = chunk.strip()
        if chunk:
            out.append(chunk)
    return out


def _auth_from_query(pairs: List[tuple]) -> tuple:
    """Return (token, headers) from the query parameters."""
    token = ""
    headers: Dict[str, str] = {}
    for key, value in pairs:
        k = key.lower()
        if k in ("token", "x_token", "x-token"):
            token = value
            headers["x-token"] = value
        elif k in ("api_key", "apikey", "pro_api_key"):
            token = value
            headers["TRON-PRO-API-KEY"] = value
        elif k == "header":
            # header=Name:value -- the escape hatch for anything unusual.
            name, _, val = value.partition(":")
            if name and val:
                headers[name.strip()] = val.strip()
                token = token or val.strip()
    return token, headers


def parse_node(spec: str) -> Optional[NodeEndpoint]:
    """Parse one entry. Returns None for anything unusable, never raises."""
    spec = (spec or "").strip()
    if not spec:
        return None

    # A bare host:port with no scheme is the gRPC shape the old config used.
    if "://" not in spec:
        host, _, port = spec.partition(":")
        if not host:
            return None
        return NodeEndpoint(
            kind="grpc", host=host, port=int(port) if port.isdigit() else 443,
            label=host,
        )

    parts = urlsplit(spec)
    scheme = (parts.scheme or "").lower()
    token, headers = _auth_from_query(parse_qsl(parts.query))

    if scheme == "grpc" or scheme == "grpcs":
        host = parts.hostname or ""
        if not host:
            return None
        return NodeEndpoint(
            kind="grpc", host=host, port=parts.port or 443,
            token=token, headers=headers, label=host,
        )

    if scheme in ("http", "https"):
        # Rebuild without the query so auth never ends up in a request URL.
        base = urlunsplit((parts.scheme, parts.netloc, parts.path, "", "")).rstrip("/")
        if not parts.netloc:
            return None
        return NodeEndpoint(
            kind="http", host=base, token=token, headers=headers,
            label=parts.hostname or base,
        )

    return None


def parse_nodes(raw: str) -> List[NodeEndpoint]:
    """Parse a whole list, dropping duplicates and keeping the given order."""
    seen = set()
    out: List[NodeEndpoint] = []
    for spec in _split_list(raw):
        ep = parse_node(spec)
        if ep is None:
            continue
        key = (ep.kind, ep.target, ep.token)
        if key in seen:
            continue
        seen.add(key)
        out.append(ep)
    return out


def tron_endpoints(
    raw_nodes: str,
    legacy_grpc: List[tuple],
    legacy_http_base: str,
    legacy_http_key: str,
) -> List[NodeEndpoint]:
    """TRON endpoints in priority order.

    `TRON_NODES` wins when set. Otherwise the previous configuration is
    reproduced exactly: the Chainstack gRPC endpoints first (primary then
    fallback), then the single TronGrid HTTP base.
    """
    parsed = parse_nodes(raw_nodes)
    if parsed:
        return parsed

    out: List[NodeEndpoint] = []
    for host, port, token in legacy_grpc:
        if not token:
            continue
        out.append(
            NodeEndpoint(
                kind="grpc", host=host, port=port, token=token,
                headers={"x-token": token}, label=host,
            )
        )
    if legacy_http_base:
        headers = (
            {"TRON-PRO-API-KEY": legacy_http_key} if legacy_http_key else {}
        )
        out.append(
            NodeEndpoint(
                kind="http", host=legacy_http_base.rstrip("/"),
                token=legacy_http_key, headers=headers,
                label=legacy_http_base,
            )
        )
    return out


def eth_endpoints(raw_nodes: str, legacy_urls: List[str]) -> List[NodeEndpoint]:
    """ETH JSON-RPC endpoints in priority order.

    `ETH_NODES` wins when set; otherwise ETH_RPC_URL / ETH_RPC_URL_FALLBACK in
    their existing order.
    """
    parsed = [e for e in parse_nodes(raw_nodes) if e.kind == "http"]
    if parsed:
        return parsed
    out: List[NodeEndpoint] = []
    seen = set()
    for url in legacy_urls:
        url = (url or "").rstrip("/")
        if not url or url in seen:
            continue
        seen.add(url)
        ep = parse_node(url)
        if ep and ep.kind == "http":
            out.append(ep)
    return out
