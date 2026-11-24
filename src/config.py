import os
import logging
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
BROADCAST_CHANNEL = os.getenv("BROADCAST_CHANNEL", "")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")

TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "")
TRONGRID_MEMPOOL_API_KEY = os.getenv("TRONGRID_MEMPOOL_API_KEY", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")
ALCHEMY_WSS_URL = f"wss://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
ALCHEMY_HTTP_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

# --- ETH mempool (pre-block) detection ---
# ETH blocks are only servable ~2s after their slot timestamp (propagation),
# so block-based detection has a hard ~2s floor. The mempool sees the tx before
# any block contains it. Off by default: enable per-deployment.
ENABLE_ETH_MEMPOOL = os.getenv("ENABLE_ETH_MEMPOOL", "false").lower() == "true"
ETH_MEMPOOL_WSS_URL = os.getenv("ETH_MEMPOOL_WSS_URL", ALCHEMY_WSS_URL)
# A pending submission that never gets mined is a phantom; reap it after this.
ETH_MEMPOOL_STALE_S = int(os.getenv("ETH_MEMPOOL_STALE_S", "900"))
ETH_MEMPOOL_REAP_INTERVAL = int(os.getenv("ETH_MEMPOOL_REAP_INTERVAL", "120"))


ENABLE_TRON = os.getenv("ENABLE_TRON", "true").lower() == "true"
ENABLE_ETH = os.getenv("ENABLE_ETH", "true").lower() == "true"

TRON_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRON_USDT_HEX = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
TRONGRID_BASE_URL = "https://api.trongrid.io"

CHAINSTACK_GRPC_HOST = os.getenv(
    "CHAINSTACK_GRPC_HOST", "tron-mainnet.core.chainstack.com"
)
CHAINSTACK_GRPC_PORT = int(os.getenv("CHAINSTACK_GRPC_PORT", "443"))
CHAINSTACK_API_TOKEN = os.getenv("CHAINSTACK_API_TOKEN", "")
USE_GRPC = os.getenv("USE_GRPC", "true").lower() == "true"

# Secondary Chainstack TRON gRPC endpoint. The primary can go dead without the
# bot noticing: an expired token answers every call with PERMISSION_DENIED /
# http2 403, get_now_block returns None, and the watcher quietly degrades to the
# slower TronGrid HTTP path. Configure a second endpoint and the client fails
# over instead. Token lives in .env, never in the repo.
CHAINSTACK_GRPC_HOST_FALLBACK = os.getenv(
    "CHAINSTACK_GRPC_HOST_FALLBACK", "tron-mainnet.core.chainstack.com"
)
CHAINSTACK_GRPC_PORT_FALLBACK = int(os.getenv("CHAINSTACK_GRPC_PORT_FALLBACK", "443"))
CHAINSTACK_API_TOKEN_FALLBACK = os.getenv("CHAINSTACK_API_TOKEN_FALLBACK", "")

# Provider credential watchdog. A dead key is silent (failover hides it), so
# probe each configured endpoint and alert ADMIN_CHAT_ID when one needs
# rotating. 300s costs ~288 probe calls/day/endpoint -- noise against the
# 0.25s block poll.
PROVIDER_PROBE_INTERVAL = float(os.getenv("PROVIDER_PROBE_INTERVAL", "300"))
PROVIDER_REMIND_HOURS = float(os.getenv("PROVIDER_REMIND_HOURS", "6"))


def _tron_grpc_endpoints() -> list:
    """Ordered (host, port, token); primary first, then any fallback."""
    eps = []
    if CHAINSTACK_API_TOKEN:
        eps.append((CHAINSTACK_GRPC_HOST, CHAINSTACK_GRPC_PORT, CHAINSTACK_API_TOKEN))
    if CHAINSTACK_API_TOKEN_FALLBACK:
        eps.append((
            CHAINSTACK_GRPC_HOST_FALLBACK,
            CHAINSTACK_GRPC_PORT_FALLBACK,
            CHAINSTACK_API_TOKEN_FALLBACK,
        ))
    return eps


TRON_GRPC_ENDPOINTS = _tron_grpc_endpoints()

# --- node endpoints as configuration ---------------------------------------
# TRON_NODES / ETH_NODES take an ordered, comma-separated list of node URLs, so
# a self-hosted or third-party node needs no code change. src/nodes.py lists
# the accepted forms. Unset means "use the variables above", which keeps an
# existing deployment byte-identical.
TRON_NODES = os.getenv("TRON_NODES", "")
ETH_NODES = os.getenv("ETH_NODES", "")

ETH_USDT_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
# Tether Gold. Owned by the same ETH MultiSig as USDT, but its freeze functions
# are the newer TetherToken ones: addToBlockedList / removeFromBlockedList.
ETH_XAUT_CONTRACT = "0x68749665ff8d2d112fa859aa293f07a622782f38"
ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
ETH_CHAIN_ID = 1
ETH_RPC_URL = os.getenv("ETH_RPC_URL", "").rstrip("/")
ETH_BLOCK_POLL_INTERVAL = float(os.getenv("ETH_BLOCK_POLL_INTERVAL", "1.0"))

# Secondary ETH JSON-RPC endpoint, same reasoning as the TRON gRPC fallback: a
# lapsed Chainstack key answers 403 on every call, rpc_call returns None, and the
# block watcher silently degrades to the 3s Etherscan path with nothing alerting.
# Both of this project's Chainstack keys died within two days of each other.
ETH_RPC_URL_FALLBACK = os.getenv("ETH_RPC_URL_FALLBACK", "").rstrip("/")


def _eth_rpc_urls() -> list:
    """Ordered RPC URLs; the existing env value stays primary."""
    urls = [u for u in (ETH_RPC_URL, ETH_RPC_URL_FALLBACK) if u]
    seen, out = set(), []
    for u in urls:                      # keep order, drop duplicates
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


ETH_RPC_URLS = _eth_rpc_urls()

from .nodes import eth_endpoints, tron_endpoints  # noqa: E402

# Resolved endpoint lists, priority-ordered; these are what the clients read.
TRON_ENDPOINTS = tron_endpoints(
    TRON_NODES, TRON_GRPC_ENDPOINTS, TRONGRID_BASE_URL, TRONGRID_API_KEY
)
ETH_ENDPOINTS = eth_endpoints(ETH_NODES, ETH_RPC_URLS)

# The legacy names now point at the resolved lists, so every existing caller
# honours TRON_NODES / ETH_NODES without being touched.
TRON_GRPC_ENDPOINTS = [
    (e.host, e.port, e.token) for e in TRON_ENDPOINTS if e.kind == "grpc"
]
TRON_HTTP_ENDPOINTS = [e for e in TRON_ENDPOINTS if e.kind == "http"]
ETH_RPC_URLS = [e.host for e in ETH_ENDPOINTS]
ETH_RPC_HEADERS = {e.host: dict(e.headers) for e in ETH_ENDPOINTS}

# Mempool sources, raced first-seen-wins. Measured on this box: Chainstack sees a
# pending tx ~449ms before Alchemy (238/250 shared hashes), while Alchemy is the
# only one offering a server-side toAddress filter. Running both takes the
# earlier of the two and survives one provider stalling. Rejecting a non-match
# costs 0.0002ms, so client-side filtering the unfiltered stream is free.
def _default_mempool_urls() -> list:
    # Derive from EVERY configured RPC endpoint, not just the primary. When the
    # primary key died its WSS answered "server rejected WebSocket connection:
    # HTTP 403" every few seconds while the healthy fallback sat unused.
    urls = []
    if ALCHEMY_API_KEY:
        urls.append(ALCHEMY_WSS_URL)
    for u in ETH_RPC_URLS:
        if u.startswith("http"):
            urls.append(u.replace("https://", "wss://").replace("http://", "ws://"))
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


ETH_MEMPOOL_WSS_URLS = [
    u.strip() for u in os.getenv("ETH_MEMPOOL_WSS_URLS", "").split(",") if u.strip()
] or _default_mempool_urls()
ETH_BLOCK_CATCHUP_CONCURRENCY = int(os.getenv("ETH_BLOCK_CATCHUP_CONCURRENCY", "4"))
ETH_BLOCK_MAX_CATCHUP = int(os.getenv("ETH_BLOCK_MAX_CATCHUP", "80"))
ETH_BLACKLIST_TOPIC = (
    "0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc"
)

TRON_MULTISIG_WALLET = "TBPxhVAsuzoFnKyXtc1o2UySEydPHgATto"
TRON_MULTISIG_HEX = "410fa695d6b065707cb4e0ef73b751c93347682bf2"
ETH_MULTISIG_WALLET = "0xC6CDE7C39eB2f0F0095F41570af89eFC2C1Ea828"

ETH_SUBMISSION_TOPIC = (
    "0xc0ba8fe4b176c1714197d43b9cc6bcf797a4a7461c5fe8d0ef6e184ae7601e51"
)
ETH_EXECUTION_TOPIC = (
    "0x33e13ecb54c3076d8e8bb8c2881800a4d972b792045ffae98fdf46df365fed75"
)

ADDBLACKLIST_SELECTOR = "0ecb93c0"
REMOVEBLACKLIST_SELECTOR = "e4997dc5"
ADDTOBLOCKEDLIST_SELECTOR = "3c7c9b90"
REMOVEFROMBLOCKEDLIST_SELECTOR = "1a14f449"
SUBMIT_TRANSACTION_SELECTOR = "c6427474"
EXECUTE_TRANSACTION_SELECTOR = "ee22610b"
CONFIRM_TRANSACTION_SELECTOR = "c01a8c84"

MAX_ALERTS_PER_CYCLE = 50
WHALE_THRESHOLD = 100_000
MAX_CONSECUTIVE_FAILURES = 5

BLOCK_POLL_INTERVAL = float(os.getenv("BLOCK_POLL_INTERVAL", "0.8"))
BLOCK_CATCHUP_CONCURRENCY = int(os.getenv("BLOCK_CATCHUP_CONCURRENCY", "4"))
BLOCK_MAX_CATCHUP = int(os.getenv("BLOCK_MAX_CATCHUP", "40"))

MULTISIG_SCAN_INTERVAL = int(os.getenv("MULTISIG_SCAN_INTERVAL", "15"))
ETH_MULTISIG_SCAN_INTERVAL = int(os.getenv("ETH_MULTISIG_SCAN_INTERVAL", "3"))

DB_PATH = os.getenv("DB_PATH", "blacklist_tracker.db")  # legacy sqlite, kept for migrate_to_pg.py
# Postgres (zarin-fd-db on 127.0.0.1:5435). Replaces the sqlite file above.
DATABASE_URL = os.getenv("DATABASE_URL", "")

logging.basicConfig(level=logging.INFO)
# httpx logs every request URL at INFO, API keys included.
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("USDTFreezeBot")
