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

ENABLE_TRON = os.getenv("ENABLE_TRON", "true").lower() == "true"
ENABLE_ETH = os.getenv("ENABLE_ETH", "true").lower() == "true"

TRON_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRON_USDT_HEX = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
TRONGRID_BASE_URL = "https://api.trongrid.io"

# Chainstack gRPC endpoint for low-latency access
CHAINSTACK_GRPC_HOST = os.getenv("CHAINSTACK_GRPC_HOST", "tron-mainnet.core.chainstack.com")
CHAINSTACK_GRPC_PORT = int(os.getenv("CHAINSTACK_GRPC_PORT", "443"))
CHAINSTACK_API_TOKEN = os.getenv("CHAINSTACK_API_TOKEN", "")
USE_GRPC = os.getenv("USE_GRPC", "true").lower() == "true"

ETH_USDT_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
ETH_CHAIN_ID = 1
ETH_BLACKLIST_TOPIC = "0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc"

TRON_MULTISIG_WALLET = "TBPxhVAsuzoFnKyXtc1o2UySEydPHgATto"
TRON_MULTISIG_HEX = "410fa695d6b065707cb4e0ef73b751c93347682bf2"
ETH_MULTISIG_WALLET = "0xC6CDE7C39eB2f0F0095F41570af89eFC2C1Ea828"

ETH_SUBMISSION_TOPIC = "0xc0ba8fe4b176c1714197d43b9cc6bcf797a4a7461c5fe8d0ef6e184ae7601e51"
ETH_EXECUTION_TOPIC = "0x33e13ecb54c3076d8e8bb8c2881800a4d972b792045ffae98fdf46df365fed75"

ADDBLACKLIST_SELECTOR = "0ecb93c0"
REMOVEBLACKLIST_SELECTOR = "e4997dc5"
SUBMIT_TRANSACTION_SELECTOR = "c6427474"
EXECUTE_TRANSACTION_SELECTOR = "ee22610b"

MAX_ALERTS_PER_CYCLE = 50
WHALE_THRESHOLD = 100_000
MAX_CONSECUTIVE_FAILURES = 5

# Live TRON path: poll new blocks (~3s chain time) and decode in-block.
BLOCK_POLL_INTERVAL = float(os.getenv("BLOCK_POLL_INTERVAL", "0.8"))
BLOCK_CATCHUP_CONCURRENCY = int(os.getenv("BLOCK_CATCHUP_CONCURRENCY", "4"))
BLOCK_MAX_CATCHUP = int(os.getenv("BLOCK_MAX_CATCHUP", "40"))

# TronGrid event API is a safety net only, not the hot path.
MULTISIG_SCAN_INTERVAL = int(os.getenv("MULTISIG_SCAN_INTERVAL", "15"))
ETH_MULTISIG_SCAN_INTERVAL = int(os.getenv("ETH_MULTISIG_SCAN_INTERVAL", "3"))

DB_PATH = os.getenv("DB_PATH", "blacklist_tracker.db")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("USDTFreezeBot")
