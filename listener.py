import os
import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message, Update
from aiogram.filters import Command

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
BROADCAST_CHANNEL = os.getenv("BROADCAST_CHANNEL", "")
ENABLE_TRON = os.getenv("ENABLE_TRON", "true").lower() == "true"
ENABLE_ETH = os.getenv("ENABLE_ETH", "true").lower() == "true"

TRON_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRONGRID_BASE_URL = "https://api.trongrid.io"

ETH_USDT_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
ETH_CHAIN_ID = 1  # Ethereum mainnet
ETH_BLACKLIST_TOPIC = (
    "0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc"
)

MAX_ALERTS_PER_CYCLE = 5

USDT_CONTRACT = TRON_USDT_CONTRACT

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("USDTFreezeBot")

DB_PATH = os.getenv("DB_PATH", "blacklist_tracker.db")


def migrate_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(blacklist)")
    columns = [col[1] for col in cursor.fetchall()]

    if "chain" not in columns:
        logger.info("Migrating database to multi-chain schema...")
        cursor.execute("ALTER TABLE blacklist ADD COLUMN chain TEXT DEFAULT 'TRON'")

        cursor.execute(
            "SELECT key, value FROM sync_state WHERE key IN ('initialized', 'last_scan', 'last_block_timestamp')"
        )
        rows = cursor.fetchall()
        for key, value in rows:
            cursor.execute(
                "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
                (f"tron_{key}", value),
            )
            cursor.execute("DELETE FROM sync_state WHERE key = ?", (key,))

        conn.commit()
        logger.info("Database migration complete.")

    conn.close()


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id INTEGER PRIMARY KEY,
            subscribed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS blacklist (
            address TEXT NOT NULL,
            chain TEXT NOT NULL DEFAULT 'TRON',
            status TEXT NOT NULL DEFAULT 'pending',
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
            confirmed_at TEXT,
            tx_hash TEXT,
            alerted INTEGER DEFAULT 0,
            PRIMARY KEY (address, chain)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """
    )
    conn.commit()
    conn.close()

    migrate_db()


def get_db():
    return sqlite3.connect(DB_PATH)


async def get_blacklist_events(min_timestamp: Optional[int] = None) -> list:
    events = []
    url = f"{TRONGRID_BASE_URL}/v1/contracts/{USDT_CONTRACT}/events"
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}
    params = {
        "event_name": "AddedBlackList",
        "only_confirmed": "false",
        "limit": 200,
        "order_by": "block_timestamp,desc",
    }
    if min_timestamp:
        params["min_block_timestamp"] = min_timestamp

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params=params, headers=headers, timeout=15)
            data = resp.json()
            if "data" in data:
                for event in data["data"]:
                    address_hex = event.get("result", {}).get("_user", "")
                    if address_hex:
                        tron_addr = hex_to_tron_address(
                            "41" + address_hex.replace("0x", "").zfill(40)
                        )
                        if tron_addr:
                            events.append(
                                {
                                    "address": tron_addr,
                                    "tx_hash": event.get("transaction_id", ""),
                                    "block_timestamp": event.get("block_timestamp", 0),
                                    "confirmed": not event.get("_unconfirmed", False),
                                }
                            )
    except Exception as e:
        logger.error(f"Error fetching events: {e}")
    return events


async def check_address_blacklisted(address: str) -> bool:
    hex_address = tron_address_to_hex(address)
    if not hex_address:
        return False
    padded_address = hex_address.zfill(64)
    url = f"{TRONGRID_BASE_URL}/wallet/triggersmartcontract"
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}
    payload = {
        "owner_address": "410000000000000000000000000000000000000000",
        "contract_address": tron_address_to_hex(USDT_CONTRACT, with_prefix=True),
        "function_selector": "isBlackListed(address)",
        "parameter": padded_address,
        "visible": False,
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, headers=headers, timeout=10)
            data = resp.json()
            if "constant_result" in data and data["constant_result"]:
                return data["constant_result"][0].endswith("1")
    except Exception as e:
        logger.error(f"Error checking blacklist: {e}")
    return False


async def get_tron_usdt_balance(address: str) -> Optional[float]:
    hex_address = tron_address_to_hex(address)
    if not hex_address:
        return None
    padded_address = hex_address.zfill(64)
    url = f"{TRONGRID_BASE_URL}/wallet/triggersmartcontract"
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}
    payload = {
        "owner_address": "410000000000000000000000000000000000000000",
        "contract_address": tron_address_to_hex(USDT_CONTRACT, with_prefix=True),
        "function_selector": "balanceOf(address)",
        "parameter": padded_address,
        "visible": False,
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, headers=headers, timeout=10)
            data = resp.json()
            if "constant_result" in data and data["constant_result"]:
                result_hex = data["constant_result"][0]
                balance_raw = int(result_hex, 16)
                return balance_raw / 1_000_000
    except Exception as e:
        logger.error(f"Error getting TRON balance: {e}")
    return None


BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def tron_address_to_hex(address: str, with_prefix: bool = False) -> Optional[str]:
    if not address or not address.startswith("T"):
        return None
    try:
        num = 0
        for char in address:
            num = num * 58 + BASE58_ALPHABET.index(char)
        hex_bytes = hex(num)[2:].zfill(50)
        return hex_bytes[:42] if with_prefix else hex_bytes[2:42]
    except Exception:
        return None


def hex_to_tron_address(hex_address: str) -> Optional[str]:
    import hashlib

    try:
        if not hex_address.startswith("41"):
            hex_address = "41" + hex_address
        address_bytes = bytes.fromhex(hex_address)
        hash1 = hashlib.sha256(address_bytes).digest()
        hash2 = hashlib.sha256(hash1).digest()
        checksum = hash2[:4]
        full_address = address_bytes + checksum
        num = int.from_bytes(full_address, "big")
        result = ""
        while num > 0:
            num, remainder = divmod(num, 58)
            result = BASE58_ALPHABET[remainder] + result
        for byte in full_address:
            if byte == 0:
                result = "1" + result
            else:
                break
        return result
    except Exception:
        return None


def is_valid_tron_address(address: str) -> bool:
    if not address or len(address) != 34 or not address.startswith("T"):
        return False
    return all(c in BASE58_ALPHABET for c in address)


def is_valid_eth_address(address: str) -> bool:
    """Validate Ethereum address format (0x + 40 hex chars)."""
    if not address or len(address) != 42 or not address.startswith("0x"):
        return False
    try:
        int(address[2:], 16)
        return True
    except ValueError:
        return False


def detect_chain(address: str) -> Optional[str]:
    """Auto-detect chain from address format."""
    if is_valid_tron_address(address):
        return "TRON"
    if is_valid_eth_address(address):
        return "ETH"
    return None


async def get_eth_blacklist_events(from_block: Optional[int] = None) -> list:
    events = []
    params = {
        "chainid": ETH_CHAIN_ID,
        "module": "logs",
        "action": "getLogs",
        "address": ETH_USDT_CONTRACT,
        "topic0": ETH_BLACKLIST_TOPIC,
        "apikey": ETHERSCAN_API_KEY,
    }
    if from_block:
        params["fromBlock"] = from_block
    else:
        params["fromBlock"] = 0

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ETHERSCAN_BASE_URL, params=params, timeout=15)
            data = resp.json()
            if data.get("status") == "1" and data.get("result"):
                for log in data["result"]:
                    data_hex = log.get("data", "")
                    if data_hex and len(data_hex) >= 42:
                        eth_address = "0x" + data_hex[-40:]
                        events.append(
                            {
                                "address": eth_address,
                                "tx_hash": log.get("transactionHash", ""),
                                "block_number": int(log.get("blockNumber", "0"), 16),
                                "confirmed": True,
                            }
                        )
    except Exception as e:
        logger.error(f"Error fetching ETH events: {e}")
    return events


async def check_eth_address_blacklisted(address: str) -> bool:
    if not is_valid_eth_address(address):
        return False

    padded_address = address[2:].lower().zfill(64)
    call_data = "0xe47d6060" + padded_address

    params = {
        "chainid": ETH_CHAIN_ID,
        "module": "proxy",
        "action": "eth_call",
        "to": ETH_USDT_CONTRACT,
        "data": call_data,
        "tag": "latest",
        "apikey": ETHERSCAN_API_KEY,
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ETHERSCAN_BASE_URL, params=params, timeout=10)
            data = resp.json()
            if "result" in data:
                return data["result"].endswith("1")
    except Exception as e:
        logger.error(f"Error checking ETH blacklist: {e}")
    return False


async def get_eth_usdt_balance(address: str) -> Optional[float]:
    if not is_valid_eth_address(address):
        return None

    padded_address = address[2:].lower().zfill(64)
    call_data = "0x70a08231" + padded_address

    params = {
        "chainid": ETH_CHAIN_ID,
        "module": "proxy",
        "action": "eth_call",
        "to": ETH_USDT_CONTRACT,
        "data": call_data,
        "tag": "latest",
        "apikey": ETHERSCAN_API_KEY,
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ETHERSCAN_BASE_URL, params=params, timeout=10)
            data = resp.json()
            if "result" in data and data["result"] != "0x":
                balance_raw = int(data["result"], 16)
                return balance_raw / 1_000_000
    except Exception as e:
        logger.error(f"Error getting ETH balance: {e}")
    return None


def format_usdt_balance(balance: Optional[float]) -> str:
    if balance is None:
        return "Unknown"
    if balance == 0:
        return "0 USDT"
    if balance >= 1_000_000:
        return f"{balance:,.0f} USDT"
    if balance >= 1000:
        return f"{balance:,.2f} USDT"
    return f"{balance:.2f} USDT"


@router.message(Command("start"))
async def start_cmd(message: Message):
    logger.info(
        f"Start command from chat_id: {message.chat.id} (type: {message.chat.type})"
    )

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (message.chat.id,)
    )
    conn.commit()
    conn.close()

    chains_enabled = []
    if ENABLE_TRON:
        chains_enabled.append("TRON")
    if ENABLE_ETH:
        chains_enabled.append("ETH")
    chains_text = " & ".join(chains_enabled) if chains_enabled else "None"

    await message.answer(
        "*USDT Blacklist Monitor*\n\n"
        f"Monitoring: {chains_text}\n"
        f"Subscribed to blacklist alerts.\n\n"
        "*Alerts:*\n"
        "PENDING - Blacklist tx detected (unconfirmed)\n"
        "BLOCKED - Address officially frozen\n\n"
        "*Commands:*\n"
        "/check <address> - Check address status\n"
        "  Supports TRON (T...) and ETH (0x...)\n"
        "/recent - Recent blacklist events\n"
        "/stats - Statistics\n"
        "/timing - Confirmation timing stats\n"
        "/stop - Unsubscribe",
        parse_mode="Markdown",
    )


@router.message(Command("stop"))
async def stop_cmd(message: Message):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM subscribers WHERE chat_id = ?", (message.chat.id,))
    conn.commit()
    conn.close()
    await message.answer("Unsubscribed. Use /start to subscribe again.")


@router.message(Command("check"))
async def check_cmd(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "Usage: /check <address>\nSupports TRON (T...) and ETH (0x...)",
            parse_mode="Markdown",
        )
        return

    address = args[1].strip()
    chain = detect_chain(address)

    if not chain:
        await message.answer("Invalid address. Use TRON (T...) or ETH (0x...) format.")
        return

    await message.answer(f"Checking {chain} address...")

    if chain == "ETH":
        is_blacklisted = await check_eth_address_blacklisted(address)
        balance = await get_eth_usdt_balance(address)
    else:
        is_blacklisted = await check_address_blacklisted(address)
        balance = await get_tron_usdt_balance(address)

    balance_text = format_usdt_balance(balance)

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status, detected_at, confirmed_at FROM blacklist WHERE address = ? AND chain = ?",
        (address, chain),
    )
    row = cursor.fetchone()
    conn.close()

    if is_blacklisted:
        confirmed_at = row[2] if row else "Unknown"
        await message.answer(
            f"*BLOCKED [{chain}]*\n\n"
            f"Address: `{address}`\n"
            f"Balance: *{balance_text}* (frozen)\n"
            f"Confirmed: {confirmed_at}\n\n"
            f"USDT is frozen.",
            parse_mode="Markdown",
        )
    elif row and row[0] == "pending":
        await message.answer(
            f"*PENDING [{chain}]*\n\n"
            f"Address: `{address}`\n"
            f"Balance at risk: *{balance_text}*\n"
            f"Detected: {row[1]}\n\n"
            f"Blacklist tx detected, not yet confirmed.",
            parse_mode="Markdown",
        )
    else:
        await message.answer(
            f"*SAFE [{chain}]*\n\n"
            f"Address: `{address}`\n"
            f"Balance: *{balance_text}*\n\n"
            f"Not blacklisted.",
            parse_mode="Markdown",
        )


@router.message(Command("recent"))
async def recent_cmd(message: Message):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT address, chain, status, detected_at FROM blacklist
        ORDER BY detected_at DESC LIMIT 10
    """
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await message.answer("No blacklist events detected yet.")
        return

    msg = "*Recent Blacklist Events:*\n\n"
    for addr, chain, status, detected in rows:
        status_text = "BLOCKED" if status == "confirmed" else "PENDING"
        msg += f"[{chain}] `{addr[:12]}...{addr[-6:]}`\n"
        msg += f"{status_text} | {detected[:16]}\n\n"

    await message.answer(msg, parse_mode="Markdown")


@router.message(Command("stats"))
async def stats_cmd(message: Message):
    conn = get_db()
    cursor = conn.cursor()

    # Overall stats
    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE status = 'confirmed'")
    confirmed = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE status = 'pending'")
    pending = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM subscribers")
    subscribers = cursor.fetchone()[0]

    # Per-chain stats
    cursor.execute(
        "SELECT COUNT(*) FROM blacklist WHERE chain = 'TRON' AND status = 'confirmed'"
    )
    tron_confirmed = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM blacklist WHERE chain = 'ETH' AND status = 'confirmed'"
    )
    eth_confirmed = cursor.fetchone()[0]

    # Last scans
    cursor.execute("SELECT value FROM sync_state WHERE key = 'tron_last_scan'")
    tron_scan = cursor.fetchone()
    cursor.execute("SELECT value FROM sync_state WHERE key = 'eth_last_scan'")
    eth_scan = cursor.fetchone()
    conn.close()

    msg = f"*Statistics*\n\n"
    msg += f"*Total:*\n"
    msg += f"Confirmed blocks: {confirmed}\n"
    msg += f"Pending blocks: {pending}\n"
    msg += f"Subscribers: {subscribers}\n\n"
    msg += f"*Per Chain:*\n"
    msg += f"TRON: {tron_confirmed} blocked\n"
    msg += f"ETH: {eth_confirmed} blocked\n\n"
    msg += f"*Last Scan:*\n"
    if ENABLE_TRON:
        msg += f"TRON: {tron_scan[0][:19] if tron_scan else 'Never'}\n"
    if ENABLE_ETH:
        msg += f"ETH: {eth_scan[0][:19] if eth_scan else 'Never'}"

    await message.answer(msg, parse_mode="Markdown")


@router.message(Command("test"))
async def test_cmd(message: Message):
    await message.answer(
        "*TEST ALERT*\n\n"
        "Address:\n`TTestAddress123456789012345678901234`\n\n"
        "This is a test alert.\n\n"
        "If you see this, alerts are working.",
        parse_mode="Markdown",
    )


@router.message(Command("timing"))
async def timing_cmd(message: Message):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT detected_at, confirmed_at FROM blacklist
        WHERE chain = 'TRON' AND confirmed_at IS NOT NULL AND detected_at IS NOT NULL
        ORDER BY confirmed_at DESC LIMIT 50
    """
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await message.answer(
            "No timing data available yet.\nWaiting for pending → confirmed transitions."
        )
        return

    times = []
    for detected_at, confirmed_at in rows:
        try:
            detected = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
            confirmed = datetime.fromisoformat(confirmed_at.replace("Z", "+00:00"))
            elapsed = (confirmed - detected).total_seconds()
            if elapsed > 0:
                times.append(elapsed)
        except Exception:
            pass

    if not times:
        await message.answer("No valid timing data available.")
        return

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    msg = "*TRON Confirmation Timing*\n\n"
    msg += f"Based on last {len(times)} blocks:\n\n"
    msg += f"Average: *{format_elapsed_time(avg_time)}*\n"
    msg += f"Fastest: *{format_elapsed_time(min_time)}*\n"
    msg += f"Slowest: *{format_elapsed_time(max_time)}*\n\n"
    msg += "_This is the time window you have to move funds after detecting a pending blacklist._"

    await message.answer(msg, parse_mode="Markdown")


dp.include_router(router)


def format_elapsed_time(seconds: Optional[float]) -> str:
    """Format elapsed time in human readable format."""
    if seconds is None:
        return "Unknown"
    if seconds < 60:
        return f"{int(seconds)} seconds"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}h {minutes}m"


async def broadcast_alert(
    address: str,
    status: str,
    tx_hash: str,
    chain: str = "TRON",
    balance: Optional[float] = None,
    elapsed_seconds: Optional[float] = None,
    is_update: bool = False,
):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id FROM subscribers")
    subscribers = cursor.fetchall()

    avg_confirm_time = None
    if chain == "TRON":
        cursor.execute(
            """
            SELECT detected_at, confirmed_at FROM blacklist
            WHERE chain = 'TRON' AND confirmed_at IS NOT NULL AND detected_at IS NOT NULL
            ORDER BY confirmed_at DESC LIMIT 20
        """
        )
        timing_rows = cursor.fetchall()
        if timing_rows:
            times = []
            for detected_at, confirmed_at in timing_rows:
                try:
                    detected = datetime.fromisoformat(
                        detected_at.replace("Z", "+00:00")
                    )
                    confirmed = datetime.fromisoformat(
                        confirmed_at.replace("Z", "+00:00")
                    )
                    elapsed = (confirmed - detected).total_seconds()
                    if elapsed > 0:
                        times.append(elapsed)
                except Exception:
                    pass
            if times:
                avg_confirm_time = sum(times) / len(times)

    conn.close()

    if not subscribers:
        logger.info(f"No subscribers for {address}")
        return

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if chain == "ETH":
        explorer_link = f"https://etherscan.io/tx/{tx_hash}"
        explorer_name = "Etherscan"
    else:
        explorer_link = f"https://tronscan.org/#/transaction/{tx_hash}"
        explorer_name = "Tronscan"

    balance_text = format_usdt_balance(balance)

    if status == "pending":
        title = f"⚠️ PENDING BLOCK DETECTED [{chain}]"
        msg = f"*{title}*\n\n"
        msg += f"Chain: {chain}\n"
        msg += f"Address:\n`{address}`\n\n"
        msg += f"Balance at risk: *{balance_text}*\n"
        msg += f"Detected at: {now_str}\n"
        if avg_confirm_time:
            msg += f"Avg confirm time: ~{format_elapsed_time(avg_confirm_time)}\n"
        msg += f"\nBlacklist transaction submitted, awaiting confirmation.\n\n"
        msg += f"[View TX on {explorer_name}]({explorer_link})"
    else:
        if is_update:
            title = f"🔒 BLOCK CONFIRMED [{chain}]"
            msg = f"*{title}*\n\n"
            msg += f"Chain: {chain}\n"
            msg += f"Address:\n`{address}`\n\n"
            msg += f"Balance frozen: *{balance_text}*\n"
            msg += f"Confirmed at: {now_str}\n"
            if elapsed_seconds is not None:
                msg += f"Time to confirm: *{format_elapsed_time(elapsed_seconds)}*\n"
            msg += f"\n🚫 Address officially blacklisted by Tether.\n"
            msg += f"USDT transfers are now frozen.\n\n"
            msg += f"[View TX on {explorer_name}]({explorer_link})"
        else:
            title = f"🔒 ADDRESS BLOCKED [{chain}]"
            msg = f"*{title}*\n\n"
            msg += f"Chain: {chain}\n"
            msg += f"Address:\n`{address}`\n\n"
            msg += f"Balance frozen: *{balance_text}*\n"
            msg += f"Blocked at: {now_str}\n"
            msg += f"\n🚫 Address officially blacklisted by Tether.\n"
            msg += f"USDT transfers are now frozen.\n\n"
            msg += f"[View TX on {explorer_name}]({explorer_link})"

    if BROADCAST_CHANNEL:
        try:
            await bot.send_message(
                chat_id=BROADCAST_CHANNEL, text=msg, parse_mode="Markdown"
            )
            logger.info(f"Sent to channel: {BROADCAST_CHANNEL}")
        except Exception as e:
            logger.error(f"Channel send failed: {e}")

    for (chat_id,) in subscribers:
        try:
            await bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
            logger.info(f"Alert sent to {chat_id}: {address[:10]}...")
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Failed to send to {chat_id}: {e}")


async def listen_to_tron_blacklist_events():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sync_state WHERE key = 'tron_initialized'")
    is_initialized = cursor.fetchone() is not None
    conn.close()

    while True:
        try:
            logger.info("Scanning TRON for blacklist events...")

            conn = get_db()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT value FROM sync_state WHERE key = 'tron_last_block_timestamp'"
            )
            row = cursor.fetchone()
            last_timestamp = int(row[0]) if row else None

            events = await get_blacklist_events(min_timestamp=last_timestamp)
            alerts_sent = 0

            for event in events:
                address = event["address"]
                tx_hash = event["tx_hash"]
                confirmed = event["confirmed"]
                status = "confirmed" if confirmed else "pending"

                cursor.execute(
                    "SELECT status, alerted, detected_at FROM blacklist WHERE address = ? AND chain = 'TRON'",
                    (address,),
                )
                existing = cursor.fetchone()

                if not existing:
                    should_alert = is_initialized and alerts_sent < MAX_ALERTS_PER_CYCLE
                    cursor.execute(
                        "INSERT INTO blacklist (address, chain, status, tx_hash, alerted) VALUES (?, 'TRON', ?, ?, ?)",
                        (address, status, tx_hash, 1 if should_alert else 0),
                    )
                    conn.commit()
                    logger.info(f"TRON NEW: {address} -> {status}")

                    if should_alert:
                        balance = await get_tron_usdt_balance(address)
                        await broadcast_alert(
                            address, status, tx_hash, chain="TRON", balance=balance
                        )
                        alerts_sent += 1
                        await asyncio.sleep(0.5)

                elif existing[0] == "pending" and status == "confirmed":
                    should_alert = alerts_sent < MAX_ALERTS_PER_CYCLE

                    elapsed_seconds = None
                    if existing[2]:  # detected_at
                        try:
                            detected_time = datetime.fromisoformat(
                                existing[2].replace("Z", "+00:00")
                            )
                            elapsed_seconds = (
                                datetime.now(timezone.utc) - detected_time
                            ).total_seconds()
                        except Exception:
                            pass

                    cursor.execute(
                        "UPDATE blacklist SET status = 'confirmed', confirmed_at = CURRENT_TIMESTAMP, alerted = ? WHERE address = ? AND chain = 'TRON'",
                        (1 if should_alert else existing[1], address),
                    )
                    conn.commit()
                    logger.info(
                        f"TRON UPDATE: {address} -> confirmed (elapsed: {elapsed_seconds:.0f}s)"
                        if elapsed_seconds
                        else f"TRON UPDATE: {address} -> confirmed"
                    )

                    if should_alert:
                        balance = await get_tron_usdt_balance(address)
                        await broadcast_alert(
                            address,
                            "confirmed",
                            tx_hash,
                            chain="TRON",
                            balance=balance,
                            elapsed_seconds=elapsed_seconds,
                            is_update=True,
                        )
                        alerts_sent += 1
                        await asyncio.sleep(0.5)

            if not is_initialized:
                cursor.execute(
                    "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('tron_initialized', '1')"
                )
                is_initialized = True
                logger.info("TRON initial sync complete. Monitoring for NEW events.")

            cursor.execute(
                "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('tron_last_scan', ?)",
                (datetime.now(timezone.utc).isoformat(),),
            )
            if events:
                max_ts = max(e["block_timestamp"] for e in events)
                cursor.execute(
                    "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('tron_last_block_timestamp', ?)",
                    (str(max_ts),),
                )

            conn.commit()
            conn.close()
            await asyncio.sleep(10)

        except Exception as e:
            logger.error(f"TRON listener error: {e}")
            await asyncio.sleep(15)


async def listen_to_eth_blacklist_events():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sync_state WHERE key = 'eth_initialized'")
    is_initialized = cursor.fetchone() is not None
    conn.close()

    while True:
        try:
            logger.info("Scanning ETH for blacklist events...")

            conn = get_db()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT value FROM sync_state WHERE key = 'eth_last_block_number'"
            )
            row = cursor.fetchone()
            last_block = int(row[0]) if row else None

            events = await get_eth_blacklist_events(from_block=last_block)
            alerts_sent = 0

            for event in events:
                address = event["address"]
                tx_hash = event["tx_hash"]
                status = "confirmed"

                cursor.execute(
                    "SELECT status, alerted FROM blacklist WHERE address = ? AND chain = 'ETH'",
                    (address,),
                )
                existing = cursor.fetchone()

                if not existing:
                    should_alert = is_initialized and alerts_sent < MAX_ALERTS_PER_CYCLE
                    cursor.execute(
                        "INSERT INTO blacklist (address, chain, status, tx_hash, alerted, confirmed_at) VALUES (?, 'ETH', ?, ?, ?, CURRENT_TIMESTAMP)",
                        (address, status, tx_hash, 1 if should_alert else 0),
                    )
                    conn.commit()
                    logger.info(f"ETH NEW: {address} -> {status}")

                    if should_alert:
                        balance = await get_eth_usdt_balance(address)
                        await broadcast_alert(
                            address, status, tx_hash, chain="ETH", balance=balance
                        )
                        alerts_sent += 1
                        await asyncio.sleep(0.5)

            if not is_initialized:
                cursor.execute(
                    "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('eth_initialized', '1')"
                )
                is_initialized = True
                logger.info("ETH initial sync complete. Monitoring for NEW events.")

            cursor.execute(
                "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('eth_last_scan', ?)",
                (datetime.now(timezone.utc).isoformat(),),
            )
            if events:
                max_block = max(e["block_number"] for e in events)
                cursor.execute(
                    "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('eth_last_block_number', ?)",
                    (str(max_block + 1),),
                )

            conn.commit()
            conn.close()
            await asyncio.sleep(15)

        except Exception as e:
            logger.error(f"ETH listener error: {e}")
            await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    listener_tasks = []

    if ENABLE_TRON:
        listener_tasks.append(asyncio.create_task(listen_to_tron_blacklist_events()))
        logger.info("TRON listener started")

    if ENABLE_ETH:
        listener_tasks.append(asyncio.create_task(listen_to_eth_blacklist_events()))
        logger.info("ETH listener started")

    if WEBHOOK_URL:
        await bot.set_webhook(
            url=f"{WEBHOOK_URL}/webhook",
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
        )
        logger.info(f"Webhook: {WEBHOOK_URL}/webhook")

    chains = []
    if ENABLE_TRON:
        chains.append("TRON")
    if ENABLE_ETH:
        chains.append("ETH")
    logger.info(f"Bot started. Monitoring {' & '.join(chains)} USDT blacklist...")
    yield

    for task in listener_tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    if WEBHOOK_URL:
        await bot.delete_webhook()
    await bot.session.close()


app = FastAPI(lifespan=lifespan)


@app.post("/webhook")
async def handle_webhook(
    request: Request, x_telegram_bot_api_secret_token: str = Header(None)
):
    if x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    update_data = await request.json()
    update = Update(**update_data)
    await dp.feed_update(bot=bot, update=update)
    return {"status": "ok"}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "chains": {
            "tron": {"enabled": ENABLE_TRON, "contract": TRON_USDT_CONTRACT},
            "eth": {"enabled": ENABLE_ETH, "contract": ETH_USDT_CONTRACT},
        },
    }


async def run_polling():
    init_db()
    listener_tasks = []

    if ENABLE_TRON:
        listener_tasks.append(asyncio.create_task(listen_to_tron_blacklist_events()))
        logger.info("TRON listener started")

    if ENABLE_ETH:
        listener_tasks.append(asyncio.create_task(listen_to_eth_blacklist_events()))
        logger.info("ETH listener started")

    chains = []
    if ENABLE_TRON:
        chains.append("TRON")
    if ENABLE_ETH:
        chains.append("ETH")
    logger.info(f"Starting bot. Monitoring {' & '.join(chains)}...")

    try:
        await dp.start_polling(bot)
    finally:
        for task in listener_tasks:
            task.cancel()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(run_polling())
