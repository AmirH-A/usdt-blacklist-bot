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
BROADCAST_CHANNEL = os.getenv("BROADCAST_CHANNEL", "")

USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRONGRID_BASE_URL = "https://api.trongrid.io"
MAX_ALERTS_PER_CYCLE = 5

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("USDTFreezeBot")

DB_PATH = os.getenv("DB_PATH", "blacklist_tracker.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id INTEGER PRIMARY KEY,
            subscribed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blacklist (
            address TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
            confirmed_at TEXT,
            tx_hash TEXT,
            alerted INTEGER DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()


def get_db():
    return sqlite3.connect(DB_PATH)


# --- TRON API ---

async def get_blacklist_events(min_timestamp: Optional[int] = None) -> list:
    events = []
    url = f"{TRONGRID_BASE_URL}/v1/contracts/{USDT_CONTRACT}/events"
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}
    params = {
        "event_name": "AddedBlackList",
        "only_confirmed": "false",
        "limit": 200,
        "order_by": "block_timestamp,desc"
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
                        tron_addr = hex_to_tron_address("41" + address_hex.replace("0x", "").zfill(40))
                        if tron_addr:
                            events.append({
                                "address": tron_addr,
                                "tx_hash": event.get("transaction_id", ""),
                                "block_timestamp": event.get("block_timestamp", 0),
                                "confirmed": event.get("_unconfirmed", True) is False
                            })
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
        "visible": False
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


# --- Address Conversion ---

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


# --- Bot Commands ---

@router.message(Command("start"))
async def start_cmd(message: Message):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (message.chat.id,))
    conn.commit()
    conn.close()

    await message.answer(
        "*USDT Blacklist Monitor*\n\n"
        "Subscribed to TRON USDT blacklist alerts.\n\n"
        "*Alerts:*\n"
        "PENDING - Blacklist tx detected (unconfirmed)\n"
        "BLOCKED - Address officially frozen\n\n"
        "*Commands:*\n"
        "/check <address> - Check address status\n"
        "/recent - Recent blacklist events\n"
        "/stats - Statistics\n"
        "/stop - Unsubscribe",
        parse_mode="Markdown"
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
        await message.answer("Usage: /check <TRON\\_address>", parse_mode="Markdown")
        return

    address = args[1].strip()
    if not is_valid_tron_address(address):
        await message.answer("Invalid TRON address.")
        return

    await message.answer(f"Checking {address}...")

    is_blacklisted = await check_address_blacklisted(address)

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT status, detected_at, confirmed_at FROM blacklist WHERE address = ?", (address,))
    row = cursor.fetchone()
    conn.close()

    if is_blacklisted:
        confirmed_at = row[2] if row else "Unknown"
        await message.answer(
            f"*BLOCKED*\n\n"
            f"Address: `{address}`\n"
            f"Confirmed: {confirmed_at}\n\n"
            f"USDT is frozen.",
            parse_mode="Markdown"
        )
    elif row and row[0] == "pending":
        await message.answer(
            f"*PENDING*\n\n"
            f"Address: `{address}`\n"
            f"Detected: {row[1]}\n\n"
            f"Blacklist tx detected, not yet confirmed.",
            parse_mode="Markdown"
        )
    else:
        await message.answer(
            f"*SAFE*\n\n"
            f"Address: `{address}`\n\n"
            f"Not blacklisted.",
            parse_mode="Markdown"
        )


@router.message(Command("recent"))
async def recent_cmd(message: Message):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT address, status, detected_at FROM blacklist
        ORDER BY detected_at DESC LIMIT 10
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        await message.answer("No blacklist events detected yet.")
        return

    msg = "*Recent Blacklist Events:*\n\n"
    for addr, status, detected in rows:
        status_text = "BLOCKED" if status == "confirmed" else "PENDING"
        msg += f"`{addr[:12]}...{addr[-6:]}`\n"
        msg += f"{status_text} | {detected[:16]}\n\n"

    await message.answer(msg, parse_mode="Markdown")


@router.message(Command("stats"))
async def stats_cmd(message: Message):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE status = 'confirmed'")
    confirmed = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE status = 'pending'")
    pending = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM subscribers")
    subscribers = cursor.fetchone()[0]
    cursor.execute("SELECT value FROM sync_state WHERE key = 'last_scan'")
    last_scan = cursor.fetchone()
    conn.close()

    await message.answer(
        f"*Statistics*\n\n"
        f"Confirmed blocks: {confirmed}\n"
        f"Pending blocks: {pending}\n"
        f"Subscribers: {subscribers}\n"
        f"Last scan: {last_scan[0][:19] if last_scan else 'Never'}",
        parse_mode="Markdown"
    )


@router.message(Command("test"))
async def test_cmd(message: Message):
    await message.answer(
        "*TEST ALERT*\n\n"
        "Address:\n`TTestAddress123456789012345678901234`\n\n"
        "This is a test alert.\n\n"
        "If you see this, alerts are working.",
        parse_mode="Markdown"
    )


dp.include_router(router)


# --- Broadcast ---

async def broadcast_alert(address: str, status: str, tx_hash: str, is_update: bool = False):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id FROM subscribers")
    subscribers = cursor.fetchall()
    conn.close()

    if not subscribers:
        logger.info(f"No subscribers for {address}")
        return

    if status == "pending":
        title = "PENDING BLOCK DETECTED"
        desc = "Blacklist transaction submitted, not yet confirmed."
        action = "Waiting for confirmation..."
    else:
        title = "ADDRESS BLOCKED" if not is_update else "BLOCK CONFIRMED"
        desc = "Address officially blacklisted by Tether."
        action = "USDT transfers frozen."

    tronscan_link = f"https://tronscan.org/#/transaction/{tx_hash}"

    msg = (
        f"*{title}*\n\n"
        f"Address:\n`{address}`\n\n"
        f"{desc}\n\n"
        f"{action}\n\n"
        f"[View on Tronscan]({tronscan_link})"
    )

    if BROADCAST_CHANNEL:
        try:
            await bot.send_message(chat_id=BROADCAST_CHANNEL, text=msg, parse_mode="Markdown")
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


# --- Listener ---

async def listen_to_blacklist_events():
    init_db()

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sync_state WHERE key = 'initialized'")
    is_initialized = cursor.fetchone() is not None
    conn.close()

    while True:
        try:
            logger.info("Scanning TRON for blacklist events...")

            conn = get_db()
            cursor = conn.cursor()

            cursor.execute("SELECT value FROM sync_state WHERE key = 'last_block_timestamp'")
            row = cursor.fetchone()
            last_timestamp = int(row[0]) if row else None

            events = await get_blacklist_events(min_timestamp=last_timestamp)
            alerts_sent = 0

            for event in events:
                address = event["address"]
                tx_hash = event["tx_hash"]
                confirmed = event["confirmed"]
                status = "confirmed" if confirmed else "pending"

                cursor.execute("SELECT status, alerted FROM blacklist WHERE address = ?", (address,))
                existing = cursor.fetchone()

                if not existing:
                    should_alert = is_initialized and alerts_sent < MAX_ALERTS_PER_CYCLE
                    cursor.execute(
                        "INSERT INTO blacklist (address, status, tx_hash, alerted) VALUES (?, ?, ?, ?)",
                        (address, status, tx_hash, 1 if should_alert else 0)
                    )
                    conn.commit()
                    logger.info(f"NEW: {address} -> {status}")

                    if should_alert:
                        await broadcast_alert(address, status, tx_hash)
                        alerts_sent += 1
                        await asyncio.sleep(0.5)

                elif existing[0] == "pending" and status == "confirmed":
                    should_alert = alerts_sent < MAX_ALERTS_PER_CYCLE
                    cursor.execute(
                        "UPDATE blacklist SET status = 'confirmed', confirmed_at = CURRENT_TIMESTAMP, alerted = ? WHERE address = ?",
                        (1 if should_alert else existing[1], address)
                    )
                    conn.commit()
                    logger.info(f"UPDATE: {address} -> confirmed")

                    if should_alert:
                        await broadcast_alert(address, "confirmed", tx_hash, is_update=True)
                        alerts_sent += 1
                        await asyncio.sleep(0.5)

            if not is_initialized:
                cursor.execute("INSERT OR REPLACE INTO sync_state (key, value) VALUES ('initialized', '1')")
                is_initialized = True
                logger.info("Initial sync complete. Monitoring for NEW events.")

            cursor.execute(
                "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('last_scan', ?)",
                (datetime.now(timezone.utc).isoformat(),)
            )
            if events:
                max_ts = max(e["block_timestamp"] for e in events)
                cursor.execute(
                    "INSERT OR REPLACE INTO sync_state (key, value) VALUES ('last_block_timestamp', ?)",
                    (str(max_ts),)
                )

            conn.commit()
            conn.close()
            await asyncio.sleep(10)

        except Exception as e:
            logger.error(f"Listener error: {e}")
            await asyncio.sleep(15)


# --- FastAPI ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    listener_task = asyncio.create_task(listen_to_blacklist_events())

    if WEBHOOK_URL:
        await bot.set_webhook(
            url=f"{WEBHOOK_URL}/webhook",
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
        )
        logger.info(f"Webhook: {WEBHOOK_URL}/webhook")

    logger.info("Bot started. Monitoring TRON USDT blacklist...")
    yield

    listener_task.cancel()
    try:
        await listener_task
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
    return {"status": "ok", "contract": USDT_CONTRACT}


async def run_polling():
    init_db()
    listener_task = asyncio.create_task(listen_to_blacklist_events())
    logger.info("Starting bot...")
    try:
        await dp.start_polling(bot)
    finally:
        listener_task.cancel()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(run_polling())
