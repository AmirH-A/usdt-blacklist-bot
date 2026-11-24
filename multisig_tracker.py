#!/usr/bin/env python3
"""
Tether MultiSig Transaction Tracker

Monitors MultiSig wallet submissions and executions on TRON and ETH,
and logs all transactions to Google Sheets.
"""

import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
import gspread
import psycopg2
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv("multisig_tracker.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("MultiSigTracker")

TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

TRONGRID_BASE_URL = "https://api.trongrid.io"
ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
ETH_CHAIN_ID = 1

TRON_MULTISIG_WALLET = "TBPxhVAsuzoFnKyXtc1o2UySEydPHgATto"
ETH_MULTISIG_WALLET = "0xC6CDE7C39eB2f0F0095F41570af89eFC2C1Ea828"

TRON_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRON_USDT_HEX = "a614f803b6fd780986a42c78ec9c7f77e6ded13c"
ETH_USDT_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"

ETH_SUBMISSION_TOPIC = "0xc0ba8fe4b176c1714197d43b9cc6bcf797a4a7461c5fe8d0ef6e184ae7601e51"
ETH_EXECUTION_TOPIC = "0x33e13ecb54c3076d8e8bb8c2881800a4d972b792045ffae98fdf46df365fed75"

SUBMIT_TRANSACTION_SELECTOR = "c6427474"
ADDBLACKLIST_SELECTOR = "0ecb93c0"
REMOVEBLACKLIST_SELECTOR = "e4997dc5"

GOOGLE_SHEETS_CREDS_FILE = os.getenv("GOOGLE_SHEETS_CREDS_FILE", "google_sheets_creds.json")
GOOGLE_SHEET_ID = os.getenv("MULTISIG_SHEET_ID", "")

SCAN_INTERVAL = 5

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

sheets_client: Optional[gspread.Worksheet] = None
processed_tron_txs: set = set()
processed_eth_txs: set = set()


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


def init_google_sheets() -> Optional[gspread.Worksheet]:
    global sheets_client

    if not GOOGLE_SHEET_ID:
        logger.warning("MULTISIG_SHEET_ID not set, Google Sheets logging disabled")
        return None

    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(GOOGLE_SHEETS_CREDS_FILE, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

        try:
            sheets_client = spreadsheet.worksheet("MultiSig")
        except gspread.exceptions.WorksheetNotFound:
            sheets_client = spreadsheet.add_worksheet(title="MultiSig", rows=1000, cols=10)
            sheets_client.append_row([
                "Timestamp",
                "Chain",
                "Event Type",
                "TX ID",
                "Target Address",
                "Action",
                "TX Hash",
                "Block",
                "Status"
            ])

        logger.info(f"Google Sheets connected: {spreadsheet.title}")
        return sheets_client
    except Exception as e:
        logger.error(f"Failed to connect to Google Sheets: {e}")
        return None


DB_URL = os.getenv("MULTISIG_DATABASE_URL", "")
_db = None
EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS multisig_events (
    id              bigserial PRIMARY KEY,
    logged_at       timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),  -- UTC, when recorded
    chain           text NOT NULL,
    event_type      text NOT NULL,          -- Submission | Execution
    tx_id           text NOT NULL DEFAULT '',
    target_address  text NOT NULL DEFAULT '',
    action          text NOT NULL DEFAULT '',
    tx_hash         text NOT NULL,
    block           bigint,
    block_timestamp bigint,                 -- ms; set by the tracker
    status          text NOT NULL DEFAULT '',
    source          text NOT NULL DEFAULT 'tracker',  -- tracker | sheet
    UNIQUE (chain, event_type, tx_hash)
);
CREATE INDEX IF NOT EXISTS idx_multisig_events_logged_at ON multisig_events (logged_at DESC);
"""


def _db_conn():
    """The events database, reconnecting after a failure; None when unavailable."""
    global _db
    if _db is not None and not _db.closed:
        return _db
    if not DB_URL:
        return None
    try:
        _db = psycopg2.connect(DB_URL, connect_timeout=5)
        _db.autocommit = True
        with _db.cursor() as cur:
            cur.execute(EVENTS_DDL)
        return _db
    except psycopg2.Error as e:
        logger.error(f"MultiSig events DB unavailable: {e}")
        _db = None
        return None


def save_event(chain, event_type, tx_id, target_address, action, tx_hash, block, status,
               block_timestamp=None) -> Optional[bool]:
    """True: a new event was saved; False: already recorded; None: DB unavailable."""
    global _db
    conn = _db_conn()
    if not conn:
        return None
    try:
        block_n = int(block) if str(block).strip().isdigit() else None
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO multisig_events (chain, event_type, tx_id, target_address, action,
                       tx_hash, block, block_timestamp, status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (chain, event_type, tx_hash) DO NOTHING""",
                (chain, event_type, str(tx_id), target_address or "", action or "", tx_hash,
                 block_n, block_timestamp, status),
            )
            return cur.rowcount == 1
    except psycopg2.Error as e:
        logger.error(f"Saving MultiSig event failed: {e}")
        try:
            conn.close()
        except Exception:
            pass
        _db = None
        return None


def _resume_value(chain: str, column: str) -> Optional[int]:
    """Newest block_timestamp or block the tracker itself recorded for a chain."""
    conn = _db_conn()
    if not conn or column not in ("block_timestamp", "block"):
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT max({column}) FROM multisig_events WHERE chain = %s AND source = 'tracker'", (chain,))
            v = cur.fetchone()[0]
            return int(v) if v is not None else None
    except psycopg2.Error as e:
        logger.error(f"Reading resume point failed: {e}")
        return None


def record_event(chain, event_type, tx_id, target_address, action, tx_hash, block, status,
                 block_timestamp=None):
    """The database is the record; the sheet gets a row only for a newly saved event
    (or when the database is unavailable), so a resume never duplicates sheet rows."""
    saved = save_event(chain, event_type, tx_id, target_address, action, tx_hash, block, status,
                       block_timestamp)
    if saved is False:
        return
    _append_sheet(chain, event_type, tx_id, target_address, action, tx_hash, block, status)


def _append_sheet(
    chain: str,
    event_type: str,
    tx_id: int,
    target_address: str,
    action: str,
    tx_hash: str,
    block: str,
    status: str
):
    global sheets_client

    if not sheets_client:
        sheets_client = init_google_sheets()
        if not sheets_client:
            return

    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        row = [
            timestamp,
            chain,
            event_type,
            str(tx_id),
            target_address,
            action,
            tx_hash,
            block,
            status
        ]

        sheets_client.append_row(row, value_input_option='USER_ENTERED')
        logger.info(f"Logged to sheets: {chain} {event_type} TX#{tx_id} -> {target_address[:16]}...")

    except gspread.exceptions.APIError as e:
        logger.error(f"Sheets API error: {e}")
        sheets_client = None
    except Exception as e:
        logger.error(f"Error logging to sheets: {e}")


def decode_tron_multisig_data(data: str) -> Optional[dict]:
    try:
        if not data or len(data) < 8:
            return None

        selector = data[:8].lower()
        if selector != SUBMIT_TRANSACTION_SELECTOR:
            return None

        params = data[8:]
        if len(params) < 256:
            return None

        destination_hex = params[24:64]

        if destination_hex.lower() != TRON_USDT_HEX.lower():
            return None

        bytes_offset = int(params[128:192], 16) * 2
        bytes_length_pos = bytes_offset

        if len(params) < bytes_length_pos + 64:
            return None

        bytes_length = int(params[bytes_length_pos:bytes_length_pos + 64], 16) * 2
        bytes_data_start = bytes_length_pos + 64

        if len(params) < bytes_data_start + bytes_length:
            return None

        inner_data = params[bytes_data_start:bytes_data_start + bytes_length]

        if len(inner_data) < 72:
            return None

        inner_selector = inner_data[:8].lower()

        if inner_selector == ADDBLACKLIST_SELECTOR:
            action = "addBlackList"
        elif inner_selector == REMOVEBLACKLIST_SELECTOR:
            action = "removeBlackList"
        else:
            return None

        address_param = inner_data[8:72]
        target_hex = "41" + address_param[-40:]
        target_address = hex_to_tron_address(target_hex)

        if not target_address:
            return None

        return {
            "action": action,
            "target_address": target_address,
        }

    except Exception as e:
        logger.debug(f"Error decoding TRON MultiSig data: {e}")
        return None


async def get_tron_multisig_events(event_name: str, min_timestamp: Optional[int] = None) -> list:
    events = []
    url = f"{TRONGRID_BASE_URL}/v1/contracts/{TRON_MULTISIG_WALLET}/events"
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}

    params = {
        "event_name": event_name,
        "only_confirmed": "false",
        "limit": 50,
        "order_by": "block_timestamp,desc",
    }

    if min_timestamp:
        params["min_block_timestamp"] = min_timestamp

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                for event in data.get("data", []):
                    tx_id = event.get("result", {}).get("transactionId")
                    if tx_id:
                        events.append({
                            "transaction_id": int(tx_id),
                            "tx_hash": event.get("transaction_id"),
                            "block_timestamp": event.get("block_timestamp"),
                            "block_number": event.get("block_number"),
                        })
    except Exception as e:
        logger.error(f"Error fetching TRON {event_name} events: {e}")

    return events


async def get_tron_tx_details(tx_hash: str) -> Optional[dict]:
    url = f"{TRONGRID_BASE_URL}/wallet/gettransactionbyid"
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json={"value": tx_hash}, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                if data and "raw_data" in data:
                    return data
    except Exception as e:
        logger.debug(f"Error getting TRON tx details: {e}")

    return None


async def get_eth_multisig_events(topic: str, from_block: Optional[int] = None) -> list:
    events = []
    params = {
        "chainid": ETH_CHAIN_ID,
        "module": "logs",
        "action": "getLogs",
        "address": ETH_MULTISIG_WALLET,
        "topic0": topic,
        "apikey": ETHERSCAN_API_KEY,
        "fromBlock": from_block if from_block else 0,
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ETHERSCAN_BASE_URL, params=params, timeout=15)
            data = resp.json()
            if data.get("status") == "1" and data.get("result"):
                for log in data["result"]:
                    tx_id = int(log["topics"][1], 16) if len(log["topics"]) > 1 else 0
                    events.append({
                        "transaction_id": tx_id,
                        "tx_hash": log["transactionHash"],
                        "block_number": int(log["blockNumber"], 16),
                        "block_timestamp": int(log.get("timeStamp", "0"), 16) * 1000,
                    })
    except Exception as e:
        logger.error(f"Error fetching ETH MultiSig events: {e}")

    return events


async def get_eth_tx_target(tx_hash: str) -> Optional[dict]:
    params = {
        "chainid": ETH_CHAIN_ID,
        "module": "proxy",
        "action": "eth_getTransactionByHash",
        "txhash": tx_hash,
        "apikey": ETHERSCAN_API_KEY,
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ETHERSCAN_BASE_URL, params=params, timeout=15)
            data = resp.json()
            result = data.get("result", {})
            if not result:
                return None

            input_data = result.get("input", "")
            if not input_data or len(input_data) < 10:
                return None

            selector = input_data[2:10].lower()
            if selector != SUBMIT_TRANSACTION_SELECTOR:
                return None

            if len(input_data) < 202:
                return None

            try:
                data_offset = int(input_data[138:202], 16)
                length_pos = 10 + (data_offset * 2)
                data_length = int(input_data[length_pos:length_pos + 64], 16)
                data_start = length_pos + 64
                inner_data = input_data[data_start:data_start + data_length * 2]

                if len(inner_data) >= 8:
                    inner_selector = inner_data[:8].lower()
                    if inner_selector in (ADDBLACKLIST_SELECTOR, REMOVEBLACKLIST_SELECTOR):
                        if len(inner_data) >= 72:
                            address_hex = inner_data[32:72]
                            target = "0x" + address_hex[-40:].lower()
                            action = "addBlackList" if inner_selector == ADDBLACKLIST_SELECTOR else "removeBlackList"
                            return {"target_address": target, "action": action}
            except (ValueError, IndexError):
                pass

    except Exception as e:
        logger.error(f"Error getting ETH tx target: {e}")

    return None


async def get_current_eth_block() -> Optional[int]:
    params = {
        "chainid": ETH_CHAIN_ID,
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": ETHERSCAN_API_KEY,
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ETHERSCAN_BASE_URL, params=params, timeout=15)
            data = resp.json()
            return int(data.get("result", "0x0"), 16)
    except Exception:
        return None


async def monitor_tron_multisig():
    logger.info("Starting TRON MultiSig monitor")

    # Resume from the last event recorded, so a restart loses nothing;
    # without one, look back 15 minutes (re-seen events are deduplicated).
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    last_timestamp = max(now_ms - 86_400_000, _resume_value("TRON", "block_timestamp") or now_ms - 900_000)
    logger.info(f"TRON MultiSig monitor resuming from block time {last_timestamp}")

    while True:
        try:
            submissions = await get_tron_multisig_events("Submission", min_timestamp=last_timestamp)
            executions = await get_tron_multisig_events("Execution", min_timestamp=last_timestamp)

            for event in submissions:
                tx_key = f"tron_sub_{event['tx_hash']}"
                if tx_key in processed_tron_txs:
                    continue

                tx_details = await get_tron_tx_details(event["tx_hash"])
                if not tx_details:
                    continue

                contracts = tx_details.get("raw_data", {}).get("contract", [])
                if not contracts:
                    processed_tron_txs.add(tx_key)
                    continue

                contract = contracts[0]
                param = contract.get("parameter", {}).get("value", {})
                data = param.get("data", "")

                decoded = decode_tron_multisig_data(data)
                if decoded:
                    logger.info(f"TRON Submission: TX#{event['transaction_id']} -> {decoded['action']} {decoded['target_address']}")

                    record_event(
                        chain="TRON",
                        event_type="Submission",
                        tx_id=event["transaction_id"],
                        target_address=decoded["target_address"],
                        action=decoded["action"],
                        tx_hash=event["tx_hash"],
                        block=str(event["block_number"]),
                        status="pending",
                        block_timestamp=event.get("block_timestamp"),
                    )

                processed_tron_txs.add(tx_key)

            for event in executions:
                tx_key = f"tron_exec_{event['tx_hash']}"
                if tx_key in processed_tron_txs:
                    continue

                logger.info(f"TRON Execution: TX#{event['transaction_id']}")

                record_event(
                    chain="TRON",
                    event_type="Execution",
                    tx_id=event["transaction_id"],
                    target_address="",
                    action="",
                    tx_hash=event["tx_hash"],
                    block=str(event["block_number"]),
                    status="executed",
                    block_timestamp=event.get("block_timestamp"),
                )

                processed_tron_txs.add(tx_key)

            if submissions or executions:
                all_events = submissions + executions
                max_ts = max(e["block_timestamp"] for e in all_events if e.get("block_timestamp"))
                if max_ts > last_timestamp:
                    last_timestamp = max_ts

            if len(processed_tron_txs) > 10000:
                processed_tron_txs.clear()

            await asyncio.sleep(SCAN_INTERVAL)

        except Exception as e:
            logger.error(f"TRON monitor error: {e}")
            await asyncio.sleep(10)


async def monitor_eth_multisig():
    logger.info("Starting ETH MultiSig monitor")

    current_block = await get_current_eth_block()
    if not current_block:
        logger.error("Could not get current ETH block")
        await asyncio.sleep(10)
        current_block = 0

    # Resume from the last block recorded (at most ~1 day back); re-seen
    # events are deduplicated by the database.
    resume = _resume_value("ETH", "block")
    last_block = max(current_block - 7200, min(current_block - 100, resume or current_block - 100))
    logger.info(f"ETH MultiSig monitor resuming from block {last_block}")

    while True:
        try:
            submissions = await get_eth_multisig_events(ETH_SUBMISSION_TOPIC, from_block=last_block)
            executions = await get_eth_multisig_events(ETH_EXECUTION_TOPIC, from_block=last_block)

            for event in submissions:
                tx_key = f"eth_sub_{event['tx_hash']}"
                if tx_key in processed_eth_txs:
                    continue

                target_info = await get_eth_tx_target(event["tx_hash"])

                if target_info:
                    logger.info(f"ETH Submission: TX#{event['transaction_id']} -> {target_info['action']} {target_info['target_address']}")

                    record_event(
                        chain="ETH",
                        event_type="Submission",
                        tx_id=event["transaction_id"],
                        target_address=target_info["target_address"],
                        action=target_info["action"],
                        tx_hash=event["tx_hash"],
                        block=str(event["block_number"]),
                        status="pending"
                    )

                processed_eth_txs.add(tx_key)

            for event in executions:
                tx_key = f"eth_exec_{event['tx_hash']}"
                if tx_key in processed_eth_txs:
                    continue

                logger.info(f"ETH Execution: TX#{event['transaction_id']}")

                record_event(
                    chain="ETH",
                    event_type="Execution",
                    tx_id=event["transaction_id"],
                    target_address="",
                    action="",
                    tx_hash=event["tx_hash"],
                    block=str(event["block_number"]),
                    status="executed"
                )

                processed_eth_txs.add(tx_key)

            if submissions or executions:
                all_events = submissions + executions
                max_block = max(e["block_number"] for e in all_events)
                if max_block >= last_block:
                    last_block = max_block + 1

            if len(processed_eth_txs) > 10000:
                processed_eth_txs.clear()

            await asyncio.sleep(SCAN_INTERVAL)

        except Exception as e:
            logger.error(f"ETH monitor error: {e}")
            await asyncio.sleep(10)


async def main():
    print("=" * 50)
    print("TETHER MULTISIG TRANSACTION TRACKER")
    print("=" * 50)
    print(f"TRON MultiSig: {TRON_MULTISIG_WALLET}")
    print(f"ETH MultiSig:  {ETH_MULTISIG_WALLET}")
    print("=" * 50)

    init_google_sheets()

    await asyncio.gather(
        monitor_tron_multisig(),
        monitor_eth_multisig(),
    )


if __name__ == "__main__":
    asyncio.run(main())
