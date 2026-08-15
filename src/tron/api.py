import asyncio
from typing import Optional, List, Dict, Any

import httpx

from ..config import (
    TRONGRID_BASE_URL,
    TRONGRID_API_KEY,
    TRON_USDT_CONTRACT,
    TIMESTAMP_OVERLAP_MS,
    logger,
)
from .address import tron_address_to_hex, hex_to_tron_address


def _get_headers() -> dict:
    return {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}


async def get_blacklist_events(
    min_timestamp: Optional[int] = None, force_full_scan: bool = False
) -> List[Dict[str, Any]]:
    events = []
    seen_addresses = set()
    url = f"{TRONGRID_BASE_URL}/v1/contracts/{TRON_USDT_CONTRACT}/events"

    query_timestamp = None
    if min_timestamp and not force_full_scan:
        query_timestamp = max(0, min_timestamp - TIMESTAMP_OVERLAP_MS)

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            params = {
                "event_name": "AddedBlackList",
                "only_confirmed": "false",
                "limit": 200,
                "order_by": "block_timestamp,desc",
            }
            if query_timestamp:
                params["min_block_timestamp"] = query_timestamp

            resp = await client.get(url, params=params, headers=_get_headers())
            if resp.status_code == 200:
                data = resp.json()
                if "data" in data:
                    for event in data["data"]:
                        address_hex = event.get("result", {}).get("_user", "")
                        if address_hex:
                            tron_addr = hex_to_tron_address(
                                "41" + address_hex.replace("0x", "").zfill(40)
                            )
                            if tron_addr and tron_addr not in seen_addresses:
                                seen_addresses.add(tron_addr)
                                events.append({
                                    "address": tron_addr,
                                    "tx_hash": event.get("transaction_id", ""),
                                    "block_timestamp": event.get("block_timestamp", 0),
                                    "confirmed": not event.get("_unconfirmed", False),
                                })
            elif resp.status_code == 429:
                logger.warning("TRON rate limited (429) - will retry next cycle")
            else:
                logger.warning(f"TRON query returned {resp.status_code}")
    except Exception as e:
        logger.error(f"Error fetching events: {e}")

    return events


async def get_unfreeze_events(
    min_timestamp: Optional[int] = None, force_full_scan: bool = False
) -> List[Dict[str, Any]]:
    events = []
    seen_addresses = set()
    url = f"{TRONGRID_BASE_URL}/v1/contracts/{TRON_USDT_CONTRACT}/events"

    query_timestamp = None
    if min_timestamp and not force_full_scan:
        query_timestamp = max(0, min_timestamp - TIMESTAMP_OVERLAP_MS)

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            params = {
                "event_name": "RemovedBlackList",
                "only_confirmed": "false",
                "limit": 200,
                "order_by": "block_timestamp,desc",
            }
            if query_timestamp:
                params["min_block_timestamp"] = query_timestamp

            resp = await client.get(url, params=params, headers=_get_headers())
            if resp.status_code == 200:
                data = resp.json()
                if "data" in data:
                    for event in data["data"]:
                        address_hex = event.get("result", {}).get("_user", "")
                        if address_hex:
                            tron_addr = hex_to_tron_address(
                                "41" + address_hex.replace("0x", "").zfill(40)
                            )
                            if tron_addr and tron_addr not in seen_addresses:
                                seen_addresses.add(tron_addr)
                                events.append({
                                    "address": tron_addr,
                                    "tx_hash": event.get("transaction_id", ""),
                                    "block_timestamp": event.get("block_timestamp", 0),
                                    "confirmed": not event.get("_unconfirmed", False),
                                })
            elif resp.status_code == 429:
                logger.warning("TRON unfreeze rate limited (429)")
    except Exception as e:
        logger.error(f"Error fetching unfreeze events: {e}")

    return events


async def check_address_blacklisted(address: str) -> bool:
    hex_address = tron_address_to_hex(address)
    if not hex_address:
        return False

    padded_address = hex_address.zfill(64)
    url = f"{TRONGRID_BASE_URL}/wallet/triggersmartcontract"
    payload = {
        "owner_address": "410000000000000000000000000000000000000000",
        "contract_address": tron_address_to_hex(TRON_USDT_CONTRACT, with_prefix=True),
        "function_selector": "isBlackListed(address)",
        "parameter": padded_address,
        "visible": False,
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, headers=_get_headers(), timeout=10)
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
    payload = {
        "owner_address": "410000000000000000000000000000000000000000",
        "contract_address": tron_address_to_hex(TRON_USDT_CONTRACT, with_prefix=True),
        "function_selector": "balanceOf(address)",
        "parameter": padded_address,
        "visible": False,
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, headers=_get_headers(), timeout=10)
            data = resp.json()
            if "constant_result" in data and data["constant_result"]:
                result_hex = data["constant_result"][0]
                balance_raw = int(result_hex, 16)
                return balance_raw / 1_000_000
    except Exception as e:
        logger.error(f"Error getting TRON balance: {e}")

    return None


async def get_current_tron_block_timestamp() -> Optional[int]:
    url = f"{TRONGRID_BASE_URL}/wallet/getnowblock"

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=_get_headers(), timeout=10)
            data = resp.json()
            if "block_header" in data and "raw_data" in data["block_header"]:
                return data["block_header"]["raw_data"].get("timestamp", 0)
    except Exception as e:
        logger.error(f"Error getting current block timestamp: {e}")

    return None


async def get_address_blacklist_timestamp(address: str) -> Optional[int]:
    url = f"{TRONGRID_BASE_URL}/v1/contracts/{TRON_USDT_CONTRACT}/events"
    params = {
        "event_name": "AddedBlackList",
        "only_confirmed": "true",
        "limit": 200,
        "order_by": "block_timestamp,desc",
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params=params, headers=_get_headers(), timeout=15)
            data = resp.json()
            if "data" in data:
                for event in data["data"]:
                    address_hex = event.get("result", {}).get("_user", "")
                    if address_hex:
                        tron_addr = hex_to_tron_address(
                            "41" + address_hex.replace("0x", "").zfill(40)
                        )
                        if tron_addr == address:
                            return event.get("block_timestamp", 0)
    except Exception as e:
        logger.error(f"Error getting blacklist timestamp: {e}")

    return None
