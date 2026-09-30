import asyncio
from typing import Optional, List, Dict, Any

import httpx

try:
    from .grpc_client import grpc_get_transaction_by_id
except ImportError:
    grpc_get_transaction_by_id = None

from ..config import (
    TRONGRID_BASE_URL,
    TRONGRID_API_KEY,
    TRON_MULTISIG_WALLET,
    TRON_USDT_HEX,
    TRON_USDT_CONTRACT,
    SUBMIT_TRANSACTION_SELECTOR,
    EXECUTE_TRANSACTION_SELECTOR,
    CONFIRM_TRANSACTION_SELECTOR,
    ADDBLACKLIST_SELECTOR,
    REMOVEBLACKLIST_SELECTOR,
    logger,
)
from .address import hex_to_tron_address


def _get_headers() -> dict:
    return {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}


def decode_multisig_submit_data(data: str) -> Optional[dict]:
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
        usdt_hex_no_prefix = (
            TRON_USDT_HEX[2:] if TRON_USDT_HEX.startswith("41") else TRON_USDT_HEX
        )

        if destination_hex.lower() != usdt_hex_no_prefix.lower():
            return None

        bytes_offset = int(params[128:192], 16) * 2
        bytes_length_pos = bytes_offset

        if len(params) < bytes_length_pos + 64:
            return None

        bytes_length = int(params[bytes_length_pos : bytes_length_pos + 64], 16) * 2
        bytes_data_start = bytes_length_pos + 64

        if len(params) < bytes_data_start + bytes_length:
            return None

        inner_data = params[bytes_data_start : bytes_data_start + bytes_length]

        if len(inner_data) < 72:
            return None

        inner_selector = inner_data[:8].lower()

        if inner_selector == ADDBLACKLIST_SELECTOR:
            action = "blacklist"
        elif inner_selector == REMOVEBLACKLIST_SELECTOR:
            action = "unblacklist"
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
            "destination": TRON_USDT_CONTRACT,
        }

    except Exception as e:
        logger.debug(f"Error decoding MultiSig data: {e}")
        return None


def decode_execute_transaction_data(data: str) -> Optional[int]:
    return _decode_uint256_call(data, EXECUTE_TRANSACTION_SELECTOR)


def decode_confirm_transaction_data(data: str) -> Optional[int]:
    return _decode_uint256_call(data, CONFIRM_TRANSACTION_SELECTOR)


def _decode_uint256_call(data: str, selector: str) -> Optional[int]:
    try:
        if not data:
            return None
        if data.startswith("0x"):
            data = data[2:]
        if len(data) < 72:
            return None
        if data[:8].lower() != selector.lower():
            return None
        return int(data[8:72], 16)
    except Exception:
        return None


async def get_multisig_submission_events(
    min_timestamp: Optional[int] = None,
) -> List[Dict[str, Any]]:
    events = []
    url = f"{TRONGRID_BASE_URL}/v1/contracts/{TRON_MULTISIG_WALLET}/events"

    params = {
        "event_name": "Submission",
        "only_confirmed": "false",
        "limit": 50,
        "order_by": "block_timestamp,desc",
    }

    if min_timestamp:
        params["min_block_timestamp"] = min_timestamp

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params, headers=_get_headers())
            if resp.status_code == 200:
                data = resp.json()
                for event in data.get("data", []):
                    tx_id = event.get("result", {}).get("transactionId")
                    if tx_id:
                        events.append(
                            {
                                "transaction_id": int(tx_id),
                                "tx_hash": event.get("transaction_id"),
                                "block_timestamp": event.get("block_timestamp"),
                                "block_number": event.get("block_number"),
                            }
                        )
            elif resp.status_code == 429:
                logger.warning("MultiSig events rate limited (429)")
            else:
                logger.warning(f"MultiSig events request failed: {resp.status_code}")
    except Exception as e:
        logger.error(f"Error fetching MultiSig Submission events: {e}")

    return events


async def get_multisig_execution_events(
    min_timestamp: Optional[int] = None,
) -> List[Dict[str, Any]]:
    events = []
    url = f"{TRONGRID_BASE_URL}/v1/contracts/{TRON_MULTISIG_WALLET}/events"

    params = {
        "event_name": "Execution",
        "only_confirmed": "false",
        "limit": 50,
        "order_by": "block_timestamp,desc",
    }

    if min_timestamp:
        params["min_block_timestamp"] = min_timestamp

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params, headers=_get_headers())
            if resp.status_code == 200:
                data = resp.json()
                for event in data.get("data", []):
                    tx_id = event.get("result", {}).get("transactionId")
                    if tx_id:
                        events.append(
                            {
                                "transaction_id": int(tx_id),
                                "tx_hash": event.get("transaction_id"),
                                "block_timestamp": event.get("block_timestamp"),
                                "block_number": event.get("block_number"),
                            }
                        )
    except Exception as e:
        logger.debug(f"Error fetching MultiSig Execution events: {e}")

    return events


async def get_multisig_tx_details(tx_hash: str, max_retries: int = 3) -> Optional[dict]:

    if grpc_get_transaction_by_id is not None:
        try:
            data = await grpc_get_transaction_by_id(tx_hash)
            if data and "raw_data" in data:
                logger.debug(f"Got tx details via gRPC: {tx_hash[:16]}...")
                return data
        except Exception as e:
            logger.debug(f"gRPC tx details failed, falling back to HTTP: {e}")

    url = f"{TRONGRID_BASE_URL}/wallet/gettransactionbyid"

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    url, json={"value": tx_hash}, headers=_get_headers()
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data and "raw_data" in data:
                        return data
                    if attempt < max_retries - 1:
                        await asyncio.sleep(0.5)
                        continue
                elif resp.status_code == 429:
                    wait_time = (attempt + 1) * 1
                    logger.warning(
                        f"MultiSig tx details rate limited, waiting {wait_time}s"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.warning(
                        f"MultiSig tx details failed: HTTP {resp.status_code}"
                    )
        except Exception as e:
            logger.warning(
                f"Error getting MultiSig tx details (attempt {attempt + 1}): {e}"
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(1)

    return None
