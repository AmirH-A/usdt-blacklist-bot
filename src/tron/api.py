from typing import Optional

import httpx

from ..config import (
    TRONGRID_BASE_URL,
    TRONGRID_API_KEY,
    TRON_USDT_CONTRACT,
    logger,
)
from .address import tron_address_to_hex


def _get_headers() -> dict:
    return {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}


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
