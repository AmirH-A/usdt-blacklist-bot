from typing import Optional, List, Dict, Any

import httpx

from ..config import (
    ETH_USDT_CONTRACT,
    ETH_XAUT_CONTRACT,
    ETHERSCAN_BASE_URL,
    ETHERSCAN_API_KEY,
    ETH_CHAIN_ID,
    ETH_BLACKLIST_TOPIC,
    logger,
)


def is_valid_eth_address(address: str) -> bool:
    if not address or len(address) != 42 or not address.startswith("0x"):
        return False
    try:
        int(address[2:], 16)
        return True
    except ValueError:
        return False


def _get_base_params() -> dict:
    return {
        "chainid": ETH_CHAIN_ID,
        "apikey": ETHERSCAN_API_KEY,
    }


async def get_eth_blacklist_events(from_block: Optional[int] = None) -> List[Dict[str, Any]]:
    events = []
    params = {
        **_get_base_params(),
        "module": "logs",
        "action": "getLogs",
        "address": ETH_USDT_CONTRACT,
        "topic0": ETH_BLACKLIST_TOPIC,
        "fromBlock": from_block if from_block else 0,
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ETHERSCAN_BASE_URL, params=params, timeout=15)
            data = resp.json()
            if data.get("status") == "1" and data.get("result"):
                for log in data["result"]:
                    data_hex = log.get("data", "")
                    if data_hex and len(data_hex) >= 42:
                        eth_address = "0x" + data_hex[-40:]
                        block_ts = int(log.get("timeStamp", "0"), 16) * 1000
                        events.append({
                            "address": eth_address,
                            "tx_hash": log.get("transactionHash", ""),
                            "block_number": int(log.get("blockNumber", "0"), 16),
                            "block_timestamp": block_ts,
                            "confirmed": True,
                        })
    except Exception as e:
        logger.error(f"Error fetching ETH events: {e}")

    return events


async def check_eth_address_blacklisted(address: str) -> bool:
    if not is_valid_eth_address(address):
        return False

    padded_address = address[2:].lower().zfill(64)
    call_data = "0xe47d6060" + padded_address

    params = {
        **_get_base_params(),
        "module": "proxy",
        "action": "eth_call",
        "to": ETH_USDT_CONTRACT,
        "data": call_data,
        "tag": "latest",
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


# Both tokens use 6 decimals.
_TOKEN_CONTRACTS = {"USDT": ETH_USDT_CONTRACT, "XAUt": ETH_XAUT_CONTRACT}


async def get_eth_usdt_balance(address: str, token: str = "USDT") -> Optional[float]:
    if not is_valid_eth_address(address):
        return None

    padded_address = address[2:].lower().zfill(64)
    call_data = "0x70a08231" + padded_address

    params = {
        **_get_base_params(),
        "module": "proxy",
        "action": "eth_call",
        "to": _TOKEN_CONTRACTS[token],
        "data": call_data,
        "tag": "latest",
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


async def get_current_eth_block() -> Optional[int]:
    params = {
        **_get_base_params(),
        "module": "proxy",
        "action": "eth_blockNumber",
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ETHERSCAN_BASE_URL, params=params, timeout=15)
            data = resp.json()
            return int(data.get("result", "0x0"), 16)
    except Exception as e:
        logger.error(f"Error getting ETH block number: {e}")

    return None
