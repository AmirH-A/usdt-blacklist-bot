from typing import Optional, List, Dict, Any

import httpx

from ..config import (
    ETHERSCAN_BASE_URL,
    ETHERSCAN_API_KEY,
    ETH_CHAIN_ID,
    ETH_MULTISIG_WALLET,
    ETH_SUBMISSION_TOPIC,
    ETH_EXECUTION_TOPIC,
    logger,
)


def _get_base_params() -> dict:
    return {
        "chainid": ETH_CHAIN_ID,
        "apikey": ETHERSCAN_API_KEY,
    }


async def get_eth_multisig_submission_events(from_block: Optional[int] = None) -> List[Dict[str, Any]]:
    events = []
    params = {
        **_get_base_params(),
        "module": "logs",
        "action": "getLogs",
        "address": ETH_MULTISIG_WALLET,
        "topic0": ETH_SUBMISSION_TOPIC,
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
                        "block_timestamp": int(log["timeStamp"], 16) * 1000,
                    })
    except Exception as e:
        logger.error(f"Error fetching ETH MultiSig Submission events: {e}")

    return events


async def get_eth_multisig_execution_events(from_block: Optional[int] = None) -> List[Dict[str, Any]]:
    events = []
    params = {
        **_get_base_params(),
        "module": "logs",
        "action": "getLogs",
        "address": ETH_MULTISIG_WALLET,
        "topic0": ETH_EXECUTION_TOPIC,
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
                        "block_timestamp": int(log["timeStamp"], 16) * 1000,
                    })
    except Exception as e:
        logger.error(f"Error fetching ETH MultiSig Execution events: {e}")

    return events


async def get_eth_multisig_tx_target(tx_hash: str) -> Optional[dict]:
    params = {
        **_get_base_params(),
        "module": "proxy",
        "action": "eth_getTransactionByHash",
        "txhash": tx_hash,
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
            if selector != "c6427474":
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
                    if inner_selector in ("0ecb93c0", "e4997dc5"):
                        if len(inner_data) >= 72:
                            address_hex = inner_data[32:72]
                            target = "0x" + address_hex[-40:].lower()
                            action = "blacklist" if inner_selector == "0ecb93c0" else "unblacklist"
                            return {"target_address": target, "action": action}
            except (ValueError, IndexError) as e:
                logger.debug(f"Failed to decode ETH MultiSig tx data: {e}")
                return None

    except Exception as e:
        logger.error(f"Error getting ETH MultiSig tx target: {e}")

    return None
