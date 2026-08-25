from typing import Optional, List, Dict, Any

import httpx

from ..config import (
    ETHERSCAN_BASE_URL,
    ETHERSCAN_API_KEY,
    ETH_CHAIN_ID,
    ETH_MULTISIG_WALLET,
    ETH_USDT_CONTRACT,
    ETH_SUBMISSION_TOPIC,
    ETH_EXECUTION_TOPIC,
    SUBMIT_TRANSACTION_SELECTOR,
    EXECUTE_TRANSACTION_SELECTOR,
    CONFIRM_TRANSACTION_SELECTOR,
    ADDBLACKLIST_SELECTOR,
    REMOVEBLACKLIST_SELECTOR,
    logger,
)


def _hex_body(data: str) -> str:
    if not data:
        return ""
    if data.startswith("0x"):
        data = data[2:]
    return data.lower()


def decode_eth_submit_input(input_data: str) -> Optional[dict]:
    data = _hex_body(input_data)
    if len(data) < 200 or data[:8] != SUBMIT_TRANSACTION_SELECTOR:
        return None
    try:
        destination = "0x" + data[32:72]
        if destination != ETH_USDT_CONTRACT.lower():
            return None
        data_offset = int(data[136:200], 16)
        length_pos = 8 + data_offset * 2
        if len(data) < length_pos + 64:
            return None
        data_length = int(data[length_pos : length_pos + 64], 16)
        data_start = length_pos + 64
        inner_data = data[data_start : data_start + data_length * 2]
        if len(inner_data) < 72:
            return None
        inner_selector = inner_data[:8]
        if inner_selector == ADDBLACKLIST_SELECTOR:
            action = "blacklist"
        elif inner_selector == REMOVEBLACKLIST_SELECTOR:
            action = "unblacklist"
        else:
            return None
        target = "0x" + inner_data[32:72]
        return {"target_address": target, "action": action}
    except (ValueError, IndexError):
        return None


def _decode_uint256_call(input_data: str, selector: str) -> Optional[int]:
    data = _hex_body(input_data)
    if len(data) < 72 or data[:8] != selector:
        return None
    try:
        return int(data[8:72], 16)
    except ValueError:
        return None


def decode_eth_execute_input(input_data: str) -> Optional[int]:
    return _decode_uint256_call(input_data, EXECUTE_TRANSACTION_SELECTOR)


def decode_eth_confirm_input(input_data: str) -> Optional[int]:
    return _decode_uint256_call(input_data, CONFIRM_TRANSACTION_SELECTOR)


def submission_id_from_logs(logs: List[Dict[str, Any]]) -> Optional[int]:
    topic0 = ETH_SUBMISSION_TOPIC.lower()
    for log in logs or []:
        topics = [str(t).lower() for t in (log.get("topics") or [])]
        if len(topics) > 1 and topics[0] == topic0:
            try:
                return int(topics[1], 16)
            except ValueError:
                return None
    return None


def execution_id_from_logs(logs: List[Dict[str, Any]]) -> Optional[int]:
    topic0 = ETH_EXECUTION_TOPIC.lower()
    for log in logs or []:
        topics = [str(t).lower() for t in (log.get("topics") or [])]
        if len(topics) > 1 and topics[0] == topic0:
            try:
                return int(topics[1], 16)
            except ValueError:
                return None
    return None


def _get_base_params() -> dict:
    return {
        "chainid": ETH_CHAIN_ID,
        "apikey": ETHERSCAN_API_KEY,
    }


async def get_eth_multisig_submission_events(
    from_block: Optional[int] = None,
) -> List[Dict[str, Any]]:
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
                    events.append(
                        {
                            "transaction_id": tx_id,
                            "tx_hash": log["transactionHash"],
                            "block_number": int(log["blockNumber"], 16),
                            "block_timestamp": int(log["timeStamp"], 16) * 1000,
                        }
                    )
    except Exception as e:
        logger.error(f"Error fetching ETH MultiSig Submission events: {e}")

    return events


async def get_eth_multisig_execution_events(
    from_block: Optional[int] = None,
) -> List[Dict[str, Any]]:
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
                    events.append(
                        {
                            "transaction_id": tx_id,
                            "tx_hash": log["transactionHash"],
                            "block_number": int(log["blockNumber"], 16),
                            "block_timestamp": int(log["timeStamp"], 16) * 1000,
                        }
                    )
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

            return decode_eth_submit_input(result.get("input", ""))

    except Exception as e:
        logger.error(f"Error getting ETH MultiSig tx target: {e}")

    return None
