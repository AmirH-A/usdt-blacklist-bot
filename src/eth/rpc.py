from typing import Any, Dict, List, Optional

import httpx

from ..config import ETH_RPC_URL, logger

_http: Optional[httpx.AsyncClient] = None


async def _client() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=12.0)
    return _http


def rpc_enabled() -> bool:
    return bool(ETH_RPC_URL)


async def rpc_call(method: str, params: list) -> Any:
    if not ETH_RPC_URL:
        return None
    client = await _client()
    resp = await client.post(
        ETH_RPC_URL,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code != 200:
        logger.warning(f"ETH RPC {method} HTTP {resp.status_code}")
        return None
    payload = resp.json()
    if payload.get("error"):
        logger.warning(f"ETH RPC {method} error: {payload['error']}")
        return None
    return payload.get("result")


def _normalize_tx(tx: dict) -> Optional[Dict[str, Any]]:
    tx_hash = (tx.get("hash") or "").lower()
    to_addr = (tx.get("to") or "").lower()
    data = tx.get("input") or ""
    if not tx_hash:
        return None
    return {
        "tx_id": tx_hash,
        "to": to_addr,
        "data": data,
    }


def _normalize_block(raw: dict) -> Optional[Dict[str, Any]]:
    if not raw or not raw.get("number"):
        return None
    number = int(raw["number"], 16)
    timestamp = int(raw.get("timestamp") or "0x0", 16) * 1000
    transactions = []
    for tx in raw.get("transactions") or []:
        if isinstance(tx, str):
            continue
        parsed = _normalize_tx(tx)
        if parsed:
            transactions.append(parsed)
    return {
        "block_number": number,
        "timestamp": timestamp,
        "transactions": transactions,
        "source": "eth_rpc",
    }


async def get_eth_block_number() -> Optional[int]:
    result = await rpc_call("eth_blockNumber", [])
    if not result:
        return None
    return int(result, 16)


async def get_eth_block_by_num(
    block_num: int, full_txs: bool = True
) -> Optional[Dict[str, Any]]:
    raw = await rpc_call("eth_getBlockByNumber", [hex(block_num), full_txs])
    return _normalize_block(raw) if raw else None


async def get_now_eth_block() -> Optional[Dict[str, Any]]:
    head = await get_eth_block_number()
    if not head:
        return None
    return await get_eth_block_by_num(head, full_txs=True)


async def get_eth_block_receipts(block_num: int) -> List[Dict[str, Any]]:
    result = await rpc_call("eth_getBlockReceipts", [hex(block_num)])
    if not isinstance(result, list):
        return []
    return result


async def get_eth_logs(
    address: str,
    topic0: str,
    block_num: int,
) -> List[Dict[str, Any]]:
    result = await rpc_call(
        "eth_getLogs",
        [
            {
                "address": address,
                "fromBlock": hex(block_num),
                "toBlock": hex(block_num),
                "topics": [topic0],
            }
        ],
    )
    if not isinstance(result, list):
        return []
    return result
