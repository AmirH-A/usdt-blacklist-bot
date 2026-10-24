"""Ethereum JSON-RPC via Chainstack (or any ETH_RPC_URL).

ETH has no Tron-style gRPC. The live path is eth_getBlockByNumber and
decode submit/execute from calldata. Execution logs are fetched only when
a confirmTransaction is in the block — not full-block receipts.
"""

from typing import Any, Dict, List, Optional

import httpx

from ..config import ETH_RPC_HEADERS, ETH_RPC_URL, ETH_RPC_URLS, logger

_http: Optional[httpx.AsyncClient] = None

# Endpoint rotation state. A dead Chainstack key answers 403 on every request, so
# staying on it means every call fails; rotate and stick to whatever answers.
_urls: list = list(ETH_RPC_URLS) or ([ETH_RPC_URL] if ETH_RPC_URL else [])
_url_i: int = 0
rpc_failovers: int = 0


def active_rpc_index() -> int:
    """Index into ETH_RPC_URLS that detection is currently using."""
    return _url_i


def active_rpc_url() -> Optional[str]:
    return _urls[_url_i] if _urls else None


def _rotate_url() -> bool:
    """Move to the next endpoint. False when there is only one."""
    global _url_i, rpc_failovers
    if len(_urls) < 2:
        return False
    _url_i = (_url_i + 1) % len(_urls)
    rpc_failovers += 1
    host = _urls[_url_i].split("/")[2] if "//" in _urls[_url_i] else _urls[_url_i]
    logger.warning(
        f"ETH RPC failing over to endpoint {_url_i + 1}/{len(_urls)}: {host}"
    )
    return True


async def _client() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=12.0)
    return _http


def rpc_enabled() -> bool:
    # Any configured endpoint counts: the primary key may be dead while the
    # fallback still serves, which is the point of the list.
    return bool(_urls)


async def rpc_call(method: str, params: list) -> Any:
    """One JSON-RPC call, rotating endpoints on transport/auth failures.

    Rotates on a non-200 (403/401/429 = dead or throttled key) and on transport
    exceptions. Does NOT rotate on a JSON-RPC `error` body: that is usually a bad
    request for this method and would be returned by every endpoint, so churning
    would only add latency. Previously this raised on connect errors instead of
    returning None, which left callers to handle it inconsistently.
    """
    if not _urls:
        return None
    client = await _client()
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}

    for _ in range(max(1, len(_urls))):
        url = _urls[_url_i]
        # Per-node auth: a key in the URL path needs no header, a self-hosted or
        # header-authenticated node does. Empty for both of today's Chainstack
        # URLs, so those requests are unchanged.
        headers = {"Content-Type": "application/json"}
        headers.update(ETH_RPC_HEADERS.get(url) or {})
        try:
            resp = await client.post(url, json=body, headers=headers)
        except Exception as e:
            logger.warning(f"ETH RPC {method} transport error: {type(e).__name__}: {e}")
            if _rotate_url():
                continue
            return None

        if resp.status_code != 200:
            logger.warning(f"ETH RPC {method} HTTP {resp.status_code}")
            if _rotate_url():
                continue
            return None

        payload = resp.json()
        if payload.get("error"):
            logger.warning(f"ETH RPC {method} error: {payload['error']}")
            return None
        return payload.get("result")

    return None


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


async def get_eth_block_by_num(block_num: int, full_txs: bool = True) -> Optional[Dict[str, Any]]:
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
