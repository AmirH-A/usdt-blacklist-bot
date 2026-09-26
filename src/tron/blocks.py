"""Normalized TRON block fetch.

Prefers gRPC GetNowBlock2 / GetBlockByNum2. Falls back to TronGrid
wallet/getnowblock so the hot path never depends on the event indexer.
"""

from typing import Optional, Dict, Any, List

import httpx

from ..config import (
    TRONGRID_BASE_URL,
    TRONGRID_API_KEY,
    USE_GRPC,
    logger,
)
from .grpc_client import grpc_available, grpc_get_now_block, grpc_get_block_by_num

_http: Optional[httpx.AsyncClient] = None


def _headers() -> dict:
    return {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}


async def _client() -> httpx.AsyncClient:
    global _http
    if _http is None or _http.is_closed:
        _http = httpx.AsyncClient(timeout=8.0)
    return _http


def _normalize_http_tx(tx: dict) -> Optional[Dict[str, Any]]:
    tx_id = tx.get("txID") or ""
    if not tx_id:
        return None

    contracts = tx.get("raw_data", {}).get("contract") or []
    if not contracts:
        return {"tx_id": tx_id.lower(), "contract_address": "", "data": ""}

    param = contracts[0].get("parameter", {}).get("value", {})
    contract_address = (param.get("contract_address") or "").lower()
    data = param.get("data") or ""
    if data.startswith("0x"):
        data = data[2:]

    return {
        "tx_id": tx_id.lower(),
        "contract_address": contract_address,
        "data": data,
    }


def _normalize_http_block(payload: dict) -> Optional[Dict[str, Any]]:
    raw = payload.get("block_header", {}).get("raw_data", {})
    number = raw.get("number")
    if number is None:
        return None

    transactions = []
    for tx in payload.get("transactions") or []:
        parsed = _normalize_http_tx(tx)
        if parsed:
            transactions.append(parsed)

    return {
        "block_number": int(number),
        "timestamp": int(raw.get("timestamp") or 0),
        "transactions": transactions,
        "source": "http",
    }


def _normalize_grpc_block(block: dict) -> Optional[Dict[str, Any]]:
    number = block.get("block_number")
    if not number:
        return None
    for tx in block.get("transactions") or []:
        if tx.get("tx_id"):
            tx["tx_id"] = tx["tx_id"].lower()
        if tx.get("contract_address"):
            tx["contract_address"] = tx["contract_address"].lower()
        data = tx.get("data") or ""
        if data.startswith("0x"):
            tx["data"] = data[2:]
    block["source"] = "grpc"
    return block


async def http_get_now_block() -> Optional[Dict[str, Any]]:
    try:
        client = await _client()
        resp = await client.post(
            f"{TRONGRID_BASE_URL}/wallet/getnowblock",
            headers=_headers(),
            json={},
        )
        if resp.status_code != 200:
            logger.warning(f"HTTP getnowblock failed: {resp.status_code}")
            return None
        return _normalize_http_block(resp.json())
    except Exception as e:
        logger.error(f"HTTP getnowblock error: {e}")
        return None


async def http_get_block_by_num(block_num: int) -> Optional[Dict[str, Any]]:
    try:
        client = await _client()
        resp = await client.post(
            f"{TRONGRID_BASE_URL}/wallet/getblockbynum",
            headers=_headers(),
            json={"num": block_num},
        )
        if resp.status_code != 200:
            logger.warning(f"HTTP getblockbynum {block_num} failed: {resp.status_code}")
            return None
        return _normalize_http_block(resp.json())
    except Exception as e:
        logger.error(f"HTTP getblockbynum error: {e}")
        return None


async def get_now_block() -> Optional[Dict[str, Any]]:
    if USE_GRPC and grpc_available():
        block = await grpc_get_now_block()
        normalized = _normalize_grpc_block(block) if block else None
        if normalized:
            return normalized
    return await http_get_now_block()


async def get_block_by_num(block_num: int) -> Optional[Dict[str, Any]]:
    if USE_GRPC and grpc_available():
        block = await grpc_get_block_by_num(block_num)
        normalized = _normalize_grpc_block(block) if block else None
        if normalized:
            return normalized
    return await http_get_block_by_num(block_num)
