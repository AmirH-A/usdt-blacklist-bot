import asyncio
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import httpx

from ..config import (
    TRONGRID_BASE_URL,
    TRONGRID_API_KEY,
    TRONGRID_MEMPOOL_API_KEY,
    TRON_USDT_HEX,
    ADDBLACKLIST_SELECTOR,
    REMOVEBLACKLIST_SELECTOR,
    logger,
)
from ..utils import trongrid_rate_limit
from .address import hex_to_tron_address
from .. import health


async def get_pending_mempool_transactions() -> List[str]:
    url = f"{TRONGRID_BASE_URL}/wallet/gettransactionlistfrompending"
    api_key = TRONGRID_MEMPOOL_API_KEY or TRONGRID_API_KEY
    headers = {"TRON-PRO-API-KEY": api_key} if api_key else {}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                tx_ids = data.get("txId", [])
                return tx_ids if isinstance(tx_ids, list) else []
            elif resp.status_code == 429:
                logger.warning("Mempool rate limited (429)")
    except Exception as e:
        logger.error(f"Error fetching mempool transactions: {e}")

    return []


async def get_transaction_details(tx_id: str) -> Optional[dict]:
    await trongrid_rate_limit()

    url = f"{TRONGRID_BASE_URL}/wallet/gettransactionbyid"
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(url, json={"value": tx_id}, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                if data and "raw_data" in data:
                    return data
            elif resp.status_code == 429:
                await asyncio.sleep(1)
    except Exception as e:
        logger.debug(f"Error getting tx {tx_id[:16]}...: {e}")

    return None


def parse_blacklist_transaction(tx_data: dict) -> Optional[dict]:
    try:
        contracts = tx_data.get("raw_data", {}).get("contract", [])
        if not contracts:
            return None

        contract = contracts[0]
        if contract.get("type") != "TriggerSmartContract":
            return None

        param = contract.get("parameter", {}).get("value", {})
        contract_address = param.get("contract_address", "")
        data = param.get("data", "")

        if contract_address != TRON_USDT_HEX:
            return None

        if len(data) < 72:
            return None

        selector = data[:8].lower()
        address_param = data[8:72]

        if selector == ADDBLACKLIST_SELECTOR:
            action = "blacklist"
        elif selector == REMOVEBLACKLIST_SELECTOR:
            action = "unblacklist"
        else:
            return None

        hex_addr = "41" + address_param[-40:]
        tron_addr = hex_to_tron_address(hex_addr)

        if not tron_addr:
            return None

        return {
            "address": tron_addr,
            "action": action,
            "tx_id": tx_data.get("txID", ""),
        }
    except Exception as e:
        logger.debug(f"Error parsing tx: {e}")
        return None


async def scan_mempool_for_blacklist() -> List[Dict[str, Any]]:
    results = []
    scan_start = datetime.now(timezone.utc)

    tx_ids = await get_pending_mempool_transactions()

    if not tx_ids:
        logger.debug("MEMPOOL SCAN: No pending transactions returned from API")
        return results

    batch_size = 3
    max_txs = 30
    batch_delay = 1.0

    total_in_mempool = len(tx_ids)
    txs_to_scan = min(total_in_mempool, max_txs)
    txs_fetched = 0
    txs_failed = 0

    for i in range(0, txs_to_scan, batch_size):
        batch = tx_ids[i:i + batch_size]

        tasks = [get_transaction_details(tx_id) for tx_id in batch]
        tx_details = await asyncio.gather(*tasks, return_exceptions=True)

        for tx_data in tx_details:
            if isinstance(tx_data, Exception) or tx_data is None:
                txs_failed += 1
                continue

            txs_fetched += 1

            parsed = parse_blacklist_transaction(tx_data)
            if parsed:
                results.append({
                    "address": parsed["address"],
                    "action": parsed["action"],
                    "tx_hash": parsed["tx_id"],
                    "source": "mempool",
                    "detected_at": datetime.now(timezone.utc).isoformat(),
                })
                logger.info(f"MEMPOOL DETECTED: {parsed['action']} -> {parsed['address']}")

        if i + batch_size < txs_to_scan:
            await asyncio.sleep(batch_delay)

    scan_duration = (datetime.now(timezone.utc) - scan_start).total_seconds()

    if results:
        logger.info(
            f"MEMPOOL SCAN: Found {len(results)} blacklist tx! "
            f"[{txs_fetched}/{txs_to_scan} fetched, {total_in_mempool} total in mempool, {scan_duration:.1f}s]"
        )
    else:
        if health.mempool_scan_count % 15 == 0:
            logger.info(
                f"MEMPOOL STATS: {txs_fetched}/{txs_to_scan} fetched, "
                f"{txs_failed} failed, {total_in_mempool} total in mempool, {scan_duration:.1f}s scan time"
            )

    return results
