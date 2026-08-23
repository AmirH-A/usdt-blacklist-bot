import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List

from ..config import (
    logger,
    TRON_MULTISIG_HEX,
    BLOCK_POLL_INTERVAL,
    BLOCK_CATCHUP_CONCURRENCY,
    BLOCK_MAX_CATCHUP,
)
from .. import database, health
from .blocks import get_now_block, get_block_by_num
from .multisig import decode_multisig_submit_data, decode_execute_transaction_data

MULTISIG_HEX = (
    TRON_MULTISIG_HEX[2:] if TRON_MULTISIG_HEX.startswith("41") else TRON_MULTISIG_HEX
).lower()


def _is_multisig_call(tx: Dict[str, Any]) -> bool:
    contract = (tx.get("contract_address") or "").lower()
    if contract.startswith("41"):
        contract = contract[2:]
    return contract == MULTISIG_HEX


def extract_multisig_ops(block: Dict[str, Any]) -> List[Dict[str, Any]]:
    ops = []
    block_number = block.get("block_number") or 0
    block_timestamp = block.get("timestamp") or 0

    for tx in block.get("transactions") or []:
        if not _is_multisig_call(tx):
            continue

        tx_hash = tx.get("tx_id") or ""
        data = tx.get("data") or ""
        if not tx_hash or not data:
            continue

        submitted = decode_multisig_submit_data(data)
        if submitted:
            ops.append(
                {
                    "kind": "submission",
                    "tx_hash": tx_hash,
                    "action": submitted["action"],
                    "target_address": submitted["target_address"],
                    "block_number": block_number,
                    "block_timestamp": block_timestamp,
                }
            )
            continue

        exec_id = decode_execute_transaction_data(data)
        if exec_id is not None:
            ops.append(
                {
                    "kind": "execution",
                    "tx_hash": tx_hash,
                    "multisig_tx_id": exec_id,
                    "block_number": block_number,
                    "block_timestamp": block_timestamp,
                }
            )

    return ops


class BlockCursor:
    """Follow chain head; fill short gaps; jump if we fell too far behind."""

    def __init__(self):
        self.last_block = 0
        self._catchup_sem = asyncio.Semaphore(BLOCK_CATCHUP_CONCURRENCY)

    def load(self):
        stored = database.get_sync_state("tron_last_block_num")
        if stored:
            try:
                self.last_block = int(stored)
            except ValueError:
                self.last_block = 0

    def persist(self):
        if self.last_block:
            database.set_sync_state("tron_last_block_num", str(self.last_block))
            database.set_sync_state(
                "tron_last_scan", datetime.now(timezone.utc).isoformat()
            )
            health.block_last_number = self.last_block

    async def _fetch_range(self, start: int, end: int) -> List[Dict[str, Any]]:
        async def one(num: int) -> Optional[Dict[str, Any]]:
            async with self._catchup_sem:
                return await get_block_by_num(num)

        fetched = await asyncio.gather(
            *[one(n) for n in range(start, end + 1)],
            return_exceptions=True,
        )
        blocks = []
        for item in fetched:
            if isinstance(item, dict) and item.get("block_number"):
                blocks.append(item)
        blocks.sort(key=lambda b: b["block_number"])
        return blocks

    async def next_blocks(self) -> List[Dict[str, Any]]:
        head = await get_now_block()
        if not head or not head.get("block_number"):
            return []

        head_num = head["block_number"]
        health.block_source = head.get("source") or health.block_source

        if self.last_block <= 0:
            self.last_block = head_num - 1
            self.persist()
            return [head]

        if head_num <= self.last_block:
            return []

        gap = head_num - self.last_block
        if gap > BLOCK_MAX_CATCHUP:
            logger.warning(
                f"Block cursor jumped {gap} blocks ({self.last_block} -> {head_num}); "
                "HTTP event fallback covers the gap"
            )
            self.last_block = head_num - 1
            self.persist()
            return [head]

        missed_start = self.last_block + 1
        missed_end = head_num - 1
        blocks = []
        if missed_start <= missed_end:
            blocks.extend(await self._fetch_range(missed_start, missed_end))
        blocks.append(head)
        return blocks
