import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..config import (
    logger,
    ETH_MULTISIG_WALLET,
    ETH_EXECUTION_TOPIC,
    ETH_BLOCK_CATCHUP_CONCURRENCY,
    ETH_BLOCK_MAX_CATCHUP,
)
from .. import database, health
from .rpc import get_now_eth_block, get_eth_block_by_num, get_eth_logs
from .multisig import (
    decode_eth_submit_input,
    decode_eth_execute_input,
    decode_eth_confirm_input,
    execution_id_from_logs,
)

MULTISIG = ETH_MULTISIG_WALLET.lower()


def extract_eth_multisig_ops(block: Dict[str, Any]) -> List[Dict[str, Any]]:
    ops = []
    block_number = block.get("block_number") or 0
    block_timestamp = block.get("timestamp") or 0

    for tx in block.get("transactions") or []:
        if (tx.get("to") or "") != MULTISIG:
            continue
        tx_hash = tx.get("tx_id") or ""
        data = tx.get("data") or ""
        if not tx_hash or not data:
            continue

        submitted = decode_eth_submit_input(data)
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

        exec_id = decode_eth_execute_input(data)
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


def _block_has_confirm(block: Dict[str, Any]) -> bool:
    for tx in block.get("transactions") or []:
        if (tx.get("to") or "") != MULTISIG:
            continue
        if decode_eth_confirm_input(tx.get("data") or "") is not None:
            return True
    return False


def _executions_from_logs(
    logs: List[Dict[str, Any]],
    block: Dict[str, Any],
) -> List[Dict[str, Any]]:
    ops = []
    block_number = block.get("block_number") or 0
    block_timestamp = block.get("timestamp") or 0
    for log in logs or []:
        exec_id = execution_id_from_logs([log])
        if exec_id is None:
            continue
        tx_hash = (log.get("transactionHash") or "").lower()
        if not tx_hash:
            continue
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


async def ops_from_eth_block(block: Dict[str, Any]) -> List[Dict[str, Any]]:
    ops = extract_eth_multisig_ops(block)
    if not _block_has_confirm(block):
        return ops

    logs = await get_eth_logs(
        ETH_MULTISIG_WALLET,
        ETH_EXECUTION_TOPIC,
        block["block_number"],
    )
    seen = {
        (op.get("multisig_tx_id"), (op.get("tx_hash") or "").lower())
        for op in ops
        if op.get("kind") == "execution"
    }
    for op in _executions_from_logs(logs, block):
        key = (op.get("multisig_tx_id"), (op.get("tx_hash") or "").lower())
        if key not in seen:
            ops.append(op)
            seen.add(key)
    return ops


class EthBlockCursor:

    def __init__(self):
        self.last_block = 0
        self._catchup_sem = asyncio.Semaphore(ETH_BLOCK_CATCHUP_CONCURRENCY)

    def load(self):
        stored = database.get_sync_state("eth_watcher_last_block")
        if stored:
            try:
                self.last_block = int(stored)
            except ValueError:
                self.last_block = 0

    def persist(self):
        if self.last_block:
            database.set_sync_state("eth_watcher_last_block", str(self.last_block))
            database.set_sync_state(
                "eth_watcher_last_scan", datetime.now(timezone.utc).isoformat()
            )
            health.eth_block_last_number = self.last_block

    async def _fetch_range(self, start: int, end: int) -> List[Dict[str, Any]]:
        async def one(num: int) -> Optional[Dict[str, Any]]:
            async with self._catchup_sem:
                return await get_eth_block_by_num(num)

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
        head = await get_now_eth_block()
        if not head or not head.get("block_number"):
            return []

        head_num = head["block_number"]
        health.eth_block_source = head.get("source") or health.eth_block_source

        if self.last_block <= 0:
            self.last_block = head_num - 1
            self.persist()
            return [head]

        if head_num <= self.last_block:
            return []

        gap = head_num - self.last_block
        if gap > ETH_BLOCK_MAX_CATCHUP:
            logger.warning(
                f"ETH block cursor jumped {gap} blocks ({self.last_block} -> {head_num}); "
                "Etherscan fallback covers the gap"
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
