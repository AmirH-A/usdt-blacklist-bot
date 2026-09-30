import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..config import (
    logger,
    ETH_MULTISIG_WALLET,
    ETH_SUBMISSION_TOPIC,
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
    submission_id_from_logs,
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

        # Finalizes are usually confirmTransaction; executeTransaction is rarer.
        exec_id = decode_eth_execute_input(data)
        if exec_id is None:
            exec_id = decode_eth_confirm_input(data)
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


def _block_has_kind(ops: List[Dict[str, Any]], kind: str) -> bool:
    return any(op.get("kind") == kind for op in ops)


def _ids_by_tx_hash_from_logs(
    logs: List[Dict[str, Any]],
    id_from_logs,
) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for log in logs or []:
        tx_hash = (log.get("transactionHash") or "").lower()
        if not tx_hash:
            continue
        sid = id_from_logs([log])
        if sid is not None:
            out[tx_hash] = sid
    return out


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
    """Decode Multisig ops from the block; attach real Submission ids from logs.

    Same-block submit+confirm used to miss confirms because pending was stored with a
    hash-prefix placeholder id, while Execution/confirm carry the real Multisig id.
    Fetching Submission logs for this block binds the real id before we record pending.
    """
    ops = extract_eth_multisig_ops(block)
    if not ops:
        return ops

    block_num = block["block_number"]
    has_submission = _block_has_kind(ops, "submission")
    has_execution = _block_has_kind(ops, "execution")

    if has_submission:
        sub_logs = await get_eth_logs(
            ETH_MULTISIG_WALLET,
            ETH_SUBMISSION_TOPIC,
            block_num,
        )
        ids_by_hash = _ids_by_tx_hash_from_logs(sub_logs, submission_id_from_logs)
        for op in ops:
            if op.get("kind") != "submission":
                continue
            real_id = ids_by_hash.get((op.get("tx_hash") or "").lower())
            if real_id is not None:
                op["multisig_tx_id"] = real_id

    # Also pull Execution logs when this block finalized anything (confirm/execute),
    # in case calldata decode missed a path.
    if has_execution or any(
        decode_eth_confirm_input(tx.get("data") or "") is not None
        for tx in (block.get("transactions") or [])
        if (tx.get("to") or "") == MULTISIG
    ):
        exec_logs = await get_eth_logs(
            ETH_MULTISIG_WALLET,
            ETH_EXECUTION_TOPIC,
            block_num,
        )
        seen = {
            (op.get("multisig_tx_id"), (op.get("tx_hash") or "").lower())
            for op in ops
            if op.get("kind") == "execution"
        }
        for op in _executions_from_logs(exec_logs, block):
            key = (op.get("multisig_tx_id"), (op.get("tx_hash") or "").lower())
            if key not in seen:
                ops.append(op)
                seen.add(key)

    # Submissions first, then executions — so same-block confirms can match.
    ops.sort(key=lambda o: 0 if o.get("kind") == "submission" else 1)
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
            database.set_sync_state("eth_last_block_number", str(self.last_block))
            database.set_sync_state(
                "eth_watcher_last_scan", datetime.now(timezone.utc).isoformat()
            )
            database.set_sync_state(
                "eth_last_scan", datetime.now(timezone.utc).isoformat()
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
