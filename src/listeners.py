import asyncio
from datetime import datetime, timezone

from .config import (
    ETH_MEMPOOL_REAP_INTERVAL,
    ETH_MEMPOOL_STALE_S,
    MAX_ALERTS_PER_CYCLE,
    MAX_CONSECUTIVE_FAILURES,
    MULTISIG_SCAN_INTERVAL,
    ETH_MULTISIG_SCAN_INTERVAL,
    BLOCK_POLL_INTERVAL,
    ETH_BLOCK_POLL_INTERVAL,
    ADMIN_CHAT_ID,
    logger,
)
from . import database, health
from .bot import bot
from .bot.alerts import (
    broadcast_early_warning_alerts,
    broadcast_confirmed_alerts,
)
from .tron import (
    decode_multisig_submit_data,
    get_multisig_submission_events,
    get_multisig_tx_details,
)
from .tron.block_watcher import BlockCursor, extract_multisig_ops
from .eth import (
    get_eth_multisig_submission_events,
    get_eth_multisig_execution_events,
    get_eth_multisig_tx_target,
    get_current_eth_block,
    rpc_enabled,
)
from .eth.block_watcher import EthBlockCursor, ops_from_eth_block


async def _notify_scanner_down(name: str, error: Exception):
    if not ADMIN_CHAT_ID:
        return
    try:
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"*{name} SCANNER ALERT*\n\n{MAX_CONSECUTIVE_FAILURES} failures!\nError: {str(error)[:200]}",
            parse_mode="Markdown",
        )
    except Exception:
        pass


def _pending_item(op: dict, chain: str) -> dict:
    return {
        "address": op["target_address"],
        "action": op["action"],
        "tx_hash": op["tx_hash"],
        "block_timestamp": op["block_timestamp"],
        "block_number": op.get("block_number"),
        "chain": chain,
        "multisig_tx_id": op.get("multisig_tx_id") or 0,
        "balance": None,
    }


def _confirmed_item(marked: dict, exec_event: dict, chain: str) -> dict:
    elapsed = None
    submitted_ts = marked.get("submitted_ts")
    confirmed_ts = exec_event.get("block_timestamp") or marked.get(
        "confirmed_block_timestamp"
    )
    if submitted_ts and confirmed_ts:
        elapsed = (confirmed_ts - submitted_ts) / 1000.0
    return {
        "address": marked["target_address"],
        "action": marked["action"],
        "tx_hash": marked["tx_hash"],
        "chain": chain,
        "block_timestamp": confirmed_ts,
        "block_number": exec_event.get("block_number"),
        "elapsed_seconds": elapsed,
        "is_update": True,
        "balance": None,
        "multisig_tx_id": exec_event.get("multisig_tx_id")
        or exec_event.get("transaction_id")
        or 0,
    }


async def _handle_block_ops(ops: list, chain: str):
    pending_alerts = []
    executions = []

    for op in ops:
        if op["kind"] == "submission":
            is_new = database.record_multisig_submission(
                chain=chain,
                tx_hash=op["tx_hash"],
                target_address=op["target_address"],
                action=op["action"],
                block_timestamp=op["block_timestamp"],
                multisig_tx_id=op.get("multisig_tx_id"),
            )
            if not is_new and op.get("block_timestamp"):
                if database.backfill_submission_block(
                    chain, op["tx_hash"], op["block_timestamp"]
                ):
                    logger.info(
                        f"{chain} BLOCK backfilled mempool submission "
                        f"{op['target_address']} ({op['tx_hash'][:16]}...)"
                    )
            if is_new:
                if chain == "TRON":
                    health.block_detections += 1
                else:
                    health.eth_block_detections += 1
                logger.info(
                    f"{chain} BLOCK {op['action'].upper()}: {op['target_address']} "
                    f"(block {op['block_number']}, tx #{op.get('multisig_tx_id') or 0}, "
                    f"{op['tx_hash'][:16]}...)"
                )
                pending_alerts.append(op)
        elif op["kind"] == "execution":
            executions.append(op)

    if pending_alerts:
        asyncio.create_task(
            broadcast_early_warning_alerts(
                [_pending_item(op, chain) for op in pending_alerts]
            )
        )

    confirmed_alerts = []
    for op in executions:
        marked = database.mark_multisig_executed(
            chain,
            exec_tx_id=op.get("multisig_tx_id"),
            exec_tx_hash=op.get("tx_hash", ""),
            confirmed_block_timestamp=op.get("block_timestamp"),
        )
        if marked:
            logger.info(
                f"{chain} BLOCK EXECUTED: {marked['action']} -> {marked['target_address']} "
                f"(block {op['block_number']})"
            )
            confirmed_alerts.append(_confirmed_item(marked, op, chain))

    if confirmed_alerts:
        asyncio.create_task(broadcast_confirmed_alerts(confirmed_alerts))


async def listen_to_tron_blocks():
    logger.info(
        f"TRON block watcher started (poll {BLOCK_POLL_INTERVAL}s, "
        "decode submitTransaction in-block)"
    )
    cursor = BlockCursor()
    cursor.load()

    while True:
        try:
            health.block_scan_count += 1
            blocks = await cursor.next_blocks()

            if not blocks:
                health.block_consecutive_failures = 0
                await asyncio.sleep(BLOCK_POLL_INTERVAL)
                continue

            for block in blocks:
                ops = extract_multisig_ops(block)
                if ops:
                    await _handle_block_ops(ops, "TRON")
                cursor.last_block = max(cursor.last_block, block["block_number"])

            cursor.persist()
            health.block_consecutive_failures = 0

            if len(blocks) > 1:
                continue
            await asyncio.sleep(BLOCK_POLL_INTERVAL)

        except Exception as e:
            health.block_consecutive_failures += 1
            logger.error(
                f"TRON block watcher error ({health.block_consecutive_failures}x): {e}"
            )
            if health.block_consecutive_failures == MAX_CONSECUTIVE_FAILURES:
                await _notify_scanner_down("TRON BLOCK WATCHER", e)
            await asyncio.sleep(2)


async def listen_to_eth_blocks():
    if not rpc_enabled():
        logger.warning("ETH_RPC_URL unset; skipping ETH block watcher")
        return

    logger.info(
        f"ETH block watcher started (poll {ETH_BLOCK_POLL_INTERVAL}s, "
        "decode submitTransaction from calldata)"
    )
    cursor = EthBlockCursor()
    cursor.load()

    while True:
        try:
            health.eth_block_scan_count += 1
            blocks = await cursor.next_blocks()

            if not blocks:
                health.eth_block_consecutive_failures = 0
                await asyncio.sleep(ETH_BLOCK_POLL_INTERVAL)
                continue

            for block in blocks:
                ops = await ops_from_eth_block(block)
                if ops:
                    await _handle_block_ops(ops, "ETH")
                cursor.last_block = max(cursor.last_block, block["block_number"])

            cursor.persist()
            health.eth_block_consecutive_failures = 0

            if len(blocks) > 1:
                continue
            await asyncio.sleep(ETH_BLOCK_POLL_INTERVAL)

        except Exception as e:
            health.eth_block_consecutive_failures += 1
            logger.error(
                f"ETH block watcher error ({health.eth_block_consecutive_failures}x): {e}"
            )
            if health.eth_block_consecutive_failures == MAX_CONSECUTIVE_FAILURES:
                await _notify_scanner_down("ETH BLOCK WATCHER", e)
            await asyncio.sleep(2)


async def listen_to_multisig_submissions():
    logger.info(
        f"TRON MultiSig HTTP fallback started (every {MULTISIG_SCAN_INTERVAL}s)"
    )

    last_timestamp_str = database.get_sync_state("multisig_last_timestamp")
    if last_timestamp_str:
        last_timestamp = int(last_timestamp_str)
    else:
        last_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        database.set_sync_state("multisig_last_timestamp", str(last_timestamp))

    while True:
        try:
            health.multisig_scan_count += 1

            events = await get_multisig_submission_events(min_timestamp=last_timestamp)
            alerts = []
            processed_timestamps = []

            for event in events:
                tx_id = event["transaction_id"]
                tx_hash = event["tx_hash"]
                block_timestamp = event["block_timestamp"]

                conn = database.get_db()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT target_address, action FROM multisig_pending WHERE submission_tx_hash = ? AND chain = 'TRON'",
                    (tx_hash,),
                )
                existing = cursor.fetchone()
                conn.close()

                if existing and existing[0]:
                    database.bind_multisig_tx_id("TRON", tx_hash, tx_id)
                    processed_timestamps.append(block_timestamp)
                    continue

                tx_details = await get_multisig_tx_details(tx_hash)
                if not tx_details:
                    continue

                data = tx_details.get("data") or ""
                if not data:
                    contracts = tx_details.get("raw_data", {}).get("contract", [])
                    if contracts:
                        data = (
                            contracts[0]
                            .get("parameter", {})
                            .get("value", {})
                            .get("data", "")
                        )
                decoded = decode_multisig_submit_data(data) if data else None
                if not decoded:
                    processed_timestamps.append(block_timestamp)
                    continue

                is_new = database.record_multisig_submission(
                    chain="TRON",
                    tx_hash=tx_hash,
                    target_address=decoded["target_address"],
                    action=decoded["action"],
                    block_timestamp=block_timestamp,
                    multisig_tx_id=tx_id,
                )
                logger.info(
                    f"HTTP FALLBACK: {decoded['action']} -> {decoded['target_address']} (TX #{tx_id})"
                )
                if is_new:
                    alerts.append(
                        (
                            decoded["target_address"],
                            decoded["action"],
                            tx_hash,
                            block_timestamp,
                            tx_id,
                        )
                    )
                processed_timestamps.append(block_timestamp)

            if alerts:
                await broadcast_early_warning_alerts(
                    [
                        {
                            "address": addr,
                            "action": action,
                            "tx_hash": tx_hash,
                            "block_timestamp": ts,
                            "chain": "TRON",
                            "multisig_tx_id": tx_id,
                            "balance": None,
                        }
                        for addr, action, tx_hash, ts, tx_id in alerts[
                            :MAX_ALERTS_PER_CYCLE
                        ]
                    ]
                )

            if processed_timestamps:
                new_ts = max(processed_timestamps)
                if new_ts > last_timestamp:
                    database.set_sync_state("multisig_last_timestamp", str(new_ts))
                    last_timestamp = new_ts

            recent_subs = await get_multisig_submission_events()
            for event in recent_subs:
                if event.get("tx_hash") and event.get("transaction_id") is not None:
                    database.bind_multisig_tx_id(
                        "TRON", event["tx_hash"], event["transaction_id"]
                    )

            # Confirms are decoded from blocks (confirmTransaction / executeTransaction).
            # Do not poll TronGrid Execution events for TRON.

            health.multisig_consecutive_failures = 0
            await asyncio.sleep(MULTISIG_SCAN_INTERVAL)

        except Exception as e:
            health.multisig_consecutive_failures += 1
            logger.error(
                f"MultiSig HTTP fallback error ({health.multisig_consecutive_failures}x): {e}"
            )
            if health.multisig_consecutive_failures == MAX_CONSECUTIVE_FAILURES:
                await _notify_scanner_down("TRON MULTISIG HTTP", e)
            await asyncio.sleep(10)


async def listen_to_eth_multisig_submissions():
    logger.info(
        f"ETH MultiSig HTTP fallback started (every {ETH_MULTISIG_SCAN_INTERVAL}s)"
    )

    last_block_str = database.get_sync_state("eth_multisig_last_block")
    last_block = int(last_block_str) if last_block_str else 0
    initialized = last_block > 0

    while True:
        try:
            if not initialized:
                current_block = await get_current_eth_block()
                if current_block:
                    last_block = current_block - 20
                    database.set_sync_state("eth_multisig_last_block", str(last_block))
                    logger.info(f"ETH MultiSig: Starting from block {last_block}")
                    initialized = True
                else:
                    await asyncio.sleep(5)
                    continue

            health.eth_multisig_scan_count += 1

            events = await get_eth_multisig_submission_events(from_block=last_block)
            if events:
                logger.info(f"ETH MultiSig: Found {len(events)} Submission events")

            alerts = []
            processed_blocks = []

            for event in events:
                tx_id = event["transaction_id"]
                tx_hash = event["tx_hash"]
                block_number = event["block_number"]
                block_timestamp = event["block_timestamp"]

                conn = database.get_db()
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT tx_id FROM multisig_pending
                       WHERE chain = 'ETH' AND (tx_id = ? OR submission_tx_hash = ?)""",
                    (tx_id, tx_hash),
                )
                existing = cursor.fetchone()
                conn.close()

                database.bind_multisig_tx_id("ETH", tx_hash, tx_id)

                if existing:
                    processed_blocks.append(block_number)
                    continue

                target_info = await get_eth_multisig_tx_target(tx_hash)
                if not target_info:
                    processed_blocks.append(block_number)
                    continue

                target_address = target_info["target_address"]
                action = target_info["action"]
                is_new = database.record_multisig_submission(
                    chain="ETH",
                    tx_hash=tx_hash,
                    target_address=target_address,
                    action=action,
                    block_timestamp=block_timestamp,
                    multisig_tx_id=tx_id,
                )
                logger.info(
                    f"ETH HTTP FALLBACK: {action} -> {target_address} (TX #{tx_id})"
                )
                if is_new:
                    alerts.append(
                        (target_address, action, tx_hash, block_timestamp, tx_id)
                    )
                processed_blocks.append(block_number)

            if alerts:
                await broadcast_early_warning_alerts(
                    [
                        {
                            "address": addr,
                            "action": action,
                            "tx_hash": tx_hash,
                            "block_timestamp": ts,
                            "chain": "ETH",
                            "multisig_tx_id": tx_id,
                            "balance": None,
                        }
                        for addr, action, tx_hash, ts, tx_id in alerts[
                            :MAX_ALERTS_PER_CYCLE
                        ]
                    ]
                )

            if processed_blocks:
                new_block = max(processed_blocks) + 1
                if new_block > last_block:
                    last_block = new_block
                    database.set_sync_state("eth_multisig_last_block", str(last_block))

            exec_from = last_block - 1000 if last_block > 1000 else 0
            exec_events = await get_eth_multisig_execution_events(from_block=exec_from)
            confirmed = []
            for exec_event in exec_events:
                marked = database.mark_multisig_executed(
                    "ETH",
                    exec_tx_id=exec_event["transaction_id"],
                    exec_tx_hash=exec_event.get("tx_hash", ""),
                    confirmed_block_timestamp=exec_event.get("block_timestamp"),
                )
                if marked:
                    confirmed.append((marked, exec_event))

            if confirmed:
                await broadcast_confirmed_alerts(
                    [
                        _confirmed_item(marked, event, "ETH")
                        for marked, event in confirmed
                    ]
                )

            database.set_sync_state(
                "eth_last_scan", datetime.now(timezone.utc).isoformat()
            )
            health.eth_multisig_consecutive_failures = 0
            await asyncio.sleep(ETH_MULTISIG_SCAN_INTERVAL)

        except Exception as e:
            health.eth_multisig_consecutive_failures += 1
            logger.error(
                f"ETH MultiSig listener error ({health.eth_multisig_consecutive_failures}x): {e}"
            )
            if health.eth_multisig_consecutive_failures == MAX_CONSECUTIVE_FAILURES:
                await _notify_scanner_down("ETH MULTISIG", e)
            await asyncio.sleep(10)


async def listen_to_eth_mempool():
    """Pre-block ETH detection. Feeds the same op pipeline as the block watcher."""
    from .eth.mempool import listen_to_eth_mempool as _stream

    await _stream(_handle_block_ops)


async def reap_eth_phantom_submissions():
    """Remove mempool submissions that never landed in a block."""
    while True:
        try:
            await asyncio.sleep(ETH_MEMPOOL_REAP_INTERVAL)
            dropped = database.reap_phantom_submissions("ETH", ETH_MEMPOOL_STALE_S)
            for row in dropped:
                health.eth_mempool_reaped += 1
                logger.warning(
                    f"ETH mempool phantom reaped: {row['action']} -> "
                    f"{row['target_address']} never mined "
                    f"({(row['tx_hash'] or '')[:16]}...)"
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"ETH phantom reaper error: {e}")
