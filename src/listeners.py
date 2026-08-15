import asyncio
from datetime import datetime, timezone

from .config import (
    MAX_ALERTS_PER_CYCLE,
    MAX_CONSECUTIVE_FAILURES,
    MEMPOOL_SCAN_INTERVAL,
    MULTISIG_SCAN_INTERVAL,
    ETH_MULTISIG_SCAN_INTERVAL,
    ADMIN_CHAT_ID,
    logger,
)
from . import database, health
from .bot import bot
from .bot.alerts import broadcast_alert, broadcast_mempool_alert, broadcast_early_warning_alert
from .tron import (
    get_blacklist_events,
    get_unfreeze_events,
    check_address_blacklisted,
    get_tron_usdt_balance,
    decode_multisig_submit_data,
    get_multisig_submission_events,
    get_multisig_execution_events,
    get_multisig_tx_details,
    scan_mempool_for_blacklist,
)
from .eth import (
    get_eth_blacklist_events,
    get_eth_usdt_balance,
    get_eth_multisig_submission_events,
    get_eth_multisig_execution_events,
    get_eth_multisig_tx_target,
    get_current_eth_block,
    listen_to_eth_pending_transactions,
)


async def listen_to_tron_blacklist_events():
    is_initialized = database.get_sync_state("tron_initialized") is not None

    while True:
        try:
            logger.info("Scanning TRON for blacklist events...")
            health.tron_scan_count += 1

            last_timestamp_str = database.get_sync_state("tron_last_block_timestamp")
            last_timestamp = int(last_timestamp_str) if last_timestamp_str else None

            events = await get_blacklist_events(min_timestamp=last_timestamp)
            alerts_sent = 0

            conn = database.get_db()
            cursor = conn.cursor()

            for event in events:
                address = event["address"]
                tx_hash = event["tx_hash"]
                confirmed = event["confirmed"]
                block_ts = event["block_timestamp"]
                status = "confirmed" if confirmed else "pending"

                cursor.execute(
                    "SELECT status, alerted, detected_at FROM blacklist WHERE address = ? AND chain = 'TRON'",
                    (address,),
                )
                existing = cursor.fetchone()

                if not existing:
                    should_alert = is_initialized and alerts_sent < MAX_ALERTS_PER_CYCLE
                    cursor.execute(
                        "INSERT INTO blacklist (address, chain, status, tx_hash, alerted, block_timestamp) VALUES (?, 'TRON', ?, ?, ?, ?)",
                        (address, status, tx_hash, 1 if should_alert else 0, block_ts),
                    )
                    conn.commit()
                    logger.info(f"TRON NEW: {address} -> {status}")

                    if should_alert:
                        balance = await get_tron_usdt_balance(address)
                        await broadcast_alert(address, status, tx_hash, chain="TRON", balance=balance)
                        alerts_sent += 1
                        await asyncio.sleep(0.5)

                elif existing[0] == "pending" and status == "confirmed":
                    should_alert = alerts_sent < MAX_ALERTS_PER_CYCLE
                    elapsed_seconds = None

                    multisig_ts = database.get_multisig_submission_timestamp(address, "TRON")
                    if multisig_ts and block_ts:
                        elapsed_seconds = (block_ts - multisig_ts) / 1000.0
                    elif existing[2]:
                        try:
                            detected_time = datetime.fromisoformat(existing[2].replace("Z", "+00:00"))
                            if detected_time.tzinfo is None:
                                detected_time = detected_time.replace(tzinfo=timezone.utc)
                            elapsed_seconds = (datetime.now(timezone.utc) - detected_time).total_seconds()
                        except Exception:
                            pass

                    cursor.execute(
                        "UPDATE blacklist SET status = 'confirmed', confirmed_at = CURRENT_TIMESTAMP, alerted = ?, confirmed_block_timestamp = ? WHERE address = ? AND chain = 'TRON'",
                        (1 if should_alert else existing[1], block_ts, address),
                    )
                    conn.commit()
                    logger.info(f"TRON UPDATE: {address} -> confirmed")

                    if should_alert:
                        balance = await get_tron_usdt_balance(address)
                        await broadcast_alert(
                            address, "confirmed", tx_hash, chain="TRON",
                            balance=balance, elapsed_seconds=elapsed_seconds, is_update=True
                        )
                        alerts_sent += 1
                        await asyncio.sleep(0.5)

            if not is_initialized:
                database.set_sync_state("tron_initialized", "1")
                is_initialized = True
                logger.info("TRON initial sync complete.")

            database.set_sync_state("tron_last_scan", datetime.now(timezone.utc).isoformat())

            if events:
                max_ts = max(e["block_timestamp"] for e in events)
                database.set_sync_state("tron_last_block_timestamp", str(max_ts))

            conn.close()

            if health.tron_consecutive_failures > 0:
                logger.info(f"TRON scanner recovered after {health.tron_consecutive_failures} failures")
                health.tron_consecutive_failures = 0

            await asyncio.sleep(2)

        except Exception as e:
            health.tron_consecutive_failures += 1
            logger.error(f"TRON listener error ({health.tron_consecutive_failures}x): {e}")

            if health.tron_consecutive_failures == MAX_CONSECUTIVE_FAILURES and ADMIN_CHAT_ID:
                try:
                    await bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=f"*TRON SCANNER ALERT*\n\n{MAX_CONSECUTIVE_FAILURES} failures!\nError: {str(e)[:200]}",
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

            await asyncio.sleep(10)


async def listen_to_eth_blacklist_events():
    is_initialized = database.get_sync_state("eth_initialized") is not None

    while True:
        try:
            logger.info("Scanning ETH for blacklist events...")
            health.eth_scan_count += 1

            last_block_str = database.get_sync_state("eth_last_block_number")
            last_block = int(last_block_str) if last_block_str else None

            events = await get_eth_blacklist_events(from_block=last_block)
            alerts_sent = 0

            conn = database.get_db()
            cursor = conn.cursor()

            for event in events:
                address = event["address"]
                tx_hash = event["tx_hash"]
                block_ts = event["block_timestamp"]
                status = "confirmed"

                cursor.execute(
                    "SELECT status, alerted FROM blacklist WHERE address = ? AND chain = 'ETH'",
                    (address,),
                )
                existing = cursor.fetchone()

                if not existing:
                    should_alert = is_initialized and alerts_sent < MAX_ALERTS_PER_CYCLE
                    cursor.execute(
                        "INSERT INTO blacklist (address, chain, status, tx_hash, alerted, confirmed_at, block_timestamp) VALUES (?, 'ETH', ?, ?, ?, CURRENT_TIMESTAMP, ?)",
                        (address, status, tx_hash, 1 if should_alert else 0, block_ts),
                    )
                    conn.commit()
                    logger.info(f"ETH NEW: {address} -> {status}")

                    if should_alert:
                        balance = await get_eth_usdt_balance(address)
                        await broadcast_alert(address, status, tx_hash, chain="ETH", balance=balance)
                        alerts_sent += 1
                        await asyncio.sleep(0.5)

            if not is_initialized:
                database.set_sync_state("eth_initialized", "1")
                is_initialized = True
                logger.info("ETH initial sync complete.")

            database.set_sync_state("eth_last_scan", datetime.now(timezone.utc).isoformat())

            if events:
                max_block = max(e["block_number"] for e in events)
                database.set_sync_state("eth_last_block_number", str(max_block + 1))

            conn.close()

            if health.eth_consecutive_failures > 0:
                logger.info(f"ETH scanner recovered after {health.eth_consecutive_failures} failures")
                health.eth_consecutive_failures = 0

            await asyncio.sleep(3)

        except Exception as e:
            health.eth_consecutive_failures += 1
            logger.error(f"ETH listener error ({health.eth_consecutive_failures}x): {e}")

            if health.eth_consecutive_failures == MAX_CONSECUTIVE_FAILURES and ADMIN_CHAT_ID:
                try:
                    await bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=f"*ETH SCANNER ALERT*\n\n{MAX_CONSECUTIVE_FAILURES} failures!\nError: {str(e)[:200]}",
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

            await asyncio.sleep(10)


async def listen_to_mempool():
    logger.info("TRON Mempool listener started")

    while True:
        try:
            health.mempool_scan_count += 1

            mempool_results = await scan_mempool_for_blacklist()
            alerts_sent = 0

            conn = database.get_db()
            cursor = conn.cursor()

            for result in mempool_results:
                address = result["address"]
                action = result["action"]
                tx_hash = result["tx_hash"]

                if tx_hash in health.seen_mempool_txs:
                    continue
                health.seen_mempool_txs.add(tx_hash)

                cursor.execute(
                    "SELECT status FROM blacklist WHERE address = ? AND chain = 'TRON'",
                    (address,),
                )
                existing = cursor.fetchone()

                if action == "blacklist":
                    if not existing:
                        cursor.execute(
                            "INSERT INTO blacklist (address, chain, status, tx_hash, alerted) VALUES (?, 'TRON', 'mempool_pending', ?, 1)",
                            (address, tx_hash),
                        )
                        conn.commit()
                        logger.info(f"MEMPOOL NEW: {address}")

                        if alerts_sent < MAX_ALERTS_PER_CYCLE:
                            await broadcast_mempool_alert(address, action, tx_hash, chain="TRON", balance=None)
                            alerts_sent += 1
                            await asyncio.sleep(0.3)

                elif action == "unblacklist":
                    if existing and existing[0] in ("pending", "confirmed", "mempool_pending"):
                        cursor.execute(
                            "UPDATE blacklist SET status = 'mempool_unfreeze', tx_hash = ? WHERE address = ? AND chain = 'TRON'",
                            (tx_hash, address),
                        )
                        conn.commit()
                        logger.info(f"MEMPOOL UNFREEZE: {address}")

                        if alerts_sent < MAX_ALERTS_PER_CYCLE:
                            await broadcast_mempool_alert(address, action, tx_hash, chain="TRON", balance=None)
                            alerts_sent += 1
                            await asyncio.sleep(0.3)

            conn.close()

            if len(health.seen_mempool_txs) > health.MAX_SEEN_CACHE:
                health.seen_mempool_txs = set(list(health.seen_mempool_txs)[health.MAX_SEEN_CACHE // 2:])

            if health.mempool_consecutive_failures > 0:
                logger.info(f"Mempool scanner recovered after {health.mempool_consecutive_failures} failures")
                health.mempool_consecutive_failures = 0

            await asyncio.sleep(MEMPOOL_SCAN_INTERVAL)

        except Exception as e:
            health.mempool_consecutive_failures += 1
            logger.error(f"Mempool listener error ({health.mempool_consecutive_failures}x): {e}")

            if health.mempool_consecutive_failures == MAX_CONSECUTIVE_FAILURES and ADMIN_CHAT_ID:
                try:
                    await bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=f"*MEMPOOL SCANNER ALERT*\n\n{MAX_CONSECUTIVE_FAILURES} failures!\nError: {str(e)[:200]}",
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

            await asyncio.sleep(5)


async def listen_to_multisig_submissions():
    logger.info("TRON MultiSig listener started")

    last_timestamp_str = database.get_sync_state("multisig_last_timestamp")
    if last_timestamp_str:
        last_timestamp = int(last_timestamp_str)
    else:
        last_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        database.set_sync_state("multisig_last_timestamp", str(last_timestamp))
        logger.info(f"MultiSig: First run - starting from timestamp {last_timestamp}")

    while True:
        try:
            health.multisig_scan_count += 1

            events = await get_multisig_submission_events(min_timestamp=last_timestamp)

            if events:
                logger.info(f"MultiSig: Found {len(events)} Submission events")

            alerts_sent = 0
            processed_timestamps = []

            for event in events:
                tx_id = event["transaction_id"]
                tx_hash = event["tx_hash"]
                block_timestamp = event["block_timestamp"]

                conn = database.get_db()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT tx_id FROM multisig_pending WHERE tx_id = ? AND chain = 'TRON'",
                    (tx_id,),
                )
                existing = cursor.fetchone()

                if existing:
                    conn.close()
                    processed_timestamps.append(block_timestamp)
                    continue

                tx_details = await get_multisig_tx_details(tx_hash)
                if not tx_details:
                    conn.close()
                    continue

                contracts = tx_details.get("raw_data", {}).get("contract", [])
                if not contracts:
                    conn.close()
                    processed_timestamps.append(block_timestamp)
                    continue

                contract = contracts[0]
                param = contract.get("parameter", {}).get("value", {})
                data = param.get("data", "")

                if not data:
                    conn.close()
                    processed_timestamps.append(block_timestamp)
                    continue

                decoded = decode_multisig_submit_data(data)

                if not decoded:
                    conn.close()
                    processed_timestamps.append(block_timestamp)
                    continue

                target_address = decoded["target_address"]
                action = decoded["action"]

                logger.info(f"MULTISIG EARLY DETECTION: {action} -> {target_address} (TX #{tx_id})")

                cursor.execute(
                    """INSERT OR REPLACE INTO multisig_pending
                       (tx_id, chain, target_address, action, submission_tx_hash, submitted_at, submission_block_timestamp, alerted)
                       VALUES (?, 'TRON', ?, ?, ?, CURRENT_TIMESTAMP, ?, 0)""",
                    (tx_id, target_address, action, tx_hash, block_timestamp),
                )

                if action == "blacklist":
                    cursor.execute(
                        "SELECT status FROM blacklist WHERE address = ? AND chain = 'TRON'",
                        (target_address,),
                    )
                    existing_bl = cursor.fetchone()

                    if not existing_bl:
                        cursor.execute(
                            """INSERT INTO blacklist
                               (address, chain, status, tx_hash, alerted, detected_at, block_timestamp)
                               VALUES (?, 'TRON', 'multisig_pending', ?, 1, CURRENT_TIMESTAMP, ?)""",
                            (target_address, tx_hash, block_timestamp),
                        )

                conn.commit()
                conn.close()

                if alerts_sent < MAX_ALERTS_PER_CYCLE:
                    await broadcast_early_warning_alert(
                        address=target_address,
                        action=action,
                        multisig_tx_id=tx_id,
                        tx_hash=tx_hash,
                        chain="TRON",
                        balance=None,
                        block_timestamp=block_timestamp,
                    )
                    alerts_sent += 1

                processed_timestamps.append(block_timestamp)

            if processed_timestamps:
                new_ts = max(processed_timestamps)
                if new_ts > last_timestamp:
                    database.set_sync_state("multisig_last_timestamp", str(new_ts))
                    last_timestamp = new_ts

            exec_events = await get_multisig_execution_events(min_timestamp=last_timestamp)
            for exec_event in exec_events:
                exec_tx_id = exec_event["transaction_id"]
                conn = database.get_db()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE multisig_pending SET executed = 1, executed_at = CURRENT_TIMESTAMP WHERE tx_id = ? AND chain = 'TRON' AND executed = 0",
                    (exec_tx_id,),
                )
                if cursor.rowcount > 0:
                    logger.info(f"MultiSig TX #{exec_tx_id} executed")
                conn.commit()
                conn.close()

            if health.multisig_consecutive_failures > 0:
                logger.info(f"MultiSig scanner recovered after {health.multisig_consecutive_failures} failures")
                health.multisig_consecutive_failures = 0

            await asyncio.sleep(MULTISIG_SCAN_INTERVAL)

        except Exception as e:
            health.multisig_consecutive_failures += 1
            logger.error(f"MultiSig listener error ({health.multisig_consecutive_failures}x): {e}")

            if health.multisig_consecutive_failures == MAX_CONSECUTIVE_FAILURES and ADMIN_CHAT_ID:
                try:
                    await bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=f"*MULTISIG SCANNER ALERT*\n\n{MAX_CONSECUTIVE_FAILURES} failures!\nError: {str(e)[:200]}",
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

            await asyncio.sleep(10)


async def listen_to_eth_multisig_submissions():
    logger.info("ETH MultiSig listener started")

    last_block = 0
    initialized = False

    while True:
        try:
            if not initialized:
                current_block = await get_current_eth_block()
                if current_block:
                    last_block = current_block - 20
                    logger.info(f"ETH MultiSig: Starting from block {last_block}")
                    initialized = True
                else:
                    await asyncio.sleep(5)
                    continue

            health.eth_multisig_scan_count += 1

            events = await get_eth_multisig_submission_events(from_block=last_block)

            if events:
                logger.info(f"ETH MultiSig: Found {len(events)} Submission events")

            alerts_sent = 0
            processed_blocks = []

            for event in events:
                tx_id = event["transaction_id"]
                tx_hash = event["tx_hash"]
                block_number = event["block_number"]
                block_timestamp = event["block_timestamp"]

                conn = database.get_db()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT tx_id FROM multisig_pending WHERE tx_id = ? AND chain = 'ETH'",
                    (tx_id,),
                )
                existing = cursor.fetchone()

                if existing:
                    conn.close()
                    processed_blocks.append(block_number)
                    continue

                target_info = await get_eth_multisig_tx_target(tx_hash)

                if not target_info:
                    conn.close()
                    processed_blocks.append(block_number)
                    continue

                target_address = target_info["target_address"]
                action = target_info["action"]

                logger.info(f"ETH MULTISIG EARLY DETECTION: {action} -> {target_address} (TX #{tx_id})")

                cursor.execute(
                    """INSERT OR REPLACE INTO multisig_pending
                       (tx_id, chain, target_address, action, submission_tx_hash, submitted_at, submission_block_timestamp, alerted)
                       VALUES (?, 'ETH', ?, ?, ?, CURRENT_TIMESTAMP, ?, 0)""",
                    (tx_id, target_address, action, tx_hash, block_timestamp),
                )

                if action == "blacklist":
                    cursor.execute(
                        "SELECT status FROM blacklist WHERE address = ? AND chain = 'ETH'",
                        (target_address,),
                    )
                    existing_bl = cursor.fetchone()

                    if not existing_bl:
                        cursor.execute(
                            """INSERT INTO blacklist
                               (address, chain, status, tx_hash, alerted, detected_at, block_timestamp)
                               VALUES (?, 'ETH', 'multisig_pending', ?, 1, CURRENT_TIMESTAMP, ?)""",
                            (target_address, tx_hash, block_timestamp),
                        )

                conn.commit()
                conn.close()

                if alerts_sent < MAX_ALERTS_PER_CYCLE:
                    balance = await get_eth_usdt_balance(target_address)
                    await broadcast_early_warning_alert(
                        address=target_address,
                        action=action,
                        multisig_tx_id=tx_id,
                        tx_hash=tx_hash,
                        chain="ETH",
                        balance=balance,
                        block_timestamp=block_timestamp,
                    )
                    alerts_sent += 1

                processed_blocks.append(block_number)

            if processed_blocks:
                new_block = max(processed_blocks) + 1
                if new_block > last_block:
                    last_block = new_block

            exec_events = await get_eth_multisig_execution_events(from_block=last_block - 1000 if last_block > 1000 else 0)
            for exec_event in exec_events:
                exec_tx_id = exec_event["transaction_id"]
                conn = database.get_db()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE multisig_pending SET executed = 1, executed_at = CURRENT_TIMESTAMP WHERE tx_id = ? AND chain = 'ETH' AND executed = 0",
                    (exec_tx_id,),
                )
                if cursor.rowcount > 0:
                    logger.info(f"ETH MultiSig TX #{exec_tx_id} executed")
                conn.commit()
                conn.close()

            if health.eth_multisig_consecutive_failures > 0:
                logger.info(f"ETH MultiSig scanner recovered after {health.eth_multisig_consecutive_failures} failures")
                health.eth_multisig_consecutive_failures = 0

            await asyncio.sleep(ETH_MULTISIG_SCAN_INTERVAL)

        except Exception as e:
            health.eth_multisig_consecutive_failures += 1
            logger.error(f"ETH MultiSig listener error ({health.eth_multisig_consecutive_failures}x): {e}")

            if health.eth_multisig_consecutive_failures == MAX_CONSECUTIVE_FAILURES and ADMIN_CHAT_ID:
                try:
                    await bot.send_message(
                        chat_id=ADMIN_CHAT_ID,
                        text=f"*ETH MULTISIG SCANNER ALERT*\n\n{MAX_CONSECUTIVE_FAILURES} failures!\nError: {str(e)[:200]}",
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass

            await asyncio.sleep(10)


async def listen_to_eth_mempool():
    logger.info("ETH Mempool listener started (Alchemy WebSocket)")

    async def on_blacklist_detected(address: str, action: str, tx_hash: str):
        conn = database.get_db()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT status FROM blacklist WHERE address = ? AND chain = 'ETH'",
            (address,),
        )
        existing = cursor.fetchone()

        if action == "blacklist":
            if not existing:
                cursor.execute(
                    "INSERT INTO blacklist (address, chain, status, tx_hash, alerted) VALUES (?, 'ETH', 'mempool_pending', ?, 1)",
                    (address, tx_hash),
                )
                conn.commit()
                logger.info(f"ETH MEMPOOL NEW: {address}")
                await broadcast_mempool_alert(address, action, tx_hash, chain="ETH", balance=None)

        elif action == "unblacklist":
            if existing and existing[0] in ("pending", "confirmed", "mempool_pending"):
                cursor.execute(
                    "UPDATE blacklist SET status = 'mempool_unfreeze', tx_hash = ? WHERE address = ? AND chain = 'ETH'",
                    (tx_hash, address),
                )
                conn.commit()
                logger.info(f"ETH MEMPOOL UNFREEZE: {address}")
                await broadcast_mempool_alert(address, action, tx_hash, chain="ETH", balance=None)

        conn.close()

    await listen_to_eth_pending_transactions(on_blacklist_detected)
