import asyncio
import json
from typing import Optional, Callable

import websockets

from ..config import (
    ALCHEMY_WSS_URL,
    ETH_USDT_CONTRACT,
    ADDBLACKLIST_SELECTOR,
    REMOVEBLACKLIST_SELECTOR,
    logger,
)
from .. import health


async def listen_to_eth_pending_transactions(on_blacklist_detected: Callable):
    subscription_id = None

    while True:
        try:
            async with websockets.connect(ALCHEMY_WSS_URL) as ws:
                subscribe_msg = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": [
                        "alchemy_pendingTransactions",
                        {
                            "toAddress": [ETH_USDT_CONTRACT],
                            "hashesOnly": False,
                        },
                    ],
                }
                await ws.send(json.dumps(subscribe_msg))

                response = await ws.recv()
                data = json.loads(response)
                subscription_id = data.get("result")

                if subscription_id:
                    logger.info(f"ETH mempool subscription active: {subscription_id}")
                else:
                    logger.error(f"ETH mempool subscription failed: {data}")
                    await asyncio.sleep(5)
                    continue

                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=60)
                        data = json.loads(msg)

                        if "params" in data and "result" in data["params"]:
                            tx = data["params"]["result"]
                            input_data = tx.get("input", "")

                            if len(input_data) >= 10:
                                selector = input_data[2:10].lower()

                                if selector == ADDBLACKLIST_SELECTOR:
                                    action = "blacklist"
                                elif selector == REMOVEBLACKLIST_SELECTOR:
                                    action = "unblacklist"
                                else:
                                    continue

                                if len(input_data) >= 74:
                                    address = "0x" + input_data[34:74].lower()
                                    tx_hash = tx.get("hash", "")

                                    if tx_hash not in health.eth_seen_pending:
                                        health.eth_seen_pending.add(tx_hash)
                                        health.eth_mempool_scan_count += 1

                                        logger.info(f"ETH MEMPOOL: {action} -> {address}")
                                        await on_blacklist_detected(address, action, tx_hash)

                                        if len(health.eth_seen_pending) > health.MAX_SEEN_CACHE:
                                            health.eth_seen_pending = set(
                                                list(health.eth_seen_pending)[health.MAX_SEEN_CACHE // 2:]
                                            )

                    except asyncio.TimeoutError:
                        await ws.ping()
                        continue

        except websockets.exceptions.ConnectionClosed:
            logger.warning("ETH mempool WebSocket closed, reconnecting...")
            health.eth_mempool_consecutive_failures += 1
            await asyncio.sleep(2)

        except Exception as e:
            health.eth_mempool_consecutive_failures += 1
            logger.error(f"ETH mempool error ({health.eth_mempool_consecutive_failures}x): {e}")
            await asyncio.sleep(5)
