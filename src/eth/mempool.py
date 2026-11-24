"""ETH pre-block (mempool) detection of Tether MultiSig blacklist submissions.

Why this exists: an ETH block is not servable by any provider until it has
propagated, measured at ~1.8-2.2s after its own slot timestamp. So block-based
detection has a hard floor around 2s that no provider, transport or poll
interval can remove (Chainstack WSS newHeads measured no faster than HTTP
polling). A pending transaction is gossiped across the p2p network *before* any
block contains it, so this is the only path to sub-second detection.

Scope is deliberately narrow: this emits `submission` ops only, never
`execution`. A phantom pending (tx dropped or replaced) is recoverable -- the
reaper in listeners removes it. A row wrongly promoted to `confirmed` is not.
Confirmation stays on-chain truth, decided by the block watcher.

Ops come out in the same shape as block_watcher.extract_eth_multisig_ops, with
block_number/block_timestamp None because no block exists yet, so
_handle_block_ops consumes them unchanged.
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

import websockets

from ..config import (
    ETH_MEMPOOL_WSS_URLS,
    ETH_MULTISIG_WALLET,
    logger,
)
from .. import health
from .multisig import decode_eth_submit_input

MULTISIG = ETH_MULTISIG_WALLET.lower()

# Alchemy filters server-side on toAddress, so we receive only multisig traffic
# instead of every pending tx on the network (~200/s, which would need one
# getTransactionByHash each and be useless for latency).
_ALCHEMY_SUB = [
    "alchemy_pendingTransactions",
    {"toAddress": [ETH_MULTISIG_WALLET], "hashesOnly": False},
]
# geth/erigon: full pending tx bodies, filtered client-side.
_GETH_FULL_SUB = ["newPendingTransactions", True]


def extract_mempool_ops(tx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Decode a pending tx into submission ops. Same shape as the block path."""
    if (tx.get("to") or "").lower() != MULTISIG:
        return []
    tx_hash = tx.get("hash") or ""
    data = tx.get("input") or ""
    if not tx_hash or not data:
        return []

    submitted = decode_eth_submit_input(data)
    if not submitted:
        return []
    return [
        {
            "kind": "submission",
            "tx_hash": tx_hash,
            "action": submitted["action"],
            "target_address": submitted["target_address"],
            "token": submitted["token"],
            # No block yet. The block watcher backfills both on arrival.
            "block_number": None,
            "block_timestamp": None,
            "source": "mempool",
        }
    ]


def _mark_seen(tx_hash: str) -> bool:
    """True if this hash is new. Bounded set, so a long uptime cannot leak."""
    h = tx_hash.lower()
    if h in health.eth_seen_pending:
        return False
    health.eth_seen_pending.add(h)
    if len(health.eth_seen_pending) > health.MAX_SEEN_CACHE:
        # Drop the oldest half. Set order is arbitrary, but a re-alert on an
        # evicted hash is harmless: record_multisig_submission dedupes in sqlite.
        keep = list(health.eth_seen_pending)[health.MAX_SEEN_CACHE // 2 :]
        health.eth_seen_pending = set(keep)
    return True


async def _subscribe(ws, params: list) -> Optional[str]:
    await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": params}))
    ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=20))
    if ack.get("error"):
        logger.warning(f"ETH mempool subscribe rejected for {params[0]}: {ack['error']}")
        return None
    return ack.get("result")


def _tx_from_message(msg: dict) -> Optional[Dict[str, Any]]:
    result = (msg.get("params") or {}).get("result")
    if isinstance(result, dict):
        return result
    return None


def _set_connected(host: str, up: bool) -> None:
    """Per-source liveness. One provider dropping must not report the pair down."""
    health.eth_mempool_sources[host] = up
    health.eth_mempool_connected = any(health.eth_mempool_sources.values())


async def _run_source(url: str, on_ops: Callable[[list, str], Awaitable[None]]):
    """One provider. Reconnects forever; _mark_seen makes the race idempotent."""
    host = url.split("/")[2] if "/" in url else url
    while True:
        try:
            async with websockets.connect(
                url, open_timeout=20, ping_interval=20, ping_timeout=20
            ) as ws:
                sub = await _subscribe(ws, _ALCHEMY_SUB)
                filtering_locally = False
                if not sub:
                    # Not an Alchemy endpoint; fall back to the standard method.
                    sub = await _subscribe(ws, _GETH_FULL_SUB)
                    filtering_locally = True
                if not sub:
                    health.eth_mempool_consecutive_failures += 1
                    logger.error(
                        f"ETH mempool[{host}]: no usable pending subscription; retrying"
                    )
                    await asyncio.sleep(10)
                    continue

                _set_connected(host, True)
                health.eth_mempool_consecutive_failures = 0
                logger.info(
                    "ETH mempool[%s] active (%s, %s filtering): %s",
                    host,
                    _GETH_FULL_SUB[0] if filtering_locally else _ALCHEMY_SUB[0],
                    "client-side" if filtering_locally else "server-side",
                    sub,
                )

                while True:
                    msg = json.loads(await ws.recv())
                    tx = _tx_from_message(msg)
                    if not tx:
                        continue
                    health.eth_mempool_scan_count += 1
                    ops = extract_mempool_ops(tx)
                    if not ops:
                        continue
                    op = ops[0]
                    if not _mark_seen(op["tx_hash"]):
                        continue
                    health.eth_mempool_detections += 1
                    health.eth_mempool_last_seen_at = datetime.now(timezone.utc).isoformat()
                    op["source"] = f"mempool:{host}"
                    logger.info(
                        f"ETH MEMPOOL[{host}] {op['action'].upper()}: "
                        f"{op['target_address']} ({op['tx_hash'][:16]}...) pre-block"
                    )
                    await on_ops(ops, "ETH")

        except asyncio.CancelledError:
            raise
        except websockets.exceptions.ConnectionClosed:
            _set_connected(host, False)
            health.eth_mempool_consecutive_failures += 1
            logger.warning(f"ETH mempool[{host}] WebSocket closed, reconnecting...")
            await asyncio.sleep(2)
        except Exception as e:
            _set_connected(host, False)
            health.eth_mempool_consecutive_failures += 1
            logger.error(
                f"ETH mempool[{host}] error "
                f"({health.eth_mempool_consecutive_failures}x): {e}"
            )
            await asyncio.sleep(5)


async def listen_to_eth_mempool(on_ops: Callable[[list, str], Awaitable[None]]):
    """Race every configured mempool source; the first to see a tx wins.

    Sources are independent -- one stalling or being rate-limited does not delay
    the others, and the shared seen-set means a tx arriving twice is acted on
    once. Latency becomes min() across providers rather than one provider's luck.
    """
    urls = list(ETH_MEMPOOL_WSS_URLS)
    if not urls:
        logger.error("ETH mempool enabled but no WSS sources configured; not starting")
        return
    logger.info(
        "ETH mempool racing %d source(s): %s",
        len(urls),
        ", ".join(u.split("/")[2] for u in urls),
    )
    await asyncio.gather(*[_run_source(u, on_ops) for u in urls])
