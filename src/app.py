import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from aiogram.types import Update

from .config import (
    WEBHOOK_URL,
    WEBHOOK_SECRET,
    ENABLE_TRON,
    ENABLE_ETH,
    ENABLE_ETH_MEMPOOL,
    TRON_USDT_CONTRACT,
    ETH_USDT_CONTRACT,
    TRON_MULTISIG_WALLET,
    ETH_MULTISIG_WALLET,
    MAX_CONSECUTIVE_FAILURES,
    logger,
)
from .database import init_db
from .bot import bot, dp
from .bot import handlers  # noqa: F401
from . import health
from .listeners import (
    listen_to_tron_blocks,
    listen_to_multisig_submissions,
    listen_to_eth_blocks,
    listen_to_eth_multisig_submissions,
    listen_to_eth_mempool,
    reap_eth_phantom_submissions,
)
from .eth.rpc import rpc_enabled


def _start_listeners():
    tasks = []
    if ENABLE_TRON:
        tasks.append(asyncio.create_task(listen_to_tron_blocks()))
        tasks.append(asyncio.create_task(listen_to_multisig_submissions()))
        logger.info("TRON block watcher + HTTP fallback started")
    if ENABLE_ETH:
        if rpc_enabled():
            tasks.append(asyncio.create_task(listen_to_eth_blocks()))
            logger.info("ETH block watcher + Etherscan fallback started")
        else:
            logger.warning("ETH_RPC_URL unset; ETH live path is Etherscan only")
        tasks.append(asyncio.create_task(listen_to_eth_multisig_submissions()))
        logger.info("ETH MultiSig Etherscan fallback started (Submission + Execution)")
        if ENABLE_ETH_MEMPOOL:
            tasks.append(asyncio.create_task(listen_to_eth_mempool()))
            tasks.append(asyncio.create_task(reap_eth_phantom_submissions()))
            logger.info("ETH mempool pre-block watcher started (+ phantom reaper)")

    chains = []
    if ENABLE_TRON:
        chains.append("TRON blocks")
    if ENABLE_ETH:
        chains.append("ETH blocks" if rpc_enabled() else "ETH MultiSig")
    logger.info(f"Bot started. Watching {' & '.join(chains) or 'nothing'} for USDT blacklist.")
    return tasks


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    listener_tasks = _start_listeners()

    if WEBHOOK_URL:
        await bot.set_webhook(
            url=f"{WEBHOOK_URL}/webhook",
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
        )
        logger.info(f"Webhook: {WEBHOOK_URL}/webhook")

    yield

    for task in listener_tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    if WEBHOOK_URL:
        await bot.delete_webhook()
    await bot.session.close()


app = FastAPI(lifespan=lifespan)


@app.post("/webhook")
async def handle_webhook(
    request: Request, x_telegram_bot_api_secret_token: str = Header(None)
):
    if x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    update_data = await request.json()
    update = Update(**update_data)
    await dp.feed_update(bot=bot, update=update)
    return {"status": "ok"}


@app.get("/accuracy")
async def accuracy_endpoint():
    from .accuracy import build_report

    return build_report()


@app.get("/health")
async def health_endpoint():
    return {
        "status": "ok",
        "chains": {
            "tron": {
                "enabled": ENABLE_TRON,
                "contract": TRON_USDT_CONTRACT,
                "multisig_wallet": TRON_MULTISIG_WALLET,
            },
            "eth": {
                "enabled": ENABLE_ETH,
                "contract": ETH_USDT_CONTRACT,
                "multisig_wallet": ETH_MULTISIG_WALLET,
            },
        },
        "scanners": {
            "tron_blocks": {
                "scans": health.block_scan_count,
                "failures": health.block_consecutive_failures,
                "last_block": health.block_last_number,
                "detections": health.block_detections,
                "source": health.block_source,
                "healthy": health.block_consecutive_failures < MAX_CONSECUTIVE_FAILURES,
            },
            "tron_multisig_fallback": {
                "scans": health.multisig_scan_count,
                "failures": health.multisig_consecutive_failures,
                "healthy": health.multisig_consecutive_failures < MAX_CONSECUTIVE_FAILURES,
            },
            "eth_blocks": {
                "enabled": rpc_enabled(),
                "scans": health.eth_block_scan_count,
                "failures": health.eth_block_consecutive_failures,
                "last_block": health.eth_block_last_number,
                "detections": health.eth_block_detections,
                "source": health.eth_block_source,
                "healthy": health.eth_block_consecutive_failures < MAX_CONSECUTIVE_FAILURES,
            },
            "eth_mempool": {
                "enabled": ENABLE_ETH_MEMPOOL,
                "connected": health.eth_mempool_connected,
                "seen": health.eth_mempool_scan_count,
                "detections": health.eth_mempool_detections,
                "reaped": health.eth_mempool_reaped,
                "last_seen_at": health.eth_mempool_last_seen_at,
                "failures": health.eth_mempool_consecutive_failures,
                "healthy": (
                    not ENABLE_ETH_MEMPOOL
                    or health.eth_mempool_consecutive_failures < MAX_CONSECUTIVE_FAILURES
                ),
            },
            "eth_multisig_fallback": {
                "scans": health.eth_multisig_scan_count,
                "failures": health.eth_multisig_consecutive_failures,
                "healthy": health.eth_multisig_consecutive_failures < MAX_CONSECUTIVE_FAILURES,
            },
        },
    }


async def run_polling():
    init_db()
    listener_tasks = _start_listeners()
    try:
        await dp.start_polling(bot)
    finally:
        for task in listener_tasks:
            task.cancel()
        await bot.session.close()
