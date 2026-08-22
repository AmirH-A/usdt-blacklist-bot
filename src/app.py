import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from aiogram.types import Update

from .config import (
    WEBHOOK_URL,
    WEBHOOK_SECRET,
    ENABLE_TRON,
    ENABLE_ETH,
    TRON_USDT_CONTRACT,
    ETH_USDT_CONTRACT,
    TRON_MULTISIG_WALLET,
    MAX_CONSECUTIVE_FAILURES,
    logger,
)
from .database import init_db
from .bot import bot, dp
from .bot import handlers  # noqa: F401
from . import health
from .listeners import (
    listen_to_tron_blacklist_events,
    listen_to_eth_blacklist_events,
    listen_to_mempool,
    listen_to_multisig_submissions,
    listen_to_eth_multisig_submissions,
    listen_to_eth_mempool,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    listener_tasks = []

    if ENABLE_TRON:
        listener_tasks.append(asyncio.create_task(listen_to_multisig_submissions()))
        logger.info("MULTISIG listener started - EARLIEST DETECTION")

        listener_tasks.append(asyncio.create_task(listen_to_mempool()))
        logger.info("MEMPOOL listener started - EARLY DETECTION")

        listener_tasks.append(asyncio.create_task(listen_to_tron_blacklist_events()))
        logger.info("TRON event listener started")

    if ENABLE_ETH:
        listener_tasks.append(asyncio.create_task(listen_to_eth_multisig_submissions()))
        logger.info("ETH MULTISIG listener started")

        listener_tasks.append(asyncio.create_task(listen_to_eth_mempool()))
        logger.info("ETH MEMPOOL listener started (Alchemy)")

        listener_tasks.append(asyncio.create_task(listen_to_eth_blacklist_events()))
        logger.info("ETH event listener started")

    if WEBHOOK_URL:
        await bot.set_webhook(
            url=f"{WEBHOOK_URL}/webhook",
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
        )
        logger.info(f"Webhook: {WEBHOOK_URL}/webhook")

    chains = []
    if ENABLE_TRON:
        chains.append("TRON (MultiSig + mempool + events)")
    if ENABLE_ETH:
        chains.append("ETH (MultiSig + mempool + events)")
    logger.info(f"Bot started. Monitoring {' & '.join(chains)} USDT blacklist...")

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
            },
        },
        "scanners": {
            "multisig": {
                "scans": health.multisig_scan_count,
                "failures": health.multisig_consecutive_failures,
                "healthy": health.multisig_consecutive_failures < MAX_CONSECUTIVE_FAILURES,
            },
            "tron_mempool": {
                "scans": health.mempool_scan_count,
                "failures": health.mempool_consecutive_failures,
                "healthy": health.mempool_consecutive_failures < MAX_CONSECUTIVE_FAILURES,
            },
            "eth_mempool": {
                "scans": health.eth_mempool_scan_count,
                "failures": health.eth_mempool_consecutive_failures,
                "healthy": health.eth_mempool_consecutive_failures < MAX_CONSECUTIVE_FAILURES,
            },
        },
    }


async def run_polling():
    init_db()
    listener_tasks = []

    if ENABLE_TRON:
        listener_tasks.append(asyncio.create_task(listen_to_multisig_submissions()))
        logger.info("MULTISIG listener started - EARLIEST DETECTION")

        listener_tasks.append(asyncio.create_task(listen_to_mempool()))
        logger.info("MEMPOOL listener started - EARLY DETECTION")

        listener_tasks.append(asyncio.create_task(listen_to_tron_blacklist_events()))
        logger.info("TRON event listener started")

    if ENABLE_ETH:
        listener_tasks.append(asyncio.create_task(listen_to_eth_multisig_submissions()))
        logger.info("ETH MULTISIG listener started")

        listener_tasks.append(asyncio.create_task(listen_to_eth_mempool()))
        logger.info("ETH MEMPOOL listener started (Alchemy)")

        listener_tasks.append(asyncio.create_task(listen_to_eth_blacklist_events()))
        logger.info("ETH event listener started")

    chains = []
    if ENABLE_TRON:
        chains.append("TRON (MultiSig + mempool + events)")
    if ENABLE_ETH:
        chains.append("ETH (MultiSig + mempool + events)")
    logger.info(f"Bot started. Monitoring {' & '.join(chains)} USDT blacklist...")

    try:
        await dp.start_polling(bot)
    finally:
        for task in listener_tasks:
            task.cancel()
        await bot.session.close()
