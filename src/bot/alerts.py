import asyncio
from datetime import datetime, timezone
from typing import Optional, Set

from . import bot
from ..config import (
    BROADCAST_CHANNEL,
    WHALE_THRESHOLD,
    logger,
)
from .. import database
from ..utils import format_usdt_balance, format_elapsed_time
from ..tron import get_tron_usdt_balance
from ..eth import get_eth_usdt_balance

_chat_locks: dict[int, asyncio.Lock] = {}
_chat_locks_guard = asyncio.Lock()


async def _lock_for(chat_id: int) -> asyncio.Lock:
    async with _chat_locks_guard:
        lock = _chat_locks.get(chat_id)
        if lock is None:
            lock = asyncio.Lock()
            _chat_locks[chat_id] = lock
        return lock


async def _send(chat_id: int, text: str, disable_preview: bool = True):
    lock = await _lock_for(chat_id)
    async with lock:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=disable_preview,
        )


async def _fanout(chat_ids: Set[int], text: str) -> list:
    if BROADCAST_CHANNEL:
        try:
            chat_ids.add(int(BROADCAST_CHANNEL))
        except (TypeError, ValueError):
            chat_ids.add(BROADCAST_CHANNEL)

    async def one(chat_id):
        try:
            return await _send(chat_id, text)
        except Exception as e:
            logger.debug(f"Failed to send alert to {chat_id}: {e}")
            return None

    if not chat_ids:
        return []
    return await asyncio.gather(*[one(cid) for cid in chat_ids], return_exceptions=True)


def _alert_targets(address: str, chain: str) -> Set[int]:
    targets = {row[0] for row in database.get_subscribers()}
    targets.update(row[0] for row in database.get_watchlist_subscribers(address, chain))
    return targets


async def update_message_with_balance(
    chat_id: int,
    message_id: int,
    address: str,
    chain: str,
    original_text: str,
):
    try:
        if chain == "TRON":
            balance = await get_tron_usdt_balance(address)
        else:
            balance = await get_eth_usdt_balance(address)

        if balance is None:
            return

        whale_tag = " " if balance >= WHALE_THRESHOLD else ""
        balance_str = f"{balance:,.2f} USDT{whale_tag}"
        new_text = original_text.replace("*Balance:* Unknown", f"*Balance:* {balance_str}")
        new_text = new_text.replace("*Balance:* Fetching...", f"*Balance:* {balance_str}")
        if new_text == original_text:
            return

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=new_text,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.debug(f"Failed to update balance for {address}: {e}")


async def broadcast_alert(
    address: str,
    status: str,
    tx_hash: str,
    chain: str = "TRON",
    balance: Optional[float] = None,
    elapsed_seconds: Optional[float] = None,
    is_update: bool = False,
):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if chain == "ETH":
        explorer_link = f"https://etherscan.io/tx/{tx_hash}"
        explorer_name = "Etherscan"
    else:
        explorer_link = f"https://tronscan.org/#/transaction/{tx_hash}"
        explorer_name = "Tronscan"

    balance_text = format_usdt_balance(balance)
    is_whale = balance is not None and balance >= WHALE_THRESHOLD
    whale_tag = " [WHALE]" if is_whale else ""

    if status == "pending":
        title = f"PENDING BLOCK [{chain}]{whale_tag}"
        msg = f"*{title}*\n\n"
        msg += f"*Address:*\n`{address}`\n\n"
        msg += f"*Balance at risk:* {balance_text}\n"
        msg += f"*Detected:* {now_str}\n\n"
        msg += f"Blacklist transaction detected, awaiting confirmation.\n\n"
        msg += f"[View on {explorer_name}]({explorer_link})"
    else:
        if is_update:
            title = f"BLOCK CONFIRMED [{chain}]{whale_tag}"
        else:
            title = f"ADDRESS BLOCKED [{chain}]{whale_tag}"

        msg = f"*{title}*\n\n"
        msg += f"*Address:*\n`{address}`\n\n"
        msg += f"*Balance frozen:* {balance_text}\n"
        msg += f"*Confirmed:* {now_str}\n"
        if elapsed_seconds is not None:
            msg += f"*Time to confirm:* {format_elapsed_time(elapsed_seconds)}\n"
        msg += f"\nAddress officially blacklisted. USDT transfers frozen.\n\n"
        msg += f"[View on {explorer_name}]({explorer_link})"

    watchlist = database.get_watchlist_subscribers(address, chain)
    if watchlist:
        watchlist_msg = "*[WATCHLIST ALERT]*\n\n" + msg
        await _fanout({row[0] for row in watchlist}, watchlist_msg)

    targets = {row[0] for row in database.get_subscribers()}
    await _fanout(targets, msg)


async def broadcast_mempool_alert(
    address: str,
    action: str,
    tx_hash: str,
    chain: str = "TRON",
    balance: Optional[float] = None,
):
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if chain == "ETH":
        explorer_link = f"https://etherscan.io/tx/{tx_hash}"
        explorer_name = "Etherscan"
    else:
        explorer_link = f"https://tronscan.org/#/transaction/{tx_hash}"
        explorer_name = "Tronscan"

    is_whale = balance is not None and balance >= WHALE_THRESHOLD
    whale_tag = " [WHALE]" if is_whale else ""
    balance_str = f"{balance:,.2f} USDT" if balance else "Fetching..."

    if action == "blacklist":
        action_text = "BLACKLIST IN MEMPOOL"
    else:
        action_text = "UNBLACKLIST IN MEMPOOL"

    msg = f"*{action_text}* [{chain}]{whale_tag}\n\n"
    msg += f"*Address:*\n`{address}`\n\n"
    msg += f"*Balance:* {balance_str}\n"
    msg += f"*Detected:* {now_str}\n\n"
    msg += f"Transaction in mempool, awaiting inclusion.\n\n"
    msg += f"[View on {explorer_name}]({explorer_link})"

    sent = await _fanout(_alert_targets(address, chain), msg)
    if balance is None:
        for result in sent:
            if result and getattr(result, "message_id", None):
                asyncio.create_task(
                    update_message_with_balance(
                        result.chat.id, result.message_id, address, chain, msg
                    )
                )


async def broadcast_early_warning_alert(
    address: str,
    action: str,
    multisig_tx_id: int,
    tx_hash: str,
    chain: str = "TRON",
    balance: Optional[float] = None,
    block_timestamp: Optional[int] = None,
):
    if block_timestamp:
        submit_time = datetime.fromtimestamp(block_timestamp / 1000, tz=timezone.utc)
        time_str = submit_time.strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        time_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if chain == "ETH":
        explorer_link = f"https://etherscan.io/tx/{tx_hash}"
    else:
        explorer_link = f"https://tronscan.org/#/transaction/{tx_hash}"

    whale_tag = ""
    if balance is not None and balance >= WHALE_THRESHOLD:
        whale_tag = " [WHALE]"
    balance_str = f"{balance:,.2f} USDT" if balance else "Fetching..."

    if action == "blacklist":
        action_text = "BLACKLIST PENDING"
    else:
        action_text = "UNBLACKLIST PENDING"

    msg = f"*{action_text}* [{chain}]{whale_tag}\n\n"
    msg += f"*Address:*\n`{address}`\n\n"
    msg += f"*Balance:* {balance_str}\n"
    msg += f"*MultiSig TX:* {multisig_tx_id}\n"
    msg += f"*Submitted:* {time_str}\n\n"
    msg += f"[View on Tronscan]({explorer_link})"

    sent = await _fanout(_alert_targets(address, chain), msg)
    if balance is None:
        for result in sent:
            if result and getattr(result, "message_id", None):
                asyncio.create_task(
                    update_message_with_balance(
                        result.chat.id, result.message_id, address, chain, msg
                    )
                )

    logger.info(f"EARLY WARNING sent: {action} -> {address} (MultiSig TX #{multisig_tx_id})")
