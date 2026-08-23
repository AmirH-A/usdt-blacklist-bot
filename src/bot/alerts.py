import asyncio
from datetime import datetime, timezone
from typing import Optional

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


async def update_message_with_balance(
    chat_id: int,
    message_id: int,
    address: str,
    chain: str,
    original_text: str
):
    try:
        if chain == "TRON":
            balance = await get_tron_usdt_balance(address)
        else:
            balance = await get_eth_usdt_balance(address)

        if balance is not None:
            whale_tag = " " if balance >= WHALE_THRESHOLD else ""
            balance_str = f"{balance:,.2f} USDT{whale_tag}"

            new_text = original_text.replace("*Balance:* Unknown", f"*Balance:* {balance_str}")
            new_text = new_text.replace("*Balance:* Fetching...", f"*Balance:* {balance_str}")

            if new_text != original_text:
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
    subscribers = database.get_subscribers()
    watchlist_subscribers = database.get_watchlist_subscribers(address, chain)

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

    for (chat_id,) in watchlist_subscribers:
        try:
            watchlist_msg = "*[WATCHLIST ALERT]*\n\n" + msg
            await bot.send_message(chat_id=chat_id, text=watchlist_msg, parse_mode="Markdown")
        except Exception as e:
            logger.debug(f"Failed to send watchlist alert to {chat_id}: {e}")

    if BROADCAST_CHANNEL:
        try:
            await bot.send_message(chat_id=BROADCAST_CHANNEL, text=msg, parse_mode="Markdown")
        except Exception as e:
            logger.debug(f"Channel send failed: {e}")

    for (chat_id,) in subscribers:
        try:
            await bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.debug(f"Failed to send to {chat_id}: {e}")


async def broadcast_mempool_alert(
    address: str,
    action: str,
    tx_hash: str,
    chain: str = "TRON",
    balance: Optional[float] = None,
):
    subscribers = database.get_subscribers()
    watchlist_subscribers = database.get_watchlist_subscribers(address, chain)

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

    all_chat_ids = set(s[0] for s in subscribers)
    all_chat_ids.update(w[0] for w in watchlist_subscribers)

    for chat_id in all_chat_ids:
        try:
            sent_msg = await bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            if balance is None:
                asyncio.create_task(
                    update_message_with_balance(chat_id, sent_msg.message_id, address, chain, msg)
                )
        except Exception as e:
            logger.debug(f"Failed to send mempool alert to {chat_id}: {e}")

    if BROADCAST_CHANNEL:
        try:
            sent_msg = await bot.send_message(
                chat_id=BROADCAST_CHANNEL,
                text=msg,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            if balance is None:
                asyncio.create_task(
                    update_message_with_balance(BROADCAST_CHANNEL, sent_msg.message_id, address, chain, msg)
                )
        except Exception as e:
            logger.debug(f"Failed to send mempool alert to channel: {e}")


async def broadcast_early_warning_alert(
    address: str,
    action: str,
    multisig_tx_id: int,
    tx_hash: str,
    chain: str = "TRON",
    balance: Optional[float] = None,
    block_timestamp: Optional[int] = None,
):
    subscribers = database.get_subscribers()
    watchlist_subscribers = database.get_watchlist_subscribers(address, chain)

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

    all_chat_ids = set(s[0] for s in subscribers)
    all_chat_ids.update(w[0] for w in watchlist_subscribers)

    for chat_id in all_chat_ids:
        try:
            sent_msg = await bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            if balance is None:
                asyncio.create_task(
                    update_message_with_balance(chat_id, sent_msg.message_id, address, chain, msg)
                )
        except Exception as e:
            logger.debug(f"Failed to send early warning to {chat_id}: {e}")

    if BROADCAST_CHANNEL:
        try:
            sent_msg = await bot.send_message(
                chat_id=BROADCAST_CHANNEL,
                text=msg,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
            if balance is None:
                asyncio.create_task(
                    update_message_with_balance(BROADCAST_CHANNEL, sent_msg.message_id, address, chain, msg)
                )
        except Exception as e:
            logger.debug(f"Failed to send early warning to channel: {e}")

    logger.info(f"EARLY WARNING sent: {action} -> {address} (MultiSig TX #{multisig_tx_id})")
