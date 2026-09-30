import asyncio
from datetime import datetime, timezone
from typing import Any, Optional, Sequence, Set

from aiogram.exceptions import TelegramRetryAfter

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


async def _send_raw(chat_id: int, text: str, disable_preview: bool = True):
    """One Telegram send. No per-chat lock — pending bursts must not queue."""
    try:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=disable_preview,
        )
    except TelegramRetryAfter as e:
        await asyncio.sleep(float(e.retry_after) + 0.05)
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=disable_preview,
        )


async def _send(chat_id: int, text: str, disable_preview: bool = True):
    lock = await _lock_for(chat_id)
    async with lock:
        return await _send_raw(chat_id, text, disable_preview=disable_preview)


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
        new_text = original_text.replace(
            "*Balance:* Unknown", f"*Balance:* {balance_str}"
        )
        new_text = new_text.replace(
            "*Balance:* Fetching...", f"*Balance:* {balance_str}"
        )
        new_text = new_text.replace(
            "*Balance frozen:* Unknown", f"*Balance frozen:* {balance_str}"
        )
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
            msg += f"*Redemption time:* {format_elapsed_time(elapsed_seconds)}\n"
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


_TELEGRAM_LIMIT = 3900


def _explorer_tx(chain: str, tx_hash: str) -> str:
    if not tx_hash:
        return ""
    if chain == "ETH":
        return f"https://etherscan.io/tx/{tx_hash}"
    return f"https://tronscan.org/#/transaction/{tx_hash}"


def _pending_time_str(block_timestamp: Optional[int]) -> str:
    if block_timestamp:
        return datetime.fromtimestamp(block_timestamp / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _pending_message(
    address: str,
    action: str,
    multisig_tx_id: int,
    tx_hash: str,
    chain: str,
    balance: Optional[float],
    block_timestamp: Optional[int],
) -> str:
    time_str = _pending_time_str(block_timestamp)
    explorer_link = _explorer_tx(chain, tx_hash)

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
    return msg


def _pending_entry(index: int, item: dict[str, Any], show_action: bool) -> str:
    address = item.get("address") or ""
    action = (item.get("action") or "blacklist").upper()
    tx_id = item.get("multisig_tx_id") or 0
    tx_hash = item.get("tx_hash") or ""
    chain = item.get("chain") or "TRON"
    link = _explorer_tx(chain, tx_hash)
    line = f"{index}. "
    if show_action:
        line += f"{action}\n"
    line += f"`{address}`\n"
    meta = f"TX #{tx_id}" if tx_id else "TX"
    if link:
        meta += f" | [tx]({link})"
    line += meta
    return line


def _confirmed_entry(index: int, item: dict[str, Any]) -> str:
    line = _pending_entry(index, item, show_action=False)
    elapsed_seconds = item.get("elapsed_seconds")
    if elapsed_seconds is not None:
        line += f"\n*Redemption time:* {format_elapsed_time(elapsed_seconds)}"
    return line


def _pending_batch_chunks(items: Sequence[dict[str, Any]]) -> list[str]:
    """One (or few) Telegram messages for every pending in the same block."""
    chain = items[0].get("chain") or "TRON"
    time_str = _pending_time_str(items[0].get("block_timestamp"))
    block_number = items[0].get("block_number")
    actions = {(item.get("action") or "blacklist") for item in items}
    show_action = len(actions) > 1
    if not show_action:
        label = (
            "BLACKLIST PENDING"
            if next(iter(actions)) == "blacklist"
            else "UNBLACKLIST PENDING"
        )
    else:
        label = "PENDING"
    header = f"*{label} [{chain}] — {len(items)} in one block*\n\n"
    header += f"*Submitted:* {time_str}\n"
    if block_number:
        header += f"*Block:* `{block_number}`\n"
    header += "\n"

    chunks: list[str] = []
    current = header
    for i, item in enumerate(items, 1):
        entry = _pending_entry(i, item, show_action)
        extra = ("\n\n" if i > 1 or current != header else "") + entry
        if len(current) + len(extra) > _TELEGRAM_LIMIT and current != header:
            chunks.append(current)
            current = header + "_(continued)_\n\n" + entry
        else:
            current += extra if current == header else "\n\n" + entry
    if current.strip():
        chunks.append(current)
    return chunks


def _group_pending_items(
    items: Sequence[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    groups: dict[tuple, list[dict[str, Any]]] = {}
    order: list[tuple] = []
    for item in items:
        key = (
            item.get("chain") or "TRON",
            item.get("block_number") or 0,
            item.get("block_timestamp") or 0,
        )
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(item)
    return [groups[k] for k in order]


def _confirmed_message(
    address: str,
    tx_hash: str,
    chain: str,
    balance: Optional[float],
    elapsed_seconds: Optional[float],
    is_update: bool,
    block_timestamp: Optional[int] = None,
) -> str:
    time_str = _pending_time_str(block_timestamp)
    explorer_link = _explorer_tx(chain, tx_hash)
    explorer_name = "Etherscan" if chain == "ETH" else "Tronscan"
    balance_text = format_usdt_balance(balance)
    whale_tag = " [WHALE]" if balance is not None and balance >= WHALE_THRESHOLD else ""
    title = (
        f"BLOCK CONFIRMED [{chain}]{whale_tag}"
        if is_update
        else f"ADDRESS BLOCKED [{chain}]{whale_tag}"
    )
    msg = f"*{title}*\n\n"
    msg += f"*Address:*\n`{address}`\n\n"
    msg += f"*Balance frozen:* {balance_text}\n"
    msg += f"*Confirmed:* {time_str}\n"
    if elapsed_seconds is not None:
            msg += f"*Redemption time:* {format_elapsed_time(elapsed_seconds)}\n"
    msg += f"\nAddress officially blacklisted. USDT transfers frozen.\n\n"
    msg += f"[View on {explorer_name}]({explorer_link})"
    return msg


def _confirmed_batch_chunks(items: Sequence[dict[str, Any]]) -> list[str]:
    chain = items[0].get("chain") or "TRON"
    time_str = _pending_time_str(items[0].get("block_timestamp"))
    block_number = items[0].get("block_number")
    header = f"*BLOCK CONFIRMED [{chain}] — {len(items)} in one block*\n\n"
    header += f"*Confirmed:* {time_str}\n"
    if block_number:
        header += f"*Block:* `{block_number}`\n"
    header += "\n"

    chunks: list[str] = []
    current = header
    for i, item in enumerate(items, 1):
        entry = _confirmed_entry(i, item)
        extra = ("\n\n" if current != header else "") + entry
        if len(current) + len(extra) > _TELEGRAM_LIMIT and current != header:
            chunks.append(current)
            current = header + "_(continued)_\n\n" + entry
        else:
            current += extra if current == header else "\n\n" + entry
    if current.strip():
        chunks.append(current)
    return chunks


def _with_broadcast(chat_ids: Set[int]) -> Set[int]:
    if not BROADCAST_CHANNEL:
        return chat_ids
    try:
        chat_ids.add(int(BROADCAST_CHANNEL))
    except (TypeError, ValueError):
        chat_ids.add(BROADCAST_CHANNEL)
    return chat_ids


async def _send_pending_one(
    chat_id: int,
    msg: str,
    address: str,
    chain: str,
    patch_balance: bool,
):
    try:
        result = await _send_raw(chat_id, msg)
    except Exception as e:
        logger.debug(f"Failed to send pending alert to {chat_id}: {e}")
        return None
    if patch_balance and result and getattr(result, "message_id", None):
        asyncio.create_task(
            update_message_with_balance(
                result.chat.id, result.message_id, address, chain, msg
            )
        )
    return result


async def broadcast_early_warning_alerts(items: Sequence[dict[str, Any]]):
    """Pending Telegram: 1 tx → one message; 2+ in the same block → one batch.

    Detection already wrote sqlite first. This must not queue 21 sends on one
    chat or TRON/ETH pending would leave ~1s/~2s.
    """
    if not items:
        return

    subscriber_chats = _with_broadcast({row[0] for row in database.get_subscribers()})
    tasks = []

    def queue_single(item: dict[str, Any], chats: Set[int]):
        address = item["address"]
        chain = item.get("chain") or "TRON"
        msg = _pending_message(
            address=address,
            action=item.get("action") or "blacklist",
            multisig_tx_id=item.get("multisig_tx_id") or 0,
            tx_hash=item.get("tx_hash") or "",
            chain=chain,
            balance=item.get("balance"),
            block_timestamp=item.get("block_timestamp"),
        )
        patch = item.get("balance") is None
        for chat_id in chats:
            tasks.append(_send_pending_one(chat_id, msg, address, chain, patch))

    for group in _group_pending_items(items):
        chain = group[0].get("chain") or "TRON"
        if len(group) == 1:
            item = group[0]
            chats = set(subscriber_chats)
            chats.update(
                row[0]
                for row in database.get_watchlist_subscribers(item["address"], chain)
            )
            queue_single(item, chats)
            continue

        for chunk in _pending_batch_chunks(group):
            for chat_id in subscriber_chats:
                tasks.append(_send_pending_one(chat_id, chunk, "", chain, False))

        for item in group:
            watchlist = database.get_watchlist_subscribers(item["address"], chain)
            if watchlist:
                queue_single(item, {row[0] for row in watchlist})

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    for item in items:
        logger.info(
            f"EARLY WARNING sent: {item.get('action')} -> {item.get('address')} "
            f"(MultiSig TX #{item.get('multisig_tx_id') or 0})"
        )


async def broadcast_confirmed_alerts(items: Sequence[dict[str, Any]]):
    """Confirmed Telegram: 1 execute → one message; 2+ in the same block → one batch."""
    if not items:
        return

    blacklist = [
        item for item in items if (item.get("action") or "blacklist") == "blacklist"
    ]
    if not blacklist:
        return

    subscriber_chats = _with_broadcast({row[0] for row in database.get_subscribers()})
    tasks = []

    def queue_single(item: dict[str, Any], chats: Set[int]):
        address = item["address"]
        chain = item.get("chain") or "TRON"
        msg = _confirmed_message(
            address=address,
            tx_hash=item.get("tx_hash") or "",
            chain=chain,
            balance=item.get("balance"),
            elapsed_seconds=item.get("elapsed_seconds"),
            is_update=item.get("is_update", True),
            block_timestamp=item.get("block_timestamp"),
        )
        if chats:
            watchlist = {
                row[0] for row in database.get_watchlist_subscribers(address, chain)
            }
            overlap = chats & watchlist
            for chat_id in chats:
                text = "*[WATCHLIST ALERT]*\n\n" + msg if chat_id in overlap else msg
                tasks.append(
                    _send_pending_one(
                        chat_id, text, address, chain, item.get("balance") is None
                    )
                )

    for group in _group_pending_items(blacklist):
        chain = group[0].get("chain") or "TRON"
        if len(group) == 1:
            item = group[0]
            chats = set(subscriber_chats)
            chats.update(
                row[0]
                for row in database.get_watchlist_subscribers(item["address"], chain)
            )
            queue_single(item, chats)
            continue

        for chunk in _confirmed_batch_chunks(group):
            for chat_id in subscriber_chats:
                tasks.append(_send_pending_one(chat_id, chunk, "", chain, False))

        for item in group:
            watchlist = database.get_watchlist_subscribers(item["address"], chain)
            if watchlist:
                queue_single(item, {row[0] for row in watchlist})

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    for item in blacklist:
        logger.info(f"CONFIRMED sent: {item.get('address')} ({item.get('chain')})")


async def broadcast_early_warning_alert(
    address: str,
    action: str,
    multisig_tx_id: int,
    tx_hash: str,
    chain: str = "TRON",
    balance: Optional[float] = None,
    block_timestamp: Optional[int] = None,
):
    await broadcast_early_warning_alerts(
        [
            {
                "address": address,
                "action": action,
                "multisig_tx_id": multisig_tx_id,
                "tx_hash": tx_hash,
                "chain": chain,
                "balance": balance,
                "block_timestamp": block_timestamp,
            }
        ]
    )
