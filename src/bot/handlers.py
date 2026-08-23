from datetime import datetime, timezone

from aiogram.types import Message
from aiogram.filters import Command

from . import router, bot
from ..config import (
    ENABLE_TRON,
    ENABLE_ETH,
    WHALE_THRESHOLD,
    MULTISIG_SCAN_INTERVAL,
    ETH_MULTISIG_SCAN_INTERVAL,
    BLOCK_POLL_INTERVAL,
    MAX_CONSECUTIVE_FAILURES,
    logger,
)
from .. import database, health
from ..utils import format_usdt_balance, format_time_ago
from ..tron import is_valid_tron_address, check_address_blacklisted, get_tron_usdt_balance
from ..eth import is_valid_eth_address, check_eth_address_blacklisted, get_eth_usdt_balance


def detect_chain(address: str):
    if is_valid_tron_address(address):
        return "TRON"
    if is_valid_eth_address(address):
        return "ETH"
    return None


@router.message(Command("start"))
async def start_cmd(message: Message):
    logger.info(f"Start command from chat_id: {message.chat.id}")
    database.add_subscriber(message.chat.id)

    chains_enabled = []
    if ENABLE_TRON:
        chains_enabled.append("TRON")
    if ENABLE_ETH:
        chains_enabled.append("ETH")
    chains_text = " & ".join(chains_enabled) if chains_enabled else "None"

    await message.answer(
        "*USDT Blacklist Monitor*\n\n"
        f"Monitoring: {chains_text}\n"
        f"Subscribed to blacklist alerts.\n\n"
        "*Alerts:*\n"
        "PENDING - Issuer MultiSig submitted addBlackList\n"
        "BLOCKED - MultiSig executed (address frozen)\n"
        "WHALE - Large balance (>$100k) blocked\n\n"
        "*Commands:*\n"
        "/check <address> - Check address status\n"
        "/watch <address> - Watch specific address\n"
        "/unwatch <address> - Stop watching\n"
        "/watchlist - Your watched addresses\n"
        "/recent - Recent blacklist events\n"
        "/whales - Recent large blocks\n"
        "/stats - Statistics\n"
        "/timing - Confirmation timing stats\n"
        "/health - Scanner health status\n"
        "/pending - Pending MultiSig submissions\n"
        "/stop - Unsubscribe",
        parse_mode="Markdown",
    )


@router.message(Command("stop"))
async def stop_cmd(message: Message):
    database.remove_subscriber(message.chat.id)
    await message.answer("Unsubscribed. Use /start to subscribe again.")


@router.message(Command("check"))
async def check_cmd(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "Usage: /check <address>\nSupports TRON (T...) and ETH (0x...)",
            parse_mode="Markdown",
        )
        return

    address = args[1].strip()
    chain = detect_chain(address)

    if not chain:
        await message.answer("Invalid address. Use TRON (T...) or ETH (0x...) format.")
        return

    await message.answer(f"Checking {chain} address...")

    if chain == "ETH":
        is_blacklisted = await check_eth_address_blacklisted(address)
        balance = await get_eth_usdt_balance(address)
    else:
        is_blacklisted = await check_address_blacklisted(address)
        balance = await get_tron_usdt_balance(address)

    balance_text = format_usdt_balance(balance)
    row = database.get_blacklist_entry(address, chain)

    if is_blacklisted:
        confirmed_at = row[2] if row else "Unknown"
        await message.answer(
            f"*BLOCKED [{chain}]*\n\n"
            f"Address: `{address}`\n"
            f"Balance: *{balance_text}* (frozen)\n"
            f"Confirmed: {confirmed_at}\n\n"
            f"USDT is frozen.",
            parse_mode="Markdown",
        )
    elif row and row[0] == "pending":
        await message.answer(
            f"*PENDING [{chain}]*\n\n"
            f"Address: `{address}`\n"
            f"Balance at risk: *{balance_text}*\n"
            f"Detected: {row[1]}\n\n"
            f"Blacklist tx detected, not yet confirmed.",
            parse_mode="Markdown",
        )
    else:
        await message.answer(
            f"*SAFE [{chain}]*\n\n"
            f"Address: `{address}`\n"
            f"Balance: *{balance_text}*\n\n"
            f"Not blacklisted.",
            parse_mode="Markdown",
        )


@router.message(Command("recent"))
async def recent_cmd(message: Message):
    rows = database.get_recent_blacklist(limit=10)

    if not rows:
        await message.answer("No blacklist events detected yet.")
        return

    msg = "*Recent Blacklist Events:*\n\n"
    for addr, chain, status, detected in rows:
        status_text = "BLOCKED" if status == "confirmed" else "PENDING"
        msg += f"[{chain}] `{addr[:12]}...{addr[-6:]}`\n"
        msg += f"{status_text} | {detected[:16]}\n\n"

    await message.answer(msg, parse_mode="Markdown")


@router.message(Command("stats"))
async def stats_cmd(message: Message):
    stats = database.get_stats()
    now = datetime.now(timezone.utc)

    tron_last = format_time_ago(stats["tron_last_scan"], now)
    eth_last = format_time_ago(stats["eth_last_scan"], now)

    msg = f"*Statistics*\n\n"
    msg += f"*Total:*\n"
    msg += f"Frozen: {stats['confirmed']}\n"
    msg += f"Pending: {stats['pending']}\n"
    msg += f"Unfrozen: {stats['unfrozen']}\n"
    msg += f"Subscribers: {stats['subscribers']}\n\n"
    msg += f"*Per Chain:*\n"
    msg += f"TRON: {stats['tron_confirmed']} frozen | {stats['tron_unfrozen']} unfrozen\n"
    msg += f"ETH: {stats['eth_confirmed']} frozen\n\n"
    msg += f"*Scanners:*\n"
    if ENABLE_TRON:
        msg += f"TRON blocks: {health.block_scan_count} polls | Last: {tron_last}\n"
    if ENABLE_ETH:
        msg += f"ETH MultiSig: {health.eth_multisig_scan_count} scans | Last: {eth_last}"

    await message.answer(msg, parse_mode="Markdown")


@router.message(Command("test"))
async def test_cmd(message: Message):
    await message.answer(
        "*TEST ALERT*\n\n"
        "Address:\n`TTestAddress123456789012345678901234`\n\n"
        "This is a test alert.\n\n"
        "If you see this, alerts are working.",
        parse_mode="Markdown",
    )


@router.message(Command("health"))
async def health_cmd(message: Message):
    now = datetime.now(timezone.utc)

    tron_scan = database.get_sync_state("tron_last_scan")
    eth_scan = database.get_sync_state("eth_last_scan")

    tron_status = "OFFLINE"
    tron_delay = "N/A"
    if tron_scan:
        try:
            last_tron = datetime.fromisoformat(tron_scan.replace("Z", "+00:00"))
            delay = (now - last_tron).total_seconds()
            tron_delay = f"{delay:.0f}s ago"
            if delay < 30:
                tron_status = "HEALTHY"
            elif delay < 60:
                tron_status = "SLOW"
            else:
                tron_status = "STALE"
        except Exception:
            pass

    eth_status = "OFFLINE"
    eth_delay = "N/A"
    if eth_scan:
        try:
            last_eth = datetime.fromisoformat(eth_scan.replace("Z", "+00:00"))
            delay = (now - last_eth).total_seconds()
            eth_delay = f"{delay:.0f}s ago"
            if delay < 30:
                eth_status = "HEALTHY"
            elif delay < 60:
                eth_status = "SLOW"
            else:
                eth_status = "STALE"
        except Exception:
            pass

    multisig_status = "ACTIVE" if health.multisig_scan_count > 0 and health.multisig_consecutive_failures == 0 else "RECOVERING" if health.multisig_consecutive_failures < MAX_CONSECUTIVE_FAILURES else "FAILING"
    eth_multisig_status = "ACTIVE" if health.eth_multisig_scan_count > 0 and health.eth_multisig_consecutive_failures == 0 else "RECOVERING" if health.eth_multisig_consecutive_failures < MAX_CONSECUTIVE_FAILURES else "FAILING"

    msg = "*Scanner Health*\n\n"
    block_status = (
        "ACTIVE"
        if health.block_scan_count > 0 and health.block_consecutive_failures == 0
        else "RECOVERING"
        if health.block_consecutive_failures < MAX_CONSECUTIVE_FAILURES
        else "FAILING"
    )
    msg += f"*TRON BLOCKS:* {block_status}\n"
    msg += f"Head: {health.block_last_number or 'n/a'}\n"
    msg += f"Source: {health.block_source or 'starting'}\n"
    msg += f"Poll: {BLOCK_POLL_INTERVAL}s | Detections: {health.block_detections}\n"
    msg += f"Failures: {health.block_consecutive_failures}\n"
    msg += f"Last scan: {tron_delay} ({tron_status})\n\n"
    msg += f"*TRON HTTP FALLBACK:* {multisig_status}\n"
    msg += f"Scans: {health.multisig_scan_count}\n"
    msg += f"Interval: {MULTISIG_SCAN_INTERVAL}s\n\n"
    msg += f"*ETH MULTISIG:* {eth_multisig_status}\n"
    msg += f"Scans: {health.eth_multisig_scan_count}\n"
    msg += f"Failures: {health.eth_multisig_consecutive_failures}\n"
    msg += f"Interval: {ETH_MULTISIG_SCAN_INTERVAL}s\n"
    msg += f"Last scan: {eth_delay} ({eth_status})\n\n"
    msg += f"*Detection:* TRON blocks (in-tx decode) + ETH events"

    await message.answer(msg, parse_mode="Markdown")


@router.message(Command("pending"))
async def pending_cmd(message: Message):
    pending = database.get_pending_multisig("TRON", limit=10)

    if not pending:
        await message.answer(
            "*Pending MultiSig Submissions*\n\nNo pending submissions found.",
            parse_mode="Markdown",
        )
        return

    msg = "*Pending MultiSig Submissions*\n\n"

    for tx_id, target, action, submitted_at in pending:
        action_text = "FREEZE" if action == "blacklist" else "UNFREEZE"
        target_short = f"{target[:8]}...{target[-6:]}" if target else "Unknown"

        msg += f"*{action_text}*\n"
        msg += f"Address: `{target_short}`\n"
        msg += f"TX: {tx_id}\n"
        if submitted_at:
            msg += f"Submitted: {submitted_at[:19]}\n"
        msg += "\n"

    await message.answer(msg, parse_mode="Markdown")


@router.message(Command("watch"))
async def watch_cmd(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Usage: /watch <address>")
        return

    address = args[1].strip()
    chain = detect_chain(address)

    if not chain:
        await message.answer("Invalid address format.")
        return

    count = database.add_watchlist(message.chat.id, address, chain)

    await message.answer(
        f"*Address Added to Watchlist*\n\n"
        f"Chain: {chain}\n"
        f"Address: `{address}`\n\n"
        f"You will receive an instant alert if this address gets blacklisted.\n"
        f"Total watched: {count}",
        parse_mode="Markdown",
    )


@router.message(Command("unwatch"))
async def unwatch_cmd(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Usage: /unwatch <address>")
        return

    address = args[1].strip()
    chain = detect_chain(address)

    if not chain:
        await message.answer("Invalid address format.")
        return

    deleted = database.remove_watchlist(message.chat.id, address, chain)

    if deleted:
        await message.answer(f"Removed `{address}` from watchlist.", parse_mode="Markdown")
    else:
        await message.answer("Address not found in your watchlist.")


@router.message(Command("watchlist"))
async def watchlist_cmd(message: Message):
    rows = database.get_user_watchlist(message.chat.id)

    if not rows:
        await message.answer("Your watchlist is empty.\nUse /watch <address> to add addresses.")
        return

    msg = "*Your Watchlist*\n\n"
    for addr, chain, added in rows:
        msg += f"[{chain}] `{addr[:16]}...{addr[-6:]}`\n"

    msg += f"\nTotal: {len(rows)} addresses"
    await message.answer(msg, parse_mode="Markdown")


@router.message(Command("whales"))
async def whales_cmd(message: Message):
    rows = database.get_confirmed_blacklist("TRON", limit=50)
    rows += database.get_confirmed_blacklist("ETH", limit=50)

    if not rows:
        await message.answer("No blocked addresses found.")
        return

    whales = []
    for addr, chain, status, detected, tx_hash in rows:
        if chain == "ETH":
            balance = await get_eth_usdt_balance(addr)
        else:
            balance = await get_tron_usdt_balance(addr)

        if balance and balance >= WHALE_THRESHOLD:
            whales.append((addr, chain, balance, detected))

        if len(whales) >= 10:
            break

    if not whales:
        await message.answer("No whale blocks (>$100k) found in recent history.")
        return

    msg = "*Recent Whale Blocks (>$100k)*\n\n"
    for addr, chain, balance, detected in whales:
        balance_text = format_usdt_balance(balance)
        msg += f"[{chain}] *{balance_text}*\n"
        msg += f"`{addr[:16]}...{addr[-6:]}`\n"
        msg += f"{detected[:16]}\n\n"

    await message.answer(msg, parse_mode="Markdown")
