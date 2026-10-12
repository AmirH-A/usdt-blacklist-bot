"""Accuracy report for the freeze detector.

Combines:
  - blacklist_tracker.db (live bot)
  - comparison_tracker.db (us vs competitor)
  - Freeze Summary sheet (Telegram alert delay vs on-chain)
  - Freeze-Onchain-Data sheet (issuer MultiSig ground truth)
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sqlite3
import time
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import psycopg2
import psycopg2.extras

from .tron.address import tron_address_to_hex

SUMMARY_SHEET_ID = os.getenv(
    "SUMMARY_SHEET_ID", "10r5M5ksCCDhjm05IztVbIYgvdyp5VOJcxK7xeHJTiyg"
)
ONCHAIN_SHEET_ID = os.getenv(
    "ONCHAIN_SHEET_ID", "1LBXx04B_w3CqllGpudsKXhy5sC3l9nCvhmQy6U_3dG0"
)
ONCHAIN_GID = os.getenv("ONCHAIN_GID", "1543539254")
BOT_DB_PATH = os.getenv("BOT_DB_PATH", "blacklist_tracker.db")  # legacy, snapshot only
# The live bot tables moved to postgres; comparison_tracker.db is still sqlite.
BOT_DSN = os.getenv("DATABASE_URL", "")
CMP_DB_PATH = os.getenv("CMP_DB_PATH", "comparison_tracker.db")
SHEET_CACHE_TTL = int(os.getenv("SHEET_CACHE_TTL", "45"))
REPORT_CACHE_TTL = int(os.getenv("REPORT_CACHE_TTL", "20"))
MAX_DELAY_S = 3600
# Mempool detection sees the tx before its block exists, so saved - onchain is
# negative. That is the win, not an error: clamp it to 0.0 for display (the
# panel shows delay, not lead) and keep the signed value in lead_s. Beyond this
# window the two timestamps are unrelated and the row is dropped as before.
MAX_LEAD_S = int(os.getenv("MAX_LEAD_S", "900"))
MAX_REDEMPTION_S = 7 * 24 * 3600
BALANCE_CACHE_TTL = int(os.getenv("BALANCE_CACHE_TTL", "300"))
BALANCE_FETCH_LIMIT = int(os.getenv("BALANCE_FETCH_LIMIT", "120"))
TRON_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRON_USDT_HEX = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
TRONGRID_BASE_URL = os.getenv("TRONGRID_BASE_URL", "https://api.trongrid.io")
TRONGRID_API_KEY = os.getenv("TRONGRID_API_KEY", "")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
ETH_USDT_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"

_log = logging.getLogger("accuracy")
# Surfaced in the report's `sources` block so a broken query is visible on the
# panel itself, not only in the journal.
_query_errors: list[str] = []

_sheet_cache: dict[str, tuple[float, list[dict]]] = {}
_report_cache: tuple[float, dict] | None = None
_balance_cache: dict[tuple[str, str], tuple[float, float]] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M",
    ):
        try:
            dt = datetime.strptime(text.replace("Z", "+00:00"), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None


def _iso_from_ms(value: Any) -> Optional[str]:
    n = _ts_ms(value)
    if n is None:
        return None
    return datetime.fromtimestamp(n / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _ms_from_iso(value: Any) -> Optional[int]:
    dt = _parse_dt(str(value) if value is not None else None)
    if dt is None:
        return None
    return int(dt.timestamp() * 1000)


def _ts_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    if n < 10_000_000_000:
        n *= 1000
    return n


def _invert_ts(ts: str) -> tuple:
    """Sort key that orders timestamp strings newest-first inside an ascending
    composite key, where `reverse=True` would wrongly invert the other parts."""
    return tuple(-ord(c) for c in ts)


def _redemption_s(start: Any, end: Any) -> Optional[float]:
    a, b = _ts_ms(start), _ts_ms(end)
    if a is None or b is None:
        return None
    elapsed = (b - a) / 1000.0
    # Include 0s (submit+confirm in the same block). Reject only negatives / absurd gaps.
    if 0 <= elapsed <= MAX_REDEMPTION_S:
        return round(elapsed, 1)
    return None


def _percentile(sorted_vals: list[float], p: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return round(sorted_vals[0], 1)
    i = (len(sorted_vals) - 1) * p / 100.0
    lo = int(i)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = i - lo
    return round(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac, 1)


def _mean(vals: list[float]) -> Optional[float]:
    if not vals:
        return None
    return round(sum(vals) / len(vals), 1)


def _fetch_csv(url: str) -> list[dict]:
    now = time.time()
    cached = _sheet_cache.get(url)
    if cached and now - cached[0] < SHEET_CACHE_TTL:
        return cached[1]
    req = urllib.request.Request(
        url, headers={"User-Agent": "freeze-accuracy-panel/1.0"}
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            raw = resp.read().decode("utf-8")
    except Exception:
        if cached:
            return cached[1]
        raise
    rows = [
        r
        for r in csv.DictReader(io.StringIO(raw))
        if any(str(v).strip() for v in r.values())
    ]
    _sheet_cache[url] = (now, rows)
    return rows


def _sheet_url(sheet_id: str, gid: str = "0") -> str:
    return (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    )


class _PgReadConn:
    """sqlite3-shaped read-only handle on the bot's postgres database.

    Every read in this module goes through _query(conn, sql, args), which calls
    conn.execute(...).fetchall() and indexes rows both by name and position.
    DictCursor rows do both, exactly like sqlite3.Row, so none of the eleven
    queries below needed changing.
    """

    def __init__(self, dsn: str):
        self._conn = psycopg2.connect(dsn, connect_timeout=5)
        # Read-only: this module must never be able to write to the bot's data.
        self._conn.set_session(readonly=True, autocommit=True)

    def execute(self, sql: str, args: tuple = ()):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # args or None: psycopg2 skips interpolation entirely when params are
        # None, so a bare % in SQL cannot be mistaken for a placeholder.
        cur.execute(sql.replace("?", "%s"), args or None)
        return cur

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


def _open_bot_db() -> Optional["_PgReadConn"]:
    if not BOT_DSN:
        return None
    try:
        return _PgReadConn(BOT_DSN)
    except psycopg2.Error as e:
        print(f"accuracy: cannot reach bot postgres: {e}")
        return None


def _open_db(path: str) -> Optional[sqlite3.Connection]:
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    try:
        conn = sqlite3.connect(
            f"file:{os.path.abspath(path)}?mode=ro", uri=True, timeout=5
        )
    except sqlite3.Error:
        conn = sqlite3.connect(path, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _query(
    conn: Optional[sqlite3.Connection], sql: str, args: tuple = ()
) -> list[sqlite3.Row]:
    if not conn:
        return []
    try:
        return conn.execute(sql, args).fetchall()
    except (sqlite3.Error, psycopg2.Error) as e:
        # Never silent: a dialect slip here renders a whole panel section
        # empty, which reads as "no data" rather than "broken query".
        _log.warning("query failed (%s): %s", type(e).__name__, str(e).strip())
        _query_errors.append(f"{type(e).__name__}: {str(e).strip()[:200]}")
        return []


def _norm_addr(addr: str) -> str:
    addr = (addr or "").strip()
    if addr.startswith("0x"):
        return addr.lower()
    return addr


def _is_wallet(addr: str) -> bool:
    a = (addr or "").strip()
    if a.startswith("0x") and len(a) == 42:
        return True
    return a.startswith("T") and 33 <= len(a) <= 36


def _norm_hash(value: str) -> str:
    h = (value or "").strip().lower()
    if h.startswith("0x"):
        h = h[2:]
    return h


def _norm_status(status: str) -> str:
    s = (status or "").strip().lower()
    if s in ("done", "blocked", "executed", "confirmed"):
        return "confirmed"
    if s in ("pending", "mempool_pending", "multisig_pending"):
        return "pending"
    return s


def _parse_delay(raw: str) -> Optional[float]:
    if raw is None:
        return None
    text = str(raw).replace(",", "").strip()
    if not text:
        return None
    try:
        val = float(text)
    except ValueError:
        return None
    if 0 <= val <= MAX_DELAY_S:
        return val
    return None


def _detection_delay_s(onchain_ms: Any, detected_iso: Any) -> Optional[float]:
    """Seconds from on-chain block time to when the bot wrote sqlite."""
    block_ms = _ts_ms(onchain_ms)
    detected = _parse_dt(str(detected_iso) if detected_iso is not None else None)
    if block_ms is None or detected is None:
        return None
    onchain = datetime.fromtimestamp(block_ms / 1000.0, tz=timezone.utc)
    return _delay_onchain_to_saved(onchain, detected)


def _delay_onchain_to_saved(
    onchain: Optional[datetime], saved: Optional[datetime]
) -> Optional[float]:
    """Latency from on-chain block time to system saved (DB write).

    Displayed as: System saved − On-chain (same as alert delay style).
    """
    if onchain is None or saved is None:
        return None
    elapsed = (saved - onchain).total_seconds()
    if -MAX_LEAD_S <= elapsed < 0:
        elapsed = 0.0
    if 0 <= elapsed <= MAX_DELAY_S:
        return round(elapsed, 1)
    return None


def _lead_s(onchain_iso: Optional[str], system_iso: Optional[str]) -> Optional[float]:
    """Seconds the system saw it BEFORE its block timestamp. None unless ahead."""
    onchain = _parse_dt(onchain_iso)
    saved = _parse_dt(system_iso)
    if onchain is None or saved is None:
        return None
    ahead = (onchain - saved).total_seconds()
    if 0 < ahead <= MAX_LEAD_S:
        return round(ahead, 1)
    return None


def _sys_delay_from_columns(
    onchain_iso: Optional[str], system_iso: Optional[str]
) -> Optional[float]:
    """Sys delay from the two timestamps shown in the panel columns."""
    return _delay_onchain_to_saved(_parse_dt(onchain_iso), _parse_dt(system_iso))


def _build_system_detected_index(
    bot: Optional[sqlite3.Connection],
) -> tuple[dict[tuple[str, str, str], dict], dict[tuple[str, str, str], dict]]:
    """Look up bot DB save time by (address, chain, status) and by tx hash."""
    by_key: dict[tuple[str, str, str], list] = {}
    by_hash: dict[tuple[str, str, str], dict] = {}

    def put(rec: dict, addr: str, chain: str, status: str, tx_hash: str = "") -> None:
        # Tether re-blacklists the same address: TFB5wGzo8T3o has 4 submissions
        # across Aug 27 - Sep 25. Keying on (address, chain, status) alone is not
        # unique, and the old "keep the earliest" rule pinned every event of that
        # address to its oldest sighting -- which then overwrote the event's own
        # on-chain time and produced a fake 1s delay a month out of place. Keep
        # all candidates and let the caller pick the one that matches in time.
        key = (_norm_addr(addr), chain, status)
        by_key.setdefault(key, []).append(rec)
        h = _norm_hash(tx_hash)
        if h:
            # tx hash is unique, so this stays authoritative.
            by_hash[(h, chain, status)] = rec

    for row in _query(
        bot,
        """SELECT target_address, chain, submission_block_timestamp, submitted_at,
                  submission_tx_hash
           FROM multisig_pending
           WHERE submission_block_timestamp IS NOT NULL""",
    ):
        chain = (row["chain"] or "TRON").upper()
        detected_at = row["submitted_at"]
        rec = {
            "system_detected_at": _iso(_parse_dt(detected_at)),
            "system_onchain_ms": _ts_ms(row["submission_block_timestamp"]),
        }
        put(rec, row["target_address"] or "", chain, "pending", row["submission_tx_hash"] or "")

    for row in _query(
        bot,
        """SELECT address, chain, status, block_timestamp, detected_at,
                  confirmed_block_timestamp, confirmed_at, tx_hash
           FROM blacklist""",
    ):
        addr = row["address"] or ""
        chain = (row["chain"] or "TRON").upper()
        status = _norm_status(row["status"] or "")
        if status == "confirmed":
            onchain = row["confirmed_block_timestamp"] or row["block_timestamp"]
            detected = row["confirmed_at"] or row["detected_at"]
            rec = {
                "system_detected_at": _iso(_parse_dt(detected)),
                "system_onchain_ms": _ts_ms(onchain),
            }
            put(rec, addr, chain, "confirmed", row["tx_hash"] or "")
        elif status in ("pending", "multisig_pending", "mempool_pending"):
            rec = {
                "system_detected_at": _iso(_parse_dt(row["detected_at"])),
                "system_onchain_ms": _ts_ms(row["block_timestamp"]),
            }
            put(rec, addr, chain, "pending", row["tx_hash"] or "")

    return by_key, by_hash


def _resolve_system_detected(
    ev: dict,
    by_key: dict[tuple[str, str, str], dict],
    by_hash: dict[tuple[str, str, str], dict],
) -> dict:
    chain = (ev.get("chain") or "TRON").upper()
    status = _norm_status(ev.get("status") or "")
    h = _norm_hash(ev.get("tx_hash") or "")
    if h:
        hit = by_hash.get((h, chain, status))
        if hit:
            return {**hit, "system_match": "hash"}

    cands = by_key.get((_norm_addr(ev.get("address") or ""), chain, status)) or []
    if not cands:
        return {}
    if len(cands) == 1:
        return {**cands[0], "system_match": "address"}

    # Several submissions for this address. Pick the one whose on-chain time is
    # nearest the event's, so a September event stops inheriting August.
    ev_dt = _parse_dt(ev.get("onchain_time"))
    if ev_dt is None:
        newest = max(
            cands,
            key=lambda r: _parse_dt(r.get("system_detected_at")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        return {**newest, "system_match": "address-newest"}

    def distance(rec: dict) -> float:
        ms = rec.get("system_onchain_ms")
        if ms is None:
            return float("inf")
        rec_dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return abs((rec_dt - ev_dt).total_seconds())

    best = min(cands, key=distance)
    return {**best, "system_match": "address-nearest"}


def _format_balance_amount(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}"
    rounded = round(value, 2)
    if rounded == int(rounded):
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".")


def _fetch_tron_balance_sync(address: str) -> Optional[float]:
    hex_address = tron_address_to_hex(address)
    if not hex_address:
        return None
    contract = tron_address_to_hex(TRON_USDT_CONTRACT, with_prefix=True)
    if not contract:
        return None
    payload = {
        "owner_address": "410000000000000000000000000000000000000000",
        "contract_address": contract,
        "function_selector": "balanceOf(address)",
        "parameter": hex_address.zfill(64),
        "visible": False,
    }
    headers = {"TRON-PRO-API-KEY": TRONGRID_API_KEY} if TRONGRID_API_KEY else {}
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                f"{TRONGRID_BASE_URL}/wallet/triggersmartcontract",
                json=payload,
                headers=headers,
            )
            data = resp.json()
            if "constant_result" in data and data["constant_result"]:
                return int(data["constant_result"][0], 16) / 1_000_000
    except Exception:
        return None
    return None


def _fetch_eth_balance_sync(address: str) -> Optional[float]:
    if not address.startswith("0x") or len(address) != 42:
        return None
    params = {
        "chainid": "1",
        "module": "proxy",
        "action": "eth_call",
        "to": ETH_USDT_CONTRACT,
        "data": "0x70a08231" + address[2:].lower().zfill(64),
        "tag": "latest",
        "apikey": ETHERSCAN_API_KEY,
    }
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(ETHERSCAN_BASE_URL, params=params)
            data = resp.json()
            result = data.get("result")
            if result and result != "0x":
                return int(result, 16) / 1_000_000
    except Exception:
        return None
    return None


def _fetch_balance_sync(chain: str, address: str) -> Optional[float]:
    chain = (chain or "TRON").upper()
    addr = _norm_addr(address)
    cache_key = (chain, addr)
    now = time.time()
    cached = _balance_cache.get(cache_key)
    if cached and now - cached[0] < BALANCE_CACHE_TTL:
        return cached[1]

    bal = (
        _fetch_eth_balance_sync(addr)
        if chain == "ETH"
        else _fetch_tron_balance_sync(addr)
    )
    if bal is not None:
        _balance_cache[cache_key] = (now, bal)
    return bal


def _build_balance_index(
    detections: list[sqlite3.Row],
) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    by_key: dict[tuple[str, str], str] = {}
    by_addr: dict[str, str] = {}
    for row in detections:
        if row["source"] != "our_bot":
            continue
        bal = row["balance"]
        if bal is None:
            continue
        text = str(bal).strip()
        if text == "":
            continue
        addr = _norm_addr(row["address"] or "")
        status = _norm_status(row["status"] or "")
        by_key[(addr, status)] = text
        by_addr.setdefault(addr, text)
    return by_key, by_addr


def _enrich_event_balances(
    events: list[dict],
    by_key: dict[tuple[str, str], str],
    by_addr: dict[str, str],
) -> None:
    missing: list[dict] = []
    for ev in events:
        text = str(ev.get("balance") or "").strip()
        if text:
            continue
        addr = ev["address"]
        status = ev["status"]
        fallback = by_key.get((addr, status)) or by_addr.get(addr)
        if fallback is not None and str(fallback).strip() != "":
            ev["balance"] = str(fallback).strip()
            continue
        missing.append(ev)

    if not missing:
        return

    def fetch_one(item: dict) -> tuple[dict, Optional[float]]:
        return item, _fetch_balance_sync(item.get("chain") or "TRON", item["address"])

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {
            pool.submit(fetch_one, ev): ev for ev in missing[:BALANCE_FETCH_LIMIT]
        }
        for fut in as_completed(futures):
            ev, bal = fut.result()
            if bal is not None:
                ev["balance"] = _format_balance_amount(bal)


def _load_summary() -> tuple[list[dict], Optional[str]]:
    try:
        rows = _fetch_csv(_sheet_url(SUMMARY_SHEET_ID, "0"))
    except Exception as exc:
        return [], str(exc)
    events = []
    for row in rows:
        addr = _norm_addr(row.get("Address") or "")
        if not addr:
            continue
        delay = _parse_delay(row.get("Delay (seconds)") or "")
        events.append(
            {
                "address": addr,
                "chain": (row.get("Chain") or "TRON").upper(),
                "status": _norm_status(row.get("Status") or ""),
                "onchain_time": _iso(_parse_dt(row.get("On-chain Time (UTC)"))),
                "alert_time": _iso(_parse_dt(row.get("Alert Time (UTC)"))),
                "delay_s": delay,
                "tx_hash": (row.get("TX Hash") or "").strip(),
                "balance": (row.get("Balance") or "").strip(),
            }
        )
    return events, None


def _load_onchain() -> tuple[list[dict], Optional[str]]:
    try:
        rows = _fetch_csv(_sheet_url(ONCHAIN_SHEET_ID, ONCHAIN_GID))
    except Exception as exc:
        return [], str(exc)
    events = []
    for row in rows:
        event_type = (row.get("Event Type") or "").strip()
        if not event_type:
            continue
        events.append(
            {
                "timestamp": _iso(_parse_dt(row.get("Timestamp"))),
                "chain": (row.get("Chain") or "TRON").upper(),
                "event_type": event_type,
                "tx_id": str(row.get("TX ID") or "").strip(),
                "address": _norm_addr(row.get("Target Address") or ""),
                "action": (row.get("Action") or "").strip(),
                "tx_hash": (row.get("TX Hash") or "").strip(),
                "block": str(row.get("Block") or "").strip(),
                "status": _norm_status(row.get("Status") or ""),
            }
        )
    return events, None


def _latency_stats(delays: list[float]) -> dict:
    delays = sorted(delays)
    buckets_def = [
        (5, "≤5s"),
        (10, "≤10s"),
        (20, "≤20s"),
        (30, "≤30s"),
        (60, "≤60s"),
        (120, "≤2m"),
        (300, "≤5m"),
    ]
    counts = []
    prev = 0.0
    for le, label in buckets_def:
        n = sum(1 for d in delays if prev < d <= le)
        counts.append({"le": le, "label": label, "n": n})
        prev = le
    over = sum(1 for d in delays if d > buckets_def[-1][0])
    counts.append({"le": None, "label": ">5m", "n": over})
    return {
        "count": len(delays),
        "min": round(delays[0], 1) if delays else None,
        "avg": _mean(delays),
        "p50": _percentile(delays, 50),
        "p90": _percentile(delays, 90),
        "p95": _percentile(delays, 95),
        "max": round(delays[-1], 1) if delays else None,
        "fast_share_le_10s": (
            round(sum(1 for d in delays if d <= 10) / len(delays), 3) if delays else 0
        ),
        "buckets": counts,
    }


def _stale_seconds(iso_value: Optional[str]) -> Optional[float]:
    dt = _parse_dt(iso_value)
    if not dt:
        return None
    return round((_now() - dt).total_seconds(), 1)


def build_report(force: bool = False) -> dict[str, Any]:
    global _report_cache
    del _query_errors[:]          # per-report; stale errors must not linger
    now = time.time()
    if not force and _report_cache and now - _report_cache[0] < REPORT_CACHE_TTL:
        return _report_cache[1]

    bot = _open_bot_db()
    cmp = _open_db(CMP_DB_PATH)
    try:
        report = _build_report(bot, cmp)
        _report_cache = (now, report)
        return report
    finally:
        if bot:
            bot.close()
        if cmp:
            cmp.close()


def _build_report(
    bot: Optional[sqlite3.Connection], cmp: Optional[sqlite3.Connection]
) -> dict:
    with ThreadPoolExecutor(max_workers=2) as pool:
        summary_f = pool.submit(_load_summary)
        onchain_f = pool.submit(_load_onchain)
        summary, summary_err = summary_f.result()
        onchain, onchain_err = onchain_f.result()

    sync = {
        row["key"]: row["value"]
        for row in _query(bot, "SELECT key, value FROM sync_state")
    }
    blacklist_groups = _query(
        bot, "SELECT status, chain, COUNT(*) AS n FROM blacklist GROUP BY status, chain"
    )
    pipeline_blacklist = defaultdict(int)
    for row in blacklist_groups:
        pipeline_blacklist[f"{row['chain'].lower()}_{row['status']}"] += row["n"]
        pipeline_blacklist[row["status"]] += row["n"]

    ms_groups = _query(
        bot,
        "SELECT executed, chain, action, COUNT(*) AS n FROM multisig_pending GROUP BY executed, chain, action",
    )
    pipeline_ms = {
        "pending": 0,
        "executed": 0,
        "blacklist": 0,
        "unblacklist": 0,
        "tron_pending": 0,
        "eth_pending": 0,
    }
    for row in ms_groups:
        if row["executed"]:
            pipeline_ms["executed"] += row["n"]
        else:
            pipeline_ms["pending"] += row["n"]
            key = f"{(row['chain'] or 'TRON').lower()}_pending"
            pipeline_ms[key] = pipeline_ms.get(key, 0) + row["n"]
        action = row["action"] or ""
        if action in pipeline_ms:
            pipeline_ms[action] += row["n"]

    # Same clock as Telegram: MultiSig submit block time → execution block time.
    # One row per executed blacklist proposal, not every historical blacklist row.
    blacklist_confirm = {
        ((row["address"] or ""), (row["chain"] or "TRON").upper()): row
        for row in _query(
            bot,
            """SELECT address, chain, status, confirmed_block_timestamp
               FROM blacklist""",
        )
    }
    redemptions: list[dict] = []
    for row in _query(
        bot,
        """SELECT chain, target_address, submission_block_timestamp, executed_at
           FROM multisig_pending
           WHERE executed = 1
             AND COALESCE(action, 'blacklist') = 'blacklist'
             AND submission_block_timestamp IS NOT NULL""",
    ):
        chain = (row["chain"] or "TRON").upper()
        if chain not in ("ETH", "TRON"):
            chain = "TRON"
        addr = row["target_address"] or ""
        submitted = row["submission_block_timestamp"]
        bl = blacklist_confirm.get((addr, chain))
        confirmed = None
        if bl and (bl["status"] or "") == "confirmed":
            confirmed = bl["confirmed_block_timestamp"]
        if confirmed is None:
            confirmed = _ms_from_iso(row["executed_at"])
        elapsed = _redemption_s(submitted, confirmed)
        if elapsed is None:
            continue
        redemptions.append(
            {
                "chain": chain,
                "address": addr,
                "submitted_at": _iso_from_ms(submitted),
                "confirmed_at": _iso_from_ms(confirmed) or _iso(_parse_dt(row["executed_at"])),
                "elapsed_s": elapsed,
            }
        )
    redemption_by_chain: dict[str, list[float]] = defaultdict(list)
    for rec in redemptions:
        redemption_by_chain[rec["chain"]].append(rec["elapsed_s"])
    redemption_tron = _latency_stats(redemption_by_chain.get("TRON") or [])
    redemption_eth = _latency_stats(redemption_by_chain.get("ETH") or [])
    redemption_all = _latency_stats(
        (redemption_by_chain.get("TRON") or []) + (redemption_by_chain.get("ETH") or [])
    )

    recent_ms = [
        {
            "tx_id": row["tx_id"],
            "chain": row["chain"],
            "address": row["target_address"],
            "action": row["action"],
            "submitted_at": row["submitted_at"],
            "executed": bool(row["executed"]),
            "executed_at": row["executed_at"],
            "tx_hash": row["submission_tx_hash"],
        }
        for row in _query(
            bot,
            """SELECT tx_id, chain, target_address, action, submitted_at, executed,
                      executed_at, submission_tx_hash
               FROM multisig_pending
               ORDER BY submitted_at DESC LIMIT 40""",
        )
    ]

    detections = _query(
        cmp,
        "SELECT address, source, status, detected_at, balance FROM detections",
    )
    comparisons = _query(
        cmp,
        """SELECT address, status, competitor_time, our_time, diff_ms, winner, created_at
           FROM comparisons ORDER BY created_at DESC""",
    )

    our_keys: set[tuple[str, str]] = set()
    competitor_keys: set[tuple[str, str]] = set()
    det_by_source = Counter()
    for row in detections:
        key = (_norm_addr(row["address"]), _norm_status(row["status"]))
        if row["source"] == "our_bot":
            our_keys.add(key)
            det_by_source["our_bot"] += 1
        elif row["source"] == "competitor":
            competitor_keys.add(key)
            det_by_source["competitor"] += 1

    for ev in summary:
        our_keys.add((ev["address"], ev["status"]))

    wins = losses = ties = 0
    win_margins: list[float] = []
    loss_margins: list[float] = []
    by_status: dict[str, Counter] = defaultdict(Counter)
    recent_cmp = []
    cmp_index: dict[tuple[str, str], dict] = {}
    daily_cmp: dict[str, Counter] = defaultdict(Counter)

    for row in comparisons:
        winner = row["winner"]
        status = _norm_status(row["status"])
        by_status[status][winner] += 1
        if winner == "US":
            wins += 1
            win_margins.append(abs(row["diff_ms"] or 0) / 1000.0)
        elif winner == "THEM":
            losses += 1
            loss_margins.append(abs(row["diff_ms"] or 0) / 1000.0)
        else:
            ties += 1
        created = (row["created_at"] or "")[:10]
        if created:
            daily_cmp[created][winner] += 1
            daily_cmp[created]["total"] += 1
        rec = {
            "address": row["address"],
            "status": status,
            "winner": winner,
            "diff_ms": row["diff_ms"],
            "diff_s": round((row["diff_ms"] or 0) / 1000.0, 1),
            "our_time": row["our_time"],
            "competitor_time": row["competitor_time"],
            "created_at": row["created_at"],
        }
        cmp_index[(_norm_addr(row["address"]), status)] = rec
        if len(recent_cmp) < 25:
            recent_cmp.append(rec)

    total_cmp = wins + losses + ties
    our_only = len(our_keys - competitor_keys)
    them_only = len(competitor_keys - our_keys)

    pending_delays = [
        e["delay_s"]
        for e in summary
        if e["delay_s"] is not None and e["status"] == "pending"
    ]
    confirmed_delays = [
        e["delay_s"]
        for e in summary
        if e["delay_s"] is not None and e["status"] == "confirmed"
    ]
    latency = _latency_stats(pending_delays)
    latency_confirmed = _latency_stats(confirmed_delays)
    latency_all = _latency_stats(pending_delays + confirmed_delays)
    daily_delay: dict[str, list[float]] = defaultdict(list)
    daily_alerts: Counter = Counter()
    daily_pending: Counter = Counter()
    daily_confirmed: Counter = Counter()
    for ev in summary:
        day = (ev["alert_time"] or ev["onchain_time"] or "")[:10]
        if not day:
            continue
        daily_alerts[day] += 1
        if ev["status"] == "pending":
            daily_pending[day] += 1
            if ev["delay_s"] is not None:
                daily_delay[day].append(ev["delay_s"])
        elif ev["status"] == "confirmed":
            daily_confirmed[day] += 1

    freeze_subs = [
        e
        for e in onchain
        if e["event_type"].lower() == "submission"
        and e["action"] == "addBlackList"
        and _is_wallet(e["address"])
    ]
    onchain_keys = {(e["chain"], e["address"]) for e in freeze_subs}
    our_addr_keys = {
        (e["chain"], e["address"]) for e in summary if _is_wallet(e["address"])
    }
    our_addr_keys |= {
        ("ETH" if a.startswith("0x") else "TRON", a)
        for a, _status in our_keys
        if _is_wallet(a)
    }
    our_hashes = {_norm_hash(e["tx_hash"]) for e in summary if e.get("tx_hash")}
    for row in _query(bot, "SELECT address, chain, tx_hash FROM blacklist"):
        our_addr_keys.add(
            ((row["chain"] or "TRON").upper(), _norm_addr(row["address"]))
        )
        if row["tx_hash"]:
            our_hashes.add(_norm_hash(row["tx_hash"]))
    for row in _query(
        bot, "SELECT target_address, chain, submission_tx_hash FROM multisig_pending"
    ):
        our_addr_keys.add(
            ((row["chain"] or "TRON").upper(), _norm_addr(row["target_address"]))
        )
        if row["submission_tx_hash"]:
            our_hashes.add(_norm_hash(row["submission_tx_hash"]))

    onchain_freezes = []
    for ev in freeze_subs:
        key = (ev["chain"], ev["address"])
        caught = key in our_addr_keys or (
            bool(_norm_hash(ev.get("tx_hash") or ""))
            and _norm_hash(ev.get("tx_hash") or "") in our_hashes
        )
        onchain_freezes.append(
            {
                "address": ev["address"],
                "chain": ev["chain"],
                "tx_id": ev["tx_id"],
                "timestamp": ev["timestamp"],
                "action": ev["action"],
                "tx_hash": ev["tx_hash"],
                "caught": caught,
            }
        )

    # Sheet lags behind live Multisig freezes — merge bot DB so Today/7d coverage
    # stays accurate when Google Sheets hasn't been updated yet.
    sheet_addrs = {(f["chain"], f["address"]) for f in onchain_freezes}
    for row in _query(
        bot,
        """SELECT chain, target_address, submission_tx_hash, submission_block_timestamp, tx_id
           FROM multisig_pending
           WHERE COALESCE(action, 'blacklist') = 'blacklist'
             AND submission_block_timestamp IS NOT NULL""",
    ):
        chain = (row["chain"] or "TRON").upper()
        if chain not in ("ETH", "TRON"):
            chain = "TRON"
        addr = _norm_addr(row["target_address"] or "")
        if not _is_wallet(addr):
            continue
        if (chain, addr) in sheet_addrs:
            continue
        ts = _iso_from_ms(row["submission_block_timestamp"])
        if not ts:
            continue
        sheet_addrs.add((chain, addr))
        onchain_keys.add((chain, addr))
        onchain_freezes.append(
            {
                "address": addr,
                "chain": chain,
                "tx_id": str(row["tx_id"] or ""),
                "timestamp": ts,
                "action": "addBlackList",
                "tx_hash": row["submission_tx_hash"] or "",
                "caught": True,
            }
        )

    onchain_freezes.sort(key=lambda f: f.get("timestamp") or "", reverse=True)
    covered = sum(1 for f in onchain_freezes if f.get("caught"))
    misses = [f for f in onchain_freezes if not f.get("caught")]
    coverage_pct = (
        round(100.0 * covered / len(onchain_freezes), 1) if onchain_freezes else None
    )
    extra_alerts = sum(
        1
        for e in summary
        if e["address"] and (e["chain"], e["address"]) not in onchain_keys
    )

    freeze_hashes = {
        _norm_hash(x.get("tx_hash") or "") for x in onchain_freezes if x.get("tx_hash")
    }
    sys_by_key, sys_by_hash = _build_system_detected_index(bot)
    events = []
    for ev in summary:
        key = (ev["address"], ev["status"])
        cmp_hit = cmp_index.get(key)
        ev_hash = _norm_hash(ev.get("tx_hash") or "")
        sys = _resolve_system_detected(ev, sys_by_key, sys_by_hash)
        system_detected_at = sys.get("system_detected_at")
        onchain_iso = ev.get("onchain_time")
        # A fuzzy address match must never rewrite the event's own on-chain time:
        # that is how a Sep 25 row ended up displaying Aug 27.
        if sys.get("system_onchain_ms") is not None and sys.get("system_match") == "hash":
            onchain_from_db = _iso_from_ms(sys["system_onchain_ms"])
            if onchain_from_db:
                onchain_iso = onchain_from_db
        system_delay_s = _sys_delay_from_columns(onchain_iso, system_detected_at)
        lead_s = _lead_s(onchain_iso, system_detected_at)
        events.append(
            {
                **ev,
                "onchain_time": onchain_iso,
                "system_detected_at": system_detected_at,
                "system_delay_s": system_delay_s,
                "lead_s": lead_s,
                "system_match": sys.get("system_match"),
                "winner": cmp_hit["winner"] if cmp_hit else None,
                "diff_s": cmp_hit["diff_s"] if cmp_hit else None,
                "onchain_matched": (
                    (
                        (ev["chain"], ev["address"]) in onchain_keys
                        or (bool(ev_hash) and ev_hash in freeze_hashes)
                    )
                    if ev["address"]
                    else False
                ),
            }
        )
    # Group each freeze's two rows together instead of sorting rows
    # independently. Execution always lands in a later block than submission,
    # so a flat newest-first sort placed CONFIRMED above its own PENDING.
    def _freeze_key(e: dict) -> tuple:
        return (e.get("chain") or "", (e.get("address") or "").lower())

    _newest: dict[tuple, str] = {}
    for _e in events:
        _k = _freeze_key(_e)
        _ts = _e.get("alert_time") or _e.get("onchain_time") or ""
        if _ts > _newest.get(_k, ""):
            _newest[_k] = _ts

    # Undated groups last, newest first, then CONFIRMED above PENDING.
    events.sort(
        key=lambda e: (
            _newest.get(_freeze_key(e), "") == "",
            _invert_ts(_newest.get(_freeze_key(e), "")),
            _freeze_key(e),
            0 if e.get("status") == "confirmed" else 1,
        )
    )

    bal_by_key, bal_by_addr = _build_balance_index(detections)
    _enrich_event_balances(events, bal_by_key, bal_by_addr)

    system_pending_delays = [
        e["system_delay_s"]
        for e in events
        if e.get("system_delay_s") is not None and e["status"] == "pending"
    ]
    system_confirmed_delays = [
        e["system_delay_s"]
        for e in events
        if e.get("system_delay_s") is not None and e["status"] == "confirmed"
    ]
    latency_system = _latency_stats(system_pending_delays)
    latency_system_confirmed = _latency_stats(system_confirmed_delays)
    latency_system_all = _latency_stats(system_pending_delays + system_confirmed_delays)

    days = sorted(set(daily_alerts) | set(daily_cmp), reverse=True)[:30]
    daily = []
    for day in reversed(days):
        dvals = daily_delay.get(day) or []
        daily.append(
            {
                "date": day,
                "alerts": daily_alerts.get(day, 0),
                "pending": daily_pending.get(day, 0),
                "confirmed": daily_confirmed.get(day, 0),
                "median_delay_s": _percentile(sorted(dvals), 50) if dvals else None,
                "wins": daily_cmp[day]["US"],
                "losses": daily_cmp[day]["THEM"],
                "comparisons": daily_cmp[day]["total"],
            }
        )

    tron_last = sync.get("tron_last_scan")
    eth_last = sync.get("eth_watcher_last_scan") or sync.get("eth_last_scan")
    tron_stale = _stale_seconds(tron_last)
    eth_stale = _stale_seconds(eth_last)

    def _sync_int(key: str) -> Optional[int]:
        raw = sync.get(key)
        if not raw:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None

    tron_block = _sync_int("tron_last_block_num")
    eth_block_candidates = [
        b
        for b in (
            _sync_int("eth_watcher_last_block"),
            _sync_int("eth_multisig_last_block"),
            _sync_int("eth_last_block_number"),
        )
        if b is not None
    ]
    eth_block = max(eth_block_candidates) if eth_block_candidates else None

    health_status = "healthy"
    if tron_stale is None or (tron_stale is not None and tron_stale > 120):
        health_status = "stale"
    elif tron_stale > 30:
        health_status = "slow"

    return {
        "generated_at": _now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "sources": {
            # Credentials stripped: this payload is served to the panels.
            "bot_db": (
                "postgresql://zarin-fd-db:5435/freeze_detector" if BOT_DSN else None
            ),
            "cmp_db": (
                os.path.abspath(CMP_DB_PATH) if os.path.exists(CMP_DB_PATH) else None
            ),
            "query_errors": list(_query_errors),
            "summary_sheet": SUMMARY_SHEET_ID,
            "onchain_sheet": ONCHAIN_SHEET_ID,
            "summary_error": summary_err,
            "onchain_error": onchain_err,
            "summary_rows": len(summary),
            "onchain_rows": len(onchain),
        },
        "health": {
            "status": health_status,
            "tron_last_scan": tron_last,
            "eth_last_scan": eth_last,
            "tron_stale_s": tron_stale,
            "eth_stale_s": eth_stale,
            "tron_last_block": tron_block,
            "eth_last_block": eth_block,
        },
        "kpis": {
            "median_delay_s": latency["p50"],
            "p95_delay_s": latency["p95"],
            "avg_delay_s": latency["avg"],
            "fast_share_le_10s": latency["fast_share_le_10s"],
            "pending_count": latency["count"],
            "confirmed_median_s": latency_confirmed["p50"],
            "confirmed_p95_s": latency_confirmed["p95"],
            "confirmed_avg_s": latency_confirmed["avg"],
            "confirmed_count": latency_confirmed["count"],
            "all_median_s": latency_all["p50"],
            "all_p95_s": latency_all["p95"],
            "all_avg_s": latency_all["avg"],
            "all_count": latency_all["count"],
            "coverage_pct": coverage_pct,
            "onchain_submissions": len(onchain_freezes),
            "covered": covered,
            "missed": len(misses),
            "extra_alerts": extra_alerts,
            "win_rate_pct": round(100.0 * wins / total_cmp, 1) if total_cmp else None,
            "comparisons": total_cmp,
            "wins": wins,
            "losses": losses,
            "ties": ties,
            "our_alerts": det_by_source["our_bot"] or len(summary),
            "competitor_alerts": det_by_source["competitor"],
            "our_only": our_only,
            "them_only": them_only,
            "avg_win_margin_s": _mean(win_margins),
            "avg_loss_margin_s": _mean(loss_margins),
            "redemption_median_s": redemption_all["p50"],
            "redemption_tron_median_s": redemption_tron["p50"],
            "redemption_eth_median_s": redemption_eth["p50"],
            "redemption_tron_count": redemption_tron["count"],
            "redemption_eth_count": redemption_eth["count"],
            "system_median_delay_s": latency_system["p50"],
            "system_p95_delay_s": latency_system["p95"],
            "system_pending_count": latency_system["count"],
        },
        "latency": latency,
        "latency_confirmed": latency_confirmed,
        "latency_all": latency_all,
        "latency_system": latency_system,
        "latency_system_confirmed": latency_system_confirmed,
        "latency_system_all": latency_system_all,
        "redemption": {
            "all": redemption_all,
            "tron": redemption_tron,
            "eth": redemption_eth,
        },
        "redemptions": redemptions,
        "competition": {
            "by_status": {k: dict(v) for k, v in by_status.items()},
            "recent": recent_cmp,
        },
        "pipeline": {
            "blacklist": dict(pipeline_blacklist),
            "multisig": pipeline_ms,
            "recent_multisig": recent_ms[:12],
        },
        "daily": daily,
        "events": events[:3000],
        "misses": misses[:500],
        "onchain_freezes": onchain_freezes[:2000],
        "onchain_recent": [
            e
            for e in reversed(onchain)
            if e["event_type"].lower() == "submission"
            and _is_wallet(e.get("address") or "")
        ][:40],
    }


def as_json() -> str:
    return json.dumps(build_report(), default=str)
