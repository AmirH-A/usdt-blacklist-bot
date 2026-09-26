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
import os
import sqlite3
import time
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Optional

SUMMARY_SHEET_ID = os.getenv(
    "SUMMARY_SHEET_ID", "10r5M5ksCCDhjm05IztVbIYgvdyp5VOJcxK7xeHJTiyg"
)
ONCHAIN_SHEET_ID = os.getenv(
    "ONCHAIN_SHEET_ID", "1LBXx04B_w3CqllGpudsKXhy5sC3l9nCvhmQy6U_3dG0"
)
ONCHAIN_GID = os.getenv("ONCHAIN_GID", "1543539254")
BOT_DB_PATH = os.getenv("BOT_DB_PATH", "blacklist_tracker.db")
CMP_DB_PATH = os.getenv("CMP_DB_PATH", "comparison_tracker.db")
SHEET_CACHE_TTL = int(os.getenv("SHEET_CACHE_TTL", "45"))
REPORT_CACHE_TTL = int(os.getenv("REPORT_CACHE_TTL", "20"))
MAX_DELAY_S = 3600

_sheet_cache: dict[str, tuple[float, list[dict]]] = {}
_report_cache: tuple[float, dict] | None = None


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
    req = urllib.request.Request(url, headers={"User-Agent": "freeze-accuracy-panel/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            raw = resp.read().decode("utf-8")
    except Exception:
        if cached:
            return cached[1]
        raise
    rows = [r for r in csv.DictReader(io.StringIO(raw)) if any(str(v).strip() for v in r.values())]
    _sheet_cache[url] = (now, rows)
    return rows


def _sheet_url(sheet_id: str, gid: str = "0") -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def _open_db(path: str) -> Optional[sqlite3.Connection]:
    if not path or not os.path.exists(path) or os.path.getsize(path) == 0:
        return None
    try:
        conn = sqlite3.connect(f"file:{os.path.abspath(path)}?mode=ro", uri=True, timeout=5)
    except sqlite3.Error:
        conn = sqlite3.connect(path, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _query(conn: Optional[sqlite3.Connection], sql: str, args: tuple = ()) -> list[sqlite3.Row]:
    if not conn:
        return []
    try:
        return conn.execute(sql, args).fetchall()
    except sqlite3.Error:
        return []


def _norm_addr(addr: str) -> str:
    addr = (addr or "").strip()
    if addr.startswith("0x"):
        return addr.lower()
    return addr


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
        "fast_share_le_10s": round(sum(1 for d in delays if d <= 10) / len(delays), 3) if delays else 0,
        "buckets": counts,
    }


def _stale_seconds(iso_value: Optional[str]) -> Optional[float]:
    dt = _parse_dt(iso_value)
    if not dt:
        return None
    return round((_now() - dt).total_seconds(), 1)


def build_report(force: bool = False) -> dict[str, Any]:
    global _report_cache
    now = time.time()
    if not force and _report_cache and now - _report_cache[0] < REPORT_CACHE_TTL:
        return _report_cache[1]

    bot = _open_db(BOT_DB_PATH)
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


def _build_report(bot: Optional[sqlite3.Connection], cmp: Optional[sqlite3.Connection]) -> dict:
    with ThreadPoolExecutor(max_workers=2) as pool:
        summary_f = pool.submit(_load_summary)
        onchain_f = pool.submit(_load_onchain)
        summary, summary_err = summary_f.result()
        onchain, onchain_err = onchain_f.result()

    sync = {row["key"]: row["value"] for row in _query(bot, "SELECT key, value FROM sync_state")}
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
    for ev in summary:
        day = (ev["alert_time"] or ev["onchain_time"] or "")[:10]
        if not day:
            continue
        daily_alerts[day] += 1
        if ev["delay_s"] is not None and ev["status"] == "pending":
            daily_delay[day].append(ev["delay_s"])

    submissions = [
        e
        for e in onchain
        if e["event_type"].lower() == "submission" and e["action"] in ("addBlackList", "removeBlackList")
    ]
    freeze_subs = [e for e in submissions if e["action"] == "addBlackList" and e["address"]]
    onchain_keys = {(e["chain"], e["address"]) for e in freeze_subs}
    our_addr_keys = {(e["chain"], e["address"]) for e in summary if e["address"]}
    our_addr_keys |= {
        ("TRON" if not a.startswith("0x") else "ETH", a)
        for a, _status in our_keys
    }
    # blacklist table is a stronger live source of "we saw it"
    for row in _query(bot, "SELECT address, chain FROM blacklist"):
        our_addr_keys.add(((row["chain"] or "TRON").upper(), _norm_addr(row["address"])))

    matched = {k for k in onchain_keys if k in our_addr_keys}
    misses = []
    for ev in freeze_subs:
        key = (ev["chain"], ev["address"])
        if key not in matched:
            misses.append(
                {
                    "address": ev["address"],
                    "chain": ev["chain"],
                    "tx_id": ev["tx_id"],
                    "timestamp": ev["timestamp"],
                    "action": ev["action"],
                    "tx_hash": ev["tx_hash"],
                }
            )
    coverage_pct = round(100.0 * len(matched) / len(onchain_keys), 1) if onchain_keys else None

    events = []
    for ev in summary:
        key = (ev["address"], ev["status"])
        cmp_hit = cmp_index.get(key)
        events.append(
            {
                **ev,
                "winner": cmp_hit["winner"] if cmp_hit else None,
                "diff_s": cmp_hit["diff_s"] if cmp_hit else None,
                "onchain_matched": (ev["chain"], ev["address"]) in onchain_keys if ev["address"] else False,
            }
        )
    events.sort(key=lambda e: e.get("alert_time") or e.get("onchain_time") or "", reverse=True)

    days = sorted(set(daily_alerts) | set(daily_cmp), reverse=True)[:30]
    daily = []
    for day in reversed(days):
        dvals = daily_delay.get(day) or []
        daily.append(
            {
                "date": day,
                "alerts": daily_alerts.get(day, 0),
                "median_delay_s": _percentile(sorted(dvals), 50) if dvals else None,
                "wins": daily_cmp[day]["US"],
                "losses": daily_cmp[day]["THEM"],
                "comparisons": daily_cmp[day]["total"],
            }
        )

    tron_last = sync.get("tron_last_scan")
    eth_last = sync.get("eth_last_scan")
    tron_stale = _stale_seconds(tron_last)
    eth_stale = _stale_seconds(eth_last)
    health_status = "healthy"
    if tron_stale is None or (tron_stale is not None and tron_stale > 120):
        health_status = "stale"
    elif tron_stale > 30:
        health_status = "slow"

    return {
        "generated_at": _now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "sources": {
            "bot_db": os.path.abspath(BOT_DB_PATH) if os.path.exists(BOT_DB_PATH) else None,
            "cmp_db": os.path.abspath(CMP_DB_PATH) if os.path.exists(CMP_DB_PATH) else None,
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
            "tron_last_block": int(sync["tron_last_block_num"]) if sync.get("tron_last_block_num") else None,
            "eth_last_block": int(sync["eth_last_block_number"]) if sync.get("eth_last_block_number") else None,
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
            "onchain_submissions": len(onchain_keys),
            "covered": len(matched),
            "missed": len(misses),
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
        },
        "latency": latency,
        "latency_confirmed": latency_confirmed,
        "latency_all": latency_all,
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
        "events": events[:80],
        "misses": misses[:40],
        "onchain_recent": onchain[-20:][::-1],
    }


def as_json() -> str:
    return json.dumps(build_report(), default=str)
