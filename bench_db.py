#!/usr/bin/env python3
"""Benchmark the detector's real query patterns: sqlite file vs postgres.

Both hold identical data (the sqlite file is the frozen pre-cutover snapshot,
4563 blacklist + 1096 multisig_pending rows). Each case runs the query the way
production runs it -- including connection acquisition, since the old code
opened a file per call and the new code takes one from a pool.

Read-only apart from the sync_state write case, which uses a probe key and
deletes it afterwards.
"""
import os
import sqlite3
import statistics
import sys
import time

import psycopg2
from psycopg2 import pool as pg_pool

SQLITE = "blacklist_tracker.db"
DSN = os.environ["DATABASE_URL"]
N = 200          # iterations per case for the cheap ones
N_HEAVY = 20     # full-scan cases

# (label, sql, args, iterations) -- `?` is rewritten to %s for postgres.
CASES = [
    ("sync_state read   (every block poll)",
     "SELECT value FROM sync_state WHERE key = ?", ("tron_last_scan",), N),
    ("multisig hash lookup (every submission)",
     "SELECT target_address, action FROM multisig_pending "
     "WHERE submission_tx_hash = ? AND chain = 'TRON'",
     ("1cc795895c2a2d4bab501f0695a90ac955d084231a425ffe0f1c38bff6061c6f",), N),
    ("blacklist PK lookup",
     "SELECT status, detected_at, confirmed_at FROM blacklist "
     "WHERE address = ? AND chain = ?",
     ("TWF63whRtEK3fF1zUYkrqpj81JtzmQBqwQ", "TRON"), N),
    ("recent listing (ORDER BY + LIMIT)",
     "SELECT address, chain, status, detected_at FROM blacklist "
     "ORDER BY detected_at DESC LIMIT 10", (), N),
    ("status counts (GROUP BY, full scan)",
     "SELECT status, chain, COUNT(*) FROM blacklist GROUP BY status, chain", (), N_HEAVY),
    ("accuracy full scan (4563 rows out)",
     "SELECT address, chain, status, block_timestamp, detected_at, "
     "confirmed_block_timestamp, confirmed_at, tx_hash FROM blacklist", (), N_HEAVY),
]


def timed(fn, n):
    samples = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    samples.sort()
    return {
        "p50": statistics.median(samples),
        "p95": samples[int(len(samples) * 0.95) - 1],
        "min": samples[0],
    }


def main():
    if not os.path.exists(SQLITE):
        sys.exit(f"missing {SQLITE}")

    pool = pg_pool.ThreadedConnectionPool(1, 10, DSN)

    def sqlite_run(sql, args):
        # Exactly what the old code did: open, query, close.
        conn = sqlite3.connect(SQLITE, timeout=5.0)
        try:
            conn.execute(sql, args).fetchall()
        finally:
            conn.close()

    def pg_run(sql, args):
        # Exactly what the new code does: take from pool, query, return.
        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute(sql.replace("?", "%s"), args or None)
            cur.fetchall()
            conn.rollback()
        finally:
            pool.putconn(conn)

    print(f"{'case':42s} {'sqlite p50':>11s} {'pg p50':>10s} {'ratio':>8s} {'pg p95':>9s}")
    print("-" * 85)
    rows = []
    for label, sql, args, n in CASES:
        s = timed(lambda: sqlite_run(sql, args), n)
        p = timed(lambda: pg_run(sql, args), n)
        ratio = p["p50"] / s["p50"] if s["p50"] else float("inf")
        rows.append((label, s, p, ratio))
        print(f"{label:42s} {s['p50']:9.3f}ms {p['p50']:8.3f}ms "
              f"{ratio:7.2f}x {p['p95']:7.3f}ms")

    # --- write path ---------------------------------------------------------
    def sqlite_write():
        conn = sqlite3.connect(SQLITE, timeout=5.0)
        try:
            conn.execute("INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
                         ("__bench", str(time.time())))
            conn.commit()
        finally:
            conn.close()

    def pg_write():
        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO sync_state (key, value) VALUES (%s, %s) "
                        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        ("__bench", str(time.time())))
            conn.commit()
        finally:
            pool.putconn(conn)

    # The sqlite file is the frozen rollback snapshot: benchmark writes on a copy.
    import shutil
    shutil.copy(SQLITE, "/tmp/bench_sqlite.db")
    globals()["SQLITE"] = "/tmp/bench_sqlite.db"
    sw = timed(sqlite_write, N)
    pw = timed(pg_write, N)
    print(f"{'sync_state WRITE (every block poll)':42s} {sw['p50']:9.3f}ms "
          f"{pw['p50']:8.3f}ms {pw['p50']/sw['p50']:7.2f}x {pw['p95']:7.3f}ms")

    conn = pool.getconn()
    cur = conn.cursor()
    cur.execute("DELETE FROM sync_state WHERE key = %s", ("__bench",))
    conn.commit()
    pool.putconn(conn)
    os.remove("/tmp/bench_sqlite.db")
    pool.closeall()

    print("\nratio > 1 means postgres is SLOWER by that factor")


if __name__ == "__main__":
    main()
