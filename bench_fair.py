#!/usr/bin/env python3
"""Isolate engine from indexing.

bench_db.py showed postgres 3x faster on the "recent listing" query, but
postgres has two indexes the sqlite file never had. This gives sqlite the same
indexes on a copy and re-runs the affected cases, so the remaining difference
is the engine and not the schema.
"""
import os
import shutil
import sqlite3
import statistics
import time

import psycopg2
from psycopg2 import pool as pg_pool

SRC = "blacklist_tracker.db"
COPY = "/tmp/bench_indexed.db"
DSN = os.environ["DATABASE_URL"]
N = 200
N_HEAVY = 20

CASES = [
    ("recent listing (ORDER BY + LIMIT)",
     "SELECT address, chain, status, detected_at FROM blacklist "
     "ORDER BY detected_at DESC LIMIT 10", (), N),
    ("pending multisig listing",
     "SELECT tx_id, target_address, action, submitted_at FROM multisig_pending "
     "WHERE executed = 0 AND chain = ? ORDER BY submitted_at DESC LIMIT 10",
     ("TRON",), N),
    ("status counts (GROUP BY, full scan)",
     "SELECT status, chain, COUNT(*) FROM blacklist GROUP BY status, chain", (), N_HEAVY),
]


def timed(fn, n):
    s = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        s.append((time.perf_counter() - t0) * 1000.0)
    s.sort()
    return statistics.median(s)


def main():
    shutil.copy(SRC, COPY)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(SRC + suffix):
            shutil.copy(SRC + suffix, COPY + suffix)

    c = sqlite3.connect(COPY)
    c.execute("CREATE INDEX IF NOT EXISTS idx_blacklist_detected_at "
              "ON blacklist(detected_at DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_multisig_submitted_at "
              "ON multisig_pending(submitted_at DESC)")
    c.execute("ANALYZE")
    c.commit()
    c.close()
    print(f"sqlite copy at {COPY} now carries the same two indexes as postgres\n")

    pool = pg_pool.ThreadedConnectionPool(1, 10, DSN)

    def sq(sql, args):
        conn = sqlite3.connect(COPY, timeout=5.0)
        try:
            conn.execute(sql, args).fetchall()
        finally:
            conn.close()

    def pg(sql, args):
        conn = pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute(sql.replace("?", "%s"), args or None)
            cur.fetchall()
            conn.rollback()
        finally:
            pool.putconn(conn)

    print(f"{'case':42s} {'sqlite+idx':>11s} {'pg p50':>10s} {'ratio':>8s}")
    print("-" * 74)
    for label, sql, args, n in CASES:
        s = timed(lambda: sq(sql, args), n)
        p = timed(lambda: pg(sql, args), n)
        print(f"{label:42s} {s:9.3f}ms {p:8.3f}ms {p/s:7.2f}x")

    pool.closeall()
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(COPY + suffix):
            os.remove(COPY + suffix)
    print("\nratio > 1 = postgres slower. Indexes now equal, so this is engine only.")


if __name__ == "__main__":
    main()
