#!/usr/bin/env python3
"""Isolate connection pooling from the storage engine on the write path.

The old code opened a sqlite connection per call; the new code takes a pooled
postgres connection. That conflates two changes. Four variants here:

  sqlite, new connection each write   <- what production actually did before
  sqlite, one persistent connection   <- what sqlite could have done
  postgres, new connection each write <- what postgres without a pool costs
  postgres, pooled connection         <- what production does now
"""
import os
import shutil
import sqlite3
import statistics
import time

import psycopg2
from psycopg2 import pool as pg_pool

COPY = "/tmp/bench_write.db"
DSN = os.environ["DATABASE_URL"]
N = 200

UPSERT_SQLITE = "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)"
UPSERT_PG = ("INSERT INTO sync_state (key, value) VALUES (%s, %s) "
             "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")


def timed(fn, n):
    s = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        s.append((time.perf_counter() - t0) * 1000.0)
    s.sort()
    return statistics.median(s), s[int(len(s) * 0.95) - 1]


def main():
    shutil.copy("blacklist_tracker.db", COPY)
    sqlite3.connect(COPY).close()

    # 1. sqlite, connection per write (the old production behaviour)
    def sq_fresh():
        c = sqlite3.connect(COPY, timeout=5.0)
        try:
            c.execute(UPSERT_SQLITE, ("__bench", str(time.time())))
            c.commit()
        finally:
            c.close()

    # 2. sqlite, one connection held open
    persistent = sqlite3.connect(COPY, timeout=5.0, check_same_thread=False)

    def sq_persistent():
        persistent.execute(UPSERT_SQLITE, ("__bench", str(time.time())))
        persistent.commit()

    # 3. postgres, connection per write
    def pg_fresh():
        c = psycopg2.connect(DSN)
        try:
            cur = c.cursor()
            cur.execute(UPSERT_PG, ("__bench", str(time.time())))
            c.commit()
        finally:
            c.close()

    # 4. postgres, pooled (current production behaviour)
    pool = pg_pool.ThreadedConnectionPool(1, 10, DSN)

    def pg_pooled():
        c = pool.getconn()
        try:
            cur = c.cursor()
            cur.execute(UPSERT_PG, ("__bench", str(time.time())))
            c.commit()
        finally:
            pool.putconn(c)

    variants = [
        ("sqlite   + new connection per write  (OLD prod)", sq_fresh),
        ("sqlite   + persistent connection", sq_persistent),
        ("postgres + new connection per write", pg_fresh),
        ("postgres + pooled connection        (NEW prod)", pg_pooled),
    ]

    print(f"{'variant':50s} {'p50':>9s} {'p95':>9s}")
    print("-" * 70)
    results = {}
    for label, fn in variants:
        p50, p95 = timed(fn, N)
        results[label] = p50
        print(f"{label:50s} {p50:7.3f}ms {p95:7.3f}ms")

    persistent.close()
    c = pool.getconn()
    cur = c.cursor()
    cur.execute("DELETE FROM sync_state WHERE key = %s", ("__bench",))
    c.commit()
    pool.putconn(c)
    pool.closeall()
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(COPY + suffix):
            os.remove(COPY + suffix)

    old = results["sqlite   + new connection per write  (OLD prod)"]
    new = results["postgres + pooled connection        (NEW prod)"]
    sqp = results["sqlite   + persistent connection"]
    print(f"\nprod before -> prod after: {old:.3f}ms -> {new:.3f}ms  ({old/new:.2f}x faster)")
    print(f"but sqlite with a persistent connection: {sqp:.3f}ms "
          f"({old/sqp:.2f}x faster than old prod, no postgres involved)")


if __name__ == "__main__":
    main()
