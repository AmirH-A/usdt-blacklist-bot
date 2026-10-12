#!/usr/bin/env python3
"""One-shot copy of blacklist_tracker.db into Postgres, then verify.

Idempotent: every insert is ON CONFLICT DO NOTHING, so a re-run adds only
rows that are missing. Verification compares per-table counts and, for the
two tables that matter, a checksum of the primary keys -- a count match alone
would not catch a row that landed with the wrong id.

Usage:  python3 migrate_to_pg.py [--sqlite PATH] [--dry-run]
"""

import argparse
import os
import sqlite3
import sys

import psycopg2
from psycopg2.extras import execute_values

TABLES = {
    # table: (columns, primary key columns)
    "subscribers": (["chat_id", "subscribed_at"], ["chat_id"]),
    "blacklist": (
        [
            "address",
            "chain",
            "status",
            "detected_at",
            "confirmed_at",
            "tx_hash",
            "alerted",
            "block_timestamp",
            "confirmed_block_timestamp",
        ],
        ["address", "chain"],
    ),
    "sync_state": (["key", "value"], ["key"]),
    "watchlist": (["chat_id", "address", "chain", "added_at"], ["chat_id", "address", "chain"]),
    "multisig_pending": (
        [
            "tx_id",
            "chain",
            "target_address",
            "action",
            "submission_tx_hash",
            "submitted_at",
            "submission_block_timestamp",
            "executed",
            "executed_at",
            "alerted",
        ],
        ["tx_id", "chain"],
    ),
}


def sqlite_rows(sq, table, cols):
    """Read a table, tolerating columns an older sqlite file never gained."""
    have = {r[1] for r in sq.execute(f"PRAGMA table_info({table})").fetchall()}
    missing = [c for c in cols if c not in have]
    select = ", ".join(c if c in have else "NULL" for c in cols)
    rows = sq.execute(f"SELECT {select} FROM {table}").fetchall()
    return rows, missing


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sqlite", default="blacklist_tracker.db")
    ap.add_argument("--dsn", default=os.getenv("DATABASE_URL", ""))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.dsn:
        sys.exit("DATABASE_URL not set and --dsn not given")
    if not os.path.exists(args.sqlite):
        sys.exit(f"no such sqlite file: {args.sqlite}")

    sq = sqlite3.connect(f"file:{os.path.abspath(args.sqlite)}?mode=ro", uri=True)
    pg = psycopg2.connect(args.dsn)
    pg.autocommit = False
    cur = pg.cursor()

    print(f"source: {args.sqlite}")
    totals = {}
    for table, (cols, _pk) in TABLES.items():
        rows, missing = sqlite_rows(sq, table, cols)
        totals[table] = len(rows)
        note = f"  (columns absent in sqlite, inserted NULL: {', '.join(missing)})" if missing else ""
        print(f"  {table}: {len(rows)} rows{note}")
        if args.dry_run or not rows:
            continue
        execute_values(
            cur,
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
            f"ON CONFLICT DO NOTHING",
            rows,
            page_size=500,
        )

    if args.dry_run:
        print("\ndry run, nothing written")
        pg.rollback()
        return

    pg.commit()

    # --- verify -------------------------------------------------------------
    print("\nverification (sqlite -> postgres):")
    ok = True
    for table, (cols, pk) in TABLES.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        pg_n = cur.fetchone()[0]
        sq_n = totals[table]
        # Primary keys, sorted and concatenated, must match on both sides.
        key_expr_sq = " || '|' || ".join(f"COALESCE(CAST({c} AS TEXT),'')" for c in pk)
        key_expr_pg = " || '|' || ".join(f"COALESCE(CAST({c} AS TEXT),'')" for c in pk)
        sq_keys = sorted(r[0] for r in sq.execute(f"SELECT {key_expr_sq} FROM {table}"))
        cur.execute(f"SELECT {key_expr_pg} FROM {table}")
        pg_keys = sorted(r[0] for r in cur.fetchall())
        same_keys = sq_keys == pg_keys
        flag = "OK " if (pg_n == sq_n and same_keys) else "FAIL"
        if flag == "FAIL":
            ok = False
        detail = "" if same_keys else "  <- primary keys differ"
        print(f"  {flag} {table}: {sq_n} -> {pg_n}{detail}")

    sq.close()
    pg.close()
    if not ok:
        sys.exit("verification FAILED -- do not switch the services over")
    print("\nall tables match")


if __name__ == "__main__":
    main()
