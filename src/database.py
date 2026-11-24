"""Freeze detector persistence, on Postgres (zarin-fd-db, 127.0.0.1:5435).

Ported from sqlite/blacklist_tracker.db. The call signatures are unchanged, so
listeners.py, handlers.py and alerts.py did not need touching; what changed:

  * `?` placeholders -> `%s`
  * INSERT OR IGNORE  -> INSERT ... ON CONFLICT DO NOTHING
  * INSERT OR REPLACE -> INSERT ... ON CONFLICT (key) DO UPDATE
  * strftime('%Y-%m-%d %H:%M:%f','now') -> _NOW (same text format, so the
    timestamps accuracy.py parses and compares are byte-identical to before)
  * get_db() hands out a pooled connection instead of opening a file. Callers
    still call .close(); that returns it to the pool rather than dropping it,
    because opening a TCP connection per block poll would add latency to the
    one path that must not get slower.

Schema lives in schema.sql at the project root and is applied by init_db().
"""

import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

import psycopg2
from psycopg2 import pool as pg_pool

from .config import DATABASE_URL, logger

# The sqlite expression every timestamp column used, in postgres form. Keeping
# the text format identical matters: accuracy.py string-compares these.
_NOW = "to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD HH24:MI:SS.MS')"
_TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

_pool: Optional[pg_pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    with _pool_lock:
        if _pool is None:
            if not DATABASE_URL:
                raise RuntimeError("DATABASE_URL is not set")
            # minconn 1 so a restart does not stampede; maxconn 10 covers the
            # block watchers, the bot handlers and the /health endpoint.
            _pool = pg_pool.ThreadedConnectionPool(1, 10, DATABASE_URL)
            logger.info("Postgres pool opened (1-10 connections)")
        return _pool


class _PooledConnection:
    """Quacks like the old sqlite3 connection: cursor/commit/close.

    close() returns the connection to the pool after a rollback, so a function
    that read without committing cannot leave an idle-in-transaction snapshot
    pinned for the next caller.
    """

    __slots__ = ("_raw", "_pool", "_closed")

    def __init__(self, pool_, raw):
        self._pool = pool_
        self._raw = raw
        self._closed = False

    def cursor(self):
        return self._raw.cursor()

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._raw.rollback()
        except Exception:
            pass
        try:
            self._pool.putconn(self._raw)
        except Exception:
            try:
                self._raw.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
        return False


def get_db() -> _PooledConnection:
    p = _get_pool()
    return _PooledConnection(p, p.getconn())


def migrate_db():
    """Kept for import compatibility.

    The sqlite version used this to ALTER TABLE older files up to the
    multi-chain schema. schema.sql already declares every column, so there is
    nothing to migrate on a fresh postgres database.
    """
    return


def init_db():
    """Apply schema.sql. Idempotent -- every statement is IF NOT EXISTS."""
    schema_path = os.path.join(os.path.dirname(__file__), os.pardir, "schema.sql")
    schema_path = os.path.abspath(schema_path)
    if not os.path.exists(schema_path):
        logger.warning(f"schema.sql not found at {schema_path}; skipping init_db")
        return
    with open(schema_path) as fh:
        ddl = fh.read()
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def _placeholder_tx_id(tx_hash: str) -> int:
    try:
        return int(tx_hash[:8], 16)
    except (TypeError, ValueError):
        return 0


def bind_multisig_tx_id(chain: str, tx_hash: str, real_tx_id: int) -> bool:
    """Replace a hash-prefix placeholder tx_id with the real MultiSig id.

    The block watcher records pending before Submission logs are available, so
    execute events (which carry the real id) would otherwise never match.
    """
    if not tx_hash or real_tx_id is None:
        return False
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """SELECT tx_id FROM multisig_pending
               WHERE chain = %s AND LOWER(submission_tx_hash) = LOWER(%s)""",
            (chain, tx_hash),
        )
        row = cursor.fetchone()
        if not row:
            return False
        current_id = row[0]
        if current_id == real_tx_id:
            return False
        cursor.execute(
            "SELECT 1 FROM multisig_pending WHERE tx_id = %s AND chain = %s",
            (real_tx_id, chain),
        )
        if cursor.fetchone():
            logger.warning(
                f"Cannot bind MultiSig tx_id {real_tx_id} to {tx_hash[:16]}...; id already used"
            )
            return False
        cursor.execute(
            """UPDATE multisig_pending SET tx_id = %s
               WHERE chain = %s AND LOWER(submission_tx_hash) = LOWER(%s)""",
            (real_tx_id, chain, tx_hash),
        )
        conn.commit()
        logger.info(
            f"Bound MultiSig tx_id {current_id} -> {real_tx_id} for {tx_hash[:16]}..."
        )
        return True
    finally:
        conn.close()


def record_multisig_submission(
    chain: str,
    tx_hash: str,
    target_address: str,
    action: str,
    block_timestamp: Optional[int],
    multisig_tx_id: Optional[int] = None,
    token: str = "USDT",
) -> bool:
    """Persist a new MultiSig submission. Returns True if this row is new."""
    if not target_address or not tx_hash:
        return False
    conn = get_db()
    cursor = conn.cursor()
    try:
        if tx_hash:
            cursor.execute(
                """SELECT 1 FROM multisig_pending
                   WHERE chain = %s AND submission_tx_hash = %s""",
                (chain, tx_hash),
            )
            if cursor.fetchone():
                return False

        if multisig_tx_id is not None:
            cursor.execute(
                "SELECT 1 FROM multisig_pending WHERE tx_id = %s AND chain = %s",
                (multisig_tx_id, chain),
            )
            if cursor.fetchone():
                return False
        else:
            multisig_tx_id = _placeholder_tx_id(tx_hash)

        cursor.execute(
            f"""INSERT INTO multisig_pending
               (tx_id, chain, target_address, action, submission_tx_hash,
                submitted_at, submission_block_timestamp, alerted, token)
               VALUES (%s, %s, %s, %s, %s, {_NOW}, %s, 0, %s)
               ON CONFLICT DO NOTHING""",
            (multisig_tx_id, chain, target_address, action, tx_hash, block_timestamp, token),
        )
        if cursor.rowcount == 0:
            conn.commit()
            return False

        if action == "blacklist" and token == "USDT":
            cursor.execute(
                "SELECT status FROM blacklist WHERE address = %s AND chain = %s",
                (target_address, chain),
            )
            if not cursor.fetchone():
                cursor.execute(
                    f"""INSERT INTO blacklist
                       (address, chain, status, tx_hash, alerted, detected_at, block_timestamp)
                       VALUES (%s, %s, 'multisig_pending', %s, 1, {_NOW}, %s)
                       ON CONFLICT DO NOTHING""",
                    (target_address, chain, tx_hash, block_timestamp),
                )

        conn.commit()
        return True
    finally:
        conn.close()


def backfill_submission_block(
    chain: str,
    tx_hash: str,
    block_timestamp: Optional[int],
) -> bool:
    """Attach the real block timestamp to a row first seen in the mempool.

    Deliberately never touches detected_at: that column holds the *earliest*
    sighting, which is the whole point of pre-block detection. Only fills where
    the timestamp is still missing, so a block replay cannot rewrite history.
    """
    if not tx_hash or not block_timestamp:
        return False
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """UPDATE multisig_pending
               SET submission_block_timestamp = %s
               WHERE chain = %s AND submission_tx_hash = %s
                 AND (submission_block_timestamp IS NULL OR submission_block_timestamp = 0)""",
            (block_timestamp, chain, tx_hash),
        )
        touched = cursor.rowcount
        cursor.execute(
            """UPDATE blacklist
               SET block_timestamp = %s
               WHERE chain = %s AND tx_hash = %s
                 AND (block_timestamp IS NULL OR block_timestamp = 0)""",
            (block_timestamp, chain, tx_hash),
        )
        touched += cursor.rowcount
        conn.commit()
        return touched > 0
    finally:
        conn.close()


def reap_phantom_submissions(chain: str, max_age_s: int) -> list:
    """Drop mempool-only submissions that never made it into a block.

    A pending tx can be dropped or replaced, so a submission still carrying no
    block timestamp after max_age_s never happened on-chain. Returns the rows
    removed so the caller can log them.
    """
    # sqlite did this with datetime('now', '-N seconds'). submitted_at is text
    # in a lexicographically sortable format, so the cutoff is computed here and
    # compared as a string -- same result, no date parsing in SQL.
    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=int(max_age_s))
    ).strftime(_TS_FORMAT)[:-3]
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """SELECT tx_id, target_address, action, submission_tx_hash, token
               FROM multisig_pending
               WHERE chain = %s
                 AND executed = 0
                 AND (submission_block_timestamp IS NULL OR submission_block_timestamp = 0)
                 AND submitted_at < %s""",
            (chain, cutoff),
        )
        rows = [
            {"tx_id": r[0], "target_address": r[1], "action": r[2], "tx_hash": r[3], "token": r[4]}
            for r in cursor.fetchall()
        ]
        for r in rows:
            cursor.execute(
                "DELETE FROM multisig_pending WHERE tx_id = %s AND chain = %s",
                (r["tx_id"], chain),
            )
            if r["token"] != "USDT":
                continue  # never wrote a blacklist row
            # Only remove the blacklist row if it is still unconfirmed. If the
            # block watcher confirmed it by another tx hash, leave it alone.
            cursor.execute(
                """DELETE FROM blacklist
                   WHERE chain = %s AND address = %s AND status = 'multisig_pending'
                     AND (block_timestamp IS NULL OR block_timestamp = 0)""",
                (chain, r["target_address"]),
            )
        conn.commit()
        return rows
    finally:
        conn.close()


def mark_multisig_executed(
    chain: str,
    exec_tx_id: Optional[int] = None,
    target_address: Optional[str] = None,
    exec_tx_hash: str = "",
    confirmed_block_timestamp: Optional[int] = None,
) -> Optional[dict]:
    conn = get_db()
    cursor = conn.cursor()
    try:
        row = None
        if exec_tx_id is not None:
            cursor.execute(
                """SELECT target_address, action, submission_tx_hash, submission_block_timestamp, token
                   FROM multisig_pending
                   WHERE tx_id = %s AND chain = %s AND executed = 0""",
                (exec_tx_id, chain),
            )
            row = cursor.fetchone()

        if not row and target_address:
            cursor.execute(
                """SELECT target_address, action, submission_tx_hash, submission_block_timestamp, token, tx_id
                   FROM multisig_pending
                   WHERE target_address = %s AND chain = %s AND executed = 0
                   ORDER BY submitted_at DESC LIMIT 1""",
                (target_address, chain),
            )
            row = cursor.fetchone()
            if row:
                exec_tx_id = row[5]

        if not row:
            return None

        target_address, action, submission_tx_hash, submitted_ts, token = row[:5]
        cursor.execute(
            "ALTER TABLE multisig_pending ADD COLUMN IF NOT EXISTS executed_block_timestamp BIGINT"
        )
        cursor.execute(
            f"""UPDATE multisig_pending
               SET executed = 1, executed_at = {_NOW},
                   executed_block_timestamp = COALESCE(%s, executed_block_timestamp)
               WHERE tx_id = %s AND chain = %s""",
            (confirmed_block_timestamp, exec_tx_id, chain),
        )

        new_status = "confirmed" if action == "blacklist" else "unfrozen"
        if token == "USDT":  # XAUt has no blacklist row; see schema.sql
            cursor.execute(
                f"""UPDATE blacklist
                   SET status = %s, confirmed_at = {_NOW}, tx_hash = %s,
                       alerted = 1, confirmed_block_timestamp = %s
                   WHERE address = %s AND chain = %s""",
                (
                    new_status,
                    exec_tx_hash or submission_tx_hash,
                    confirmed_block_timestamp,
                    target_address,
                    chain,
                ),
            )
        conn.commit()
        return {
            "target_address": target_address,
            "action": action,
            "tx_hash": exec_tx_hash or submission_tx_hash,
            "submitted_ts": submitted_ts,
            "confirmed_block_timestamp": confirmed_block_timestamp,
            "token": token,
        }
    finally:
        conn.close()


def get_subscribers() -> List[Tuple[int]]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id FROM subscribers")
    rows = cursor.fetchall()
    conn.close()
    return rows


def add_subscriber(chat_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO subscribers (chat_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (chat_id,),
    )
    conn.commit()
    conn.close()


def remove_subscriber(chat_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM subscribers WHERE chat_id = %s", (chat_id,))
    conn.commit()
    conn.close()


def get_watchlist_subscribers(address: str, chain: str) -> List[Tuple[int]]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT chat_id FROM watchlist WHERE address = %s AND chain = %s",
        (address, chain),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_blacklist_entry(address: str, chain: str) -> Optional[Tuple]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status, detected_at, confirmed_at FROM blacklist WHERE address = %s AND chain = %s",
        (address, chain),
    )
    row = cursor.fetchone()
    conn.close()
    return row


def get_sync_state(key: str) -> Optional[str]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sync_state WHERE key = %s", (key,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def set_sync_state(key: str, value: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO sync_state (key, value) VALUES (%s, %s)
           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value""",
        (key, value),
    )
    conn.commit()
    conn.close()


def get_recent_blacklist(limit: int = 10) -> List[Tuple]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT address, chain, status, detected_at FROM blacklist ORDER BY detected_at DESC LIMIT %s",
        (limit,),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_confirmed_blacklist(chain: str, limit: int = 50) -> List[Tuple]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT address, chain, status, detected_at, tx_hash FROM blacklist
           WHERE chain = %s AND status = 'confirmed'
           ORDER BY detected_at DESC LIMIT %s""",
        (chain, limit),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_stats() -> dict:
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE status = 'confirmed'")
    confirmed = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE status = 'pending'")
    pending = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE status = 'mempool_pending'")
    mempool_pending = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE status = 'unfrozen'")
    unfrozen = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM subscribers")
    subscribers = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM blacklist WHERE chain = 'TRON' AND status = 'confirmed'"
    )
    tron_confirmed = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM blacklist WHERE chain = 'TRON' AND status = 'unfrozen'"
    )
    tron_unfrozen = cursor.fetchone()[0]

    cursor.execute(
        "SELECT COUNT(*) FROM blacklist WHERE chain = 'ETH' AND status = 'confirmed'"
    )
    eth_confirmed = cursor.fetchone()[0]

    cursor.execute("SELECT value FROM sync_state WHERE key = 'tron_last_scan'")
    tron_scan = cursor.fetchone()

    cursor.execute("SELECT value FROM sync_state WHERE key = 'eth_last_scan'")
    eth_scan = cursor.fetchone()

    conn.close()

    return {
        "confirmed": confirmed,
        "pending": pending,
        "mempool_pending": mempool_pending,
        "unfrozen": unfrozen,
        "subscribers": subscribers,
        "tron_confirmed": tron_confirmed,
        "tron_unfrozen": tron_unfrozen,
        "eth_confirmed": eth_confirmed,
        "tron_last_scan": tron_scan[0] if tron_scan else None,
        "eth_last_scan": eth_scan[0] if eth_scan else None,
    }


def get_pending_multisig(chain: str, limit: int = 10) -> List[Tuple]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """SELECT tx_id, target_address, action, submitted_at
           FROM multisig_pending
           WHERE executed = 0 AND chain = %s
           ORDER BY submitted_at DESC LIMIT %s""",
        (chain, limit),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def add_watchlist(chat_id: int, address: str, chain: str) -> int:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO watchlist (chat_id, address, chain) VALUES (%s, %s, %s)
           ON CONFLICT DO NOTHING""",
        (chat_id, address, chain),
    )
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM watchlist WHERE chat_id = %s", (chat_id,))
    count = cursor.fetchone()[0]
    conn.close()
    return count


def remove_watchlist(chat_id: int, address: str, chain: str) -> bool:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM watchlist WHERE chat_id = %s AND address = %s AND chain = %s",
        (chat_id, address, chain),
    )
    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def get_user_watchlist(chat_id: int) -> List[Tuple]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT address, chain, added_at FROM watchlist WHERE chat_id = %s ORDER BY added_at DESC",
        (chat_id,),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_multisig_submission_timestamp(address: str, chain: str) -> Optional[int]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT submission_block_timestamp FROM multisig_pending WHERE target_address = %s AND chain = %s",
        (address, chain),
    )
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None
