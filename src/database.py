import sqlite3
from typing import Optional, List, Tuple

from .config import DB_PATH, logger


def get_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH, timeout=5.0)


def migrate_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(blacklist)")
    columns = [col[1] for col in cursor.fetchall()]

    if "chain" not in columns:
        logger.info("Migrating database to multi-chain schema...")
        cursor.execute("ALTER TABLE blacklist ADD COLUMN chain TEXT DEFAULT 'TRON'")

        cursor.execute(
            "SELECT key, value FROM sync_state WHERE key IN ('initialized', 'last_scan', 'last_block_timestamp')"
        )
        rows = cursor.fetchall()
        for key, value in rows:
            cursor.execute(
                "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
                (f"tron_{key}", value),
            )
            cursor.execute("DELETE FROM sync_state WHERE key = ?", (key,))

        conn.commit()
        logger.info("Database migration complete.")

    if "block_timestamp" not in columns:
        logger.info("Adding block_timestamp column...")
        cursor.execute("ALTER TABLE blacklist ADD COLUMN block_timestamp INTEGER")
        cursor.execute("ALTER TABLE blacklist ADD COLUMN confirmed_block_timestamp INTEGER")
        conn.commit()
        logger.info("Block timestamp columns added.")

    conn.close()


def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id INTEGER PRIMARY KEY,
            subscribed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blacklist (
            address TEXT NOT NULL,
            chain TEXT NOT NULL DEFAULT 'TRON',
            status TEXT NOT NULL DEFAULT 'pending',
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
            confirmed_at TEXT,
            tx_hash TEXT,
            alerted INTEGER DEFAULT 0,
            block_timestamp INTEGER,
            confirmed_block_timestamp INTEGER,
            PRIMARY KEY (address, chain)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            chat_id INTEGER NOT NULL,
            address TEXT NOT NULL,
            chain TEXT NOT NULL,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chat_id, address, chain)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS multisig_pending (
            tx_id INTEGER NOT NULL,
            chain TEXT NOT NULL DEFAULT 'TRON',
            target_address TEXT NOT NULL,
            action TEXT NOT NULL,
            submission_tx_hash TEXT,
            submitted_at TEXT DEFAULT CURRENT_TIMESTAMP,
            submission_block_timestamp INTEGER,
            executed INTEGER DEFAULT 0,
            executed_at TEXT,
            alerted INTEGER DEFAULT 0,
            PRIMARY KEY (tx_id, chain)
        )
    """)

    try:
        cursor.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_multisig_pending_hash
               ON multisig_pending(chain, submission_tx_hash)
               WHERE submission_tx_hash IS NOT NULL AND submission_tx_hash != ''"""
        )
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()
    migrate_db()


def _placeholder_tx_id(tx_hash: str) -> int:
    try:
        return int(tx_hash[:8], 16)
    except (TypeError, ValueError):
        return 0


def record_multisig_submission(
    chain: str,
    tx_hash: str,
    target_address: str,
    action: str,
    block_timestamp: Optional[int],
    multisig_tx_id: Optional[int] = None,
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
                   WHERE chain = ? AND submission_tx_hash = ?""",
                (chain, tx_hash),
            )
            if cursor.fetchone():
                return False

        if multisig_tx_id is not None:
            cursor.execute(
                "SELECT 1 FROM multisig_pending WHERE tx_id = ? AND chain = ?",
                (multisig_tx_id, chain),
            )
            if cursor.fetchone():
                return False
        else:
            multisig_tx_id = _placeholder_tx_id(tx_hash)

        cursor.execute(
            """INSERT OR IGNORE INTO multisig_pending
               (tx_id, chain, target_address, action, submission_tx_hash,
                submitted_at, submission_block_timestamp, alerted)
               VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 0)""",
            (multisig_tx_id, chain, target_address, action, tx_hash, block_timestamp),
        )
        if cursor.rowcount == 0:
            conn.commit()
            return False

        if action == "blacklist":
            cursor.execute(
                "SELECT status FROM blacklist WHERE address = ? AND chain = ?",
                (target_address, chain),
            )
            if not cursor.fetchone():
                cursor.execute(
                    """INSERT INTO blacklist
                       (address, chain, status, tx_hash, alerted, detected_at, block_timestamp)
                       VALUES (?, ?, 'multisig_pending', ?, 1, CURRENT_TIMESTAMP, ?)""",
                    (target_address, chain, tx_hash, block_timestamp),
                )

        conn.commit()
        return True
    finally:
        conn.close()


def mark_multisig_executed(
    chain: str,
    exec_tx_id: Optional[int] = None,
    target_address: Optional[str] = None,
    exec_tx_hash: str = "",
) -> Optional[dict]:
    conn = get_db()
    cursor = conn.cursor()
    try:
        row = None
        if exec_tx_id is not None:
            cursor.execute(
                """SELECT target_address, action, submission_tx_hash, submission_block_timestamp
                   FROM multisig_pending
                   WHERE tx_id = ? AND chain = ? AND executed = 0""",
                (exec_tx_id, chain),
            )
            row = cursor.fetchone()

        if not row and target_address:
            cursor.execute(
                """SELECT target_address, action, submission_tx_hash, submission_block_timestamp, tx_id
                   FROM multisig_pending
                   WHERE target_address = ? AND chain = ? AND executed = 0
                   ORDER BY submitted_at DESC LIMIT 1""",
                (target_address, chain),
            )
            row = cursor.fetchone()
            if row:
                exec_tx_id = row[4]

        if not row:
            return None

        target_address, action, submission_tx_hash, submitted_ts = row[:4]
        cursor.execute(
            """UPDATE multisig_pending
               SET executed = 1, executed_at = CURRENT_TIMESTAMP
               WHERE tx_id = ? AND chain = ?""",
            (exec_tx_id, chain),
        )

        new_status = "confirmed" if action == "blacklist" else "unfrozen"
        cursor.execute(
            """UPDATE blacklist
               SET status = ?, confirmed_at = CURRENT_TIMESTAMP, tx_hash = ?, alerted = 1
               WHERE address = ? AND chain = ?""",
            (new_status, exec_tx_hash or submission_tx_hash, target_address, chain),
        )
        conn.commit()
        return {
            "target_address": target_address,
            "action": action,
            "tx_hash": exec_tx_hash or submission_tx_hash,
            "submitted_ts": submitted_ts,
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
    cursor.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,))
    conn.commit()
    conn.close()


def remove_subscriber(chat_id: int):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()


def get_watchlist_subscribers(address: str, chain: str) -> List[Tuple[int]]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT chat_id FROM watchlist WHERE address = ? AND chain = ?",
        (address, chain),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_blacklist_entry(address: str, chain: str) -> Optional[Tuple]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT status, detected_at, confirmed_at FROM blacklist WHERE address = ? AND chain = ?",
        (address, chain),
    )
    row = cursor.fetchone()
    conn.close()
    return row


def get_sync_state(key: str) -> Optional[str]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sync_state WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None


def set_sync_state(key: str, value: str):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()
    conn.close()


def get_recent_blacklist(limit: int = 10) -> List[Tuple]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT address, chain, status, detected_at FROM blacklist ORDER BY detected_at DESC LIMIT ?",
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
           WHERE chain = ? AND status = 'confirmed'
           ORDER BY detected_at DESC LIMIT ?""",
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

    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE chain = 'TRON' AND status = 'confirmed'")
    tron_confirmed = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE chain = 'TRON' AND status = 'unfrozen'")
    tron_unfrozen = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM blacklist WHERE chain = 'ETH' AND status = 'confirmed'")
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
           WHERE executed = 0 AND chain = ?
           ORDER BY submitted_at DESC LIMIT ?""",
        (chain, limit),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def add_watchlist(chat_id: int, address: str, chain: str) -> int:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO watchlist (chat_id, address, chain) VALUES (?, ?, ?)",
        (chat_id, address, chain),
    )
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM watchlist WHERE chat_id = ?", (chat_id,))
    count = cursor.fetchone()[0]
    conn.close()
    return count


def remove_watchlist(chat_id: int, address: str, chain: str) -> bool:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM watchlist WHERE chat_id = ? AND address = ? AND chain = ?",
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
        "SELECT address, chain, added_at FROM watchlist WHERE chat_id = ? ORDER BY added_at DESC",
        (chat_id,),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_multisig_submission_timestamp(address: str, chain: str) -> Optional[int]:
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT submission_block_timestamp FROM multisig_pending WHERE target_address = ? AND chain = ?",
        (address, chain),
    )
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else None
