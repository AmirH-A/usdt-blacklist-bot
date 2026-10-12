-- Freeze detector schema, ported from blacklist_tracker.db.
--
-- Two deliberate choices:
--   * Timestamps stay TEXT in the exact sqlite format
--     (strftime('%Y-%m-%d %H:%M:%f') -> to_char(..., 'YYYY-MM-DD HH24:MI:SS.MS')).
--     accuracy.py parses these strings and compares them lexicographically;
--     switching to timestamptz would silently change those comparisons.
--   * Every id is BIGINT, not INTEGER. sqlite INTEGER is 64-bit, and both
--     Telegram chat_ids and the hash-prefix placeholder tx_ids
--     (int(tx_hash[:8], 16), up to 4294967295) overflow postgres int4.

CREATE TABLE IF NOT EXISTS subscribers (
    chat_id       BIGINT PRIMARY KEY,
    subscribed_at TEXT DEFAULT to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD HH24:MI:SS.MS')
);

CREATE TABLE IF NOT EXISTS blacklist (
    address                   TEXT NOT NULL,
    chain                     TEXT NOT NULL DEFAULT 'TRON',
    status                    TEXT NOT NULL DEFAULT 'pending',
    detected_at               TEXT DEFAULT to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD HH24:MI:SS.MS'),
    confirmed_at              TEXT,
    tx_hash                   TEXT,
    alerted                   SMALLINT DEFAULT 0,
    block_timestamp           BIGINT,
    confirmed_block_timestamp BIGINT,
    PRIMARY KEY (address, chain)
);

CREATE TABLE IF NOT EXISTS sync_state (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS watchlist (
    chat_id  BIGINT NOT NULL,
    address  TEXT NOT NULL,
    chain    TEXT NOT NULL,
    added_at TEXT DEFAULT to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD HH24:MI:SS.MS'),
    PRIMARY KEY (chat_id, address, chain)
);

CREATE TABLE IF NOT EXISTS multisig_pending (
    tx_id                      BIGINT NOT NULL,
    chain                      TEXT NOT NULL DEFAULT 'TRON',
    target_address             TEXT NOT NULL,
    action                     TEXT NOT NULL,
    submission_tx_hash         TEXT,
    submitted_at               TEXT DEFAULT to_char(now() AT TIME ZONE 'utc', 'YYYY-MM-DD HH24:MI:SS.MS'),
    submission_block_timestamp BIGINT,
    executed                   SMALLINT DEFAULT 0,
    executed_at                TEXT,
    alerted                    SMALLINT DEFAULT 0,
    PRIMARY KEY (tx_id, chain)
);

-- record_multisig_submission dedupes on the submission hash, and
-- bind_multisig_tx_id rewrites tx_id in place; this partial unique index is
-- what makes both safe under concurrent writers.
CREATE UNIQUE INDEX IF NOT EXISTS idx_multisig_pending_hash
    ON multisig_pending (chain, submission_tx_hash)
    WHERE submission_tx_hash IS NOT NULL AND submission_tx_hash != '';

-- The hot read paths: "newest first" listings and the per-chain status counts.
-- ponytail: only these two. 4.5k rows needs no more; add when a query plan says so.
CREATE INDEX IF NOT EXISTS idx_blacklist_detected_at ON blacklist (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_multisig_submitted_at ON multisig_pending (submitted_at DESC);
