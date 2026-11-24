"""Self-check for panel event pairing.

Reproduces the reported bug: a Sep 25 event displaying On-chain 2026-08-27 with
a fake 1s Sys delay, because the same address had 4 submissions and the index
collapsed them to the oldest.

Run: python3 test_accuracy_pairing.py
"""

import os
import sqlite3
import tempfile
from datetime import datetime, timezone

os.environ.setdefault("BOT_TOKEN", "test")
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "acc.db")

from src.accuracy import (  # noqa: E402
    _build_system_detected_index,
    _fill_missing_timing,
    _resolve_system_detected,
    _sys_delay_from_columns,
)

ADDR = "TFB5wGzo8T3oWW"


def _db():
    """Bot DB shaped like prod, with the real 4-submission history."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE multisig_pending (
            tx_id INTEGER, chain TEXT, target_address TEXT, action TEXT,
            submission_tx_hash TEXT, submitted_at TEXT,
            submission_block_timestamp INTEGER, executed INTEGER DEFAULT 0,
            token TEXT NOT NULL DEFAULT 'USDT'
        );
        CREATE TABLE blacklist (
            address TEXT, chain TEXT, status TEXT, tx_hash TEXT,
            detected_at TEXT, block_timestamp INTEGER,
            confirmed_at TEXT, confirmed_block_timestamp INTEGER
        );
        """
    )
    # ms epochs for the four real submissions
    aug27 = 1787836413000       # 2026-08-27 13:13:33
    sep25_1149 = 1790337346000  # 2026-09-25 11:49:06
    sep25_1221 = 1790339277000  # 2026-09-25 12:21:57
    sep25_1254 = 1790341215000  # 2026-09-25 12:54:15
    rows = [
        (10580, "TRON", ADDR, "blacklist", "0xaug27", "2026-08-27 13:13:34", aug27, 1),
        (11108, "TRON", ADDR, "blacklist", "0xsep1149", "2026-09-25 11:49:06", sep25_1149, 1),
        (11112, "TRON", ADDR, "blacklist", "0xsep1221", "2026-09-25 12:21:57", sep25_1221, 1),
        (11114, "TRON", ADDR, "unblacklist", "0xsep1254", "2026-09-25 12:54:15", sep25_1254, 1),
    ]
    conn.executemany(
        "INSERT INTO multisig_pending (tx_id, chain, target_address, action, "
        "submission_tx_hash, submitted_at, submission_block_timestamp, executed) "
        "VALUES (?,?,?,?,?,?,?,?)", rows
    )
    # prod carries this as 'unfrozen', which the old index dropped entirely
    conn.execute(
        "INSERT INTO blacklist VALUES (?,?,?,?,?,?,?,?)",
        (ADDR, "TRON", "unfrozen", "0xsep1254", "2026-08-27 13:13:34", aug27,
         "2026-09-25 12:55:33", 1790341333000),
    )
    conn.commit()
    return conn


def test_september_event_does_not_inherit_august():
    """The actual reported symptom."""
    by_key, by_hash = _build_system_detected_index(_db())
    ev = {
        "address": ADDR, "chain": "TRON", "status": "pending",
        "onchain_time": "2026-09-25 12:21:57", "tx_hash": "",  # no hash -> fuzzy path
    }
    sys = _resolve_system_detected(ev, by_key, by_hash)
    saved = sys.get("system_detected_at") or ""
    assert saved.startswith("2026-09-25"), f"inherited the wrong submission: {saved}"
    assert "2026-08-27" not in saved, f"August leaked into a September event: {saved}"
    assert sys.get("system_match") == "address-nearest", sys.get("system_match")


def test_each_submission_resolves_to_its_own():
    by_key, by_hash = _build_system_detected_index(_db())
    for onchain, expect in (
        ("2026-08-27 13:13:33", "2026-08-27"),
        ("2026-09-25 11:49:06", "2026-09-25 11:49"),
        ("2026-09-25 12:21:57", "2026-09-25 12:21"),
    ):
        ev = {"address": ADDR, "chain": "TRON", "status": "pending",
              "onchain_time": onchain, "tx_hash": ""}
        got = _resolve_system_detected(ev, by_key, by_hash).get("system_detected_at") or ""
        assert got.startswith(expect), f"onchain {onchain} -> {got}, wanted {expect}*"


def test_hash_match_still_wins():
    by_key, by_hash = _build_system_detected_index(_db())
    ev = {"address": ADDR, "chain": "TRON", "status": "pending",
          "onchain_time": "2026-08-27 13:13:33", "tx_hash": "0xsep1149"}
    sys = _resolve_system_detected(ev, by_key, by_hash)
    assert sys.get("system_match") == "hash", sys
    assert (sys.get("system_detected_at") or "").startswith("2026-09-25 11:49")


def test_unfrozen_row_stays_out_of_confirmed():
    """Deliberately NOT indexed: blacklist has PRIMARY KEY (address, chain).

    One row per address cannot represent four re-blacklists, so its single
    'unfrozen' status would attach to every confirmed event of that address
    regardless of date -- which produced a bogus 1785s delay where the panel
    had honestly shown "-". Per-event confirmation must come from
    multisig_pending.executed_at instead. An empty result is the correct
    answer until that lands.
    """
    by_key, by_hash = _build_system_detected_index(_db())
    ev = {"address": ADDR, "chain": "TRON", "status": "confirmed",
          "onchain_time": "2026-09-25 12:25:48", "tx_hash": ""}
    sys = _resolve_system_detected(ev, by_key, by_hash)
    assert not sys.get("system_detected_at"), (
        f"a single blacklist row must not be paired to a dated confirmed event: {sys}"
    )


def test_blank_unblacklist_row_gets_onchain_and_alert_delay():
    """Sheet row for UNBLACKLIST PENDING has no hash and no on-chain time."""
    block = datetime(2026, 9, 25, 12, 54, 15, tzinfo=timezone.utc)
    sys = {
        "system_detected_at": "2026-09-25 12:54:15",
        "system_onchain_ms": int(block.timestamp() * 1000),
        "system_match": "address-newest",
    }
    onchain, delay = _fill_missing_timing(None, None, "2026-09-25 12:54:16", sys)
    assert onchain == "2026-09-25 12:54:15", onchain
    assert delay == 1.0, delay
    kept, kept_delay = _fill_missing_timing(
        "2026-08-27 13:13:33", 4.0, "2026-09-25 12:54:16", sys
    )
    assert kept == "2026-08-27 13:13:33"
    assert kept_delay == 4.0


def test_delay_is_sane_after_pairing():
    """The bogus '1s delay, one month out of place' must not be computable."""
    by_key, by_hash = _build_system_detected_index(_db())
    ev = {"address": ADDR, "chain": "TRON", "status": "pending",
          "onchain_time": "2026-09-25 12:21:57", "tx_hash": ""}
    sys = _resolve_system_detected(ev, by_key, by_hash)
    delay = _sys_delay_from_columns(ev["onchain_time"], sys.get("system_detected_at"))
    assert delay is not None and 0 <= delay <= 5, f"delay {delay} is not plausible"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("\nall checks passed")
