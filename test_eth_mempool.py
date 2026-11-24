"""Self-check for ETH pre-block (mempool) detection.

Run: python3 test_eth_mempool.py

Covers the logic that actually breaks: calldata decoding, the wrong-target
rejection that made the previous version dead code, and the DB lifecycle where
a mempool sighting must keep its early detected_at when the block lands.
"""

import os
import sqlite3
import tempfile

os.environ.setdefault("BOT_TOKEN", "test")
_tmp = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_tmp, "t.db")

from src.config import ETH_MULTISIG_WALLET, ETH_USDT_CONTRACT  # noqa: E402
from src.eth.mempool import extract_mempool_ops  # noqa: E402
from src import database  # noqa: E402


def _submit_calldata(target: str, action_selector: str, destination: str) -> str:
    """Build submitTransaction(address,uint256,bytes) the way Tether's multisig does."""
    dest = destination.lower().replace("0x", "").rjust(64, "0")
    value = "0" * 64
    data_offset = "%064x" % 96  # three head words
    inner = action_selector + target.lower().replace("0x", "").rjust(64, "0")
    inner_len = "%064x" % (len(inner) // 2)
    return "0x" + "c6427474" + dest + value + data_offset + inner_len + inner.ljust(128, "0")


TARGET = "0x1111111111111111111111111111111111111111"
ADD = "0ecb93c0"
REMOVE = "e4997dc5"


def test_decodes_blacklist_submission():
    tx = {
        "to": ETH_MULTISIG_WALLET,
        "hash": "0xaaa1",
        "input": _submit_calldata(TARGET, ADD, ETH_USDT_CONTRACT),
    }
    ops = extract_mempool_ops(tx)
    assert len(ops) == 1, ops
    op = ops[0]
    assert op["kind"] == "submission"
    assert op["action"] == "blacklist"
    assert op["target_address"] == TARGET
    # No block exists yet -- this is what the backfill later fills in.
    assert op["block_timestamp"] is None
    assert op["block_number"] is None


def test_decodes_unblacklist():
    tx = {
        "to": ETH_MULTISIG_WALLET.lower(),
        "hash": "0xaaa2",
        "input": _submit_calldata(TARGET, REMOVE, ETH_USDT_CONTRACT),
    }
    ops = extract_mempool_ops(tx)
    assert len(ops) == 1 and ops[0]["action"] == "unblacklist", ops


def test_ignores_tx_to_usdt_directly():
    """The bug in the old version: it watched USDT, not the multisig.

    Tether blacklists via the multisig, so no top-level tx has to=USDT with an
    addBlacklist selector. Watching USDT catches nothing.
    """
    tx = {
        "to": ETH_USDT_CONTRACT,
        "hash": "0xaaa3",
        "input": "0x" + ADD + TARGET.replace("0x", "").rjust(64, "0"),
    }
    assert extract_mempool_ops(tx) == []


def test_ignores_unrelated_multisig_traffic():
    # confirmTransaction to the multisig: real, but not a submission. Execution
    # must stay block-only, so the mempool path drops it.
    tx = {"to": ETH_MULTISIG_WALLET, "hash": "0xaaa4", "input": "0xc01a8c84" + "%064x" % 7}
    assert extract_mempool_ops(tx) == []
    # submitTransaction whose destination is not USDT
    tx = {
        "to": ETH_MULTISIG_WALLET,
        "hash": "0xaaa5",
        "input": _submit_calldata(TARGET, ADD, "0x9999999999999999999999999999999999999999"),
    }
    assert extract_mempool_ops(tx) == []
    assert extract_mempool_ops({"to": None, "hash": "0x1", "input": "0x"}) == []
    assert extract_mempool_ops({"to": ETH_MULTISIG_WALLET, "hash": "", "input": "0x"}) == []


def test_backfill_preserves_early_detected_at():
    """The core guarantee: the block must not overwrite the mempool sighting."""
    database.init_db()
    assert database.record_multisig_submission(
        chain="ETH", tx_hash="0xbbb1", target_address=TARGET,
        action="blacklist", block_timestamp=None, multisig_tx_id=901,
    )
    conn = sqlite3.connect(os.environ["DB_PATH"])
    before = conn.execute(
        "SELECT detected_at, block_timestamp FROM blacklist WHERE address=?", (TARGET,)
    ).fetchone()
    conn.close()
    assert before[0], "mempool sighting must stamp detected_at"
    assert not before[1], "no block timestamp yet"

    # Block lands 2.4s later (ETH propagation floor), block ts is *earlier* than
    # our sighting because we saw it pre-block.
    block_ts = 1790000000000
    assert database.backfill_submission_block("ETH", "0xbbb1", block_ts)

    conn = sqlite3.connect(os.environ["DB_PATH"])
    after = conn.execute(
        "SELECT detected_at, block_timestamp FROM blacklist WHERE address=?", (TARGET,)
    ).fetchone()
    conn.close()
    assert after[1] == block_ts, "block timestamp must be filled"
    assert after[0] == before[0], "detected_at must NOT move -- that is the lead"

    # Idempotent: a block replay must not rewrite an already-filled timestamp.
    assert not database.backfill_submission_block("ETH", "0xbbb1", 1790000009999)
    conn = sqlite3.connect(os.environ["DB_PATH"])
    assert conn.execute(
        "SELECT block_timestamp FROM blacklist WHERE address=?", (TARGET,)
    ).fetchone()[0] == block_ts
    conn.close()


def test_reaper_drops_only_phantoms():
    database.init_db()
    mined, phantom = "0x2222222222222222222222222222222222222222", "0x3333333333333333333333333333333333333333"
    database.record_multisig_submission(
        chain="ETH", tx_hash="0xccc1", target_address=mined,
        action="blacklist", block_timestamp=1790000000000, multisig_tx_id=902,
    )
    database.record_multisig_submission(
        chain="ETH", tx_hash="0xccc2", target_address=phantom,
        action="blacklist", block_timestamp=None, multisig_tx_id=903,
    )
    # Age the phantom past the window.
    conn = sqlite3.connect(os.environ["DB_PATH"])
    conn.execute(
        "UPDATE multisig_pending SET submitted_at = datetime('now','-3600 seconds') WHERE tx_id=903"
    )
    conn.commit()
    conn.close()

    dropped = database.reap_phantom_submissions("ETH", 900)
    assert [d["target_address"] for d in dropped] == [phantom], dropped

    conn = sqlite3.connect(os.environ["DB_PATH"])
    left = {r[0] for r in conn.execute("SELECT address FROM blacklist").fetchall()}
    conn.close()
    assert phantom not in left, "phantom must be removed"
    assert mined in left, "a mined submission must survive the reaper"


def test_timestamps_have_millisecond_resolution():
    """Whole-second timestamps quantised every delay to an integer.

    ETH read exactly 2.0/3.0/4.0 not because it was stable but because
    CURRENT_TIMESTAMP truncates. Sub-second detection needs ms to be visible.
    """
    database.init_db()
    addr = "0x4444444444444444444444444444444444444444"
    database.record_multisig_submission(
        chain="ETH", tx_hash="0xddd1", target_address=addr,
        action="blacklist", block_timestamp=None, multisig_tx_id=904,
    )
    conn = sqlite3.connect(os.environ["DB_PATH"])
    got = conn.execute(
        "SELECT detected_at FROM blacklist WHERE address=?", (addr,)
    ).fetchone()[0]
    sub = conn.execute(
        "SELECT submitted_at FROM multisig_pending WHERE tx_id=904"
    ).fetchone()[0]
    conn.close()
    assert "." in got, f"detected_at lost sub-second precision: {got}"
    assert len(got.split(".")[1]) >= 3, f"expected ms, got {got}"
    assert "." in sub, f"submitted_at lost sub-second precision: {sub}"

    # And the parser the panel relies on must still read it.
    from src.accuracy import _parse_dt
    assert _parse_dt(got) is not None, f"_parse_dt cannot read {got}"


def test_race_between_sources_acts_once():
    """Two providers see the same tx; the loser must be a no-op, not a dupe."""
    from src.eth.mempool import _mark_seen, _set_connected
    from src import health

    health.eth_seen_pending.clear()
    h = "0xRACE" + "0" * 10
    assert _mark_seen(h) is True, "first source wins"
    assert _mark_seen(h) is False, "second source must drop it"
    assert _mark_seen(h.upper()) is False, "case must not defeat dedupe"

    # Per-source liveness: one down must not report the pair down.
    health.eth_mempool_sources.clear()
    _set_connected("a.example", True)
    _set_connected("b.example", True)
    assert health.eth_mempool_connected is True
    _set_connected("a.example", False)
    assert health.eth_mempool_connected is True, "b still up"
    _set_connected("b.example", False)
    assert health.eth_mempool_connected is False, "both down"


def test_delay_metric_handles_pre_block_detection():
    from src.accuracy import _lead_s, _sys_delay_from_columns

    # Saw it 2.4s before the block timestamp.
    onchain, saved = "2026-09-27 07:13:00", "2026-09-27 07:12:57.6"
    assert _sys_delay_from_columns(onchain, saved) == 0.0, "pre-block clamps to 0, not None"
    assert _lead_s(onchain, saved) == 2.4
    # Normal block-path detection is unchanged.
    assert _sys_delay_from_columns("2026-09-27 07:13:00", "2026-09-27 07:13:04") == 4.0
    assert _lead_s("2026-09-27 07:13:00", "2026-09-27 07:13:04") is None
    # Garbage still rejected.
    assert _sys_delay_from_columns("2020-01-01 00:00:00", "2026-09-27 07:13:04") is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok  {name}")
    print("\nall checks passed")
