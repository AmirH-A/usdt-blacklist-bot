"""Self-check for XAUt (Tether Gold) freeze detection on ETH.

Run: python3 test_xaut.py
DB part needs a throwaway Postgres:
     FD_TEST_DATABASE_URL=postgres://... python3 test_xaut.py

The fixture is real: MultiSig tx #5795 proposed addToBlockedList on XAUt and
was confirmed in block 26301003 (0x2d3c3452...).
"""

import os

os.environ.setdefault("BOT_TOKEN", "123456:test")
os.environ["DATABASE_URL"] = os.environ.get("FD_TEST_DATABASE_URL", "")

from src.config import ETH_USDT_CONTRACT, ETH_XAUT_CONTRACT  # noqa: E402
from src.eth.multisig import decode_eth_submit_input  # noqa: E402
from src.bot.alerts import _pending_message, _confirmed_message  # noqa: E402

TARGET = "0x5227f603c1ee34ce3c06efe6477003bdfccb2f8c"


def _submit(destination: str, selector: str, target: str = TARGET) -> str:
    """submitTransaction(address,uint256,bytes), as Tether's MultiSig sends it."""
    dest = destination.lower()[2:].rjust(64, "0")
    inner = selector + target[2:].rjust(64, "0")
    return ("0xc6427474" + dest + "0" * 64 + "%064x" % 96
            + "%064x" % (len(inner) // 2) + inner.ljust(128, "0"))


def test_decode():
    # The real mainnet proposal.
    assert decode_eth_submit_input(_submit(ETH_XAUT_CONTRACT, "3c7c9b90")) == {
        "target_address": TARGET, "action": "blacklist", "token": "XAUt"}
    assert decode_eth_submit_input(_submit(ETH_XAUT_CONTRACT, "1a14f449"))["action"] == "unblacklist"
    # USDT unchanged.
    assert decode_eth_submit_input(_submit(ETH_USDT_CONTRACT, "0ecb93c0")) == {
        "target_address": TARGET, "action": "blacklist", "token": "USDT"}
    # Selectors don't cross: XAUt has no addBlackList, USDT no addToBlockedList.
    assert decode_eth_submit_input(_submit(ETH_XAUT_CONTRACT, "0ecb93c0")) is None
    assert decode_eth_submit_input(_submit(ETH_USDT_CONTRACT, "3c7c9b90")) is None
    assert decode_eth_submit_input(_submit("0x" + "22" * 20, "3c7c9b90")) is None


def test_messages():
    usdt = _pending_message(TARGET, "blacklist", 1, "0xab", "ETH", None, None)
    assert "*BLACKLIST PENDING* [ETH]\n" in usdt  # parsers depend on this
    xaut = _pending_message(TARGET, "blacklist", 1, "0xab", "ETH", 2.5, None, "XAUt")
    assert "[ETH · XAUt]" in xaut and "2.50 XAUt" in xaut
    done = _confirmed_message(TARGET, "0xab", "ETH", None, 12.0, True, token="XAUt")
    assert "BLOCK CONFIRMED [ETH · XAUt]" in done and "XAUt transfers frozen" in done


def test_db():
    from src import database
    database.init_db()
    conn = database.get_db(); c = conn.cursor()
    c.execute("TRUNCATE multisig_pending, blacklist"); conn.commit(); conn.close()

    def status():
        conn = database.get_db(); c = conn.cursor()
        c.execute("SELECT status FROM blacklist WHERE address = %s", (TARGET,))
        row = c.fetchone(); conn.close()
        return row and row[0]

    # Address is USDT-frozen.
    assert database.record_multisig_submission("ETH", "0xu1", TARGET, "blacklist", 1, 100)
    assert database.mark_multisig_executed("ETH", exec_tx_id=100)["token"] == "USDT"
    assert status() == "confirmed"

    # XAUt freeze + XAUt unblock of the same address must not touch the USDT state.
    assert database.record_multisig_submission("ETH", "0xx1", TARGET, "blacklist", 2, 101, token="XAUt")
    assert database.mark_multisig_executed("ETH", exec_tx_id=101)["token"] == "XAUt"
    assert database.record_multisig_submission("ETH", "0xx2", TARGET, "unblacklist", 3, 102, token="XAUt")
    marked = database.mark_multisig_executed("ETH", exec_tx_id=102)
    assert marked["token"] == "XAUt" and marked["action"] == "unblacklist"
    assert status() == "confirmed", status()

    # An XAUt mempool phantom must not reap a USDT pending row.
    other = "0x" + "33" * 20
    database.record_multisig_submission("ETH", "0xu2", other, "blacklist", None, 103)
    database.record_multisig_submission("ETH", "0xx3", other, "blacklist", None, 104, token="XAUt")
    conn = database.get_db(); c = conn.cursor()
    c.execute("UPDATE multisig_pending SET submitted_at = '2000-01-01 00:00:00.000' WHERE tx_id = 104")
    conn.commit(); conn.close()
    assert [r["tx_id"] for r in database.reap_phantom_submissions("ETH", 60)] == [104]
    conn = database.get_db(); c = conn.cursor()
    c.execute("SELECT status FROM blacklist WHERE address = %s", (other,))
    assert c.fetchone()[0] == "multisig_pending"
    conn.close()


if __name__ == "__main__":
    test_decode()
    test_messages()
    if os.environ["DATABASE_URL"]:
        test_db()
    else:
        print("skipped DB part (FD_TEST_DATABASE_URL unset)")
    print("ok")
