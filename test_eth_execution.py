"""Self-check for ETH freeze execution and fallback lookups.

Run: python3 test_eth_execution.py

Tether's ETH Multisig needs 3 confirmations. The block watcher used to treat
every confirmTransaction as the execution, so the 2nd owner's confirm marked a
freeze executed before it happened. And the HTTP fallback dropped submissions
for good when Etherscan rate-limited the target lookup.
"""

import asyncio
import os

os.environ.setdefault("BOT_TOKEN", "test")

from src.config import ETH_MULTISIG_WALLET, ETH_EXECUTION_TOPIC  # noqa: E402
from src.eth import block_watcher, multisig  # noqa: E402

MS = ETH_MULTISIG_WALLET.lower()
CONFIRM_5840 = "0xc01a8c84" + "%064x" % 5840


def _block(logs):
    async def fake_logs(address, topic0, block_num):
        return [l for l in logs if l["topics"][0] == topic0]

    block_watcher.get_eth_logs = fake_logs
    return {
        "block_number": 26386400,
        "timestamp": 1794336431000,
        "transactions": [{"to": MS, "tx_id": "0xconfirm", "data": CONFIRM_5840}],
    }


def test_confirm_without_execution_log_is_not_execution():
    ops = asyncio.run(block_watcher.ops_from_eth_block(_block([])))
    assert ops == [], ops


def test_execution_comes_from_log():
    log = {"transactionHash": "0xconfirm", "topics": [ETH_EXECUTION_TOPIC, "0x%064x" % 5840]}
    ops = asyncio.run(block_watcher.ops_from_eth_block(_block([log])))
    assert [(o["kind"], o["multisig_tx_id"], o["block_timestamp"]) for o in ops] == [
        ("execution", 5840, 1794336431000)
    ], ops


def test_rate_limited_lookup_raises():
    async def no_rpc(method, params):
        return None

    class Resp:
        def json(self):
            return {"status": "0", "message": "NOTOK", "result": "Max calls per sec rate limit reached (5/sec)"}

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, *a, **k):
            return Resp()

    multisig.rpc_call = no_rpc
    multisig.httpx.AsyncClient = Client
    try:
        asyncio.run(multisig.get_eth_multisig_tx_target("0xabc"))
    except LookupError:
        return
    raise AssertionError("rate-limited lookup must raise, not return None")


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
