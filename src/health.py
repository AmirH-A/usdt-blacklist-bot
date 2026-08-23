import asyncio

trongrid_semaphore = asyncio.Semaphore(3)
trongrid_last_request = 0

trongrid_mempool_semaphore = asyncio.Semaphore(3)
trongrid_mempool_last_request = 0

tron_consecutive_failures = 0
eth_consecutive_failures = 0
eth_mempool_consecutive_failures = 0
mempool_consecutive_failures = 0
multisig_consecutive_failures = 0
eth_multisig_consecutive_failures = 0

tron_scan_count = 0
eth_scan_count = 0
eth_mempool_scan_count = 0
mempool_scan_count = 0
multisig_scan_count = 0
eth_multisig_scan_count = 0

total_synced_count = 0
total_missed_count = 0
detection_accuracy = 100.0

MAX_SEEN_CACHE = 10000
seen_mempool_txs: set = set()
eth_seen_pending: set = set()
