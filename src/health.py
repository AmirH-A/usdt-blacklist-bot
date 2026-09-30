multisig_consecutive_failures = 0
eth_multisig_consecutive_failures = 0

multisig_scan_count = 0
eth_multisig_scan_count = 0

block_scan_count = 0
block_consecutive_failures = 0
block_last_number = 0
block_detections = 0
block_source = ""

eth_block_scan_count = 0
eth_block_consecutive_failures = 0
eth_block_last_number = 0
eth_block_detections = 0
eth_block_source = ""

# ETH mempool (pre-block) watcher
eth_mempool_scan_count = 0
eth_mempool_consecutive_failures = 0
eth_mempool_detections = 0
eth_mempool_connected = False  # any source up; derived from eth_mempool_sources
eth_mempool_sources: dict[str, bool] = {}
eth_mempool_last_seen_at = ""
eth_mempool_reaped = 0

# tx hashes already acted on, so a re-broadcast does not re-alert
MAX_SEEN_CACHE = 5000
eth_seen_pending: set[str] = set()
