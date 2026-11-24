"""
TronGrid API Manager - Production-Grade Rate Limiting & Load Balancing

Features:
- Multiple API key rotation with health tracking
- Request prioritization (critical, high, medium, low)
- Intelligent rate limiting with token bucket algorithm
- Exponential backoff with jitter
- Response caching with TTL
- Circuit breaker pattern for failing endpoints
- Metrics and logging

Author: Senior Engineering Implementation
"""

import asyncio
import hashlib
import httpx
import json
import logging
import os
import random
import time
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple
from functools import wraps

logger = logging.getLogger("TronAPIManager")


class Priority(IntEnum):
    """Request priority levels - lower number = higher priority."""
    CRITICAL = 1   # MultiSig Submission events (PENDING alerts)
    HIGH = 2       # Execution events, AddedBlackList (CONFIRMED alerts)
    MEDIUM = 3     # Mempool, RemovedBlackList, transaction details
    LOW = 4        # Balance checks, non-urgent queries


@dataclass
class APIEndpoint:
    """Represents a TronGrid API endpoint with health tracking."""
    url: str
    api_key: Optional[str] = None
    name: str = "default"

    # Health tracking
    consecutive_failures: int = 0
    last_failure_time: float = 0
    last_success_time: float = 0
    total_requests: int = 0
    total_failures: int = 0

    # Circuit breaker
    circuit_open: bool = False
    circuit_open_until: float = 0

    # Rate limit tracking
    rate_limited_until: float = 0

    def record_success(self):
        """Record a successful request."""
        self.consecutive_failures = 0
        self.last_success_time = time.time()
        self.total_requests += 1
        self.circuit_open = False

    def record_failure(self, is_rate_limit: bool = False):
        """Record a failed request."""
        now = time.time()
        self.consecutive_failures += 1
        self.last_failure_time = now
        self.total_requests += 1
        self.total_failures += 1

        if is_rate_limit:
            # Exponential backoff for rate limits: 2, 4, 8, 16... seconds (max 30s)
            backoff = min(30, 2 ** min(self.consecutive_failures, 5))
            self.rate_limited_until = now + backoff
            logger.warning(f"[{self.name}] Rate limited, backoff {backoff}s")

        # Open circuit breaker after 5 consecutive failures
        if self.consecutive_failures >= 5:
            self.circuit_open = True
            self.circuit_open_until = now + 60  # 60s circuit breaker
            logger.error(f"[{self.name}] Circuit breaker OPEN for 60s")

    def is_available(self) -> bool:
        """Check if endpoint is available for requests."""
        now = time.time()

        # Check circuit breaker
        if self.circuit_open:
            if now < self.circuit_open_until:
                return False
            # Half-open: allow one request to test
            self.circuit_open = False
            logger.info(f"[{self.name}] Circuit breaker half-open, testing...")

        # Check rate limit
        if now < self.rate_limited_until:
            return False

        return True

    @property
    def health_score(self) -> float:
        """Calculate health score (0-1, higher is better)."""
        if self.total_requests == 0:
            return 1.0

        success_rate = 1 - (self.total_failures / self.total_requests)
        recency_bonus = 0.1 if time.time() - self.last_success_time < 10 else 0
        failure_penalty = min(0.5, self.consecutive_failures * 0.1)

        return max(0, min(1, success_rate + recency_bonus - failure_penalty))


@dataclass
class CacheEntry:
    """Cache entry with TTL."""
    data: Any
    timestamp: float
    ttl: float

    def is_valid(self) -> bool:
        return time.time() - self.timestamp < self.ttl


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: float):
        """
        Args:
            rate: Tokens per second to add
            capacity: Maximum tokens in bucket
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> float:
        """
        Acquire tokens, waiting if necessary.
        Returns the time waited.
        """
        async with self._lock:
            now = time.time()

            # Refill tokens based on time elapsed
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            # Calculate wait time
            needed = tokens - self.tokens
            wait_time = needed / self.rate

            await asyncio.sleep(wait_time)

            self.tokens = 0
            self.last_update = time.time()

            return wait_time


class TronAPIManager:
    """
    Production-grade TronGrid API manager with:
    - Multi-endpoint load balancing
    - Request prioritization
    - Intelligent rate limiting
    - Response caching
    - Circuit breaker pattern
    """

    # Default cache TTLs by request type (seconds)
    DEFAULT_CACHE_TTL = {
        "submission_events": 1.5,    # Critical - very short cache
        "execution_events": 3,       # High priority
        "blacklist_events": 5,       # Medium priority
        "unfreeze_events": 10,       # Lower priority
        "transaction_details": 60,   # Can be cached longer (immutable)
        "balance": 30,               # Balance can be slightly stale
        "block_timestamp": 3,        # Current block changes frequently
    }

    def __init__(
        self,
        api_keys: Optional[List[str]] = None,
        base_url: str = "https://api.trongrid.io",
        requests_per_second: float = 10.0,
        max_concurrent: int = 5,
        cache_enabled: bool = True,
    ):
        """
        Initialize the API manager.

        Args:
            api_keys: List of TronGrid API keys (optional)
            base_url: Base URL for TronGrid API
            requests_per_second: Max requests per second (token bucket rate)
            max_concurrent: Max concurrent requests
            cache_enabled: Whether to enable response caching
        """
        # Initialize endpoints
        self.endpoints: List[APIEndpoint] = []

        # Add endpoints with API keys
        if api_keys:
            for i, key in enumerate(api_keys):
                if key:
                    self.endpoints.append(APIEndpoint(
                        url=base_url,
                        api_key=key,
                        name=f"trongrid-{i+1}"
                    ))

        # Always add a default endpoint (no key)
        if not self.endpoints:
            self.endpoints.append(APIEndpoint(
                url=base_url,
                api_key=None,
                name="trongrid-default"
            ))

        # Rate limiting
        self.rate_limiter = TokenBucket(
            rate=requests_per_second,
            capacity=requests_per_second * 2  # Allow small bursts
        )
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # Caching
        self.cache_enabled = cache_enabled
        self.cache: Dict[str, CacheEntry] = {}
        self.cache_hits = 0
        self.cache_misses = 0

        # Request tracking
        self.pending_requests: Dict[str, asyncio.Event] = {}
        self.request_results: Dict[str, Any] = {}

        # Priority queues (for metrics/logging)
        self.requests_by_priority = {p: 0 for p in Priority}

        # Global backoff (when ALL endpoints are rate limited)
        self.global_backoff_until = 0

        logger.info(f"TronAPIManager initialized with {len(self.endpoints)} endpoint(s)")

    def _get_cache_key(self, endpoint: str, params: Optional[Dict] = None) -> str:
        """Generate a cache key for a request."""
        key_data = f"{endpoint}:{json.dumps(params or {}, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """Get a value from cache if valid."""
        if not self.cache_enabled:
            return None

        entry = self.cache.get(cache_key)
        if entry and entry.is_valid():
            self.cache_hits += 1
            return entry.data

        self.cache_misses += 1
        return None

    def _set_cache(self, cache_key: str, data: Any, ttl: float):
        """Set a value in cache."""
        if self.cache_enabled and data is not None:
            self.cache[cache_key] = CacheEntry(
                data=data,
                timestamp=time.time(),
                ttl=ttl
            )

    def _select_endpoint(self, priority: Priority) -> Optional[APIEndpoint]:
        """
        Select the best available endpoint.
        Uses weighted random selection based on health scores.
        """
        available = [ep for ep in self.endpoints if ep.is_available()]

        if not available:
            # For critical requests, try the healthiest endpoint anyway
            if priority == Priority.CRITICAL:
                # Sort by health score and try the best one
                sorted_eps = sorted(self.endpoints, key=lambda x: x.health_score, reverse=True)
                if sorted_eps:
                    logger.warning("No available endpoints, forcing critical request")
                    return sorted_eps[0]
            return None

        if len(available) == 1:
            return available[0]

        # Weighted random selection based on health scores
        total_score = sum(ep.health_score for ep in available)
        if total_score == 0:
            return random.choice(available)

        r = random.uniform(0, total_score)
        cumulative = 0
        for ep in available:
            cumulative += ep.health_score
            if r <= cumulative:
                return ep

        return available[-1]

    async def request(
        self,
        method: str,
        endpoint: str,
        priority: Priority = Priority.MEDIUM,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        cache_ttl: Optional[float] = None,
        timeout: float = 15.0,
    ) -> Optional[Dict]:
        """
        Make an API request with rate limiting, retry, and caching.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            priority: Request priority
            params: Query parameters
            json_data: JSON body for POST requests
            cache_ttl: Cache TTL in seconds (None to use default)
            timeout: Request timeout

        Returns:
            JSON response or None on failure
        """
        self.requests_by_priority[priority] += 1

        # Check global backoff
        now = time.time()
        if now < self.global_backoff_until:
            if priority > Priority.CRITICAL:
                logger.debug(f"Skipping {priority.name} request during global backoff")
                return None
            # Wait for critical requests
            await asyncio.sleep(self.global_backoff_until - now)

        # Check cache first
        cache_key = self._get_cache_key(endpoint, params or json_data)
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        # Request deduplication - wait for in-flight request with same key
        if cache_key in self.pending_requests:
            event = self.pending_requests[cache_key]
            await event.wait()
            return self.request_results.get(cache_key)

        # Mark request as pending
        self.pending_requests[cache_key] = asyncio.Event()

        try:
            result = await self._do_request(
                method=method,
                endpoint=endpoint,
                priority=priority,
                params=params,
                json_data=json_data,
                timeout=timeout,
            )

            # Cache successful response
            if result is not None:
                ttl = cache_ttl or self._get_default_ttl(endpoint)
                self._set_cache(cache_key, result, ttl)

            # Store result for waiting requests
            self.request_results[cache_key] = result

            return result

        finally:
            # Signal waiting requests
            if cache_key in self.pending_requests:
                self.pending_requests[cache_key].set()
                del self.pending_requests[cache_key]

            # Clean up result after a short delay
            asyncio.create_task(self._cleanup_result(cache_key))

    async def _cleanup_result(self, cache_key: str):
        """Clean up stored result after waiting requests have retrieved it."""
        await asyncio.sleep(1)
        self.request_results.pop(cache_key, None)

    def _get_default_ttl(self, endpoint: str) -> float:
        """Get default cache TTL based on endpoint type."""
        endpoint_lower = endpoint.lower()

        if "submission" in endpoint_lower:
            return self.DEFAULT_CACHE_TTL["submission_events"]
        elif "execution" in endpoint_lower:
            return self.DEFAULT_CACHE_TTL["execution_events"]
        elif "addedblacklist" in endpoint_lower:
            return self.DEFAULT_CACHE_TTL["blacklist_events"]
        elif "removedblacklist" in endpoint_lower:
            return self.DEFAULT_CACHE_TTL["unfreeze_events"]
        elif "balance" in endpoint_lower or "account" in endpoint_lower:
            return self.DEFAULT_CACHE_TTL["balance"]
        elif "transaction" in endpoint_lower:
            return self.DEFAULT_CACHE_TTL["transaction_details"]
        else:
            return 5.0  # Default 5s

    async def _do_request(
        self,
        method: str,
        endpoint: str,
        priority: Priority,
        params: Optional[Dict],
        json_data: Optional[Dict],
        timeout: float,
    ) -> Optional[Dict]:
        """Execute the actual HTTP request with retries."""

        max_retries = 3 if priority <= Priority.HIGH else 2
        last_error = None

        for attempt in range(max_retries):
            # Select endpoint
            ep = self._select_endpoint(priority)
            if ep is None:
                logger.warning(f"No available endpoints for {priority.name} request")
                if attempt < max_retries - 1:
                    # Add jitter to backoff
                    backoff = (2 ** attempt) + random.uniform(0, 1)
                    await asyncio.sleep(backoff)
                    continue
                return None

            # Rate limit
            async with self.semaphore:
                wait_time = await self.rate_limiter.acquire()
                if wait_time > 0:
                    logger.debug(f"Rate limited, waited {wait_time:.2f}s")

                try:
                    # Build URL
                    url = f"{ep.url}{endpoint}"

                    # Build headers
                    headers = {"Accept": "application/json"}
                    if ep.api_key:
                        headers["TRON-PRO-API-KEY"] = ep.api_key

                    async with httpx.AsyncClient(timeout=timeout) as client:
                        if method.upper() == "GET":
                            response = await client.get(url, params=params, headers=headers)
                        else:
                            response = await client.post(url, json=json_data, headers=headers)

                        if response.status_code == 200:
                            ep.record_success()
                            return response.json()

                        elif response.status_code == 429:
                            ep.record_failure(is_rate_limit=True)
                            last_error = "rate_limited"

                            # If all endpoints are rate limited, set global backoff
                            if all(not e.is_available() for e in self.endpoints):
                                self.global_backoff_until = time.time() + 5
                                logger.warning("All endpoints rate limited, global backoff 5s")

                            continue

                        else:
                            ep.record_failure()
                            last_error = f"HTTP {response.status_code}"
                            logger.warning(f"[{ep.name}] {endpoint}: HTTP {response.status_code}")
                            continue

                except httpx.TimeoutException:
                    ep.record_failure()
                    last_error = "timeout"
                    logger.warning(f"[{ep.name}] {endpoint}: Timeout")
                    continue

                except Exception as e:
                    ep.record_failure()
                    last_error = str(e)
                    logger.error(f"[{ep.name}] {endpoint}: {e}")
                    continue

        logger.error(f"All retries failed for {endpoint}: {last_error}")
        return None

    # =========================================================================
    # Convenience methods for common API calls
    # =========================================================================

    async def get_contract_events(
        self,
        contract: str,
        event_name: str,
        min_timestamp: Optional[int] = None,
        limit: int = 200,
        priority: Priority = Priority.MEDIUM,
    ) -> List[Dict]:
        """Get contract events."""
        endpoint = f"/v1/contracts/{contract}/events"
        params = {
            "event_name": event_name,
            "only_confirmed": "false",
            "limit": limit,
            "order_by": "block_timestamp,desc",
        }
        if min_timestamp:
            params["min_block_timestamp"] = min_timestamp

        result = await self.request("GET", endpoint, priority=priority, params=params)

        if result and "data" in result:
            return result["data"]
        return []

    async def get_transaction_by_id(
        self,
        tx_id: str,
        priority: Priority = Priority.MEDIUM,
    ) -> Optional[Dict]:
        """Get transaction details by ID."""
        endpoint = "/wallet/gettransactionbyid"
        result = await self.request(
            "POST",
            endpoint,
            priority=priority,
            json_data={"value": tx_id},
            cache_ttl=300,  # Transactions are immutable, cache 5 min
        )
        return result

    async def get_account_balance(
        self,
        address: str,
        contract: str,
        priority: Priority = Priority.LOW,
    ) -> Optional[Dict]:
        """Get TRC20 token balance."""
        endpoint = f"/v1/accounts/{address}"
        result = await self.request(
            "GET",
            endpoint,
            priority=priority,
            cache_ttl=30,
        )
        return result

    async def get_pending_transactions(
        self,
        priority: Priority = Priority.MEDIUM,
    ) -> List[Dict]:
        """Get pending mempool transactions."""
        endpoint = "/wallet/gettransactionlistfrompending"
        result = await self.request(
            "POST",
            endpoint,
            priority=priority,
            json_data={},
            cache_ttl=1,  # Very short cache for mempool
        )

        if result and "transaction" in result:
            return result["transaction"]
        return []

    # =========================================================================
    # Metrics and monitoring
    # =========================================================================

    def get_metrics(self) -> Dict:
        """Get API manager metrics."""
        return {
            "endpoints": [
                {
                    "name": ep.name,
                    "health_score": round(ep.health_score, 2),
                    "total_requests": ep.total_requests,
                    "total_failures": ep.total_failures,
                    "consecutive_failures": ep.consecutive_failures,
                    "circuit_open": ep.circuit_open,
                    "available": ep.is_available(),
                }
                for ep in self.endpoints
            ],
            "cache": {
                "enabled": self.cache_enabled,
                "entries": len(self.cache),
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate": round(
                    self.cache_hits / max(1, self.cache_hits + self.cache_misses), 2
                ),
            },
            "requests_by_priority": {
                p.name: count for p, count in self.requests_by_priority.items()
            },
        }

    def clear_cache(self):
        """Clear the response cache."""
        self.cache.clear()
        logger.info("Cache cleared")

    async def health_check(self) -> bool:
        """Perform a health check on all endpoints."""
        results = []
        for ep in self.endpoints:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    response = await client.get(
                        f"{ep.url}/wallet/getnowblock",
                        headers={"TRON-PRO-API-KEY": ep.api_key} if ep.api_key else {}
                    )
                    results.append(response.status_code == 200)
            except Exception:
                results.append(False)

        return any(results)


# =========================================================================
# Global instance (singleton pattern)
# =========================================================================

_api_manager: Optional[TronAPIManager] = None


def get_api_manager() -> TronAPIManager:
    """Get or create the global API manager instance."""
    global _api_manager
    if _api_manager is None:
        # Load API keys from environment
        api_keys = []
        for i in range(1, 5):  # Support up to 4 API keys
            key = os.getenv(f"TRONGRID_API_KEY_{i}")
            if key:
                api_keys.append(key)

        # Also check the original env var
        default_key = os.getenv("TRONGRID_API_KEY")
        if default_key and default_key not in api_keys:
            api_keys.insert(0, default_key)

        _api_manager = TronAPIManager(
            api_keys=api_keys if api_keys else None,
            requests_per_second=float(os.getenv("TRONGRID_RPS", "10")),
            cache_enabled=os.getenv("TRONGRID_CACHE", "true").lower() == "true",
        )

    return _api_manager


def reset_api_manager():
    """Reset the global API manager (for testing)."""
    global _api_manager
    _api_manager = None
