"""Provider credential watchdog.

A dead or quota-exhausted Chainstack key never raises here: the failover in
grpc_client/rpc swallows the 401/403 and detection quietly continues on the
slow TronGrid/Etherscan path. Both outages this month were found by hand, hours
late -- block_consecutive_failures stayed 0 the whole time, so
_notify_scanner_down never fired. This probes every configured credential
directly and says which one to rotate, because rotating is the only fix a
401/403 has.

Probes over each provider's HTTPS endpoint rather than gRPC: one httpx call
gives the exact status code, which is what separates "wrong key" (401) from
"out of credits" (403) -- different actions for whoever reads the alert.
# ponytail: HTTP probe stands in for the gRPC path. Same host, same token, but
# a node could in principle allow REST while refusing gRPC. Probe over gRPC if
# that ever actually happens.
"""

import asyncio
import time
from typing import Optional

import httpx

from .config import (
    ADMIN_CHAT_ID,
    CHAINSTACK_GRPC_HOST,
    ETH_RPC_URLS,
    PROVIDER_PROBE_INTERVAL,
    PROVIDER_REMIND_HOURS,
    TRON_GRPC_ENDPOINTS,
    logger,
)
from . import health
from .bot import bot

# What a status code means for the person who has to fix it.
_MEANING = {
    401: "key not recognised -- wrong or revoked key",
    403: "key refused -- quota/credits exhausted",
    429: "rate limited -- plan too small or poll too fast",
}


def _meaning(code: int) -> str:
    return _MEANING.get(code, f"HTTP {code}")


def _decide(state: dict, ok: bool, now: float, remind_s: float) -> tuple:
    """Pure transition logic. Returns (action, new_state).

    action is "down" on the first failure, "remind" when still down after
    remind_s, "up" on recovery, or None when nothing needs saying.
    """
    was_dead = state.get("dead", False)
    if ok:
        if was_dead:
            return "up", {"dead": False, "since": now, "notified_at": 0.0}
        return None, {
            "dead": False,
            "since": state.get("since", now),
            "notified_at": 0.0,
        }
    if not was_dead:
        return "down", {"dead": True, "since": now, "notified_at": now}
    if now - state.get("notified_at", 0.0) >= remind_s:
        return "remind", {**state, "notified_at": now}
    return None, state


async def _probe_tron(client: httpx.AsyncClient, host: str, token: str) -> Optional[int]:
    """None on success, else the HTTP status that rejected the credential."""
    try:
        r = await client.post(
            f"https://{host}/wallet/getnowblock",
            headers={"x-token": token, "Content-Type": "application/json"},
            json={},
        )
        return None if r.status_code == 200 else r.status_code
    except Exception as e:
        logger.debug(f"TRON provider probe transport error on {host}: {e}")
        return 0  # 0 = unreachable, not a credential verdict


async def _probe_eth(client: httpx.AsyncClient, url: str) -> Optional[int]:
    try:
        r = await client.post(
            url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_blockNumber",
                "params": [],
            },
        )
        return None if r.status_code == 200 else r.status_code
    except Exception as e:
        logger.debug(f"ETH provider probe transport error: {e}")
        return 0


def _label(url: str) -> str:
    """Endpoint identity for the alert, without leaking the whole key."""
    tail = url.rstrip("/").split("/")[-1]
    return "..." + tail[-6:] if len(tail) > 6 else tail


async def _notify(text: str):
    if not ADMIN_CHAT_ID:
        logger.warning(f"PROVIDER ALERT (no ADMIN_CHAT_ID set, not sent): {text}")
        return
    try:
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logger.error(f"Failed to send provider alert: {e}")


def _down_message(
    chain: str, name: str, code: int, env_var: str, live: int, total: int
) -> str:
    head = "CHANGE API KEY" if code in (401, 403) else "PROVIDER UNREACHABLE"
    src = health.block_source if chain == "TRON" else health.eth_block_source
    msg = f"*FD -- {head} [{chain}]*\n\n"
    msg += f"Endpoint: `{name}`\n"
    msg += f"Status: {_meaning(code) if code else 'unreachable (network/DNS)'}\n"
    msg += f"Working endpoints: {live}/{total}\n"
    msg += f"Detection now via: `{src or 'starting'}`\n\n"
    if code in (401, 403):
        msg += "Fix: Chainstack -> node -> Access and credentials, then set\n"
        msg += f"`{env_var}` in `/root/zarin-freeze-detector/.env`\n"
        msg += "and `systemctl restart zarin-bot`"
    return msg


async def watch_providers():
    """Probe every configured RPC credential and alert when one needs rotating."""
    tron = list(TRON_GRPC_ENDPOINTS)
    eth = list(ETH_RPC_URLS)
    if not tron and not eth:
        logger.warning("provider watchdog: nothing configured to probe")
        return

    remind_s = PROVIDER_REMIND_HOURS * 3600.0
    tron_env = {0: "CHAINSTACK_API_TOKEN", 1: "CHAINSTACK_API_TOKEN_FALLBACK"}
    eth_env = {0: "ETH_RPC_URL", 1: "ETH_RPC_URL_FALLBACK"}
    state: dict = {}
    logger.info(
        f"provider watchdog started (every {PROVIDER_PROBE_INTERVAL}s, "
        f"{len(tron)} TRON + {len(eth)} ETH endpoints)"
    )

    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            try:
                tron_codes = await asyncio.gather(
                    *[_probe_tron(client, h, t) for h, _p, t in tron]
                )
                eth_codes = await asyncio.gather(*[_probe_eth(client, u) for u in eth])

                for chain, codes, eps, envs in (
                    ("TRON", tron_codes, tron, tron_env),
                    ("ETH", eth_codes, eth, eth_env),
                ):
                    live = sum(1 for c in codes if c is None)
                    for i, code in enumerate(codes):
                        key = f"{chain}:{i}"
                        name = (
                            f"{CHAINSTACK_GRPC_HOST} #{i + 1}"
                            if chain == "TRON"
                            else _label(eps[i])
                        )
                        action, state[key] = _decide(
                            state.get(key, {}), code is None, time.time(), remind_s
                        )
                        if action in ("down", "remind"):
                            prefix = "*[still down]*\n" if action == "remind" else ""
                            await _notify(
                                prefix
                                + _down_message(
                                    chain,
                                    name,
                                    code or 0,
                                    envs.get(i, "the endpoint"),
                                    live,
                                    len(codes),
                                )
                            )
                        elif action == "up":
                            await _notify(
                                f"*FD -- endpoint recovered [{chain}]*\n\n"
                                f"`{name}` is answering again "
                                f"({live}/{len(codes)} working)."
                            )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"provider watchdog error: {e}")

            await asyncio.sleep(PROVIDER_PROBE_INTERVAL)
