"""
USDT Blacklist Monitor Bot

Watches Tether's issuer MultiSig wallets on TRON and ETH:
Submission (decode addBlackList / removeBlackList) then Execution.

Usage:
    python listener.py
    uvicorn listener:app --host 0.0.0.0 --port 8000
"""

import asyncio

from src.app import app, run_polling

__all__ = ["app"]

if __name__ == "__main__":
    asyncio.run(run_polling())
