"""
USDT Blacklist Monitor Bot

Multi-chain USDT blacklist monitoring with early detection via:
- MultiSig wallet monitoring (hours before execution)
- Mempool scanning (seconds before confirmation)
- On-chain event detection

Usage:
    Polling mode (development):
        python listener.py

    Webhook mode (production):
        uvicorn listener:app --host 0.0.0.0 --port 8000
"""

import asyncio

from src.app import app, run_polling

__all__ = ["app"]

if __name__ == "__main__":
    asyncio.run(run_polling())
