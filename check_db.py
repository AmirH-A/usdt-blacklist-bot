#!/usr/bin/env python3
import os

import psycopg2
import httpx
import asyncio

async def main():
    # Get current block timestamp
    async with httpx.AsyncClient() as client:
        resp = await client.post("https://api.trongrid.io/wallet/getnowblock", timeout=10)
        data = resp.json()
        current_ts = data["block_header"]["raw_data"]["timestamp"]
        print(f"Current TRON block timestamp: {current_ts}")

    # Check database
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    c = conn.cursor()
    c.execute("SELECT address, status, block_timestamp, confirmed_block_timestamp FROM blacklist WHERE chain='TRON' ORDER BY detected_at DESC LIMIT 5")
    rows = c.fetchall()

    print("\nRecent TRON entries:")
    print("-" * 80)
    for addr, status, block_ts, confirmed_ts in rows:
        print(f"Address: {addr[:20]}...")
        print(f"  Status: {status}")
        print(f"  Block timestamp (pending): {block_ts}")
        print(f"  Confirmed block timestamp: {confirmed_ts}")
        if block_ts:
            elapsed = (current_ts - block_ts) / 1000.0
            print(f"  Time since pending (if confirmed now): {elapsed:.1f}s ({elapsed/60:.1f}m)")
        print()

asyncio.run(main())
