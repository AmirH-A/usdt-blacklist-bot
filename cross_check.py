#!/usr/bin/env python3
"""Do any Tether-frozen addresses carry a 'low_risk / exchange' label in ChainRisk?

A label that says low_risk is not neutral -- it actively vouches for an address.
If any of the detector's confirmed freezes is labelled low_risk, ChainRisk is
asserting the opposite of a known legal fact.

Reads both databases read-only. Prints counts and at most a few examples.
"""
import os
import subprocess

FD_DSN = os.environ["DATABASE_URL"]  # the freeze detector's postgres


def chainrisk_query(sql):
    """chainrisk-postgres has no host port mapping for us; go through docker."""
    out = subprocess.run(
        ["docker", "exec", "-i", "chainrisk-postgres",
         "psql", "-U", "chainrisk", "-d", "chainrisk", "-t", "-A", "-c", sql],
        capture_output=True, text=True, timeout=120,
    )
    if out.returncode != 0:
        raise RuntimeError(out.stderr.strip())
    return [l.strip() for l in out.stdout.splitlines() if l.strip()]


import psycopg2

fd = psycopg2.connect(FD_DSN)
cur = fd.cursor()
cur.execute("SELECT address FROM blacklist WHERE chain = 'TRON' AND status IN ('confirmed','multisig_pending')")
frozen = {r[0] for r in cur.fetchall()}
fd.close()
print(f"detector: {len(frozen)} TRON addresses frozen or pending")

labels = {}
for row in chainrisk_query(
    "SELECT address || '|' || risk_type || '|' || category FROM address_labels WHERE chain = 'tron'"
):
    addr, risk, cat = row.split("|", 2)
    labels[addr] = (risk, cat)
print(f"chainrisk: {len(labels)} TRON address labels")

overlap = sorted(frozen & set(labels))
print(f"\noverlap: {len(overlap)} frozen addresses carry a ChainRisk label")

vouched = [a for a in overlap if labels[a][0] == "low_risk"]
print(f"  of which labelled low_risk: {len(vouched)}")
for a in vouched[:5]:
    print(f"    {a}  -> {labels[a][0]} / {labels[a][1]}")

# And the reverse coverage question: how many frozen addresses does ChainRisk
# know about at all, via its blocklist?
bl = set()
for row in chainrisk_query("SELECT address FROM blocklist_entries WHERE chain = 'tron'"):
    bl.add(row)
known = frozen & bl
print(f"\nfrozen addresses present in ChainRisk's blocklist: {len(known)} / {len(frozen)}"
      f"  ({100.0*len(known)/max(1,len(frozen)):.1f}%)")
