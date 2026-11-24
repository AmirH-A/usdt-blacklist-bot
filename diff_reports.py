#!/usr/bin/env python3
"""Diff the accuracy report built from sqlite against the one built from postgres.

The sqlite file froze at the cutover while postgres kept taking writes, so
small numeric drift is expected and fine. What this is looking for is
structural damage: a key that vanished, or a section that went empty.

Run from /root/zarin-freeze-detector.
"""
import importlib.util
import json
import os
import sys

OLD = "backups/pre-providerwatch-20261010-093508/src/accuracy.py"  # sqlite version
TOLERANCE = 0.05  # 5% numeric drift allowed; sqlite is a frozen snapshot


def load_old_package():
    """Import the pre-migration accuracy module as part of its own package.

    It uses relative imports (from .tron.address import ...), so the whole
    backed-up src tree is staged at /tmp/oldpkg/src_old and imported from
    there -- loading the single file standalone cannot resolve them.
    """
    sys.path.insert(0, "/tmp/oldpkg")
    import src_old.accuracy as m

    return m


def flatten(obj, prefix=""):
    """Leaf paths -> values. Lists collapse to their length."""
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(flatten(v, f"{prefix}.{k}" if prefix else k))
    elif isinstance(obj, list):
        out[f"{prefix}[len]"] = len(obj)
    else:
        out[prefix] = obj
    return out


def close_enough(a, b):
    if a == b:
        return True
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if a == 0 or b == 0:
            return abs(a - b) <= 2
        return abs(a - b) / max(abs(a), abs(b)) <= TOLERANCE
    return False


def main():
    staged = "/tmp/oldpkg/src_old/accuracy.py"
    if not os.path.exists(staged):
        sys.exit(f"pre-migration src tree not staged at {staged}")

    # --- postgres (current code) -------------------------------------------
    sys.path.insert(0, ".")
    from src.accuracy import build_report as pg_report

    pg = pg_report(force=True)
    print(f"postgres report built; query_errors={pg['sources'].get('query_errors')}")

    # --- sqlite (pre-migration code, pointed at the frozen file) ----------
    # The old module reads BOT_DB_PATH and opens it with sqlite3 directly.
    os.environ["BOT_DB_PATH"] = "blacklist_tracker.db"
    old = load_old_package()
    sq = old.build_report(force=True)
    print("sqlite report built from the frozen snapshot")

    fp, fs = flatten(pg), flatten(sq)
    only_pg = sorted(set(fp) - set(fs))
    only_sq = sorted(set(fs) - set(fp))
    shared = sorted(set(fp) & set(fs))

    print(f"\nleaf fields: sqlite={len(fs)} postgres={len(fp)} shared={len(shared)}")

    if only_sq:
        print(f"\n!! {len(only_sq)} field(s) present in sqlite but MISSING in postgres:")
        for k in only_sq[:40]:
            print(f"   - {k} = {fs[k]!r}")

    if only_pg:
        print(f"\n   {len(only_pg)} field(s) new in postgres (expected: query_errors):")
        for k in only_pg[:10]:
            print(f"   + {k} = {fp[k]!r}")

    # Emptiness regressions are the dangerous class: non-zero -> zero/None.
    emptied, drifted = [], []
    for k in shared:
        a, b = fs[k], fp[k]
        if close_enough(a, b):
            continue
        was_data = isinstance(a, (int, float)) and a not in (0, None)
        now_empty = b in (0, None)
        (emptied if (was_data and now_empty) else drifted).append((k, a, b))

    if emptied:
        print(f"\n!! {len(emptied)} field(s) had data in sqlite and are EMPTY in postgres:")
        for k, a, b in emptied[:40]:
            print(f"   - {k}: {a!r} -> {b!r}")
    else:
        print("\nOK  no field lost its data (nothing non-zero went to 0/None)")

    if drifted:
        print(f"\n   {len(drifted)} field(s) drifted beyond {int(TOLERANCE*100)}% "
              f"(sqlite is frozen at cutover, so some drift is correct):")
        for k, a, b in drifted[:25]:
            print(f"   ~ {k}: {a!r} -> {b!r}")

    verdict = "STRUCTURALLY IDENTICAL" if not emptied and not only_sq else "REGRESSIONS FOUND"
    print(f"\nverdict: {verdict}")
    return 1 if (emptied or only_sq) else 0


if __name__ == "__main__":
    sys.exit(main())
