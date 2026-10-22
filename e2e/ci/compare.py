#!/usr/bin/env python3
"""Compare Trino TSV answers with the DuckDB reference answers in e2e/ci/reference/<sf>.

    compare.py <reference-dir> <actual-dir> <queries-dir> <summary.tsv> ["q01 q03 ..."]

Normalisation (same as the reference generator): numeric cells are compared after rounding to
4 decimal places (doubles also pass within a relative 1e-9); rows are sorted unless the query has
an ORDER BY. A Trino DECIMAL with fewer than 4 fractional digits also passes when it equals the
DuckDB value rounded to that scale: Trino keeps the DECIMAL type for avg() and decimal division
(TPC-H q01 avg_qty 25.52, q17), where DuckDB returns DOUBLE. Such cells are counted in the detail. Exit status 1 if any query differs or is missing.
"""
import os
import re
import sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, ROUND_HALF_UP

Q4 = Decimal("0.0001")


def num(s):
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def trino_scale_equal(expected, actual):
    """actual is a DECIMAL rendered with scale < 4 and equals expected rounded to that scale."""
    m = re.fullmatch(r"-?\d+\.(\d{1,3})", actual)
    de = num(expected)
    if not m or de is None or not de.is_finite():
        return False
    q = Decimal(1).scaleb(-len(m.group(1)))
    return Decimal(actual) in (de.quantize(q, ROUND_HALF_UP), de.quantize(q, ROUND_HALF_EVEN))


def cell_equal(a, b):
    if a == b:
        return True
    da, db = num(a), num(b)
    if da is None or db is None or not da.is_finite() or not db.is_finite():
        return False
    if da.quantize(Q4, ROUND_HALF_EVEN) == db.quantize(Q4, ROUND_HALF_EVEN):
        return True
    scale = max(abs(da), abs(db))
    return scale != 0 and abs(da - db) / scale <= Decimal("1e-9")


def norm_key(cell):
    d = num(cell)
    if d is not None and d.is_finite():
        return (0, d.quantize(Q4, ROUND_HALF_EVEN), "")
    return (1, Decimal(0), cell)


def read_rows(path):
    with open(path, encoding="utf-8") as f:
        return [line.rstrip("\n").split("\t") for line in f if line.rstrip("\n") != ""]


def compare(name, ref_dir, act_dir, q_dir):
    act_path = os.path.join(act_dir, name + ".tsv")
    if not os.path.exists(act_path):
        err = os.path.join(act_dir, name + ".err")
        detail = open(err, encoding="utf-8").read().strip().splitlines()[-1:] if os.path.exists(err) else []
        return "FAIL", "no answer " + " ".join(detail)[:300]
    ref = read_rows(os.path.join(ref_dir, name + ".tsv"))
    act = read_rows(act_path)
    sql = open(os.path.join(q_dir, name + ".sql"), encoding="utf-8").read()
    if not re.search(r"\border\s+by\b", sql, re.IGNORECASE):
        ref.sort(key=lambda r: [norm_key(c) for c in r])
        act.sort(key=lambda r: [norm_key(c) for c in r])
    if len(ref) != len(act):
        return "FAIL", f"rows: expected {len(ref)}, got {len(act)}"
    scaled = 0
    for i, (r, a) in enumerate(zip(ref, act)):
        if len(r) != len(a):
            return "FAIL", f"row {i}: expected {len(r)} columns, got {len(a)}"
        for j, (x, y) in enumerate(zip(r, a)):
            if cell_equal(x, y):
                continue
            if trino_scale_equal(x, y):
                scaled += 1
                continue
            return "FAIL", f"row {i} col {j}: expected {x!r}, got {y!r}"
    detail = f"{len(ref)} rows"
    if scaled:
        detail += f", {scaled} cells equal at Trino DECIMAL scale"
    return "OK", detail


def main():
    ref_dir, act_dir, q_dir, summary = sys.argv[1:5]
    names = sorted(f[:-4] for f in os.listdir(q_dir) if f.endswith(".sql"))
    if len(sys.argv) > 5:
        names = [n for n in names if n in sys.argv[5].replace(",", " ").split()]
    failed = 0
    with open(summary, "w", encoding="utf-8") as out:
        out.write("query\tresult\tdetail\n")
        for name in names:
            result, detail = compare(name, ref_dir, act_dir, q_dir)
            failed += result != "OK"
            out.write(f"{name}\t{result}\t{detail}\n")
            print(f"{name}\t{result}\t{detail}")
    print(f"{len(names) - failed}/{len(names)} OK")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
