#!/usr/bin/env python3
"""Compare every table of the Parquet 'types' schema read through Trino + this plugin with DuckDB
reading the same Parquet files directly (no committed reference: generated in the job).

    types_check.py <types-dir> <trino-host:port> <summary.tsv> [catalog] [schema]

<types-dir>/<table>/*.parquet is one table. Trino is read with the Python client in raw mode
(values exactly as the Trino protocol sends them); DuckDB columns rendered by Trino as text or
date/time types are cast to VARCHAR in DuckDB. Both sides are normalised per Trino type:
- integers / decimals: numeric value (trailing fractional zeros ignored);
- real: the float32 value; double: the exact double (NaN and +-Infinity included);
- date / time / timestamp: fractional seconds padded to 9 digits, time zone must be UTC;
- varbinary: bytes; varchar and everything else: the text.
Rows are compared as sorted multisets (no row order). Columns the plugin does not expose (types
it does not map) are listed as SKIPPED and do not fail the check; a table missing in Trino, a
missing row, or a differing value does. Exit status 1 on any failure.
"""
import base64
import math
import os
import re
import struct
import sys
import time
from decimal import Decimal

import duckdb
import trino

TEMPORAL = re.compile(r"^(date|time|timestamp)")
TS_TEXT = re.compile(
    r"^(?P<date>-?\d{4,}-\d\d-\d\d)?[ T]?(?P<time>\d\d:\d\d:\d\d)?(?:\.(?P<frac>\d+))?"
    r"\s*(?P<zone>.*?)(?P<bc> \(BC\))?$")


def norm_number(v):
    s = format(Decimal(str(v)), "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def norm_float(v, single):
    f = float(v)  # Trino sends NaN / Infinity / -Infinity as strings, float() parses all three
    if math.isnan(f):
        return "NaN"
    if single and not math.isinf(f):
        f = struct.unpack("<f", struct.pack("<f", f))[0]
    return repr(f)


def norm_temporal(v):
    """Text of a date/time/timestamp from either side -> canonical text; None if unparseable."""
    s = str(v).strip()
    m = TS_TEXT.match(s)
    if not m or not (m.group("date") or m.group("time")):
        return "?" + s
    zone = m.group("zone").strip()
    if zone not in ("", "UTC", "Z", "+00", "+00:00", "Etc/UTC"):
        return "?" + s
    parts = []
    if m.group("date"):
        parts.append(m.group("date") + (" BC" if m.group("bc") else ""))
    if m.group("time"):
        parts.append(m.group("time") + "." + (m.group("frac") or "").ljust(9, "0"))
    return " ".join(parts) + (" UTC" if zone else "")


def normaliser(trino_type):
    t = trino_type.lower()
    if t in ("tinyint", "smallint", "integer", "bigint") or t.startswith("decimal"):
        return norm_number
    if t == "real":
        return lambda v: norm_float(v, True)
    if t == "double":
        return lambda v: norm_float(v, False)
    if t == "boolean":
        return lambda v: str(v).lower()
    if t == "varbinary":
        return lambda v: (base64.b64decode(v) if isinstance(v, str) else bytes(v)).hex()
    if TEMPORAL.match(t):
        return norm_temporal
    return str


def duckdb_select(name, trino_type):
    t = trino_type.lower()
    if t.startswith("varchar") or t.startswith("char") or TEMPORAL.match(t):
        return f'CAST("{name}" AS VARCHAR)'
    return f'"{name}"'


def sort_key(row):
    return tuple((v is None, v or "") for v in row)


def first_diff(expected, actual):
    i = 0
    while i < len(expected) and i < len(actual) and expected[i] == actual[i]:
        i += 1
    e = expected[i] if i < len(expected) else "<none>"
    a = actual[i] if i < len(actual) else "<none>"
    return f"first difference at sorted row {i}: expected {e!r}, got {a!r}"[:400]


def check_table(table, path, cur, ddb, out):
    failed = 0
    ddb_cols = ddb.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}/*.parquet')").fetchall()
    ddb_types = {c[0].lower(): (c[0], c[1]) for c in ddb_cols}
    try:
        cur.execute(f'DESCRIBE "{table}"')
        trino_cols = [(r[0], r[1]) for r in cur.fetchall()]
    except Exception as e:  # noqa: BLE001 - reported, not raised
        out(table, "*", "", "", "FAIL", f"DESCRIBE failed: {e}"[:400])
        return 1
    exposed = {c.lower() for c, _ in trino_cols}
    for name, dtype in ddb_types.values():
        if name.lower() not in exposed:
            out(table, name, dtype, "", "SKIPPED", "not exposed by the plugin (type not mapped)")
    unknown = [c for c, _ in trino_cols if c.lower() not in ddb_types]
    if unknown:
        out(table, ",".join(unknown), "", "", "FAIL", "columns not in the Parquet schema")
        failed += 1
    cols = [(c, t) for c, t in trino_cols if c.lower() in ddb_types]
    if not cols:
        out(table, "*", "", "", "FAIL", "no comparable columns")
        return failed + 1
    norms = [normaliser(t) for _, t in cols]

    def norm_rows(rows):
        return sorted((tuple(None if v is None else f(v) for f, v in zip(norms, r)) for r in rows), key=sort_key)

    start = time.time()
    select = ", ".join(duckdb_select(ddb_types[c.lower()][0], t) for c, t in cols)
    expected = norm_rows(ddb.execute(f"SELECT {select} FROM read_parquet('{path}/*.parquet')").fetchall())
    t_ddb = time.time() - start
    start = time.time()
    try:
        cur.execute(f'SELECT {", ".join(chr(34) + c + chr(34) for c, _ in cols)} FROM "{table}"')
        actual = norm_rows(cur.fetchall())
    except Exception as e:  # noqa: BLE001
        out(table, "*", "", "", "FAIL", f"SELECT failed: {e}"[:400])
        return failed + 1
    t_trino = time.time() - start

    for i, (c, t) in enumerate(cols):
        e_col = sorted((r[i] for r in expected), key=lambda v: (v is None, v or ""))
        a_col = sorted((r[i] for r in actual), key=lambda v: (v is None, v or ""))
        nulls = sum(v is None for v in e_col)
        if e_col == a_col:
            out(table, c, ddb_types[c.lower()][1], t, "OK", f"{len(e_col)} values, {nulls} null")
        else:
            failed += 1
            out(table, c, ddb_types[c.lower()][1], t, "FAIL",
                f"rows {len(e_col)} vs {len(a_col)}; " + first_diff(e_col, a_col))
    if expected == actual:
        out(table, "*", "", "", "OK",
            f"{len(expected)} rows x {len(cols)} columns equal (duckdb {t_ddb:.1f}s, trino {t_trino:.1f}s)")
    else:
        failed += 1
        out(table, "*", "", "", "FAIL", f"rows {len(expected)} vs {len(actual)}; " + first_diff(expected, actual))
    return failed


def main():
    types_dir, server, summary = sys.argv[1:4]
    catalog = sys.argv[4] if len(sys.argv) > 4 else "flight"
    schema = sys.argv[5] if len(sys.argv) > 5 else "types"
    host, port = server.rsplit(":", 1)
    conn = trino.dbapi.connect(host=host, port=int(port), user="ci", catalog=catalog, schema=schema,
                               legacy_primitive_types=True)
    cur = conn.cursor()
    ddb = duckdb.connect()
    ddb.execute("SET TimeZone = 'UTC'")
    tables = sorted(d for d in os.listdir(types_dir) if os.path.isdir(os.path.join(types_dir, d)))
    cur.execute("SHOW TABLES")
    trino_tables = {r[0] for r in cur.fetchall()}
    failed = 0
    with open(summary, "w", encoding="utf-8") as f:
        f.write("table\tcolumn\tparquet_type\ttrino_type\tresult\tdetail\n")

        def out(*cells):
            line = "\t".join(str(c).replace("\t", " ").replace("\n", " ") for c in cells)
            f.write(line + "\n")
            print(line, flush=True)

        for table in tables:
            if table.lower() not in trino_tables:
                out(table, "*", "", "", "FAIL", f"table not listed by Trino (has: {sorted(trino_tables)})")
                failed += 1
                continue
            failed += check_table(table.lower(), os.path.join(types_dir, table), cur, ddb, out)
    print(f"types: {len(tables)} tables, {failed} failure(s)")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
