#!/usr/bin/env python3
"""TPC-H data and DuckDB reference answers for the connector comparison (e2e/compare).

    tpch_data.py gen <sf> <out-dir>              dbgen at scale <sf> -> <out-dir>/<table>/data_N.parquet
    tpch_data.py ref <data-dir> <queries-dir> <ref-dir>
                                                 run every <queries-dir>/qNN.sql in DuckDB over <data-dir>/<table>/*.parquet,
                                                 write <ref-dir>/qNN.tsv (+ MANIFEST.tsv)
    tpch_data.py sum <queries-dir> <answers-dir> [q01 ...]
                                                 print "query rows checksum" of the answers in <answers-dir>

The checksum is the same for a reference and a Trino answer of the same rows: numeric cells rounded to 2 decimal places
(Trino keeps DECIMAL for avg(), DuckDB returns DOUBLE), rows sorted unless the query has an ORDER BY, then sha256 (12 hex).
compare.py (e2e/ci) stays the authority on whether an answer is right; the checksum is the short form for the report.
"""
import datetime
import decimal
import hashlib
import os
import re
import sys
import time

TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]


def connect(path=":memory:"):
    import duckdb
    con = duckdb.connect(path)
    con.execute("SET preserve_insertion_order = false")
    return con


def gen(sf, out):
    db = os.path.join(out, "..", f"dbgen-sf{sf}.duckdb")
    con = connect(db)
    con.execute(os.environ.get("TPCH_INSTALL", "INSTALL tpch")); con.execute("LOAD tpch")
    t = time.time()
    con.execute(f"CALL dbgen(sf={sf})")
    print(f"dbgen sf={sf}: {time.time() - t:.0f} s", flush=True)
    for table in TABLES:
        target = os.path.join(out, table)
        con.execute(f"COPY {table} TO '{target}' (FORMAT parquet, COMPRESSION snappy, FILE_SIZE_BYTES '256MB', "
                    "OVERWRITE_OR_IGNORE true)")
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"{table}: {n} rows, {len(os.listdir(target))} files", flush=True)
    con.close()
    os.remove(db)


def cell(v):
    if v is None:
        return ""
    if isinstance(v, decimal.Decimal):
        return format(v, "f")
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return str(v)


def ref(data, qdir, rdir):
    os.makedirs(rdir, exist_ok=True)
    con = connect()
    for table in TABLES:
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{data}/{table}/*.parquet')")
    with open(os.path.join(rdir, "MANIFEST.tsv"), "w", encoding="utf-8") as man:
        man.write("query\trows\tcolumns\tchecksum\tseconds\n")
        for name in sorted(f[:-4] for f in os.listdir(qdir) if f.endswith(".sql")):
            sql = open(os.path.join(qdir, name + ".sql"), encoding="utf-8").read().strip().rstrip(";")
            t = time.time()
            cur = con.execute(sql)
            rows = cur.fetchall()
            secs = time.time() - t
            with open(os.path.join(rdir, name + ".tsv"), "w", encoding="utf-8") as f:
                for r in rows:
                    f.write("\t".join(cell(v) for v in r) + "\n")
            n, ck = checksum(qdir, rdir, name)
            man.write(f"{name}\t{n}\t{len(cur.description)}\t{ck}\t{secs:.2f}\n")
            print(f"{name}\t{n}\t{ck}\t{secs:.2f}", flush=True)


def norm(c):
    try:
        d = decimal.Decimal(c)
    except decimal.InvalidOperation:
        return c
    if not d.is_finite():
        return c
    return format(d.quantize(decimal.Decimal("0.01"), decimal.ROUND_HALF_UP), "f")


def checksum(qdir, adir, name):
    path = os.path.join(adir, name + ".tsv")
    if not os.path.exists(path):
        return "-", "-"
    rows = [[norm(c) for c in line.rstrip("\n").split("\t")]
            for line in open(path, encoding="utf-8") if line.rstrip("\n")]
    sql = open(os.path.join(qdir, name + ".sql"), encoding="utf-8").read()
    if not re.search(r"\border\s+by\b", sql, re.IGNORECASE):
        rows.sort()
    h = hashlib.sha256("\n".join("\t".join(r) for r in rows).encode()).hexdigest()[:12]
    return str(len(rows)), h


def main():
    cmd = sys.argv[1]
    if cmd == "gen":
        os.makedirs(sys.argv[3], exist_ok=True)
        gen(sys.argv[2], sys.argv[3])
    elif cmd == "ref":
        ref(*sys.argv[2:5])
    elif cmd == "sum":
        qdir, adir = sys.argv[2:4]
        names = sys.argv[4:] or sorted(f[:-4] for f in os.listdir(qdir) if f.endswith(".sql"))
        for name in names:
            print(name, *checksum(qdir, adir, name), sep="\t")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
