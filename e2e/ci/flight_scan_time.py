"""Times one scan straight to the Flight SQL server over ADBC (C++/Go Flight client), as one statement and as the plugin's
range splits run concurrently (one connection per split, like Trino drivers). The counterpart of FlightReadBench.java,
which reads the same statements through the plugin's Java client. Diagnostics only (profile.sh).

usage: FLIGHT_TOKEN=... python flight_scan_time.py grpc://host:port <schema> <repeat> scan.sql [splits ...]
prints: adbc-s<splits> <TAB> best seconds <TAB> rows <TAB> MB (Arrow buffers) <TAB> MB/s   (or ERROR and the message)
"""
import math
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import adbc_driver_flightsql.dbapi as flightsql


def connect(uri):
    return flightsql.connect(uri, db_kwargs={"adbc.flight.sql.authorization_header": "Bearer " + os.environ["FLIGHT_TOKEN"]})


def fetch(uri, sql):
    with connect(uri) as conn, conn.cursor() as cur:
        cur.execute(sql)
        table = cur.fetch_arrow_table()
        return table.num_rows, table.nbytes


def statements(uri, scan, splits):
    if splits <= 1:
        return [scan]
    column = re.match(r"(?is)SELECT\s+([^,\s]+)", scan).group(1)
    table = re.search(r"(?is)\bFROM\s+(\S+)", scan).group(1)
    with connect(uri) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT min({column}), max({column}) FROM {table}")
        low, high = cur.fetchone()
    width = (high - low + 1) / splits
    out, lo = [], low
    for i in range(splits):  # the plugin's predicates (AbstractArrowFlightSplitManager)
        last = i == splits - 1
        hi = high if last else math.floor(low + width * (i + 1))
        if i == 0:
            pred = f"({column} < {hi} OR {column} IS NULL)"
        elif last:
            pred = f"({column} >= {lo})"
        else:
            pred = f"({column} >= {lo} AND {column} < {hi})"
        out.append(f"{scan} WHERE {pred}")
        lo = hi
    return out


def main():
    uri, schema, repeat, path = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
    with open(path) as f:
        scan = re.sub(r"\bFROM\s+(\w+)", rf"FROM {schema}.\1", f.read().strip().rstrip(";"))
    for splits in [int(s) for s in sys.argv[5:]] or [1, 4]:
        try:
            sqls = statements(uri, scan, splits)
            best = None
            with ThreadPoolExecutor(len(sqls)) as pool:
                for _ in range(repeat):
                    start = time.monotonic()
                    results = list(pool.map(lambda s: fetch(uri, s), sqls))
                    elapsed = time.monotonic() - start
                    if best is None or elapsed < best[0]:
                        best = (elapsed, sum(r[0] for r in results), sum(r[1] for r in results))
            mb = best[2] / 1e6
            print(f"adbc-s{splits}\t{best[0]:.2f}\t{best[1]}\t{mb:.0f}\t{mb / best[0]:.0f}", flush=True)
        except Exception as e:  # noqa: BLE001 - every failure is reported, not raised
            print(f"adbc-s{splits}\tERROR\t{str(e).splitlines()[0][:300] if str(e) else type(e).__name__}", flush=True)


if __name__ == "__main__":
    main()
