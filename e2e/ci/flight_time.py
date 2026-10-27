"""Times TPC-H query files sent straight to the Flight SQL server (ADBC: GetFlightInfo + DoGet of every endpoint, the
whole query run by DuckDB), i.e. the server's share without Trino and this plugin. Diagnostics only (profile.sh).

usage: FLIGHT_TOKEN=... python flight_time.py grpc://host:port <schema> <repeat> q01.sql [q02.sql ...]
prints: query <TAB> best seconds <TAB> rows   (or ERROR and the first line of the error)
"""
import os
import re
import sys
import time

import adbc_driver_flightsql.dbapi as flightsql

TABLES = r"\b(lineitem|orders|customer|part|partsupp|supplier|nation|region)\b"


def main():
    uri, schema, repeat, files = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4:]
    kwargs = {"adbc.flight.sql.authorization_header": "Bearer " + os.environ["FLIGHT_TOKEN"]}
    with flightsql.connect(uri, db_kwargs=kwargs) as conn:
        for path in files:
            name = os.path.basename(path)[:-4]
            with open(path) as f:
                # qualify the bare TPC-H table names (not ones already qualified or column prefixes like l_)
                sql = re.sub(r"(?<![.\w])" + TABLES, schema + r".\1", f.read().strip().rstrip(";"))
            best, rows = None, 0
            try:
                for _ in range(repeat):
                    start = time.monotonic()
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        rows = cur.fetch_arrow_table().num_rows
                    elapsed = time.monotonic() - start
                    best = elapsed if best is None else min(best, elapsed)
                print(f"{name}\t{best:.2f}\t{rows}", flush=True)
            except Exception as e:  # noqa: BLE001 - every failure is reported, not raised
                print(f"{name}\tERROR\t{str(e).splitlines()[0][:300] if str(e) else type(e).__name__}", flush=True)


if __name__ == "__main__":
    main()
