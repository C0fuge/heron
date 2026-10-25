"""Runs SQL statements directly against a Flight SQL server (ADBC: GetFlightInfo + DoGet, no schema call
first) and prints one line per statement: rows or the first line of the error. Diagnostics only.

usage: FLIGHT_TOKEN=... python flight_probe.py grpc://host:port 'SQL' ['SQL' ...]
"""
import os
import sys

import adbc_driver_flightsql.dbapi as flightsql


def main():
    uri, statements = sys.argv[1], sys.argv[2:]
    kwargs = {"adbc.flight.sql.authorization_header": "Bearer " + os.environ["FLIGHT_TOKEN"]}
    with flightsql.connect(uri, db_kwargs=kwargs) as conn:
        for sql in statements:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    table = cur.fetch_arrow_table()
                rows = table.to_pylist()
                shown = rows if len(rows) <= 3 else f"{len(rows)} rows"
                print(f"probe\tOK\t{sql}\t{shown}")
            except Exception as e:  # noqa: BLE001 - every failure is reported, not raised
                print(f"probe\tERROR\t{sql}\t{str(e).splitlines()[0][:600] if str(e) else type(e).__name__}")


if __name__ == "__main__":
    main()
