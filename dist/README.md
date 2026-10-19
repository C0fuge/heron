# Arrow Flight SQL connectors — Trino & StarRocks

Connectors for the Hadoop Arrow Flight SQL server
(<https://github.com/VadimSurpin/arrow-flight-server>).

## Trino — `trino-arrow-flight/` — fully working, tested end-to-end

Read-only Trino connector: schema/table discovery, column-projection and predicate
pushdown, one split per Flight endpoint. Verified against a real Flight server and a real
Trino 455: `SHOW SCHEMAS/TABLES`, `DESCRIBE`, aggregates, `EXPLAIN` confirming pushdown (no
residual `Filter` node) — all correct on a 50k-row table.

**The deployable artifact is already built — no Maven needed to install it:**
`trino-arrow-flight/plugin/` (the connector jar + all ~86 runtime dependency jars, ready to drop in).

```bash
mkdir -p $TRINO_HOME/plugin/arrow-flight
cp trino-arrow-flight/plugin/*.jar $TRINO_HOME/plugin/arrow-flight/
cp trino-arrow-flight/etc/catalog/flight.properties.example $TRINO_HOME/etc/catalog/flight.properties
# edit host/port/auth, then restart Trino
```

Full details, JVM flags, catalog properties, version-coupling notes: `trino-arrow-flight/README.md`.

To rebuild from source: `mvn clean package` at this directory's root (needs a JDK matching
your target Trino's SPI — `trino-arrow-flight/README.md` has the specifics).

## StarRocks — `starrocks-arrow-flight/` — honest status, one route tested and working

**A connector that speaks the Arrow Flight SQL wire protocol to StarRocks does not exist
and cannot be built without patching StarRocks' C++ backend** — StarRocks has no plugin
SPI (connectors are a hardcoded Java enum) and the actual row-scanning always happens in
BE (C++), which would need a new native scanner. That's real, scoped, multi-day work in an
unfamiliar codebase, not something to fake.

What **is** tested and working today, against a real StarRocks 4.1.4: StarRocks reading
the exact same Parquet files the Flight server serves, directly, via StarRocks' own
`FILES()` table function — bypassing the Flight protocol, but returning results identical
to what the Trino plugin returns over the real protocol. See
`starrocks-arrow-flight/direct-parquet-access/query.sql`.

A generic JDBC external catalog (`type="jdbc"` + the Arrow Flight SQL JDBC driver) was also
tried and **does not work** — StarRocks hardcodes its JDBC catalog to a fixed list of
drivers (MySQL, PostgreSQL, Oracle, SQL Server, ClickHouse) and rejects anything else.
Kept as `starrocks-arrow-flight/jdbc-catalog/` only as a record of that dead end.

`starrocks-arrow-flight/DESIGN.md` has the full picture, including the FE-side Java
connector skeleton (`src/starrocks/java/…`) and the enum patch (`patch/`) that are a
correct starting point for the native route, and exactly what BE work remains.
