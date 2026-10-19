# StarRocks Arrow Flight connector — design & status

## TL;DR

| Goal | Route | Status |
|---|---|---|
| Query the Flight server's data from StarRocks **today** | `FILES()` / `INSERT ... SELECT FROM FILES()` reading the **same underlying Parquet files** directly | ✅ tested against a real StarRocks 4.1.4 instance, returns results identical to the Trino plugin |
| Query **through the Arrow Flight SQL protocol** via a generic JDBC catalog | `CREATE EXTERNAL CATALOG ... type="jdbc"` + the Arrow Flight SQL JDBC driver | ❌ **does not work** — StarRocks rejects it, see below (an earlier version of this doc claimed otherwise; that was wrong and untested) |
| A **native `arrow_flight` catalog** with per-endpoint parallel reads over the real protocol | patch FE (`ConnectorType` enum) + new FE ScanNode + **new BE C++ scanner** + Thrift | 🟡 FE metadata side written (`src/starrocks/java/…`); BE scanner not started, out of scope for this project |

## The JDBC route was tried and does not work — corrected here

`com.starrocks.connector.jdbc.JDBCMetadata.createSchemaResolver()` hardcodes a **fixed
allow-list** of driver classes it knows how to speak to: MySQL/MariaDB, PostgreSQL,
Oracle, SQL Server, ClickHouse. Anything else — including
`org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver` — is rejected outright:

```
ERROR 1064 (HY000): org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver not support yet
```

(Confirmed live: `CREATE EXTERNAL CATALOG` succeeds, but every metadata query throws that
error, both from `fe-core-4.1.4.jar`'s `JDBCMetadata.<init>`/`createSchemaResolver` and
from `JDBCConnector`'s own driver-checksum step.) There's no generic/pass-through option —
`JDBCMetadata.createSchemaResolverFromProperty` accepts the same five names and nothing
else. So the JDBC catalog is **not** a viable path to an arbitrary Flight SQL server,
despite superficially looking like one. `jdbc-catalog/` in this directory is kept only as
a record of that dead end (see its README) — do not use it.

## What does work today: read the same Parquet files directly

The Flight server's backing store is plain Parquet on disk. StarRocks has a first-class,
fully supported way to query Parquet files directly — the `FILES()` table function — so
pointing it at the same directory sidesteps the Arrow Flight wire protocol entirely and
just works:

```sql
SELECT * FROM FILES(
  "path"   = "file:///path/to/parquet/demo/t/data.parquet",
  "format" = "parquet"
) WHERE id < 5 ORDER BY id;

-- or materialize it as a regular StarRocks table:
CREATE TABLE t LIKE FILES("path" = "file:///path/to/parquet/demo/t/*.parquet", "format" = "parquet");
INSERT INTO t SELECT * FROM FILES("path" = "file:///path/to/parquet/demo/t/*.parquet", "format" = "parquet");
```

Verified: `count(*)` = 50000 and the sampled rows are byte-identical to what the Trino
plugin returns for the same table over the actual Flight protocol. See
`direct-parquet-access/` for the tested commands. The honest caveat: this is **not**
going through Arrow Flight SQL at all — no projection/predicate pushdown to the Flight
server, no DuckDB execution, just StarRocks reading Parquet itself. For deployments where
the Flight server's storage (local disk / HDFS / S3) is reachable from StarRocks BE nodes,
it is nonetheless the practical way to get this data into StarRocks today.

## What a native connector needs (unchanged, still out of scope)

1. **FE (Java)** — written here, compiles against `com.starrocks:fe-core` with `-Pstarrocks`:
   * `ArrowFlightConnector implements com.starrocks.connector.Connector` — reads catalog
     `PROPERTIES`, owns one `FlightSqlSession` (from `arrow-flight-common`).
   * `ArrowFlightMetadata implements com.starrocks.connector.ConnectorMetadata` —
     `listDbNames` → `listSchemas`, `listTableNames` → `listTables`,
     `resolveColumns` maps the Arrow result schema to `com.starrocks.catalog.Type`.
   * `patch/ConnectorType.java.patch` registers `ARROW_FLIGHT("arrow_flight", …)`.
2. **FE (Java)** — *not done*: an `ArrowFlightTable extends com.starrocks.catalog.Table`
   and an `ArrowFlightScanNode` that emits one `TScanRangeLocations` per Flight endpoint,
   each carrying the opaque ticket bytes + node locations. `FlightScanPlanner`
   (`src/main/java/…`, engine-neutral, compiles today) already produces exactly that data.
3. **Thrift** — *not done*: a `TArrowFlightScanRange { binary ticket; list<string> locations; }`
   message and a `TArrowFlightScanNode`, wired through `PlanNode`.
4. **BE (C++)** — *not done, the bulk of the work*: a `ArrowFlightScanner` on the scan
   path that connects with Arrow Flight C++, calls `DoGet(ticket)`, and feeds the
   resulting `arrow::RecordBatch`es into a StarRocks `Chunk`. This is a genuine multi-day
   C++ engineering effort in an unfamiliar codebase — explicitly not attempted here.

Pushdown (projection + `WHERE`) is rendered by `FlightScanPlanner` (DuckDB dialect, same
limits as the Trino side: no server-side `ORDER BY` / `LIMIT`), ready for whenever the BE
scanner exists.

## Files

```
starrocks-arrow-flight/
├── src/main/java/…/FlightScanPlanner.java     engine-neutral scan planner (compiles in the default build)
├── src/starrocks/java/com/starrocks/connector/arrowflight/
│   ├── ArrowFlightConnector.java              FE connector      (only with -Pstarrocks)
│   └── ArrowFlightMetadata.java               FE metadata bridge (only with -Pstarrocks)
├── patch/ConnectorType.java.patch             enum registration + how-to
├── direct-parquet-access/                     the route that actually works today
├── jdbc-catalog/                               dead end, kept only as a record — see its README
└── DESIGN.md
```

## Build

```bash
# engine-neutral part only (default reactor excludes this module):
mvn -f starrocks-arrow-flight/pom.xml package

# with the FE-side classes — needs com.starrocks:fe-core in ~/.m2 (build StarRocks first):
mvn -f starrocks-arrow-flight/pom.xml -Pstarrocks package
```
