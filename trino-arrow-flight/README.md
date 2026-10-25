# Trino Arrow Flight SQL connector

A read-only Trino connector for the **Hadoop Arrow Flight SQL server**
(<https://github.com/VadimSurpin/arrow-flight-server>).

It lets you run

```sql
SELECT c_region, sum(o_totalprice)
FROM flight.tpch.orders
WHERE o_orderdate >= DATE '1995-01-01'
GROUP BY c_region;
```

from Trino, while the scan itself is pushed to the Flight server (DuckDB over Parquet),
streamed back as Arrow record batches, and turned into Trino pages with no row-by-row
JDBC serialization in between.

## Why a connector and not JDBC

The Flight server already speaks the columnar wire format Trino wants. Every
`FlightEndpoint` it returns from `GetFlightInfo` is one Arrow stream from the Flight node that
owns the data. A scan becomes one Trino split, or several key-range splits for large tables
(`arrow-flight.split-count`); each split plans its query on the worker (so join dynamic filters
can be added) and reads that query's endpoints, one after another or several at a time
(`arrow-flight.endpoint-read-parallelism`).

## How it maps onto Trino's SPI

| Trino SPI piece | Class | What it does |
|---|---|---|
| `Plugin` | `ArrowFlightPlugin` | registers the connector factory |
| `ConnectorFactory` | `ArrowFlightConnectorFactory` | name `arrow_flight`, boots Guice |
| config | `ArrowFlightConfig` | catalog properties (mirrors the Spark connector options) |
| Flight access | `ArrowFlightClient` | wraps `FlightSqlClient`: handshake (BasicAuth / bearer), `getSchemas`, `getTables`, `getExecuteSchema`, `execute`, `getStream`, endpoint routing, retry/backoff |
| `ConnectorMetadata` | `ArrowFlightMetadata` | schema/table listing, column resolution via `getExecuteSchema`, `applyProjection`, `applyFilter` |
| `ConnectorSplitManager` | `ArrowFlightSplitManager` | one split per scan, or key-range splits on an integer/date column for large scans |
| `ConnectorSplit` | `ArrowFlightSplit` | the range predicate, if any (re-runnable) |
| `ConnectorPageSourceProvider` / `ConnectorPageSource` | `ArrowFlightPageSource*` | builds `SELECT … WHERE …` (plus dynamic filters), calls `GetFlightInfo`, reads the endpoints' streams (`ParallelStreamReader` when several at a time), converts each `VectorSchemaRoot` to a `Page` |
| Arrow ↔ Trino types | `ArrowTypeConverter` | `BIT→BOOLEAN`, int family, `FLOAT4→REAL`, `FLOAT8→DOUBLE`, `DECIMAL(256)→DECIMAL`, `(LARGE)VARCHAR→VARCHAR`, `BINARY→VARBINARY`, `DATE→DATE`, `TIME→TIME(6)`, `TIMESTAMP→TIMESTAMP(6)` |
| WHERE rendering | `SqlQueryBuilder` | `TupleDomain` → SQL for `BIGINT/INTEGER/SMALLINT/TINYINT/DOUBLE/BOOLEAN/DATE/VARCHAR` |

### Pushdown

* **Projection** – only the referenced columns are put in the `SELECT` list.
* **Filters** – `TupleDomain` on the supported scalar types is translated to a `WHERE`
  clause the server evaluates exactly (DuckDB), so Trino does not re-filter those
  predicates. Anything else stays in Trino as a residual filter.
* **`LIMIT`** – the scan stops reading (and cancels the stream) once enough rows arrived;
  Trino keeps its final `LIMIT`.
* **`ORDER BY … LIMIT n` (TopN)** – off by default (`arrow-flight.topn-pushdown.enabled=true`
  or session property `topn_pushdown_enabled`). When every sort key is a plain column of an
  integer, decimal, boolean, date or timestamp type, the server query gets
  `ORDER BY … LIMIT n`; each endpoint and range split returns its own top `n` rows and Trino
  keeps its TopN over them. `DOUBLE`/`REAL` (NaN order) and `VARCHAR` (collation) keys are
  not pushed.
* **Aggregations** – count/sum/min/max/avg + `GROUP BY` (session property
  `aggregation_pushdown_enabled`).
* **Join dynamic filters** – off by default (`arrow-flight.dynamic-filtering.enabled=true` or
  session property `dynamic_filtering_enabled`). The server query of a scan is built on the
  worker right before the first read; a scan on the probe side of a join waits up to
  `arrow-flight.dynamic-filtering.wait-timeout-ms` (session `dynamic_filtering_wait_timeout_ms`)
  for the build side's keys and adds them to its `WHERE` (an `IN` list of up to
  `arrow-flight.dynamic-filtering.max-values` values, default 1000, otherwise their `[min, max]` range). Not applied under a pushed TopN. Off by default because
  every query then has a different SQL text and the server keeps a prepared statement per
  distinct text (memory grows with the number of queries).

## Build

Built with Maven 3.9.16 on JDK 25 (the `trino-maven-plugin` is compiled for Java 25),
targeting `--release 22` (the JDK the `trinodb/trino:455` image runs). Reproducible build
in a container:

```bash
podman build -t arrow-flight-connectors -f ../Containerfile ..
podman run --rm -v "$PWD/..":/app:z -w /app -v flight-m2:/root/.m2 \
    arrow-flight-connectors mvn -B clean verify
```

or on a host with JDK >= 25:

```bash
mvn clean package                       # whole repo
mvn -pl trino-arrow-flight -am package  # just this connector + arrow-flight-common
```

Produces `target/trino-arrow-flight-0.1.0-SNAPSHOT.zip` — the plugin jar plus every
runtime jar it needs (`trino-maven-plugin` builds it and generates the
`META-INF/services/io.trino.spi.Plugin` descriptor).

### Trino versions

One source tree builds for three Trino releases; a Maven profile picks the SPI version, the
Java release and a small per-SPI source root (`src/trino-<version>/java`: page source, page
source provider, split manager, block builders). **455 is the default.**

| Trino | Command | Target JDK | SPI differences handled in `src/trino-<v>/java` |
|---|---|---|---|
| 455 | `mvn clean verify` (= `-P trino-455`) | 22 | `getNextPage()` |
| 477 | `mvn -P trino-477 clean verify` | 24 | `getNextSourcePage()` / `SourcePage` |
| 483 | `mvn -P trino-483 clean verify` | 25 | as 477, plus `getSplits(..., Set<ColumnHandle>, ...)`, `createPageSource(..., Optional<ConnectorTableCredentials>, ...)`, null masks as validity bitmaps; bundles Jackson Blackbird 2.22.1 (Trino 483 needs it in the plugin classloader) |

Every build needs JDK 25. Install the zip of the matching profile only: a 455 build does not
load in 477/483 and vice versa. CI builds all three on request (`build` workflow,
`all-versions`) and runs the end-to-end suite with input `trino` = 455 / 477 / 483. Trino 483 no
longer has the `enable_large_dynamic_filters` session property (the e2e script passes it only
where `SHOW SESSION` lists it).

Bundled dependencies are pinned in the root `pom.xml` (Netty 4.1.139, gRPC 1.78.0, protobuf
3.25.9, Jackson 2.18.12 — 2.22.1 for 483). gRPC cannot go past 1.78 while Arrow is 18.1 (gRPC 1.79
removed an internal method Arrow's Flight client calls). The CI build compares the jars in each zip
with `bundle/trino-<version>.txt` (`bundle/check-bundle.sh`) and fails on any difference, so a
version change has to update that list in the same commit.

## Install

```bash
mkdir -p $TRINO_HOME/plugin/arrow-flight
unzip -j target/trino-arrow-flight-0.1.0-SNAPSHOT.zip -d $TRINO_HOME/plugin/arrow-flight
cp etc/catalog/flight.properties.example \
      $TRINO_HOME/etc/catalog/flight.properties   # then edit host/port/auth
```

Arrow needs a couple of JVM flags; add to `etc/jvm.config` if not already present
(`--sun-misc-unsafe-memory-access=allow` only on JDK >= 23, i.e. not the 455 image):

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--enable-native-access=ALL-UNNAMED
-Darrow.enable_unsafe_memory_access=true
```

Restart Trino, then:

```sql
SHOW SCHEMAS FROM flight;
SHOW TABLES FROM flight.test_schema;
SELECT * FROM flight.test_schema.test_table LIMIT 10;
```

## Verified end-to-end

`../e2e/` runs the whole chain in podman containers: the reference Flight SQL server (from
its own repo, built for JDK 21, fed a DuckDB-generated Parquet file) + `trinodb/trino:455`
with this plugin. `SHOW SCHEMAS/TABLES`, `DESCRIBE`, `count(*)`, projection + predicate
pushdown (`EXPLAIN` shows no residual `Filter` node), `GROUP BY`, and `avg/min/max` all
return correct results against 50k rows.

## Catalog properties

| Property | Default | Notes |
|---|---|---|
| `arrow-flight.host` | – | required unless `arrow-flight.hosts` is set |
| `arrow-flight.hosts` | – | instead of `arrow-flight.host`: comma-separated coordinators, `host` or `host:port` (`[ipv6]:port`), default port `arrow-flight.port`. Tried in order: the catalog stays on the one it reached and moves to the next only when it cannot be reached (refused, unresolved, UNAVAILABLE, or no handshake answer within 2 x `connect-timeout-millis`); a server or credentials error is never retried elsewhere. With several hosts the coordinator connection also sends keepalive pings (`keepalive-millis`, answered within `connect-timeout-millis`), so a coordinator that vanishes during a call is given up within ~15 s |
| `arrow-flight.port` | `32010` | |
| `arrow-flight.user` | – | BasicAuth user |
| `arrow-flight.password` | – | BasicAuth password (secret) |
| `arrow-flight.bearer-token` | – | alternative to user/password (secret) |
| `arrow-flight.default-schema` | – | sent as the `SCHEMA` Flight header; also fallback for `SHOW SCHEMAS` |
| `arrow-flight.identifier-quote` | *(empty)* | quote char around schema/table/column names in generated SQL. The reference server parses SQL with regexes and needs **bare** names, so the default is none; set to `"` for a server with a real parser |
| `arrow-flight.tls.enabled` | `false` | |
| `arrow-flight.tls.verify-server` | `true` | ignored when a truststore is given |
| `arrow-flight.tls.truststore-path` | – | `.jks` or PEM |
| `arrow-flight.tls.truststore-password` | – | secret, `.jks` only |
| `arrow-flight.routing-tag` / `arrow-flight.routing-queue` | – | workload-management headers |
| `arrow-flight.max-retries` | `3` | |
| `arrow-flight.retry-backoff-millis` | `1000` | exponential |
| `arrow-flight.request-timeout-millis` | `0` | `0` = no deadline |
| `arrow-flight.connect-timeout-millis` | `5000` | connect timeout of a data stream, per endpoint location. When a location refuses, times out or drops the connection **before any data was read**, the endpoint's next location (a backup node the server lists, server fo1+) is read instead; after the first batch a failure is final (no duplicate rows). A stream on a pooled connection that fails this way is first retried once on a new connection to the same location (the pooled one may be stale: node restarted); with a node that stays down this can add one connect timeout to the first such stream `0` = Netty default (30 s) |
| `arrow-flight.keepalive-millis` | `10000` | keepalive pings on data streams of servers that list backup locations (they accept pings every 5 s); a node that does not answer within the connect timeout counts as gone, so a vanished node costs seconds, not TCP's ~15 min. `0` = no pings |
| `arrow-flight.dynamic-filtering.enabled` | `false` | add join dynamic filters to the server SQL (see Pushdown) |
| `arrow-flight.dynamic-filtering.wait-timeout-ms` | `10000` | how long a probe-side scan waits for them; `0` = do not wait |
| `arrow-flight.dynamic-filtering.max-values` | `1000` | largest `IN` list a dynamic filter becomes (session `dynamic_filtering_max_values`); more values are sent as their `[min, max]` range |
| `arrow-flight.split-count` | `4` | range splits per large plain scan (session `split_count`); each split is one server query, `1` = one query per scan |
| `arrow-flight.split-min-rows` | `1000000` | no range split smaller than this many rows (after the scan's filters, estimated) |
| `arrow-flight.split-columns` | *(empty)* | range-split column per table, e.g. `tpch.lineitem=l_orderkey`; default: the first integer/date column |
| `arrow-flight.endpoint-read-parallelism` | `1` | endpoints of one split read at the same time (session `endpoint_read_parallelism`). The server answers each split's query with one endpoint per Flight node; `1` reads them one after another, so a split keeps one node busy at a time. Plain scans only; failover rules per endpoint are unchanged |
| `arrow-flight.aggregation-sorted-merge.enabled` | `true` | grouped aggregation pushed to a cluster: each Flight node returns its own partial groups, which the plugin merges. With this on, the server SQL gets `ORDER BY <group columns> NULLS FIRST` and the plugin merges the endpoints as sorted streams (one group in memory, pages emitted as they are merged); off = all groups collected in one hash table first (slow and memory-hungry with millions of groups, e.g. `GROUP BY l_orderkey` in TPC-H q04/q18). Only for GROUP BY columns of integer/decimal/boolean/date/timestamp(6) type; skipped once a query saw a single endpoint. A stream that arrives out of order fails the query (session `aggregation_sorted_merge_enabled`) |

### Iceberg time travel

`SELECT … FROM flight.ice.t FOR VERSION AS OF <snapshot id>` and
`… FOR TIMESTAMP AS OF TIMESTAMP '2026-10-25 10:00:00 UTC'` are forwarded to the server after the table
name (`FROM ice.t FOR VERSION AS OF 123` / `FOR TIMESTAMP AS OF TIMESTAMP '2026-10-25 10:00:00.000000 UTC'`)
in every query of that scan (columns, statistics, splits, scans, aggregations), so the columns and rows
are those of that snapshot. Needs a server build with Iceberg time travel (from `c3b276054cc3`); a
timestamp without zone is read in the session time zone, as in Trino's Iceberg connector. Branch or
tag names (`FOR VERSION AS OF 'main'`) and version ranges are refused. On a table that is not an
Iceberg table the server answers with an error.

## Limitations / TODO

* Read-only. No `INSERT` / `CREATE TABLE` (the server's writer path is experimental).
* `TIMESTAMP WITH TIME ZONE` is read as `TIMESTAMP(6)` (zone dropped).
* `REAL` predicates are not pushed (kept as Trino residual filters); they still return
  correct results.
* Schema/table discovery relies on the server implementing Flight SQL
  `CommandGetDbSchemas` / `CommandGetTables`; otherwise set `arrow-flight.default-schema`
  and address tables directly.
* Not yet wired to `trino-testing` / `AbstractTestQueryFramework`; only
  `SqlQueryBuilderTest` runs offline.
