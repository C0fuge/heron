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
`FlightEndpoint` it returns from `GetFlightInfo` becomes **one Trino split**, so a
multi-node Flight cluster is read in parallel, each Trino worker pulling Arrow batches
straight from the Flight node that owns the data (locality hints are passed through as
split addresses).

## How it maps onto Trino's SPI

| Trino SPI piece | Class | What it does |
|---|---|---|
| `Plugin` | `ArrowFlightPlugin` | registers the connector factory |
| `ConnectorFactory` | `ArrowFlightConnectorFactory` | name `arrow_flight`, boots Guice |
| config | `ArrowFlightConfig` | catalog properties (mirrors the Spark connector options) |
| Flight access | `ArrowFlightClient` | wraps `FlightSqlClient`: handshake (BasicAuth / bearer), `getSchemas`, `getTables`, `getExecuteSchema`, `execute`, `getStream`, endpoint routing, retry/backoff |
| `ConnectorMetadata` | `ArrowFlightMetadata` | schema/table listing, column resolution via `getExecuteSchema`, `applyProjection`, `applyFilter` |
| `ConnectorSplitManager` | `ArrowFlightSplitManager` | builds `SELECT … WHERE …`, calls `GetFlightInfo`, one split per endpoint |
| `ConnectorSplit` | `ArrowFlightSplit` | self-contained ticket + location URIs (re-runnable) |
| `ConnectorPageSourceProvider` / `ConnectorPageSource` | `ArrowFlightPageSource*` | opens the Flight stream for a ticket, converts each `VectorSchemaRoot` to a `Page` |
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

### Version coupling

`dep.trino.version` in the root `pom.xml` is **455**. For a different Trino, set it to your
version and re-check two signatures that have drifted across releases:

* `ArrowFlightMetadata.getTableHandle(...)` – the version-argument overload.
* `ArrowFlightPageSourceProvider.createPageSource(...)` – newer Trino adds a
  `Constraint` parameter.

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
| `arrow-flight.host` | – | required |
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
| `arrow-flight.connect-timeout-millis` | `5000` | connect timeout of a data stream, per endpoint location. When a location refuses, times out or drops the connection **before any data was read**, the endpoint's next location (a backup node the server lists, server fo1+) is read instead; after the first batch a failure is final (no duplicate rows). `0` = Netty default (30 s) |
| `arrow-flight.keepalive-millis` | `10000` | keepalive pings on data streams of servers that list backup locations (they accept pings every 5 s); a node that does not answer within the connect timeout counts as gone, so a vanished node costs seconds, not TCP's ~15 min. `0` = no pings |

## Limitations / TODO

* Read-only. No `INSERT` / `CREATE TABLE` (the server's writer path is experimental).
* `TIMESTAMP WITH TIME ZONE` is read as `TIMESTAMP(6)` (zone dropped).
* `REAL` and `DECIMAL` predicates are not pushed (kept as Trino residual filters);
  they still return correct results.
* No aggregation / `LIMIT` pushdown yet.
* Schema/table discovery relies on the server implementing Flight SQL
  `CommandGetDbSchemas` / `CommandGetTables`; otherwise set `arrow-flight.default-schema`
  and address tables directly.
* Not yet wired to `trino-testing` / `AbstractTestQueryFramework`; only
  `SqlQueryBuilderTest` runs offline.
