# Arrow Flight SQL connectors

Query-engine connectors for the **Hadoop Arrow Flight SQL server**
(<https://github.com/VadimSurpin/arrow-flight-server>) — a server that runs SQL over
Parquet with DuckDB and hands results back as Apache Arrow record batches.

This is a Maven multi-module build:

| Module | Type | Status |
|---|---|---|
| `arrow-flight-common` | plain library | ✅ engine-neutral Flight client (`FlightConfig`, `FlightSqlSession`) |
| `trino-arrow-flight` | **Trino plugin** | ✅ working read-only connector |
| `starrocks-arrow-flight` | **StarRocks connector** | 🟡 skeleton + design only; see its `DESIGN.md` |

```
arrow-flight-connectors
├── arrow-flight-common/     shared: connect, handshake, list schemas/tables,
│                            resolve result schema, get endpoints, open Arrow streams
├── trino-arrow-flight/      Trino SPI plugin  (Plugin → Connector → Metadata/SplitManager/PageSource)
└── starrocks-arrow-flight/  StarRocks com.starrocks.connector.* skeleton (built only with -Pstarrocks)
```

## Build

```bash
# Trino plugin + shared library (StarRocks module is excluded by default)
mvn clean package

# result: trino-arrow-flight/target/trino-arrow-flight-0.1.0-SNAPSHOT.zip  (plugin + all runtime jars)
```

Needs JDK 25 to build (the `trino-maven-plugin` is compiled for Java 25); the default profile targets
Trino `455` (`--release 22`). Trino 477 and 483: `mvn -P trino-477 clean package` / `mvn -P trino-483 clean package`
(see `trino-arrow-flight/README.md`, "Trino versions").

## Install into Trino

```bash
mkdir -p $TRINO_HOME/plugin/arrow-flight
unzip -j trino-arrow-flight/target/trino-arrow-flight-0.1.0-SNAPSHOT.zip -d $TRINO_HOME/plugin/arrow-flight
cp trino-arrow-flight/etc/catalog/flight.properties.example \
      $TRINO_HOME/etc/catalog/flight.properties      # then edit host / port / auth
```

Add to `$TRINO_HOME/etc/jvm.config` (Arrow needs off-heap access):

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--sun-misc-unsafe-memory-access=allow
-Darrow.enable_unsafe_memory_access=true
```

Then:

```sql
SHOW SCHEMAS FROM flight;
SHOW TABLES FROM flight.test_schema;
SELECT * FROM flight.test_schema.test_table LIMIT 10;
```

## What each connector does

Both do the same thing, mapped onto two different plugin APIs:

1. **List** schemas/tables via Flight SQL `CommandGetDbSchemas` / `CommandGetTables`.
2. **Resolve columns** with `getExecuteSchema` (`SELECT ... ` schema without fetching rows),
   then map Arrow types to engine types.
3. **Plan a scan**: render `SELECT <needed cols> FROM <schema>.<table> WHERE <pushed predicate>`,
   send it in `GetFlightInfo`, and turn **each returned `FlightEndpoint` into one split /
   scan range** (opaque ticket + node locations).
4. **Read**: each worker opens the Arrow stream for its ticket and converts every
   `VectorSchemaRoot` batch into the engine's columnar page/chunk.
5. **Pushdown**: column pruning + simple `WHERE` predicates (the server evaluates them
   exactly in DuckDB), aggregations, `LIMIT` (connector-side) and, optionally, `ORDER BY … LIMIT n`
   (see `trino-arrow-flight/README.md`).

See `trino-arrow-flight/README.md` and `starrocks-arrow-flight/DESIGN.md` for the details.

## Local toolchain

`../tools/` (created by this session) holds a private Maven, the Trino CLI, the Trino
server tarball, and a shallow StarRocks source checkout — none of it is needed to just
`mvn package`, only to actually run the engines.
