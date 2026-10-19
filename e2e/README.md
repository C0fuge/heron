# End-to-end test — the "lab" model

One privileged outer container (`quay.io/podman/stable`) runs its **own** podman; the
Flight SQL server and Trino run as **nested containers inside it**. The host podman only
ever sees the `lab` container — nothing else, no ports published to the host.

```
host podman
└── lab  (quay.io/podman/stable, --privileged, --device /dev/fuse)   ← nested podman
    ├── flight-server  (eclipse-temurin:21-jdk + VadimSurpin/arrow-flight-server jar)
    └── trino-e2e      (trinodb/trino:455 + this plugin)   on nested network 'flightnet'
```

## Run

```bash
export FLIGHT_SRC=/path/to/arrow-flight-server   # checkout with target/ jar + libs built (JDK 21)
export DATA=/path/to/parquet-root                 # contains demo/t/data.parquet
./run.sh
```

`run.sh` (host side): unpacks the plugin zip, `podman save`s `trino:455` + `temurin:21-jdk`
to tar (so the lab loads them offline), starts the lab, then `podman exec lab sh /lab/inside.sh`.

`lab-inside.sh` (inside the lab): `podman load` the tars, create a nested network, run
`flight-server` and `trino-e2e`, run the query suite.

Query the running stack:
```bash
podman exec lab podman exec -i trino-e2e trino --execute "SELECT * FROM flight.demo.t LIMIT 5"
```
Tear down: `podman rm -f lab` (removes the lab and everything nested in it).

## What it proves (observed 2026-09, run fully nested)

| Check | Result |
|---|---|
| plugin loads into Trino 455, catalog `flight` registers | ✅ |
| `SHOW SCHEMAS FROM flight` | `demo`, `information_schema` |
| `SHOW TABLES` / `DESCRIBE` | `t`; 6 cols, Arrow→Trino types (`integer/boolean/bigint/double/varchar/date`) |
| `SELECT count(*)` | `50000` |
| projection + predicate pushdown | `EXPLAIN` = `Output → TableScan`, **no `Filter` node**; only projected columns in the scan layout |
| `GROUP BY` + `sum` + `ORDER BY` + `LIMIT`, `avg/min/max` + `WHERE bool = true` | correct |

## Gotchas found while wiring this up

* `trinodb/trino:455` runs **JDK 22** → the plugin must be compiled `--release <= 22`.
* The build host / image needs **JDK 25** → the `trino-maven-plugin` is compiled for Java 25.
* The Flight server container needs `--add-opens=java.base/java.nio=ALL-UNNAMED` or its
  Arrow `MemoryUtil` fails on the data path.
* The server parses SQL with regexes → identifiers must be **unquoted**
  (`arrow-flight.identifier-quote` defaults to empty).
