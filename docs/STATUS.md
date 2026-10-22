# Status

Kept up to date after every work session so work can be resumed from this repository alone.

## Context

- Production-like target: Trino **455** (JDK 23 runtime; the stand runs JDK 25 with
  `--add-modules=jdk.incubator.vector --add-opens=java.base/java.nio=ALL-UNNAMED`), offline deployment. Builds for
  Trino **477** and **483** are wanted too (`dep.trino.version`).
- Current released plugin = **v10** (commit 6ee7b0e + 07676d4 here): endpoint failover (try an endpoint's
  locations in order, switch only before any data was delivered and only on "unreachable" errors; never on
  server errors), headers `x-flight-result-mode: partial` + `x-flight-location-failover: 1`, connect timeout 5 s,
  keepalive 10 s, statistics from `SELECT count(*)` (+ min/max) answered by the server from Parquet footers,
  aggregate/filter/limit pushdown via `SqlQueryBuilder`/`ExpressionTranslator`.
- Validated on the maintainer's stand: TPC-H SF10 22/22 (checksum 05bc0dc2713a) on single node and 3-node
  clusters, plugin v9/v10 against server builds fix8–fix12, s3p4, tu1/to1; Trino 455.
- Server candidate for the next release: **int4** (branch integration-2: DuckDB 1.4.5, S3/lakeFS as plugins,
  cluster fixes for "losing 2 of 3 members returns a partial table" and "Invalid Flight endpoint ticket with token
  auth on a cluster"). Both bugs were seen through this plugin on the old server; they were server bugs.

## Invariants (must not regress)

- Never return wrong or partial results silently: failover only before the first batch; server errors surface.
- Token / bearer / basic auth and TLS options keep working (`flight.properties.example`).
- Works against the old server builds too (s3p4 behaviour: one location per endpoint).

## CI inputs

- Release `e2e-assets` (download with `gh release download e2e-assets`): `arrow-flight-server-1.0.0-int4.jar`,
  `int4-plugins.tar.zst` (`plugins/s3`, `plugins/lakefs`, **START-NOTES.txt — how to start the server**),
  `flight-server-libs.tar.zst` (`libs/`, on the classpath next to the jar), `test-data-sf1-types.tar.zst`
  (`sf1/tpch/<table>/*.parquet`, `types/<table>/*.parquet`), `SHA256SUMS`.
- In the repo: `e2e/ci/queries/q01..q22.sql` (TPC-H as run on the stand, schema `tpch`),
  `e2e/ci/reference/sf1/` (DuckDB 1.4.1 answers on the same SF1 files: `qNN.tsv`, `qNN.cols`, `MANIFEST.tsv`).
- Server start (S3 mode through MinIO is the simplest in CI; see START-NOTES): JDK 21, `LANG=C.UTF-8`,
  `-cp "<jar>:libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer --localhost <host> --hosts <all>`,
  `-DdataDir=s3://lake/parquet -Ds3Endpoint=http://minio:9000 -Darrowflight.plugins.dir=<dir>/plugins`,
  httpfs **v1.4.5** (`http://extensions.duckdb.org/v1.4.5/linux_amd64/httpfs.duckdb_extension.gz`, keep the
  file name `httpfs.duckdb_extension`), MinIO credentials via `FLIGHT_S3_ACCESS_KEY`/`FLIGHT_S3_SECRET_KEY`
  (throwaway, generated in the job), auth `FLIGHT_ALLOW_INSECURE=true` or
  `FLIGHT_AUTH_PROVIDER=token FLIGHT_AUTH_TOKEN=<t>`, `-DresultMode=partial|complete`. Two or three servers
  (`--hosts a,b,c`) form a cluster with backup locations.
- Trino: `trinodb/trino:455` (and 477/483) image, plugin dir mounted at `/usr/lib/trino/plugin/arrow-flight`,
  catalog `connector.name=arrow_flight`, JVM flags from README.

## Next steps

1. **CI**: workflow `build.yml` (Maven build + unit tests on push, Trino 455; matrix 477/483 on request) and
   `e2e.yml` (manual + on plugin changes: MinIO + 1 server + Trino 455; TPC-H SF1 22/22 vs `e2e/ci/reference/sf1`
   with the same normalisation as the reference generator — decimals rounded to 4 places, sort unless ORDER BY;
   types table `SELECT *` vs DuckDB; token auth on; then a 3-server variant with a node killed before a query
   (must still answer via backup) and during a read (clean error, never wrong rows)). Results summary to
   `docs/E2E-RESULTS.md`.
2. **Connection reset after one failed stream**: today one failed stream makes the plugin drop all its pooled
   connections (small inefficiency). Keep healthy connections; unit test.
3. **`ORDER BY … LIMIT` (TopN) pushdown** to the server (`applyTopN`): render `ORDER BY … LIMIT n` in
   `SqlQueryBuilder` when all sort keys are pushable columns; in partial mode each endpoint returns its own top n
   and Trino keeps its final TopN (do not report the TopN as fully guaranteed then); complete mode can guarantee
   it. Unit + e2e (TPC-H q03/q10/q18-style queries, results unchanged, fewer rows read).
4. **Dynamic filters**: review how dynamic filters are used (wait time, pushing IN-lists / ranges into the
   remote SQL, size limits). Measure on e2e SF1 joins (q05, q09); keep correct results.
5. **Split count**: review the number of splits per endpoint / per query against server nodes and Trino workers.
6. **Trino 477 and 483 builds**: make the plugin compile and pass e2e on both (SPI changes); document per-version
   build commands; keep 455 the default.

## Open questions

(none yet)

## Log

- 2026-10-22 (maintainer's local session): repo created from the local project (HEAD 07676d4 = plugin v10 +
  failover test fix); CI inputs added; e2e assets uploaded.
