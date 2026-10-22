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
- CI implementation: `.github/workflows/build.yml` (push; `mvn -P trino-455 clean verify`, JDK 25; 477/483 via
  workflow_dispatch `all-versions`), `.github/workflows/e2e.yml` → `e2e/ci/run-e2e.sh` + `e2e/ci/compare.py`
  (push on plugin/e2e changes, matrix 1 and 3 servers; workflow_dispatch inputs trino / result-mode / servers).
  Every run also compares schema `types` (`e2e/ci/types_check.py`, DuckDB 1.4.1 + trino client from pip in the
  job) and, with 3 servers, runs the failover checks (see `docs/E2E-RESULTS.md`). Input `server-build` = int4|int5.
  MinIO is no longer distributed (Docker Hub repo gone, quay.io unauthorized, release binaries 410), so S3 is
  `rclone/rclone:1.68.2 serve s3`. The server needs a writable working directory (DuckDB `.tmp`). The release
  is a draft, so the e2e job needs `contents: write` to download assets. Job logs: `gh api .../logs` is blocked
  by the session proxy (blob storage); the GitHub MCP `get_job_logs` tool works; artifacts can't be downloaded
  from the session.
- Server start (S3 mode; see START-NOTES, printed at the start of every e2e job): JDK 21, `LANG=C.UTF-8`,
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

1. **Server int5 with Iceberg** (release asset `arrow-flight-server-1.0.0-int5.jar` sha256 01feeb77…5808 +
   `int5-plugins.tar.zst` sha256 f600585d…5f68 with `plugins/{s3,lakefs,iceberg}` and START-NOTES.txt = int4 + catalog
   plugin `iceberg`: kinds `iceberg-hadoop` (warehouse dir on local/HDFS/S3) and `iceberg-rest` (Gravitino/Polaris);
   `-Dcatalog=` list; partial mode pins the snapshot per query, falls back to a complete answer with delete files /
   nested schema changes / joins). Add an Iceberg variant to CI: generate Iceberg tables in the job (e.g. pyiceberg
   with a local SQL catalog writing parquet into the S3 bucket, or Spark/iceberg if simpler): TPC-H SF1 lineitem/orders
   as Iceberg + a table with position/equality deletes + a renamed column; check the engine sees them, results equal
   the parquet tables / DuckDB, partial and complete modes; a commit between two queries is seen by the second only.
   The e2e job already takes `server-build=int5` (workflow input / `SERVER_BUILD`); int5 baseline on parquet is green (run 39841317139).
   Plan: an `iceberg` step in `run-e2e.sh` behind `SERVER_BUILD=int5`, pyiceberg (pinned) + SqlCatalog (sqlite) writing
   to `s3://lake/iceberg` via rclone, server started with the catalog flags from START-NOTES; answers compared with
   `types_check.py`-style DuckDB references.
1b. **e2e: a kill in the middle of a stream** that already delivered data (must be a clean error, never fewer rows);
   all kills so far landed before the first batch (needs a larger/slower read, e.g. `SELECT *` of lineitem).
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

- e2e S3 is `rclone serve s3` instead of MinIO (MinIO images/binaries are no longer downloadable). OK, or is
  there a MinIO build you want to put into `e2e-assets`?
- The release now holds int4 and int5; the e2e job picks one with `server-build` (default int4, the current
  release candidate). Switch the default to int5 once you consider it the candidate?
- `e2e-assets` is a draft release, so the e2e job runs with `contents: write`. Publishing it as a (private-repo)
  pre-release would let the job use `contents: read`. Your call — no change made.

## Log

- 2026-10-22 (maintainer's local session): repo created from the local project (HEAD 07676d4 = plugin v10 +
  failover test fix); CI inputs added; e2e assets uploaded.
- 2026-10-22 (cloud session 1): item 1 (CI). Added `build.yml` (green: run 39826570714) and `e2e.yml` +
  `e2e/ci/run-e2e.sh` + `e2e/ci/compare.py`. Trino 455, int4, token auth: partial mode 1 server TPC-H SF1 22/22
  (runs 39829220892, 39830281255) and 3 servers 22/22 + failover (flight2 killed before: 5/5 exact; flight3
  killed during q18: exact answer) (run 39830281255); complete mode 1 server 22/22 (run 39831337458).
  q01/q17 differ from DuckDB only by Trino's DECIMAL avg/division scale (comparator accepts, counts them).
  Fixes on the way: MinIO → rclone, writable server cwd, `extensionDir`, pipefail-safe pipes. No plugin code
  changed; no invariant affected.
- 2026-10-22 (cloud session 2): item 1 (CI) finished. a) `e2e/ci/types_check.py`: `types.alltypes` (23 cols) and
  `types.alltypes_full` (25 cols) x 5000 rows equal to DuckDB; interval/list/struct/map not exposed (skipped).
  b) failover checks rebuilt: node lost before (partitioned, then killed) 5+5 exact, but the int4 server never
  hands out a lost node, so it tests server re-planning only; kill during q18 proves plugin failover (3–4 streams
  "failed before any data … reading it from flight1", exact answer); two of three down 5/5 exact.
  c) complete mode on 3 servers 22/22 + failover (run 39836376906). Also fixed e2e broken by the second server jar
  in the release (`server-build` input). Runs: 39835656012 (1 srv), 39836376906 (complete 3 srv),
  39840087384 (partial 1+3 srv, final, green); failing intermediate runs 39836535174, 39837701866, 39838862758
  were the evidence checks being tuned. int5 baseline (parquet only, 1 server, partial): 22/22 + types 2/2
  (run 39841317139). No plugin code changed; no invariant affected.
