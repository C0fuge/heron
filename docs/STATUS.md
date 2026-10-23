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
  job) and, with 3 servers, runs the failover checks (see `docs/E2E-RESULTS.md`). Input `server-build` = int4|int5|int6 (int5+ also run `e2e/ci/iceberg.sh`).
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

> **Use server int6 instead of int5** (2026-10-22, local coordinator): int5 overflows its stack on the first table refresh ~30 s after start when any catalog is configured AND the data dir has a table (then every call fails "Application error processing RPC"). int6 = int5 + fix + catalog plugin `hive` (Hive Metastore; SPI 1.1): assets `arrow-flight-server-1.0.0-int6.jar` sha256 78f06212…8d09, `int6-plugins.tar.zst` sha256 fa870ddb…adaf (plugins s3/lakefs/iceberg/hive + START-NOTES.txt). With the iceberg plugin present, `catalog=hive` needs the iceberg+avro extensions or `hive.icebergTables=skip`.

1. **Iceberg in CI (server int6)** — in progress. Done (session 3): `e2e/ci/iceberg.sh`, on by default for
   `server-build` int5/int6 (`ICEBERG=0|1`): Spark 3.5.3 + Iceberg 1.6.1 (`apache/spark:3.5.3`, runtime jar from Maven
   Central) writes a HadoopCatalog warehouse `$WORK/wh` (namespace `ice`) from the SF1 Parquet; every server mounts it
   at `/wh` (`-Dcatalog=iceberg-hadoop -Darrowflight.iceberg-hadoop.warehouse=/wh -Darrowflight.catalog.localRootsShared=true`,
   DuckDB iceberg+avro v1.4.5, `core-site.xml` with `fs.file.impl` — see Open questions). Checks through Trino: tables
   listed; full copies lineitem/orders/customer = Parquet (row count + per-column `checksum`); rename (`c_comment` →
   `c_note`) OK; TPC-H q01 q03 q04 q06 q12 q13 q18 q22 on schema `ice` = DuckDB reference; a commit between two queries is
   seen by the second only. Green on int6, partial, 1 server: run 39852719842 (TPC-H 22/22 + types 2/2 + Iceberg, deletes KNOWN). Open:
   a) `ice.orders_del` (merge-on-read position deletes): server bug, KNOWN in CI (see Open questions); recheck when a
      fixed server build arrives and turn KNOWN back into FAIL.
   b) done (session 4): int6 complete mode 1 server green (run 39874470743: Iceberg reads the same as partial, 0
      answeredCompletely fallbacks — the iceberg_scan refusal only hits `orders_del`'s view, the others are read
      from their data files); int6 partial 3 servers green incl. failover on Iceberg tables (run 39878210446).
      int6 complete mode 3 servers green (session 6, run 39886319914, 14 min).
   c) maybe an S3 warehouse (`s3://lake/parquet/...` under the dataDir) instead of the local one, if Spark+s3a is cheap
      enough; equality deletes (need Flink/pyiceberg) are not covered.
0. **Plugin bug: optimizer loop on `checksum(ROW(...))`** (found session 6, run 39889582040): `SELECT count(*),
   checksum(ROW(l_orderkey, …, l_comment)) FROM lineitem` failed after 180 s with "The optimizer exhausted the time
   limit … PushProjectionIntoTableScan: 1237642 invocations, 1237642 applications" — `ArrowFlightMetadata.applyProjection`
   keeps returning a result for a projection it cannot push (likely a non-variable projection such as a ROW constructor,
   either via `ExpressionTranslator.toExpr` accepting it or the `newProjection.equals(effectiveColumns())` check never
   matching). Clean error, no wrong rows, but any query projecting ROW(...) (or similar) over this connector fails.
   Fix: return `Optional.empty()` whenever nothing changes; unit test calling applyProjection twice (second call must
   be empty) with a ROW-constructor projection; e2e query with `checksum(ROW(...))`.
1b. **e2e: a kill in the middle of a stream** — done (session 6). Plugin logs `Flight stream at … failed after
   delivering data (…); not retried` (behaviour unchanged). `run-e2e.sh` `kill_mid_stream` (3 servers): baseline of
   `SELECT count(*), checksum(<col>) x16 FROM lineitem` on the healthy cluster, then flight3 killed after
   `KILL_STREAM_DELAYS` (9 8 10 11 s); answer must equal the baseline or be a clean error (wrong answer = FAIL).
   Green: run 39891124604 (attempt 1, 9 s: one flight3 stream failed after data → query failed cleanly "Network
   closed for unknown reason", two others re-read from flight1). A miss is still INFO; turn it into FAIL in partial
   mode once a few more runs show the 9 s delay hits reliably (streams open ~7–8 s after submission).
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
- (resolved) Workflow dispatch from cloud sessions: `gh workflow run` fails (GraphQL blocked by the session proxy);
  the REST call works: `gh api -X POST repos/C0fuge/heron/actions/workflows/e2e.yml/dispatches -f ref=main
  -f 'inputs[servers]=3' …` (session 6).
- **Server int6, Iceberg tables with delete files are unusable (local warehouse)**: the complete-answer view
  `iceberg_scan('/wh/ice/orders_del/metadata/v2.metadata.json')` is skipped at refresh — DuckDB: `Permission Error: Cannot
  access file "file:/wh/ice/orders_del/metadata/snap-….avro" - file system operations are disabled by configuration` —
  so the table is listed but `SELECT` fails "Table with name orders_del does not exist" (clean error, no wrong rows).
  The same skip is logged for other Iceberg tables' views (e.g. ice.commits), yet complete mode on 1 server still
  read every table without deletes correctly (run 39874470743), so only tables with delete files are affected. Metadata written by
  Spark/Hadoop uses `file:/…` URIs. Server fix needed (allow the catalog roots / `file:` scheme in DuckDB).
- **Server int6 with a local (`file`) warehouse fails at startup** with `No FileSystem for scheme "file"`
  (HadoopDataStoreProvider; the Hadoop jars in `libs/` do not register LocalFileSystem). CI works around it with a
  `core-site.xml` (`fs.file.impl=org.apache.hadoop.fs.LocalFileSystem`) on the server classpath. Fix in the server?

- e2e S3 is `rclone serve s3` instead of MinIO (MinIO images/binaries are no longer downloadable). OK, or is
  there a MinIO build you want to put into `e2e-assets`?
- The release now holds int4, int5 and int6; the e2e job picks one with `server-build` (default int4, the current
  release candidate). Switch the default to int6 once you consider it the candidate?
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
- 2026-10-22 (cloud session 3): item 1 (Iceberg in CI). Added `e2e/ci/iceberg.sh` (sourced by `run-e2e.sh`, on for
  server-build int5/int6): Spark-written HadoopCatalog warehouse, checks of copies/rename/TPC-H/commit visibility through
  the plugin. int5 dropped as the maintainer asked (stack overflow at the first refresh with a catalog: the int5 run
  39845246259 hung and was cancelled). int6 partial 1 server green: run 39852719842 (parquet 22/22, types 2/2, Iceberg
  copies lineitem 6,001,215 / orders / customer equal, rename OK, 8/8 TPC-H on `ice`, commit seen by the later query;
  `orders_del` KNOWN: server skips its iceberg_scan view, see Open questions). Intermediate failing runs 39844249030
  (Spark hostname), 39844696722 (`No FileSystem for scheme "file"`), 39849017993 (CLI warning in output), 39850026875 and
  39851301285 (orders_del, diagnosis). All commits `[skip ci]`; runs dispatched by hand. No plugin code changed; no
  invariant affected (int4 path unchanged: `ICEBERG=0` by default for int4).
- 2026-10-23 (cloud session 4): item 1b (Iceberg in CI, other modes). int6 complete 1 server green (run 39874470743;
  expected Iceberg failures did not happen, only `orders_del` KNOWN). int6 partial 3 servers: run 39874473171 failed in
  the Iceberg diagnostics (`grep` with no match under pipefail ended the job) → fixed; run 39875413990 green. Added
  failover checks on schema `ice` (kill of flight3 during q18, two nodes down q01 q03 q18) and fixed a latent bug in the
  kill retry (flight3 restarted alone never starts while flight2 is down: the server waits for all `--hosts`; run
  39876649252 failed on it) → flight2+flight3 restarted together; run 39878210446 green (Iceberg kill hit on attempt 3,
  streams re-read from flight2). All commits `[skip ci]`, runs dispatched by hand (~47 Actions minutes). No plugin code
  changed; no invariant affected.
- 2026-10-23 (cloud session 5): item 1 b) (int6 complete mode, 3 servers) not run: the session's permission check
  refused the workflow dispatch (see Open questions). No code changes, no CI run, ~0 Actions minutes.
- 2026-10-23 (cloud session 6): item 1 b) int6 complete mode 3 servers green (run 39886319914). Item 1b (mid-stream
  kill): plugin WARN "failed after delivering data; not retried" (log only; build/unit tests green run 39886485366),
  e2e `kill_mid_stream`; runs 39886487789 (green; q18 kill already hit mid-stream: clean error), 39888037652 (green,
  dedicated check missed: INFO), 39889582040 (FAILED: `checksum(ROW(...))` made Trino's optimizer loop in the plugin's
  applyProjection → new Next steps item 0), 39891124604 (green, mid-stream hit on attempt 1, clean error). Workflow
  dispatch works through the REST API (`gh api …/dispatches`). ~75 Actions minutes. Invariants hold (no wrong rows).
