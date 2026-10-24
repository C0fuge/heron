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
  job) and, with 3 servers, runs the failover checks (see `docs/E2E-RESULTS.md`). Input `server-build` = int4|int5|int6|int7 (int5+ also run `e2e/ci/iceberg.sh`). int7 is not in `SHA256SUMS`;
  the job prints the selected build's hashes (full int7 hashes: jar 2b9595ab2966a5a3c80b51c96bd7a2a9a9e87d97e746fc94d146ccc8a55606fa,
  plugins 4d2416f4c71d5329094a83fb270a7177b178e83de44fd6e8a1687b48a1de1af4 — match the maintainer's note).
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

> **Stand, 3-node S3 cluster on int7 (2026-10-25):** plugin correct everywhere (22/22, B2, B5, mid-stream kill). Follow-ups: (a) q04 takes 8.1 s on the 3-node cluster vs 1.7 s single node while the servers answer equally fast — the time is in merging results of several endpoints in the plugin; profile and fix; (b) after a node restart the first query reuses the stale pooled connection to that node, fails, then reads the backup (+~1 s) — validate/evict pooled connections of a restarted node; (c) the catalog points at one host — support a list of coordinator hosts (`arrow-flight.hosts=a,b,c`) so a dead first host does not fail the query.
>
> (a) **Done (session 40), pending stand check.** Cause found in the code: with a grouped aggregation pushed to a cluster every
> Flight node returns its own partial groups, and the plugin collected all of them in one boxed `LinkedHashMap` (one `ArrayList`
> key per group, unaccounted memory) and emitted a single page. q04's `EXISTS` and q18's `IN` become `GROUP BY l_orderkey` on
> lineitem (15M groups at SF10; likely also why q18 needed an 8 GB Trino heap on the 3-node stand). Now the server SQL gets
> `ORDER BY <group columns> ASC NULLS FIRST` and `AggregationReader.SortedMerger` merges the endpoints as sorted streams
> (k-way, one group in memory, pages of 8192 rows); a stream out of order fails the query (never a duplicated group). Only for
> GROUP BY types the server orders like Trino (`isTopNSortable`: integer/decimal/boolean/date/timestamp(6); not VARCHAR/DOUBLE);
> skipped when the catalog's last FlightInfo had one endpoint (single node: no server sort); the old hash merge stays the
> fallback. `arrow-flight.aggregation-sorted-merge.enabled` / session `aggregation_sorted_merge_enabled`, default true. Unit
> tests `AggregationReaderTest` (6, incl. random equality with the hash merge) + `SqlQueryBuilderTest`; e2e block `aggmerge`
> (q04, q18, `GROUP BY l_orderkey` checksum: sorted = hash answers, EXPLAIN shows the pushed aggregation). Run 40140961985
> (int7, 3 servers, SF1): all three equal; hash/sorted s: q04 2.12/1.89, q18 3.42/3.07, groups 1.83/1.60 (SF1 = 1.5M groups,
> 4 vCPU runner); run 40142038380 (same setup): q04 2.46/2.69, q18 4.75/4.58, groups 2.32/2.38 — at SF1 on the runner the
> difference is within noise; the gain is expected at SF10 over the network (memory: one group instead of all). **Stand:** rerun q04/q18 on the 3-node cluster with the new plugin, with
> `SET SESSION flight.aggregation_sorted_merge_enabled` true and false, and report times (and whether q18 runs with a
> 4 GB heap now). Next: (b) stale pooled connection after a node restart (`FlightSqlSession` per-location generations only drop
> pools after a failure seen by this Trino), then (c) a list of coordinator hosts.

> **Queue empty (session 24, 2026-10-23; superseded by the stand note above):** items 1–6 are done and every remaining item-7 follow-up waits on an
> answer in Open questions (TopN / dynamic-filter / split defaults, Arrow upgrade vs. 18.1 pins, bundle cleanup,
> complete-mode throttled kill, int7 as push default, S3 warehouse default). Actions use since 2026-10-01 is already
> ≥ 850 min of run time (92 runs). Until the maintainer answers one of them or adds a new item, a session should take
> the lock, find nothing new here, release it and stop — no CI runs for coverage alone.

> **Dependency audit 2026-10-23** (local coordinator): the built plugin tarball does not match the pom pins — it ships Netty 4.1.100 modules, gRPC 1.65.0 and protobuf 3.25.4 (CVE-2024-7254); Jackson 2.18.0. Align via BOMs (netty-bom 4.1.137+ or 4.2.17+ as Trino's classloader allows, grpc-bom current, protobuf 3.25.5+/4.x, jackson 2.18.11+), rebuild for 455/477/483, verify the tarball contents in CI (fail the build on drift), e2e green. Do this before other open follow-ups. **Done (session 20)**: root `pom.xml` pins Netty 4.1.139 (netty-bom; tcnative 2.0.84),
> gRPC **1.78.0** (grpc-bom; newest line Arrow 18.1 runs on — gRPC 1.79 removed `ReadableBuffer.readBytes(ByteBuffer)`, which
> Arrow's `GetReadableBuffer` calls: NoSuchMethodError on every stream, build run 39990774489), protobuf 3.25.9, Jackson 2.18.12
> (jackson-bom; 483 keeps 2.22.1), Guava 33.5.0 (needed by gRPC 1.78). `build.yml` runs `trino-arrow-flight/bundle/check-bundle.sh`
> on every zip: the jar list must equal `bundle/trino-<v>.txt` and Netty/gRPC/protobuf/Jackson must each ship one version.
> Build all versions run 39991724522 (455/477/483 green, bundle check passed); e2e int4 Trino 455 partial 1+3 servers run
> 39991081221 green (TPC-H, failover incl. mid-stream kill); e2e int7 Trino 483 partial 1 server run 39991939760 green.
> Note: the audit's "Netty 4.1.100 / gRPC 1.65.0" was not what this repo's pom built (it already pinned 4.1.114 / 1.65.1);
> protobuf 3.25.4 and Jackson 2.18.0 were real.

> **Use server int7** (2026-10-23, local coordinator): `arrow-flight-server-1.0.0-int7.jar` sha256 2b9595ab…06fa + `int7-plugins.tar.zst` sha256 4d2416f4…1af4 (plugins s3/lakefs/iceberg/hive + START-NOTES.txt) = int6b + all stand fixes (lakeFS: JOIN commit mixing, complete-mode lag, tags; Iceberg: file:/s3a roots, LocalFileSystem, Gravitino errors; Hive: HDFS paths with space/%, out-of-root partitions hidden, metastore fail-fast, UNAVAILABLE on outage; Iceberg equality deletes read all columns — correct, slow). Replace int6/int6b in CI with int7; rerun the full suite once. **Done (session 19)**: Trino 455, local warehouse —
> partial 1+3 servers run 39982788809, complete 3 servers run 39982792807, all green, `orders_del` deletes OK.

> **Use server int6 instead of int5** (2026-10-22, local coordinator): int5 overflows its stack on the first table refresh ~30 s after start when any catalog is configured AND the data dir has a table (then every call fails "Application error processing RPC"). int6 = int5 + fix + catalog plugin `hive` (Hive Metastore; SPI 1.1): assets `arrow-flight-server-1.0.0-int6.jar` sha256 78f06212…8d09, `int6-plugins.tar.zst` sha256 fa870ddb…adaf (plugins s3/lakefs/iceberg/hive + START-NOTES.txt). With the iceberg plugin present, `catalog=hive` needs the iceberg+avro extensions or `hive.icebergTables=skip`.

1. **Iceberg in CI (server int6/int7)** — done (session 19). Done (session 3): `e2e/ci/iceberg.sh`, on by default for
   `server-build` int5/int6 (`ICEBERG=0|1`): Spark 3.5.3 + Iceberg 1.6.1 (`apache/spark:3.5.3`, runtime jar from Maven
   Central) writes a HadoopCatalog warehouse `$WORK/wh` (namespace `ice`) from the SF1 Parquet; every server mounts it
   at `/wh` (`-Dcatalog=iceberg-hadoop -Darrowflight.iceberg-hadoop.warehouse=/wh -Darrowflight.catalog.localRootsShared=true`,
   DuckDB iceberg+avro v1.4.5, `core-site.xml` with `fs.file.impl` — see Open questions). Checks through Trino: tables
   listed; full copies lineitem/orders/customer = Parquet (row count + per-column `checksum`); rename (`c_comment` →
   `c_note`) OK; TPC-H q01 q03 q04 q06 q12 q13 q18 q22 on schema `ice` = DuckDB reference; a commit between two queries is
   seen by the second only. Green on int6, partial, 1 server: run 39852719842 (TPC-H 22/22 + types 2/2 + Iceberg, deletes KNOWN). Open:
   a) `ice.orders_del` (merge-on-read position deletes): server bug in int5/int6 (local `file:` warehouse), **fixed in
      int7**: deletes OK, 1,285,715 rows = Parquet, in partial 1+3 servers (run 39982788809) and complete 3 servers
      (run 39982792807). `iceberg.sh` reports KNOWN only for int5/int6 now; any later build FAILs.
   b) done (session 4): int6 complete mode 1 server green (run 39874470743: Iceberg reads the same as partial, 0
      answeredCompletely fallbacks — the iceberg_scan refusal only hits `orders_del`'s view, the others are read
      from their data files); int6 partial 3 servers green incl. failover on Iceberg tables (run 39878210446).
      int6 complete mode 3 servers green (session 6, run 39886319914, 14 min).
   c) S3 warehouse — done (session 13), opt-in: workflow input `iceberg-warehouse=s3` (`ICEBERG_WAREHOUSE`, default
      `local`). Spark writes through S3A (hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 from Maven Central, `fs.s3.impl`
      = S3A so metadata carries `s3://` URIs) into `s3://lake/parquet/wh`. The server requires an s3 catalog root below the
      dataDir ("needs a second s3 store; this server opens one store per storage plugin"). rclone workarounds: table
      directories pre-created, each with a hidden `.keep` object; no `CREATE NAMESPACE`; S3A markers kept (rclone answers
      500 on parallel PUTs into a new directory and refuses `dir/` marker objects). Green on int6 partial 1 server: run
      39941848259 (TPC-H 22/22, types 2/2, all Iceberg checks OK **including `orders_del` position deletes**, 1,285,715
      rows = Parquet: the deletes problem in a) is specific to the local `file:` warehouse). Run 39939466381 had one clean
      error right after the mid-run commit ("Column 'c_custkey' not found. Available: []"): HadoopCatalog commits on S3 are
      not atomic (S3A rename = copy + delete). In S3 mode such errors are now retried within the commit wait and reported
      as INFO `commit-window`. Session 14, int6 S3 warehouse, 3 servers: partial run 39949170568 (18.5 min) and complete
      run 39949173080 (28.3 min) both green: TPC-H 22/22, types 2/2, every Iceberg check OK incl. `orders_del` 1,285,715
      rows and TPC-H 8/8 on `ice`, failover on `ice` (flight3 killed during a read, re-read from a backup), two nodes down;
      `commit-window` INFO 1 resp. 2 clean errors ("Table ice.commits names version 2 in its version hint, but has no
      metadata file for it" = the non-atomic HadoopCatalog commit). Equality deletes (need Flink/pyiceberg) are not
      covered. Item 1 complete with int7 (session 19).
0. **Plugin bug: optimizer loop on `checksum(ROW(...))`** — done (session 7). Cause: Trino 455's `PartialTranslator`
   collects the projections in a HashMap keyed by node identity, so their order changes on every
   `PushProjectionIntoTableScan` invocation; `applyProjection` compared the projected column *list* with the handle's
   and never saw "no change". Now compared as sets (column order in the handle does not matter: the page source reads
   Trino's column list). Unit test `ArrowFlightMetadataProjectionTest`; e2e `projection` check
   (`checksum(ROW(all 16 lineitem columns))`, 60 s planning cap, count 6001215): run 39897404708, 25 s.
1b. **e2e: a kill in the middle of a stream** — done (session 6). Plugin logs `Flight stream at … failed after
   delivering data (…); not retried` (behaviour unchanged). `run-e2e.sh` `kill_mid_stream` (3 servers): baseline of
   `SELECT count(*), checksum(<col>) x16 FROM lineitem` on the healthy cluster, then flight3 killed after
   `KILL_STREAM_DELAYS` (9 8 10 11 s); answer must equal the baseline or be a clean error (wrong answer = FAIL).
   Green: run 39891124604 (attempt 1, 9 s: one flight3 stream failed after data → query failed cleanly "Network
   closed for unknown reason", two others re-read from flight1). Hit on attempt 1 again in run 39897404708 (int4).
   Session 11: a miss on every delay is now FAIL in partial mode (hit on attempt 1 in every 3-server partial run
   since 39891124604; again in run 39920601013, Trino 455); stays INFO in complete mode, where int6 run 39920606099
   (Trino 483) missed on all four delays (9/8/10/11 s): flight3's streams had not started yet ("failed before any
   data … reading it from flight1", answers exact), so complete-mode streams open later than ~11 s there.
2. **Connection reset after one failed stream** — done (session 7, `FlightSqlSession`): a stream failure that means
   a node was lost drops only that location's pooled stream connections (per-location generations); the coordinator
   connection is renewed only when the coordinator itself was lost or on UNAUTHENTICATED (re-handshake). A failed
   coordinator *call* still drops every pool (the whole cluster may have restarted; gRPC fails stale channels for
   up to ~2 min). Unit tests `lostNodeDropsOnlyItsOwnPooledConnections`, `lostCoordinatorKeepsOtherNodesPooledConnections`.
3. **`ORDER BY … LIMIT` (TopN) pushdown** — done (session 7), **off by default**
   (`arrow-flight.topn-pushdown.enabled` / session `topn_pushdown_enabled`). `applyTopN` renders
   `ORDER BY <keys> NULLS FIRST|LAST LIMIT n` into every scan query (`TopN` in the table handle) when all keys are
   plain columns of integer/decimal/boolean/date/timestamp type; never guaranteed (each endpoint and range split
   returns its own top n). Kept apart from the client-side LIMIT (that one counts rows across a split's endpoints).
   Not combined with a LIMIT below, an aggregation, a later filter or dynamic filters. Unit tests
   `ArrowFlightMetadataTopNTest`; e2e `topn` check: 4 queries directly over scans (lineitem/orders/customer, n = 5…1000)
   equal with the property on and off, EXPLAIN shows `topN=` in the scan. TPC-H q03/q10/q18 are unaffected (their
   TopN sits above joins/aggregations, never on a scan). Rows read measured (session 15, `topn-rows.tsv`, int6 partial
   1 server, Trino 483, run 39956762839; off → on): lineitem top 10 6,001,215 → 40 (4 range splits x 10), orders top 20
   729,413 → 20, customer top 5 150,000 → 5, lineitem top 1000 6,001,215 → 4,000. Wall time on the runner (off/on):
   2.73/3.65 s, 0.54/0.37 s, 0.21/0.17 s, 2.07/2.87 s — the two lineitem queries were *slower* with pushdown (the
   server sorts 6M rows per split; locally the transfer it saves costs little). Trino 477 (run 39959398904): same rows,
   1.90/2.09, 0.58/0.43, 0.15/0.16, 1.54/1.90 s. Enable by default? (see Open questions).
4. **Dynamic filters** — measured (session 8); defaults unchanged (off, 1000 values). Review: the server SQL of a scan is
   built on the worker right before the first read, waiting up to `wait-timeout-ms` (10 s) for the build side; domains go
   only onto plain columns of pushable types, never under a pushed TopN; correct by construction (a span is a superset).
   New: `arrow-flight.dynamic-filtering.max-values` / session `dynamic_filtering_max_values` (was a constant 1000; more
   values become `[min, max]`), unit tests in `SqlQueryBuilderTest`, README/properties docs. e2e `dynfilter` check
   (`DF_QUERIES` = q02 q03 q05 q08 q09 q10 q17 q18 q20 q21, modes off / on / big = 100000 values +
   Trino `enable_large_dynamic_filters`): all answers = reference in every mode, 1 and 3 servers. Rows read from the server
   (on/off, big/off; int4 partial 1 server, run 39905241338): q02 0.50/0.10, q03 1.00/1.00, q05 0.98/0.23, q08 0.98/0.08,
   q09 1.00/0.32, q10 1.00/1.00, q17 0.50/0.50, q18 0.18/0.18, q20 0.99/0.99, q21 0.75/0.75. Wall time at SF1 on one
   runner: no clear gain (q18 −30 %, q09 big +45 %: large IN lists cost DuckDB time; data is local, so transfer savings do
   not show). Open: q03/q10 get nothing even with big (orders/customer keys > 100k → span); measure on the stand where
   rows cross the network; server statement cache (see Open questions).
5. **Split count** — reviewed, new option (session 9), defaults unchanged. How it works today: a scan is one split, or up to
   `split-count` (4) key-range splits when ≥ 2×`split-min-rows` (1M) rows are expected after filters (integer/date column);
   aggregation and LIMIT scans are never range-split. Each split builds its SQL on the worker and calls `GetFlightInfo`;
   the server returns one endpoint per Flight node, and these were read **one after another** inside the split, so a split
   kept one node busy at a time (3 nodes x 4 splits = 12 streams, only 4 open at once). Splits carry no host address
   (any worker reads any node). New: `arrow-flight.endpoint-read-parallelism` / session `endpoint_read_parallelism`
   (default 1 = old sequential path): `ParallelStreamReader` reads up to N endpoints of a plain-scan split at once
   (`next()` on daemon threads, driver takes the batch that is ready, `isBlocked()` waits on them; per-stream failover
   unchanged; close cancels open streams). Unit tests `ParallelStreamReaderTest` (5). e2e `splits` block: split_count 1/4
   x parallelism 1/3 on q03 q12 q14 q18 q19 + full lineitem scan checksum, all answers = reference in every setting,
   1 and 3 servers (run 39908551054). Times (best of 2, s; s=split_count, p=parallelism) on one 4-vCPU runner:
   3 servers — scan s1p1 15.98 / s1p3 12.80 / s4p1 14.06 / s4p3 15.73; q03 5.81 / 4.58 / 6.23 / 4.58; others within noise.
   1 server — scan 8.98 / 8.61 / 11.08 / 11.06 (4 range splits slower than 1 there: server and Trino share 4 cores).
   Open: measure on the stand (real nodes, network); see Open questions.
6. **Trino 477 and 483 builds** — done (session 10). The per-SPI source roots (`src/trino-455|477|483/java`) and
   profiles already existed; first CI build of all three (run 39914179868): 455 and 477 green, 483 unit tests failed
   (`NoClassDefFoundError: JsonSerializeAs` — the profile's Jackson 2.22.1 BOM lifted jackson-databind while
   jackson-annotations stayed pinned at 2.18.2) → property `dep.jackson-annotations.version`, 2.22 in the 483 profile
   (build run 39914348258: 455/477/483 green, 26/26 tests each). e2e int4 partial, 1 + 3 servers, all green:
   477 run 39914595927, 483 run 39914598648 (TPC-H 22/22, projection, TopN 4/4, dynfilter 10/10 x 3 modes, splits
   4 settings, types 2/2, failover incl. kill during a read and mid-stream kill = clean error). e2e script: Trino's
   `enable_large_dynamic_filters` (gone in 483) is passed only where `SHOW SESSION` lists it. Build commands per
   version: `trino-arrow-flight/README.md` "Trino versions". Session 11: 483 + int6 complete mode 3 servers green
   (run 39920606099: TPC-H, Iceberg incl. failover on `ice`, two nodes down); the dynfilter guard on 455 passes the
   flag (`--session enable_large_dynamic_filters=true` logged, rows-read ratios = session 8; run 39920601013).
   Session 12: 477 + int6 complete mode 3 servers: every check passed (TPC-H 22/22, projection, TopN, dynfilter,
   splits, types 2/2, Iceberg incl. failover on `ice`, two nodes down), but the job hit its 30-min limit 4 s before
   the end (run 39927746471, cancelled) → `timeout-minutes: 40`. Not run: 483 int6 on 1 server, 477 int6 1 server.
7. Open follow-ups (pick by value): int7 coverage — done so far (session 21, after the session-20 dependency pins): 477 complete
   3 servers run 39998499713 (23 min) and 483 partial 3 servers `iceberg-warehouse=s3` run 39998502629 (27 min), both green
   (TPC-H, types, Iceberg, failover, two nodes down; mid-stream kill hit after data on attempt 1 in both = clean error, also
   in complete mode). Not yet run with int7: 483 complete 3 servers, 477 with `iceberg-warehouse=s3`, 477 partial — low value,
   run only if a plugin change touches them. Session 22: int7 does **not** need the CI `core-site.xml` (`fs.file.impl`)
   workaround: new workflow input `iceberg-core-site` (`ICEBERG_CORE_SITE`, empty = 1 for int5/int6, 0 otherwise); int7
   Trino 455 partial 1 server without it, run 40006799225, green (server starts, TPC-H 22/22, every Iceberg check OK incl.
   `orders_del`). Session 23: int7 Trino 455 partial **3 servers** with `iceberg-warehouse=s3`, without it, run 40014711279
   (28.5 min): green (TPC-H, Iceberg incl. failover on `ice`, kill during a read, mid-stream kill hit after data on attempt 1
   = clean error, two nodes down). The workaround is not needed for int7 in any tested setup.
   Maintainer answers in Open questions
   (defaults for TopN, dynamic filters, split count / parallelism). Mid-stream kill in complete mode — investigated
   (session 12), stays INFO: delays now default to 50/65/80/90 % of the healthy baseline in complete mode; in run
   39927746471 (baseline 17.9 s, kills at 8/11/14/16 s) all 4 kills still found flight3's streams open but empty
   ("failed before any data", answers exact). In complete mode a node sends nothing until the server has the whole
   answer, then the batches go out in a short burst, so a timed kill practically never lands inside it. A real
   after-data check there would need a slowed stream (e.g. `tc` throttling on flight3, or a much larger table) —
   only worth it if the maintainer wants it (see Open questions). Session 14: in the complete-mode S3-warehouse run
   39949173080 (3 servers, baseline 18.1 s, delays 9/11/14/16 s) the first kill (9 s) **did** hit two flight3 streams
   after data → clean error "Network closed for unknown reason", 0 wrong answers. So a hit in complete mode is possible
   (with the S3 warehouse); still INFO there. Session 15: 483 int6 partial 1 server green (run 39956762839: TPC-H 22/22,
   TopN 4/4, dynfilter, splits, types 2/2, Iceberg checks OK, `orders_del` KNOWN as in a)); 477 int6 partial 1 server
   green too (run 39959398904, same checks). Every Trino version x int6 x 1/3 servers has now run green at least once.

## Open questions
- **`e2e-assets` re-uploaded 2026-10-24 21:51 UTC with int7 only** (int4–int6 removed): the push e2e default is now int7
  (session 40). The int7 jar hash is unchanged (2b9595ab…06fa), but `int7-plugins.tar.zst` is now
  `bad288e050ca1175271024dc01f6fcbdbb3c710308216ba2b56b30ae63e3183b` (STATUS had 4d2416f4…1af4): a new plugins build —
  intended? What changed? (TPC-H, Iceberg and failover checks are green with it, run 40140961985; the server answers the
  mid-stream baseline faster, 7.5 s.)
- **Stand answers 2026-10-23 (local stand agent; split count / parallelism, TopN, dynamic filters, statement cache,
  collation).** Setup: 3-node HDFS cluster (REPL 2, 512 MB blocks, TPC-H SF10), server **int7** (sha256 2b9595ab…06fa) on
  each node with the stand's flags (`-Xmx2g`, `duckDbMemoryLimit=1GB`, `duckDbThreads=2`, 4 GB container), partial mode;
  Trino 455 single node, plugin = CI artifact of run 39914348258 (e0ebab5, the last plugin code change), **8 GB heap**
  (4 GB and 6 GB both failed q18 with `Java heap space`). Times = median of 3 warm runs after one warm-up, settings
  interleaved per round (s); rows = Trino `physicalInputPositions`. Every answer was identical across settings;
  full TPC-H 22 = **05bc0dc2713a** with defaults, with the recommended settings (TopN on), with s8/p3 and with large
  dynamic filters.
  - Split count / endpoint read parallelism (`s` = `flight.split_count`, `p` = `flight.endpoint_read_parallelism`):

    | query | s1 p1 | s4 p1 (default) | s8 p1 | s1 p3 | s4 p3 | s8 p3 |
    |---|---|---|---|---|---|---|
    | full-scan `checksum(ROW(16 cols))` | 43.05 | 13.71 | 11.64 | 42.81 | 13.70 | 11.85 |
    | q03 | 3.11 | 1.66 | 2.01 | 2.02 | 1.70 | 2.05 |
    | q18 | 11.10 | 7.37 | 7.92 | 11.91 | 7.45 | 10.08 |

    Rows read equal in all settings (59,986,052 / 39,923,968 / 91,486,052). One split is 3x slower on the scan (one
    Trino driver consumes the split; reading its 3 endpoints in parallel does not change that: 43.05 vs 42.81). 8 splits
    help only the bare scan (−15 %) and cost q03 +21 % and q18 +7 % (s8 p3 q18 +37 %). Parallelism 3 helps only with one
    split (q03 3.11 → 2.02). **Recommended defaults: keep `split-count` 4 and `endpoint-read-parallelism` 1.** Full suite
    s8 p3: 52.3 s vs 48.9 s default (sum of medians).
  - TopN pushdown (`flight.topn_pushdown_enabled` off / on): `SELECT l_orderkey, l_linenumber, l_extendedprice FROM
    lineitem ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber LIMIT 10` **0.86 → 0.26 s**, rows 59,986,052 → 100
    (80 in one run; ≤ 10 per split endpoint); q03 1.86 / 1.75, q10 1.54 / 1.50, q18 9.04 / 6.74 (off range 7.10–9.17:
    noise) with identical rows read (their TopN sits above a join/aggregation, as expected). Over the network the
    server-side sort wins, unlike the CI runner. Full suite with it on: 22/22 05bc0dc2713a, 48.7 s (default 48.9 s).
    **Recommended default: `arrow-flight.topn-pushdown.enabled=true`.**
  - Dynamic filters (off / on = 1000 values / big = 100000 values + `enable_large_dynamic_filters=true`):

    | query | time off / on / big | rows read off / on / big |
    |---|---|---|
    | q02 | 0.48 / 0.44 / 1.07 | 16,207,906 / 16,044,525 / 1,656,923 |
    | q05 | 3.29 / 3.26 / 2.22 | 63,861,997 / 62,573,983 / 14,617,286 |
    | q08 | 2.04 / 2.06 / 5.02 | 66,157,068 / 64,950,660 / 5,373,915 |
    | q09 | 2.97 / 3.06 / 7.17 | 83,194,859 / 83,194,859 / 83,194,456 |

    "on" changes nothing measurable; "big" cuts rows 4–12x on q02/q05/q08 but is faster only on q05 (−33 %) and 2.2–2.5x
    slower on q02/q08/q09 (DuckDB evaluating 100k-value IN lists); full suite with big 68.5 s vs 48.9 s. **Memory:** in
    the first big run two of the three servers were OOM-killed by their 4 GB container limit during q08 (later queries
    failed cleanly "Connection refused" / "server unavailable", no wrong answers); a server's RSS after a q08 is
    0.6–0.8 GB with filters off and 2.0–3.3 GB with big (6 runs, no growth from run to run, i.e. per-query memory, not
    a leak). **Recommended defaults: keep `dynamic-filtering.enabled=false`, `max-values` 1000;** do not use the big
    setting on nodes with ~4 GB.
  - Statement cache (int7 code, `DuckDbAdapter`): **bounded.** Each Flight worker thread has its own DuckDB
    connection with an LRU `StatementCache` (`LinkedHashMap` access order) of at most
    `arrowflight.duckdb.statementCacheSize` / `-DduckDbStatementCacheSize` (default 64) prepared statements; evicted ones
    are closed; sessions of ended threads are closed every 15 s (server commit 01c156f, 2026-10-14). The Hazelcast
    `statement-cache` map entries (handles, parameters) have a 10-min TTL. The bound is a count, not bytes: 64 big
    dynamic-filter texts per thread are kept, but the repeated-run test above showed no growth.
  - Collation: **binary.** The server sets no collation (no `default_collation`, no `COLLATE` in the code);
    `current_setting('default_collation')` = `''`; clients cannot change it (lockdown on by default →
    `lock_configuration = true`; `SET default_collation = 'nocase'` → "Only queries are allowed, not SET"). Test on
    the stand: `ORDER BY s` over 'b','B','a','A','e','Z','_','aa','a ','~', chr(127), chr(200), chr(228), chr(233),
    chr(8364) gives `A|B|Z|_|a|a |aa|b|e|~|DEL|È|ä|é|€` on the server, **identical** to Trino's order of the same
    values (code point / UTF-8 byte order). So VARCHAR sort keys could be pushed in TopN (code change + unit test; holds
    while the server keeps lockdown on).
- **Dependency ceiling from Arrow 18.1** (session 20): gRPC cannot go past 1.78.x (and so Netty stays 4.1, protobuf 3.25)
  until Arrow Java is upgraded (Arrow 19.0.0 uses gRPC 1.79, Netty 4.2.9, protobuf 4.33). Upgrade Arrow (bigger change:
  memory/Flight API, e2e on all versions), or stay on 18.1 with the current pins? Also: the zip bundles
  `jackson-annotations` and `opentelemetry-api/context` 1.37.0 although the pom declares them `provided` (the
  trino-maven-plugin resolves its own set; SPI packages are loaded from Trino anyway, so harmless) — clean up or leave?
- **Mid-stream kill in complete mode** (session 12): kills at up to 90 % of the query time never hit a stream after
  its first batch (the server streams a node's part only once complete, in a burst). The partial-mode check covers
  the plugin's "no retry after data" rule. Add a throttled-network variant for complete mode, or leave it as INFO?
  (Session 14: with `iceberg-warehouse=s3`, complete mode, the 9 s kill did hit after data — run 39949173080; a hit is
  possible but not reliable.)
- **Split count / endpoint read parallelism** (session 9; **answered on the stand 2026-10-23, see "Stand answers" above**): on the CI runner (everything on 4 vCPUs) neither more range splits
  nor parallel endpoint reads give a clear gain; parallel reads helped the 3-server full scan (16.0 → 12.8 s) and q03. On
  the stand, try `SET SESSION flight.endpoint_read_parallelism = 3` (3 nodes) and `flight.split_count` 1/4/8 on SF10
  (e.g. the full-scan checksum and q03/q18) and tell whether to change the defaults (1 and 4).
- **Dynamic filters** (session 8; **answered on the stand 2026-10-23, see "Stand answers" above**): with them on, every query's SQL text differs. The config comment says the server
  keeps a prepared statement per distinct text forever (`DuckDbAdapter.stmtCache`). Is that cache bounded in int4/int6?
  If yes, dynamic filtering could be tried on the stand (`arrow-flight.dynamic-filtering.enabled=true`; with Trino's
  `enable_large_dynamic_filters=true` and `arrow-flight.dynamic-filtering.max-values=100000` it cut rows read 3–12× on
  q02/q05/q08/q09 at SF1, answers exact). Measurements: Next steps item 4.
- **TopN pushdown default** (session 7; **answered on the stand 2026-10-23, see "Stand answers" above**): implemented and green on int4 (partial 1+3 servers, complete 3 servers — see
  Log) but **off by default** because the README said the server had no `ORDER BY`/`LIMIT`. The int4 server does
  accept them. Enable by default on the stand (`arrow-flight.topn-pushdown.enabled=true`) once you have tried it?
  Session 15 measurement (Next steps item 3): rows read drop to n per split (6M → 40), but on the CI runner the two
  lineitem queries took ~35 % longer with it on (server-side sort of every split vs. cheap local transfer); on the stand,
  where rows cross the network, compare e.g. `SELECT … FROM lineitem ORDER BY l_extendedprice DESC LIMIT 10` with the
  session property `flight.topn_pushdown_enabled` on and off. VARCHAR
  sort keys are not pushed: is the server's DuckDB collation always binary (then they could be)?
- (resolved) Workflow dispatch from cloud sessions: `gh workflow run` fails (GraphQL blocked by the session proxy);
  the REST call works: `gh api -X POST repos/C0fuge/heron/actions/workflows/e2e.yml/dispatches -f ref=main
  -f 'inputs[servers]=3' …` (session 6).
- **Resolved in int7 (session 19, runs 39982788809/39982792807: `orders_del` OK on the local warehouse).** Kept for history:
  **Server int6, Iceberg tables with delete files are unusable (local warehouse only)**: with an S3 warehouse
  (`iceberg-warehouse=s3`, session 13, run 39941848259) `orders_del` reads correctly, so this affects `file:` roots only.
  Old description: the complete-answer view
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
- **S3 Iceberg warehouse in CI** (session 13): should `iceberg-warehouse=s3` become the default for int6 runs? It covers
  deletes (local `file:` cannot), but costs ~1 min more (S3A jars) and its commit check can hit a clean error in the
  non-atomic HadoopCatalog commit window. Also: is an s3 catalog root outside the dataDir meant to be refused, or should
  the server open a second store for it?
- The release now holds int4…int7; the e2e job picks one with `server-build` (default int4, the current
  release candidate). Switch the push default to int7 once you consider it the candidate? (int7 not yet in `SHA256SUMS`.)
  (Answered, session 22: int7 runs without the `core-site.xml` workaround, run 40006799225; CI now adds it for int5/int6 only.)
- (resolved, session 16) `e2e-assets` is a published pre-release (since 2026-10-22), so the e2e job now runs with
  `contents: read`; asset download verified in run 39964823185.

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
- 2026-10-23 (cloud session 7): item 0 done: `applyProjection` compared projected columns as an ordered list while
  Trino 455 passes projections in random order (HashMap in `PartialTranslator`) → optimizer loop on
  `checksum(ROW(...))`; now a set comparison (build/unit run 39897405278; e2e run 39897404708 int4 partial 1+3 servers
  green incl. new `projection` check, 25 s; mid-stream kill hit on attempt 1). Item 2 done: a lost node drops only its
  own pooled stream connections (build run 39898217668, 12/12 failover unit tests; e2e run 39898217754 green 1+3
  servers). Item 3 done, off by default: TopN pushdown (build run 39899483599; e2e run 39899483536 int4 partial 1+3
  servers and run 39900292543 int4 complete 3 servers green, TopN 4/4 on = off). READMEs updated (they still said the
  server had no ORDER BY/LIMIT and no aggregation pushdown). ~60 Actions minutes. Invariants hold (failover rules
  unchanged, TopN off by default and never guaranteed; no wrong rows).
- 2026-10-23 (cloud session 8): item 1 left as is (a) waits on a server fix, c) optional). Item 4 (dynamic filters):
  e2e `dynfilter` check (TPC-H joins off / on / big, answers vs reference, Trino physicalInputPositions per mode) — runs
  39902691979 (off/on), 39903809735 (+ 100000-value cap), 39905241338 (+ Trino large dynamic filters), all green 1+3
  servers int4 partial; new property `arrow-flight.dynamic-filtering.max-values` (default 1000 = old behaviour), build/unit
  run 39903809864. Defaults unchanged (dynamic filtering stays off). ~45 Actions minutes. Invariants hold (no
  failover/auth change; dynamic filters only narrow by supersets; all answers exact).
- 2026-10-23 (cloud session 9): item 5 (split count) reviewed: endpoints of a split were read sequentially. Added
  `arrow-flight.endpoint-read-parallelism` (default 1, old behaviour) with `ParallelStreamReader` + 5 unit tests (build run
  39908551065, 26/26 tests) and the e2e `splits` measurement (run 39908551054 int4 partial 1+3 servers green: TPC-H 22/22,
  splits 4 settings x 6 queries exact, failover checks incl. kill during a read green). README: corrected the stale
  "one split per endpoint" description, documented split properties. ~25 Actions minutes. Invariants hold (default path
  unchanged; with parallelism > 1 each stream keeps its failover-before-first-batch rule; no wrong rows in any setting).
- 2026-10-23 (cloud session 10): item 6 (Trino 477/483) done: 483 profile fix (jackson-annotations 2.22), build run
  39914348258 all three versions green; e2e int4 partial 1+3 servers green on 477 (run 39914595927) and 483 (run
  39914598648); e2e script tolerates Trino versions without `enable_large_dynamic_filters` (verified on 483; the 455
  path with the guard runs on the next plugin push). README per-version build table; root README build note corrected
  (release 22, JDK 25). Earlier build run 39914179868 (483 red, the bug above); a push-triggered 455 e2e (39914340347)
  was cancelled (change was 483-only). ~70 Actions minutes. Invariants hold (455 artifact unchanged; no plugin code
  changed).
- 2026-10-23 (cloud session 11): item 7 follow-ups. e2e mid-stream kill: a miss on every delay now fails the job in partial
  mode (INFO in complete mode). Push run 39920601013 (Trino 455 int4 partial 1+3 servers) green: mid-stream kill hit on
  attempt 1, dynfilter guard passes Trino's large-filters flag on 455. Dispatch run 39920606099 (Trino 483 int6 complete 3
  servers) green; mid-stream kill missed 4/4 there (flight3 streams not yet open, all re-read from backups, exact) →
  follow-up for complete-mode delays. ~45 Actions minutes. No plugin code changed; invariants hold.
- 2026-10-23 (cloud session 12): item 7 follow-ups. e2e: complete-mode mid-stream kill delays derived from the healthy
  baseline (50/65/80/90 %). Dispatch run 39927746471 (Trino 477 int6 complete 3 servers): all checks passed, 0 FAIL, but
  cancelled by the 30-min job limit just before exit → e2e timeout 40 min; mid-stream kill 4/4 "before any data" (exact
  answers, INFO) → cause documented, Open question added. ~31 Actions minutes. No plugin code changed; invariants hold.
- 2026-10-23 (cloud session 13): item 1 c) S3 Iceberg warehouse, opt-in (`iceberg-warehouse=s3`). Intermediate failing runs
  (all int6 partial 1 server): 39935266435 and 39937732484 (rclone 500 on parallel PUTs / S3A directory markers; errors now
  printed), 39938202657 and 39938584442 (rclone refuses S3A's `wh/ice/` marker), 39939037384 (server: catalog root must be
  below the dataDir), 39939466381 (all checks OK incl. deletes, one clean error in the commit window → retried as INFO).
  Green: run 39941848259. Commits `[skip ci]`, runs dispatched by hand, ~60 Actions minutes. No plugin code changed;
  invariants hold (default `local` path unchanged; no wrong rows in any run).
- 2026-10-23 (cloud session 14): item 1 c) completed: S3 Iceberg warehouse on int6 with 3 servers, partial run
  39949170568 and complete run 39949173080, both green (all checks OK incl. `orders_del` deletes and failover on `ice`;
  commit-window INFO 1/2 clean errors; complete-mode mid-stream kill hit after data once, clean error). No new server
  build in `e2e-assets`, so 1 a) stays open. ~47 Actions minutes. No code changed; invariants hold.
- 2026-10-23 (cloud session 15): no new server build in `e2e-assets` (1 a) still waits). Item 3 follow-up: the e2e TopN
  check now records rows read off/on (`topn-rows.tsv`; `df_stats` moved up, unchanged): rows drop to n per split, but the
  lineitem TopN queries run 10–35 % slower with pushdown on the runner (Open question updated). Item 7 optional done:
  int6 partial 1 server green on Trino 483 (run 39956762839) and 477 (run 39959398904). Commits `[skip ci]`, runs
  dispatched by hand, ~34 Actions minutes. No plugin code changed; invariants hold.
- 2026-10-23 (cloud session 16): no new server build in `e2e-assets` (1 a) still waits; other items wait on Open
  questions). `e2e-assets` turned out to be a published pre-release, so the e2e job now runs with `contents: read`
  (least privilege); verified by run 39964823185 (int4 partial 1 server, Trino 455, green, 16 min: assets listed and
  downloaded with the read token, all checks passed). ~16 Actions minutes. No plugin code changed; invariants hold.
- 2026-10-23 (cloud session 17): nothing actionable: `e2e-assets` unchanged since 2026-10-22 20:16 (no server build
  after int6, 1 a) waits), every other item waits on Open questions. `gh` was not authenticated in this session (GH_TOKEN
  rejected); the GitHub MCP tools worked. No CI run, 0 Actions minutes, no code changed.
- 2026-10-23 (cloud session 18): nothing actionable, same as session 17: `e2e-assets` unchanged (newest asset int6,
  2026-10-22 20:16), no maintainer answers in Open questions. `gh release view` fails (GraphQL blocked);
  `gh api repos/C0fuge/heron/releases/tags/e2e-assets` works. No CI run, 0 Actions minutes, no code changed.
- 2026-10-23 (cloud session 19): maintainer note "use server int7". CI: `server-build=int7` documented in the workflow
  and scripts, the job prints the selected build's sha256 (int7 is not in `SHA256SUMS`; verified in the session: both
  hashes match the note), the `orders_del` KNOWN exception limited to int5/int6. Trino 455 int7 local warehouse: partial
  1+3 servers run 39982788809 (18 / 26 min) and complete 3 servers run 39982792807 (22 min), all green — TPC-H 22/22,
  types, every Iceberg check OK **incl. `orders_del` position deletes** (1 a) fixed by int7), failover incl. kill during
  a read and mid-stream kill (hit after data in complete mode too, clean error), two nodes down. ~66 Actions minutes.
  No plugin code changed; invariants hold. The maintainer's "Dependency audit" note arrived during this session: it is
  the next item (before other follow-ups); not started.
- 2026-10-23 (cloud session 20): dependency audit item done. Pins Netty 4.1.139 / gRPC 1.78.0 / protobuf 3.25.9 / Jackson
  2.18.12 / Guava 33.5.0; first try with gRPC 1.82.4 failed the unit tests (run 39990774489, Arrow 18.1 needs a gRPC internal
  removed in 1.79); bundle drift check in `build.yml` (first version failed on the zip's file naming, run 39991081290;
  green 39991483490). Build all versions run 39991724522 green; e2e int4 455 partial 1+3 servers run 39991081221 and int7 483
  partial 1 server run 39991939760 green. Push e2e 39990774578 cancelled (broken build). ~55 Actions minutes. Plugin code
  unchanged (dependencies only); invariants hold (failover checks incl. mid-stream kill green, token auth in every e2e run).
- 2026-10-23 (cloud session 21): item 7, int7 coverage with the new dependency pins. e2e int7 Trino 477 complete 3 servers run
  39998499713 and Trino 483 partial 3 servers with `iceberg-warehouse=s3` run 39998502629: both green (TPC-H 22/22, types,
  Iceberg incl. `orders_del`, kill during a read, mid-stream kill hit after data = clean error, two nodes down). ~50 Actions
  minutes. No code changed; invariants hold.
- 2026-10-23 (cloud session 22): item 7, open question "does int7 still need `core-site.xml`?". e2e input `iceberg-core-site`
  (default: workaround only for int5/int6); int7 Trino 455 partial 1 server without it, run 40006799225: green (TPC-H 22/22,
  TopN, dynfilter, types, Iceberg incl. `orders_del` deletes). e2e harness only, pushed with `[skip ci]` (int4 push matrix
  would not exercise it). ~19 Actions minutes. No plugin code changed; invariants hold.
- 2026-10-23 (cloud session 23): item 7, last open int7 combination without the `core-site.xml` workaround: e2e int7 Trino 455
  partial 3 servers, `iceberg-warehouse=s3`, `iceberg-core-site` empty (= 0), run 40014711279: green. ~29 Actions minutes. No
  code changed; invariants hold. Remaining item-7 work waits on the maintainer's Open questions.
- 2026-10-23 (cloud session 24): nothing actionable — Next steps hold only items that wait on Open questions; last runs all
  green (latest 40014711279). No CI run, no code change. Added the "Queue empty" note to Next steps so later sessions stop early
  (≥ 850 Actions minutes used this month). Invariants untouched.
- 2026-10-23 (local stand agent): open questions on split count / endpoint parallelism, TopN, dynamic filters,
  statement cache and collation measured on the 3-node HDFS SF10 stand with server int7 and plugin e0ebab5 (CI run
  39914348258); answers + recommended defaults in Open questions "Stand answers 2026-10-23". No code changed, no CI run.
- 2026-10-24 (cloud session 40): Next steps stand item (a), q04 slow on 3 nodes: multi-endpoint aggregation merge rewritten as a
  sorted k-way stream merge (server `ORDER BY` group columns; property `aggregation-sorted-merge.enabled`, default true; hash
  merge kept as fallback). Build run 40140697443 red (testCompile: varargs in the new test), 40140813291 green (455). `e2e-assets`
  now holds only int7 (re-uploaded 21:51 UTC, new plugins tarball hash, see Open questions) → push e2e default int7; run
  40140813293 failed at asset download (int4 gone). Run 40140961985 (int7 455 partial 1+3 servers): 1 server green, 3 servers
  all checks OK incl. new `aggmerge` (sorted = hash on q04/q18/GROUP BY l_orderkey) except the mid-stream kill: fixed 8-11 s
  delays all after the 7.5 s baseline → partial-mode delays now 30-75 % of the baseline; run 40142038380 green 1+3 servers
  (mid-stream kill hit after data on attempt 1 at 5.6 s = clean error). ~45 Actions minutes. Invariants hold (no failover/auth
  change; an unsorted server stream fails cleanly; all answers exact). Stand recheck of q04/q18 requested in Next steps.
