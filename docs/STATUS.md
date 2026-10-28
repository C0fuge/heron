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
  job) and, with 3 servers, runs the failover checks (see `docs/E2E-RESULTS.md`). Input `server-build` = int7 or a server-CI build id (default `cadd1aca9c23` since session 82, `4e77c6bda59a` in session 77-81, `45fc0a9e6e02` in session 70-76, `775100bf923f` in session 68-69, `27f583adccd7` (DuckDB 1.5.6 extensions) in session 66-67, `57d252b1437f` in session 61-65, `7fbe2d75827c` in session 57-60, `60e888bc26ec` in session 54-56 (briefly 20b887839edf), before that `4eaab052deaf` since session 47; int4-int6 are gone from the release; all but int4 run `e2e/ci/iceberg.sh`). `SHA256SUMS` now lists int7 and 4eaab052deaf (the job checks them);
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
  httpfs **v1.4.5** (**v1.5.6** for builds after 57d252b1437f) (`http://extensions.duckdb.org/v1.4.5/linux_amd64/httpfs.duckdb_extension.gz`, keep the
  file name `httpfs.duckdb_extension`), MinIO credentials via `FLIGHT_S3_ACCESS_KEY`/`FLIGHT_S3_SECRET_KEY`
  (throwaway, generated in the job), auth `FLIGHT_ALLOW_INSECURE=true` or
  `FLIGHT_AUTH_PROVIDER=token FLIGHT_AUTH_TOKEN=<t>`, `-DresultMode=partial|complete`. Two or three servers
  (`--hosts a,b,c`) form a cluster with backup locations.
- Trino: `trinodb/trino:455` (and 477/483) image, plugin dir mounted at `/usr/lib/trino/plugin/arrow-flight`,
  catalog `connector.name=arrow_flight`, JVM flags from README.

## Next steps

> **New server build `server-1.0.0-a9e43ca5e296`** (2026-10-28, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` in this repo's assets release). Since cadd1aca9c23: LDAP / Active Directory login (`FLIGHT_AUTH_PROVIDER=ldap`,
> opt-in LDAP groups into Ranger via `FLIGHT_LDAP_GROUP_SEARCH_BASE`) and OIDC / Knox JWT login (`FLIGHT_AUTH_PROVIDER=jwt`)
> as optional modules (server release asset `flight-auth-...tar.zst`, not in these assets; need Flight TLS), opt-in Hive ORC
> tables `hive.orcTables` and Hive text tables `hive.textTables` (default false), S3 small-query tail fix (`duckDbThreads`
> default at least 4 on S3/lakeFS), S3/lakeFS read cache now held at its size in heap + opt-in admission
> `arrowflight.s3.cacheAdmission=reuse`, opt-in caps `duckDbThreadsUnderLoad` / `duckDbProducingStreams` (default 0 = off),
> opt-in zero-copy writes `flightZeroCopyWrites`, opt-in distributed complete mode `distributedComplete` (default false), and
> size-aware DuckDB sizing (item 19b: `duckDbThreads` cores/4 between 2 and 16, a per-connection `memory_limit` from host
> cores + memory by default, `duckDbMemoryLimit=none` restores DuckDB's default). Same DuckDB 1.5.6 extension set. Run the
> suite once on it (note timings) and make it the CI default if green.
>
> **Session 86: not yet the CI default (cadd1aca9c23 stays), because of one intermittent heap-check failure.** On a9e43ca5e296:
> complete mode 3 servers run 40683589412 green; partial 3 servers green (40683586005 job e2e (3)); partial 1 server **failed
> once** (40683586005 job e2e (1), the only red check: `1/eps4 1G default q18 ERROR` "Query exceeded per-node memory limit of
> 307.20MB [Allocated: 305.67MB …HashBuilderOperator=228.13MB]"), then green twice (40687927967, 40690539517; peaks 249 / 427
> MB live). cadd1aca9c23 baseline run 40687878656 green (eps4 default peak 409 MB). Everything else (TPC-H 22/22, types,
> Iceberg, TopN, splits, dynamic filters, concurrent, failover) green on a9e43ca5e296. **Cause (EXPLAIN, now printed by
> `heap_plan` in `e2e/ci/heap-probe.sh` for eps4 runs and failures):** q18's `InnerJoin[o_orderkey = l_orderkey, PARTITIONED]`
> builds its hash table from the lineitem scan (6001215 rows, 103 MB estimated) because the IN-subquery side is estimated
> `rows: ?`; whether the build stays small depends on how early the semi-join side (the pushed `GROUP BY l_orderkey` merged from
> 4 endpoints) finishes - a timing race, not a server or plugin answer difference; the margin at Trino 1G (30 % per-node limit)
> is ~1.5 MB. **Next:** make the guard deterministic before switching (options: measure the failure rate with a heap-probe-only
> loop on both builds; run the eps4 check with `query_max_memory_per_node`-independent criterion such as "no
> OutOfMemoryError / answer exact" and report the memory-limit error as INFO; or give q18 a join hint so lineitem is not the
> build side), then switch the default (`e2e.yml` two places + `run-e2e.sh`). **Timings** (profile run 40683593135, settings
> `default / flight.split_count=1 / flight.endpoint_splits_enabled=true`, best of 2, SF1): 3 servers **28.94 / 24.88 / 23.08 s**
> on AMD EPYC 9V74 (cadd1aca9c23 3 servers on EPYC 7763: 34.2 / 27.1 / 26.6); 1 server **34.40 / 27.79 / 31.79 s** on EPYC
> 7763 (cadd1aca9c23 1 server on 9V74: 30.6 / 25.5 / 27.8) - CPU models swapped between the two runs, so no conclusion beyond
> "same range". Raw lineitem read: ADBC 1 stream 1.51 s (3 srv) / 1.96 s (1 srv), Trino split 1 1.90 / 1.96 s.


> **Maintainer, 2026-10-28: yes to result compression as a connector opt-in.** Add a catalog property (off by default) that
> asks the server for `arrow-compression: zstd|lz4` and decodes compressed IPC bodies; never on unless set. Test: answers
> equal with and without (full suite on one setting), and a slow-link check in CI (`tc netem delay 10ms rate 1gbit` on the
> runner's loopback or between containers) reporting MB/s with off / lz4 / zstd. The server side is in builds since
> `cadd1aca9c23` (assets). Then continue with your own open items.
>
> **Done (session 84): `arrow-flight.result-compression` = `NONE` (default) | `ZSTD` | `LZ4`.** Set, data streams (`DoGet` of
> `openStream` only; metadata calls and info streams never) send `arrow-compression: zstd|lz4`; the reader decodes the
> compressed IPC bodies. Arrow Java 18's FlightStream takes no codec factory and its default one comes from a `ServiceLoader`
> lookup on the thread context class loader, done once per class loader (unreliable inside a Trino plugin), so
> `StreamDecompression` swaps the stream's `VectorLoader` (reflection on `FlightStream.loader`/`root`/`cancelled`, checked at
> catalog start) for one with `arrow-compression`'s `CommonsCompressionFactory`, after the schema and before the first batch;
> failures there go through the normal failover path. `NONE` sends nothing: wire and code path unchanged. Bundle +5 jars
> (arrow-compression, zstd-jni 1.5.6-6, commons-compress 1.27.1, commons-io 2.16.1, commons-lang3 3.16.0; all three Trino lists).
> Unit tests `FlightSqlSessionCompressionTest` (11: off sends no header, zstd/lz4 asked and decoded, only DoGet asks, old server
> ignoring it, TCCL-independent decoding, failover on UNAVAILABLE / refused location, INVALID_ARGUMENT not failed over, stream
> ending without schema does not hang) + config test; build run 40670140762 green on 455/477/483. **e2e:** full suite with
> `result-compression=zstd` run 40670144631 green on 1 and 3 servers (TPC-H 22/22, types, Iceberg, TopN, splits, failover);
> default (off) run 40671042992 green on 1 and 3 servers (Invariants). New e2e inputs `result-compression` and `slow-link=only`
> (`e2e/ci/slowlink.sh`: lineitem `checksum(ROW(16 cols))`, nothing pushed, catalogs flight / flight_lz4 / flight_zstd, 2 runs
> each, plain link then `tc netem delay 10ms rate 1gbit limit 100000` on the host side of Trino's veth; answers must match).
> **Slow-link run 40670149146** (1 server, partial, SF1, best of 2; effective MB/s = plain Arrow bytes / elapsed):
>
> | link | setting | s | MB on link | effective MB/s |
> |---|---|---|---|---|
> | plain | off | 6.24 | 1051.8 | 168.6 |
> | plain | lz4 | 11.24 | 412.2 | 93.6 |
> | plain | zstd | 7.39 | 245.0 | 142.3 |
> | 1 Gbit + 10 ms | off | 9.41 | 1051.8 | 111.8 |
> | 1 Gbit + 10 ms | lz4 | 10.95 | 412.1 | 96.1 |
> | 1 Gbit + 10 ms | zstd | 7.22 | 245.0 | 145.7 |
>
> ZSTD: 4.3x fewer bytes and **23 % faster than off on the 1 Gbit link** (7.22 vs 9.41 s), 18 % slower on the unshaped Docker
> link. LZ4 (2.55x) is slower than off on both: CPU-bound, the plugin's LZ4 decoder is commons-compress's pure-Java frame
> codec (ZSTD is native zstd-jni). Suggested value where the link is ≤ ~1 Gbit: `ZSTD`. Off stays the default. The stand (real
> network, SF10) should measure off vs ZSTD before anyone turns it on (see Open questions).

> **New server build `server-1.0.0-cadd1aca9c23`** (2026-10-28, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` in this repo's assets release). Since 7562a5bdb7b9 (so it also covers what 4e77c6bda59a brought): opt-in result
> stream compression ZSTD/LZ4 (stage 3 item 20; the client asks via call header or Flight SQL session option `arrow-compression`;
> buffers compressed in parallel, `flightCompressionThreads`; no default or wire change for clients that do not ask), opt-in S3
> read cache `arrowflight.s3.cacheMegabytes`, size-aware `duckDbThreads` default (cores/4, between 2 and 8; was 4), Iceberg time
> travel planned in partial mode, position deletes grouped. Needs the DuckDB 1.5.6 extension set (server repo `ci-assets`). Run
> the suite once on it (note timings) and make it the CI default if green.
>
> **Done (session 82): green, now the e2e default** (`e2e.yml`, `run-e2e.sh`; 4e77c6bda59a and older stay selectable; same DuckDB
> 1.5.6 extension set, server started unchanged, plugin does not ask for compression so the wire is unchanged). Partial 1+3
> servers run 40617506171, complete 3 servers run 40617513105: full suite green (TPC-H 22/22, types, Iceberg, TopN, splits,
> failover incl. two down 5/5 + 3/3, coordinator-down 13.8 s on 3 servers partial). Job wall time 1 server 13.6 min, 3 servers
> 19.4 / 19.2 min. **Timings** (profile run 40617519351, settings `default flight.split_count=1 flight.endpoint_splits_enabled=true`,
> Trino elapsed, best of 2, SF1): 3 servers **34.2 / 27.1 / 26.6 s** on AMD EPYC 7763 (4e77c6bda59a on the same CPU model,
> session 78 pair 2: 36.0 / 27.4 / 27.4 s) - equal within noise; 1 server **30.6 / 25.5 / 27.8 s** on AMD EPYC 9V74 (no
> earlier run on that model to compare). Raw reads 3 servers: Java client one stream 1.77 s, ADBC 1.75 s, Trino split 1 1.92 s;
> 1 server 1.82 / 2.02 / 2.05 s. Small queries 3 servers q01 / q04 / q06 0.68 / 0.88 / 0.79 default, 0.64 / 0.85 / 0.72 split 1.
> No regression; the new server features (compression, S3 cache, `duckDbThreads`) are opt-in or invisible at SF1 on 4 vCPUs.
>
> *(Session 82 follow-up candidate "plugin opt-in for result compression": answered by the maintainer 2026-10-28, done in session 84, above.)*


> **Session 71 (TopN pushes VARCHAR sort keys).** Every item below is done or waits on the stand / a maintainer answer, so this
> session took the stand's 2026-10-23 collation finding (server compares strings by UTF-8 bytes = Trino's order): with TopN
> pushdown on (still **off by default**), `VARCHAR` sort keys are now pushed too (`SqlQueryBuilder.isTopNSortable`; CHAR, DOUBLE,
> REAL still not). The sorted aggregation merge keeps its old rule (fixed-width keys only, new `isServerOrderedFixedWidth`).
> Unit test `ArrowFlightMetadataTopNTest.varcharSortKeysArePushedBecauseTheServerComparesUtf8Bytes` (+ CHAR refused); 58 tests,
> build 40477030899 green. e2e TopN check +3 VARCHAR queries (q4 `o_comment`, q5 `s_comment DESC` mixed case, q6 `c_mktsegment
> DESC, c_name`): push run 40477030751 green on 1 and 3 servers (full suite), on = off exactly, TopN in the scan; rows read on 3
> servers 1,500,000 → 150 / 10,000 → 30 / 150,000 → 25. **Stand:** nothing new needed beyond the TopN default question.

> **New server build `server-1.0.0-4e77c6bda59a`** (uploaded 2026-10-27 20:51-20:53 UTC to `e2e-assets`, jar, plugins tar, libs
> tar + `SHA256SUMS`; no change note came with it - the maintainer may want to add one). Plugins tar +60 KB vs 45fc0a9e6e02.
>
> **Done (session 77): green, now the e2e default** (`e2e.yml`, `run-e2e.sh`; 45fc0a9e6e02 and older stay selectable; same DuckDB
> 1.5.6 extension set, server started unchanged). Partial 1+3 servers run 40582176690, complete 3 servers run 40582181234: full
> suite green (TPC-H 22/22, types, Iceberg, TopN, splits, failover incl. two down 5/5 + 3/3, coordinator-down 13.6 s on 3 servers
> partial). Job wall time 1 server 15.7 min, 3 servers 18.9 / 17.3 min. **Timings** (profile run 40582216959, settings
> `default flight.split_count=1 flight.endpoint_splits_enabled=true`, Trino elapsed, best of 2, SF1; session 70's run 40469761749
> on 45fc0a9e6e02 in brackets): 3 servers **26.0 / 22.0 / 20.3 s** (37.9 / 29.8 / 30.3); 1 server **27.6 / 23.8 / 25.8 s**
> (42.7 / 31.1 / 36.9, slow runner). Small queries 3 servers q01 / q04 / q06 0.58 / 0.81 / 0.68 default (1.00 / 0.97 / 0.92),
> 0.57 / 0.73 / 0.67 split 1 (0.70 / 0.93 / 0.81). Raw reads did not change much (3 servers: Java client 1 stream 1.45 s, ADBC
> 1.42 s, Trino split 1 / 4 1.67 / 1.81 s), so the gain is per-query overhead - the largest drop seen on CI since the DuckDB
> 1.5.6 build. Not yet repeated back to back with 45fc0a9e6e02 on the same runner type; the stand (SF10) should confirm.
>
> **Back to back (session 78): the ~30 % above was mostly runner hardware.** Two same-time pairs, settings as above, Trino 455
> partial (old 45fc0a9e6e02 → new 4e77c6bda59a, TPC-H SF1 sums default / split 1 / endpoint splits). GitHub's `ubuntu-latest`
> hands out different CPUs (Trino's own CPU time for the same queries, which does not depend on the server, differed up to 1.7x);
> `profile.sh` now logs the runner (`runner: 4 vCPU, <model>`). Pair 1 (runs 40590270774 old / 40590274657 new, CPU model not
> yet logged): 3 servers 38.0 / 29.2 / 29.4 → 25.8 / 22.8 / 20.9 s, but Trino CPU 13.5 → 8.7 s (faster runner for the new
> build); 1 server 37.3 / 28.5 / 34.4 → 26.7 / 24.5 / 24.9 s with Trino CPU 12.4 → 11.5 s and equal raw reads (Java client one
> stream 1.79 vs 1.89 s, ADBC 1.83 vs 2.01 s), so on 1 server the gain looks real (heavier queries: q17 3.19 → 2.08, q21 6.40 →
> 2.66 s default). Pair 2, 3 servers (runs 40592375074 old on AMD EPYC 9V45 / 40592379356 new on AMD EPYC 7763): old 27.6 /
> 23.3 / 21.8 s (Trino CPU 9.3 s) vs new 36.0 / 27.4 / 27.4 s (15.6 s) - reversed by the runner. Matched by runner class (Trino
> CPU ~9 s: old pair 2 vs new pair 1; ~13-16 s: old pair 1 vs new pair 2) **the new build is ~3-7 % faster on 3 servers**;
> 1 server 14-28 % from one pair only. **CI timing comparisons across runs are only valid on the same CPU model** (check the
> `runner:` line; compare Trino CPU too). The stand (SF10, fixed hardware) remains the real check.


> **New server build `server-1.0.0-45fc0a9e6e02`** (2026-10-27, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` in this repo's assets release) = 27f583adccd7 + per-query overhead cuts (stage 2 item 15: describe once per views
> snapshot, parse check once per node, partial-mode sample-file cache, executionPath log line written asynchronously). Needs
> the DuckDB 1.5.6 extension set (server repo `ci-assets`). Candidate for the presentation build: run the suite once on it
> (note small-query timings) and make it the CI default if green.
>
> **Done (session 70): green, now the e2e default** (`e2e.yml`, `run-e2e.sh`; 775100bf923f and older stay selectable; same
> DuckDB 1.5.6 extension set, server started unchanged). Partial 1+3 servers run 40469746196, complete 3 servers run 40469753759:
> full suite green (TPC-H 22/22, types, Iceberg, concurrent, splits incl. ep1, failover: two down 5/5, coordinator-down 14.0 s on
> 3 servers partial). **Timings** (profile run 40469761749, settings `default flight.split_count=1 flight.endpoint_splits_enabled=true`,
> Trino elapsed, best of 2, SF1; session 69's run 40463937582 on 775100bf923f in brackets): 3 servers 37.9 / 29.8 / 30.3 s
> (37.3 / 29.3 / 30.0) - unchanged; small queries q01 / q04 / q06 1.00 / 0.97 / 0.92 default, 0.70 / 0.93 / 0.81 split 1.
> 1 server 42.7 / 31.1 / 36.9 s (37.7 / 28.5 / 35.2), but that runner was slow on raw streams too (plugin Java client one stream
> 3.49 s, ADBC 4.50 s, usually ~1.9 s). Back-to-back read benchmark on 1 server (runs 40472082623 new / 40472093615 old):
> ADBC one stream 2.16 vs 2.18 s, Trino split 1 / 4 2.38 / 2.04 vs 2.29 / 2.48 s - **no regression; the 1-server profile was
> runner noise.** Direct ADBC q01 0.52 s (1 server) / 0.73 s (3 servers). As with 775100bf923f, the per-query overhead cuts are
> below what a 4-vCPU runner resolves at SF1; the stand (SF10) is where they should show.


> **New server build `server-1.0.0-775100bf923f`** (2026-10-27, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` in this repo's assets release) = 27f583adccd7 + complete mode describes a query once per catalog-views snapshot
> (per-query overhead, stage 2 item 15 step 1). Needs the DuckDB 1.5.6 extension set (server repo `ci-assets`). Run the suite
> once on it (note small-query timings) and make it the CI default if green.
>
> **Done (session 68): green, now the e2e default** (`e2e.yml`, `run-e2e.sh`; 27f583adccd7 and older stay selectable; same
> DuckDB 1.5.6 extension set, server started unchanged). Partial 1+3 servers run 40457717079, complete 3 servers run 40457720101:
> TPC-H 22/22, types 2/2, Iceberg 8/8 incl. time travel, concurrent 4/4, splits block 5/5 + checksum = reference (3 servers
> full scan s1p1 8.03 / s4p1 5.00 / ep1 4.92 s), heap `1 1G default q18 OK` 297 MB, `1/eps4` 298 MB, `3 1G` 416 / 473 MB,
> failover: kill during read clean, mid-stream kill hit after data `tx8M@2.4` (partial) / `tx8M@3.2` (complete), 0 wrong answers,
> restarted exact after 14.3 / 16.4 s with 0 backup reads, two down 5/5 + 3/3, coordinator-down 8.1 / 13.6 s. Job wall time
> 1 server 16.4 min, 3 servers 14.7 / 19.8 min. **Timings** (profile run 40457722482, settings `default flight.split_count=1`,
> Trino elapsed, best of 2 passes, SF1; compare the 27f583adccd7 table below):
>
> | | 1 server 27f583adccd7 → 775100bf923f | 3 servers 27f583adccd7 → 775100bf923f |
> |---|---|---|
> | 22 queries + scan, default (split 4) | 36.1 → 36.6 s | 28.9 → 35.7 s (run 40452517908 on the old build: 36.3-36.9 s) |
> | same, `split_count=1` | 27.7 → 27.8 s | 24.5 → 28.1 s (run 40452517908: 28.5-29.6 s) |
> | q01 / q04 / q06 | 0.80 / 0.75 / 0.85 → 0.68 / 0.76 / 0.83 | 0.68 / 0.78 / 0.90 → 0.68 / 0.96 / 0.79 |
> | other small (q02 q11 q13 q16 q22) | 0.68 0.45 0.69 0.53 0.37 → 0.78 0.46 0.84 0.43 0.39 | 0.64 0.35 0.64 0.45 0.28 → 0.73 0.69 0.71 0.59 0.41 |
> | raw scan, Trino split 1 / 4 | 1.93 / 2.35 → 1.98 / 2.55 | 1.74 / 1.81 → 2.08 / 2.37 |
> | plugin Java client, 1 stmt / 4 range splits | 1.78 / 2.40 → 1.93 / 2.56 | 1.71 / 1.88 → 1.90 / 2.40 |
>
> **No measurable change on CI.** The small queries are within run noise (the 3-server sums of the 27f583adccd7 build itself
> varied 28.9 → 36.3 s between runs 40448316888 and 40452517908); the per-query describe saving of 775100bf923f is below what
> a 4-vCPU runner resolves at SF1. Direct ADBC q01 0.80 s (1 server) / 0.65 s (3 servers). The stand (SF10, dedicated cores) is
> where the per-query overhead should show; see also the direct-ADBC q04 note in Open questions.


> **New server build `server-1.0.0-27f583adccd7`** (2026-10-27, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` in this repo's assets release) = 57d252b1437f + **DuckDB 1.5.6** (was 1.4.5; upstream comparison: about half of
> the small-query latency gap) + HDFS Kerberos ticket cache from a keytab + cluster crashed-member wait documented. **It needs
> the DuckDB 1.5.6 extension set** (the server checks extension versions at startup): `duckdb-extensions-v1.5.6.tar.zst`
> (httpfs, iceberg, avro, hadoopfs `51b7220-afc3`, layout `extensions/v1.5.6/linux_amd64/`) and
> `hadoopfs-v1.5.6-afc3.duckdb_extension` are in the server repo's `ci-assets` release (sha256 in its SHA256SUMS); official
> httpfs/iceberg/avro 1.5.6 builds also work. Run the suite once on it (note small-query timings) and make it the CI default if green.
>
> **Done (session 66): green, now the e2e default** (`e2e.yml`, `run-e2e.sh`; older builds stay selectable). `run-e2e.sh`
> picks the DuckDB extension version from the build (v1.4.5 for int* .. 57d252b1437f, **v1.5.6** for newer builds; override
> `DUCKDB_EXT_VERSION`); official httpfs/iceberg/avro v1.5.6 from extensions.duckdb.org, server started unchanged otherwise.
> Partial 1+3 servers run 40446519100, complete 3 servers run 40446521466: TPC-H 22/22, types 2/2, Iceberg incl. time
> travel, heap and concurrent checks, failover (kill during read exact/clean errors, mid-stream kill hit after data
> `tx8M@3.0`, restarted exact after 16.3 s with 0 backup reads, two down 5/5, coordinator-down 13.6 s). Job wall time 1 server
> 11.3 min, 3 servers 18.0 / 18.5 min. **Timings** (profile run 40448316888 vs run 40442908161 on 57d252b1437f, same settings,
> Trino elapsed, best of 2 passes, SF1, 4-vCPU runner):
>
> | | 1 server old → new | 3 servers old → new |
> |---|---|---|
> | 22 queries + scan, default (split 4) | 41.5 → 36.1 s | 45.5 → 28.9 s |
> | same, `split_count=1` | 29.9 → 27.7 s | 33.2 → 24.5 s |
> | q01 / q04 / q06 | 1.32 / 0.79 / 0.83 → 0.80 / 0.75 / 0.85 | 1.40 / 1.31 / 1.01 → 0.68 / 0.78 / 0.90 |
> | other small (q02 q11 q13 q16 q22) | 0.78 0.43 0.81 0.61 0.47 → 0.68 0.45 0.69 0.53 0.37 | 0.85 0.49 0.73 0.60 0.43 → 0.64 0.35 0.64 0.45 0.28 |
> | raw scan, Trino split 1 / 4 | 1.88 / 3.24 → 1.93 / 2.35 | 1.93 / 3.39 → 1.74 / 1.81 |
> | plugin Java client, 1 stmt / 4 range splits | 1.80 / 3.07 → 1.78 / 2.40 | 1.79 / 3.60 → 1.71 / 1.88 |
>
> Small queries are 0.1-0.7 s faster (q01 biggest, q06/q11 flat on 1 server); range splits cost much less server work with
> DuckDB 1.5.6 (4 splits 1.35x one statement on 1 server, was 1.70x), so the split_count gap of session 65 shrinks (1 server
> default vs split 1: 36.1 vs 27.7 s; 3 servers 28.9 vs 24.5 s). Direct ADBC q01 1.45 → 0.78 s (1 server). **Stand:** the SF10
> comparison (item "Stand (next)" below) should be run on 27f583adccd7.


> **Stage 2, approved by the maintainer 2026-10-26 - plugin throughput (take first).** On the stand, Trino TPC-H SF10 takes
> ~32-38 s whatever the server settings (1-16 DuckDB threads, HDFS), while the same queries through JDBC complete mode take
> 7.7-11 s on the same server: the time is spent in the plugin/Trino path. Upstream's own connector is 17 % faster on SF1.
> Profile one SF10 run (where the worker time goes: Arrow->Block conversion, page building, endpoint read waits, merging) and
> attack the biggest share: zero-copy or fewer-copy Arrow->Block for fixed-width types, parallel endpoint reads by default
> where safe, bigger/fewer splits if split overhead dominates, q18 with a 6 GB heap on several endpoints. Report before/after
> SF1/SF10 times from CI; the coordinator re-measures on the stand.
>
> **Session 64 (CI profile + two low-risk changes; stand measurement pending).** New e2e mode `profile=only`
> (`e2e/ci/profile.sh`, `profile_report.py`, `flight_time.py`; workflow inputs `profile`, `profile-settings`): TPC-H SF1 q01-q22
> + a raw `lineitem` scan (8 columns, aggregation pushdown off) per setting, Trino query stats per query (elapsed, CPU, plugin
> read time = Flight read + Arrow->Block, scan blocked time), JFR CPU samples of the Trino JVM grouped by category, and the same
> SQL straight to flight1 over ADBC. Findings (4-vCPU runner, SF1, server 57d252b1437f; runs 40435218497 baseline, 40436253774,
> 40437349225, 40438299101):
> - **The plugin is not CPU-bound.** JFR: Arrow->Block conversion 0.5-1.4 % of CPU samples, Arrow IPC/gRPC < 1 %, Trino
>   operators 73-76 % (join hash build/probe, group-by hash, exchange). Trino CPU is 9-21 s per 22 queries against 29-46 s
>   elapsed, i.e. Trino is waiting, not computing.
> - **Scans wait for the server streams.** Without read-ahead 83-87 % of all operator wall time is the scan drivers inside the
>   plugin's synchronous `next()`. Raw scan 6.0M rows / 543 MB: Trino CPU 0.15-0.25 s but elapsed 2.4-4.4 s (1 server, 4 range
>   splits) and 2.4-2.6 s (3 servers), while **one direct ADBC stream of the whole table takes 1.84 s** on the same server. So the
>   plugin's Flight path (4 range-split queries x endpoints) delivers rows no faster than one plain client stream: per-stream
>   rate on the server/gRPC side (range-predicate re-scans, Java Flight client flow control, the server's shared outbound byte
>   budget) is the suspect, not Trino. Not resolvable on a 4-vCPU runner where DuckDB, the servers and Trino share the cores.
> - Direct server baseline exists only for single-table SQL (the server refuses joins/subqueries: "Can only select from ONE
>   table"): q01 1.43 / q04 0.88 / q06 0.95 s direct vs Trino elapsed 1.3-2.2 / 0.7-1.7 / 0.8-1.5 s (pushed down: same work).
> - `split_count` 16 vs 4 and read-ahead on vs off: totals within run-to-run noise on CI (e.g. 1 server elapsed sums 36.6-43.6 s
>   default vs 32.0-36.7 s read-ahead off vs 41.7-44.2 s split 16 in run 40438299101; earlier runs the other way round).
>
> Changes (commit "Plugin throughput: bulk Arrow->Block copies, read-ahead of the next batch"): (1) `ArrowTypeConverter` copies
> fixed-width values, decimal pairs and varchar offsets in bulk (little-endian NIO views) and reads nulls from the validity bitmap;
> (2) **read-ahead** (`arrow-flight.read-ahead.enabled` / session `read_ahead_enabled`, default **on**): every plain scan goes
> through `ParallelStreamReader`, and once a batch is converted `release()` lets the stream fetch/decode the next batch in the
> background while Trino processes the page; the driver yields (`isBlocked`) instead of sitting in `next()`. Same streams, same
> failover rules, no extra batch held. Unit tests 49 green (build 40435458428; slice test now also checks the bulk paths against
> the value-by-value reader for 13 vector kinds; `ParallelStreamReaderTest.releaseStartsTheNextReadBeforeTheNextPoll`); full e2e
> push run 40435458521 green on 1 and 3 servers (22/22, types, Iceberg, failover, heap and concurrent checks). CI shows no
> speed-up from either change (expected given the profile); they remove per-row work and stop holding a driver thread during
> network waits, which can matter with many drivers on the stand.
>
> **Session 65 (CI: Java client cleared, range splits are the cost).** New `profile=bench` mode (also runs at the end of
> `profile=only`): `e2e/ci/FlightReadBench.java` reads the raw `lineitem` scan (`scan-direct.sql`, 6.0M rows, 645 MB of Arrow
> buffers) through the plugin's own client (`FlightSqlSession`/`FlightDataStream`, the plugin jars, run with `java` inside the
> Trino container, drain only, no Trino) as one statement and as the plugin's range splits on `l_orderkey`; `flight_scan_time.py`
> does the same over ADBC; then the Trino scan with `flight.split_count` 1/2/4. Runs 40441072747, 40441623193, 40442318786
> (bench) and 40442908161 (`profile=only`, settings `default flight.split_count=1`), server 57d252b1437f, best of 2 (s):
>
> | | Java 1 stmt | Java 2 splits | Java 4 splits | Java 4 splits one after another | ADBC 1 / 2 / 4 | Trino split_count 1 / 2 / 4 |
> |---|---|---|---|---|---|---|
> | 1 server | 1.80-1.97 | 2.76-2.87 | 3.07-3.15 | 2.84-2.89 | 1.78-2.00 / 2.63-2.72 / 3.07-3.15 | 1.88 / 2.72 / 3.24 |
> | 3 servers | 1.79-3.49 | 2.45-3.82 | 3.60-3.97 | 2.57-4.70 | 1.54-2.82 / 2.42-3.79 / 2.52-4.41 | 1.93 / 2.83 / 3.39 |
>
> - **The plugin's Java Flight client is as fast as ADBC** (C++/Go) in every shape, within run noise; reading a statement's
>   endpoints in parallel changes nothing measurable. Not the bottleneck.
> - **Range splits cost server work**: 4 range statements run *one after another* already take ~1.5x one statement (2.88 vs
>   1.97 s): each range query re-reads the files (the `l_orderkey` ranges do not prune row groups well); concurrently on the
>   4-vCPU runner they are slower still. **Trino itself adds almost nothing**: with `split_count=1` the Trino scan (1.88-1.95 s,
>   CPU 0.24 s) equals one direct stream.
> - **Correction to session 64:** the profile's per-query times were wall times of `docker exec trino trino` and included
>   ~1.4 s of CLI JVM start per query (Trino's own elapsed for the scan was 2.37 s where the wall time said 3.82 s).
>   `profile_query` now reports Trino's `elapsedTime`.
> - **TPC-H SF1, Trino elapsed, sum of 22 queries + scan** (run 40442908161): 1 server default (split 4) 42.3-50.8 s vs
>   `split_count=1` 30.4-31.1 s; 3 servers 45.5-56.5 s vs 33.8-35.0 s (best per query 45.5 vs 33.2) = **~27 % faster with one
>   split** on CI, biggest on the scan-heavy joins (q17 3.3-3.4 → 1.4-1.5 s, q21 5.0-5.1 → 3.1-3.3, q08/q09 2.6-3.2 → 1.2-1.6,
>   q03 2.1-3.1 → 1.5-1.6); no query is slower by more than noise. Answers 22/22 exact in both settings.
> - Not changed: the stand (2026-10-23, SF10, int7, 2 DuckDB threads per node) measured the opposite for a Trino-CPU-heavy
>   full scan (`checksum(ROW(16 cols))` s1 43.05 vs s4 13.71 s) and q03/q18 better with 4; CI shares 4 vCPUs between DuckDB
>   and Trino, so it cannot decide the default. See Open questions (split_count default) and the stand item below (b).
>
> **Session 67 (opt-in endpoint splits; stand measurement wanted).** New `arrow-flight.endpoint-splits.enabled` / session
> `flight.endpoint_splits_enabled` (default **false**, so nothing changes unless it is set): a plain scan is planned once on the
> coordinator (one `GetFlightInfo`) and every endpoint of that FlightInfo becomes its own Trino split, which carries the ticket,
> the locations and the planned column names. Trino gets one driver per endpoint (3 on a 3-node cluster, `endpointsPerServer`
> on one server), and the server runs the scan once instead of once per key range. Same read path and failover rules per
> endpoint (`openStream` over the endpoint's locations). Not used while dynamic filtering is on (it needs the query planned on the worker), and not for pushed
> aggregations or LIMIT. A page source refuses (clear error, never NULLs) an endpoint split planned without a column it needs.
> Risk to check on the stand: tickets are read when Trino schedules the split, so they must stay valid on the server for
> as long as the scan runs. Unit tests `EndpointSplitsTest` (3; 52 in total): build 40452390354, all versions 40452515733
> (455/477/483 green). The e2e `splits` block has a new setting `ep1`: push run 40452390252, 1 and 3 servers, all green (5/5 +
> full-scan checksum = reference; TPC-H 22/22, failover, Iceberg, concurrent all OK). **CI timings, 3 servers:**
> - Trino-CPU-heavy full scan (`checksum(ROW(16 cols))`, e2e splits block, best of 2): s1p1 13.38 / s4p1 7.93 / **ep1 7.80 s**.
>   On 1 server ep1 = one endpoint = s1 (11.70 vs s1 12.85 / s4 7.66).
> - Profile run 40452517908 (TPC-H SF1 22 + raw scan, Trino elapsed, 2 passes): default (split 4) 36.9 s, `split_count=1`
>   29.5-29.6 s, **endpoint splits 30.1-31.0 s**; raw scan 2.64 / 1.86 / 1.86-1.90 s.
>
> So on a cluster endpoint splits get split 4's speed on the CPU-heavy scan (the stand's 2026-10-23 case for keeping 4) and
> split 1's speed on TPC-H, without range re-scans. **Stand:** on the 3-node cluster, SF10 TPC-H and the checksum scan with
> `SET SESSION flight.endpoint_splits_enabled=true` vs the default and `split_count=1` (watch for expired-ticket errors on long
> scans); on one server the same with `endpointsPerServer` 4. If it wins there, the default could change (Open question).

> **Session 69 (endpoint splits fall back to key ranges on one endpoint).** With `endpoint_splits_enabled` a single server
> answered with one endpoint = one split (= `split_count=1`, slow on the CPU-heavy scan). Now a coordinator plan with exactly one
> endpoint is replaced by the `split_count` key-range splits when the scan is large enough (small scans keep their endpoint split;
> the dropped FlightInfo is never read: GetFlightInfo plans, DoGet runs), and once the client expects a single endpoint (100
> single-endpoint answers in a row, `ArrowFlightClient.expectsSeveralEndpoints`) key ranges are planned directly without the extra
> GetFlightInfo. Several endpoints = endpoint splits as before. Default (off) unchanged. Unit tests `EndpointSplitsTest` +4 (56 in
> total), build 40463892150; e2e push run 40463892049 green on 1 and 3 servers (TPC-H 22/22, types, Iceberg, failover incl.
> mid-stream `tx8M@2.1`, heap, concurrent, splits 5/5 + checksum). Splits block full-scan checksum, best of 2: 1 server s1p1 7.75 /
> s4p1 4.52 / **ep1 4.40 s** (session 67: ep1 = s1); 3 servers 9.13 / 5.19 / **4.93 s**. Profile run 40463937582 (TPC-H SF1 22 +
> raw scan, Trino elapsed sums, settings default / `split_count=1` / endpoint splits): 1 server 37.7 / 28.5 / 35.2 s (endpoint
> splits now = range splits there), 3 servers 37.3 / 29.3 / 30.0 s. So endpoint splits on = cluster: split 1's TPC-H speed and
> split 4's CPU-heavy scan speed; single server: unchanged from today's default. A default change still waits on the stand (Open
> question split_count).

> **Stand (next):** on SF10, one Trino node, single server and 3-node cluster: (a) the raw scan through Trino
> (`SELECT max(l_orderkey), …, max(l_comment) FROM lineitem` with `SET SESSION flight.aggregation_pushdown_enabled=false`, file
> `e2e/ci/profile-queries/scan.sql`) vs the same 8 columns fetched by one plain Flight client (`scan-direct.sql`, e.g.
> `flight_time.py`); (b) TPC-H SF10 with `flight.split_count` **1** / 4 / 16 and `flight.read_ahead_enabled` true / false, timed by Trino's
`elapsedTime` (not CLI wall time; session 65: CI is ~27 % faster with 1 split because range splits re-scan the files); (c) the
> query JSON (`/v1/query/<id>`) of q17/q21 (scan blocked vs plugin read time). If (a) shows the plugin path much slower than one
> plain stream, the next CI step is a plugin-only Flight read benchmark (no Trino) to separate the Java client from the server.


> **New server build `server-1.0.0-57d252b1437f`** (2026-10-26, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` in this repo's assets release) = 4d707e2aedfc + one outbound byte budget shared by all Flight streams
> (the stand saw CANCELLED streams and a dead node under 8 concurrent partial-mode scans on a 3-node cluster). Run the
> suite once on it (failover and concurrent-scan items especially) and make it the CI default if green.
>
> **Done (session 61): green, now the e2e default** (`e2e.yml`, `run-e2e.sh`; 7fbe2d75827c, 4d707e2aedfc and older stay
> selectable). Trino 455, local Iceberg, server started unchanged: partial 1+3 servers run 40310816951, complete 3 servers run
> 40310820425 — TPC-H 22/22, types, Iceberg incl. time travel, heap checks (`1 1G` 436 MB, `1/eps4 1G` 335 MB, `3 1G` 281 / 446 MB
> peak), failover: mid-stream kill hit after data on attempt 1 (`tx8M@5.3` partial, `tx8M@2.7` complete), `restarted` exact after
> 18.2 / 14.4 s with 0 backup reads, two down, coordinator-down 14.2 / 13.6 s. Job wall time 1 server 13.4 min, 3 servers partial
> 18.2 / complete 14.0 min. **New e2e check `concurrent`** (every run, 1 and 3 servers; `CONC_QUERIES`, default 4 full `lineitem`
> checksum scans + q01 q06 q12 q14 at the same time): every answer exact and every server still running afterwards. Green on
> 7fbe2d75827c too (push run 40311008965, 1+3 servers), i.e. CI does not reproduce the stand's CANCELLED streams / dead node with
> 8 concurrent queries (SF1, 4-vCPU runner); on 57d252b1437f green as well (push run 40313494859, 1+3 servers; 3-server
> mid-stream kill `tx8M@5.8`, restarted 18.4 s, coordinator-down 14.6 s). **Stand:** rerun the 8-concurrent-scan
> case on the 3-node cluster with 57d252b1437f.


> **Stand finding 2026-10-26 (local coordinator, perf campaign): plugin memory with several endpoints.** TPC-H SF10
> q18 fails `Java heap space` with a 6 GB Trino heap whenever a query has more than one endpoint per table - 3-node
> cluster in partial mode (servers 4eaab052deaf / c3b276054cc3, 1 and 8 DuckDB threads alike) and a single server
> with `endpointsPerServer` 2/4/8; with one endpoint it passes, and 8 GB passes everywhere (see "Stand answers
> 2026-10-23"). All other 21 queries are unaffected. Find where the plugin holds batches per endpoint (read-ahead
> queue depth x endpoints x batch size, page builder, `endpoint_read_parallelism`), bound it by a memory budget so
> memory does not grow with the endpoint count, account it in Trino's memory context, and add an e2e check: q18
> SF1/SF10 with a small heap and 3 endpoints.
>
> **Done in CI (session 58), stand check pending.** Cause found with a new e2e heap probe (`e2e/ci/heap-probe.sh`, workflow input
> `heap-probe=only|1`: q18 at Trino -Xmx 640M/1G/1536M x settings default / hash merge / no aggregation pushdown, live-heap
> histograms via `jcmd GC.class_histogram`). On 3 servers the heap held ~1.5M `LinkedHashMap$Entry` + `Int128` + `Long` +
> `ArrayList` (~300 MB at SF1, so ~3 GB at SF10, invisible to Trino) = the plugin's **in-memory merge of all `GROUP BY l_orderkey`
> groups** of q18's `IN` subquery, although the sorted merge is on (run 40289618273; 1 server: none of it). The "several endpoints
> expected" hint followed the *last* FlightInfo, and a cluster answers some scans with one endpoint (a key range on one node's
> files; also as the very first query of a fresh JVM), so the next aggregation skipped the server sort (new INFO line: "merged in
> memory (sorted merge not requested, a single endpoint was expected)", run 40290830857). Read-ahead / page builder / parallelism
> were not the cause (no Arrow or page objects in the histograms). Fix: (1) the hint stays on for an hour after any FlightInfo
> with several endpoints and turns off only after 100 single-endpoint answers in a row (single node); (2) the hash merge (sort
> off or GROUP BY types the server cannot order) is filled one batch per `getNextPage`, reported in `getMemoryUsage()` (~128 +
> 40 B per raw value per group) and drained in 8192-row pages, so Trino's memory limit stops it instead of `Java heap space`;
> (3) an INFO line whenever several endpoints are merged in memory, with the reason. Probe after the fix (run 40291859743,
> 3 servers): default q18 OK at 1G and 1536M with no merge objects in the heap (before: ERROR at 1G); hash mode now fails with
> Trino's "exceeded per-node memory limit" (ScanFilterAndProject 130-210 MB = the accounted merger). Every e2e run now has a heap
> check: q18 with Trino -Xmx1G must be exact (`HEAP_CHECK_XMX`). **Stand:** rerun q18 SF10 with a 6 GB (and 4 GB) Trino heap on
> the 3-node cluster and with `endpointsPerServer` 2/4/8 with this plugin; grep the Trino log for "merged in memory".
>
> **Session 59:** the 1-server e2e job now also covers the single-server case: flight1 is restarted with
> `-Darrowflight.cluster.endpointsPerServer=4` (`HEAP_CHECK_ENDPOINTS`, default 4; 0/1 = off) and q18 runs at Trino -Xmx1G again
> (`default` must be exact; `hash` only reports). Push run 40295577297: `1/eps4 1G default q18 OK` (peak live heap 402 MB vs 179 MB
> with one endpoint), `hash` = Trino's "exceeded per-node memory limit" with the INFO line "Grouped aggregation from 4 endpoints
> merged in memory (sorted merge disabled)" = 4 endpoints confirmed. Adds ~1.5 min to the 1-server job. SF10 stays a stand check.
>
> **Session 60:** the sorted merge kept, per endpoint, the whole server batch converted to Trino blocks (all rows of an
> endpoint's partial groups if the server sends them in one batch). Now the batch stays in Arrow memory and is converted 8192 rows
> at a time (`ArrowTypeConverter.toBlock(vector, type, offset, positions)`, `AggregationReader.rawPage(root, offset, n)`); the held
> pages are reported in `getMemoryUsage()` (`SortedMerger.retainedBytes()`). Unit tests: `ArrowTypeConverterSliceTest` (every
> slice of 9 vector kinds incl. nulls/decimals/varchar = the same rows of the whole batch), `AggregationReaderTest` (+1). Build run
> 40303409490 green; e2e run 40303631295 (workflow_dispatch, 1+3 servers, partial) green: 22/22, aggmerge sorted = hash, failover
> OK, heap checks `1 1G default q18 OK` (peak 332 MB), `1/eps4 1G default OK` (363 MB), `3 1G default OK` (204 MB). **The CI heap
> numbers did not move**: eps4 `[J` 169 MB (session 59: 175 MB), and the 1-endpoint peak itself varied 179 → 332 MB between runs,
> so the remaining `long[]` is not the merge's (likely Trino's own operators; the histogram is one sample near the peak). The
> change bounds what the plugin can hold per endpoint regardless; whether it matters at SF10 is for the stand (same check as above).

> **Server build `server-1.0.0-4d707e2aedfc` appeared in `e2e-assets`** (uploaded 2026-10-26 08:28 UTC, sums in `SHA256SUMS`: jar
> 69cd8a1e…2a585) **without a coordinator note** (what changed vs 7fbe2d75827c?). Session 59 ran the suite on it unchanged
> (Trino 455, local Iceberg, scripts of 44d9328): partial 1+3 servers run 40295372006 and complete 3 servers run 40295376402 green
> (job wall time 1 server 12.9 min, 3 servers partial 18.9 / complete 19.1 min). **Not made the e2e default** — waiting for the
> coordinator's note (see Open questions); if it says "make it the default", change `e2e.yml` + `run-e2e.sh` (7fbe2d75827c stays selectable).
> **Resolved (session 63):** coordinator note 2026-10-26 — 4d707e2aedfc = 7fbe2d75827c + DuckDB retries S3 connect errors for ~6 s
> (stand: every complete-mode query failed "Could not establish connection" on S3 at `duckDbThreads=8`); no protocol change. Moot as
> default: superseded by 57d252b1437f (= 4d707e2aedfc + shared outbound byte budget), the e2e default since session 61.

> **New server build `server-1.0.0-7fbe2d75827c`** (2026-10-26, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` in this repo's assets release) = 60e888bc26ec + planning no longer waits for the cluster to settle after
> a node left (fixes the intermittent GetFlightInfo TIMED_OUT with a stopped member) + `s3a://`/`s3n://` catalog roots +
> the deployment's HDFS short-circuit setting is kept. Run the suite once on it and make it the CI default if green.
>
> **Done (session 57): green, now the e2e default** (`e2e.yml`, `run-e2e.sh`; 60e888bc26ec, 20b887839edf, 4eaab052deaf,
> c3b276054cc3, int7 stay selectable). Trino 455, local Iceberg, server started unchanged: partial 1+3 servers run 40282449340,
> complete 3 servers run 40282451851 — all checks OK: TPC-H 22/22, types, Iceberg, failover (node lost before planning, kill during a
> read on tpch/ice = clean error or backup reads, mid-stream kill hit after data on attempt 1 in partial (`tx8M@4.8`), `restarted`
> exact after 18.3 s with 0 backup reads, two down, coordinator-down 14.1 s in both modes). No GetFlightInfo TIMED_OUT seen (it was
> intermittent; one run proves little). Job wall time 1 server 12.3 min, 3 servers partial 17.8 / complete 15.6 min (similar to
> 60e888bc26ec). `s3a://` roots and the HDFS short-circuit setting are not exercised in CI (rclone S3 via `s3://`, no HDFS).


> **Queue empty again (session 55, 2026-10-26).** Every item below is done or waits on the stand / a maintainer answer
> (Open questions). Done this session: push e2e run 40252652443 on the 60e888bc26ec default confirmed green; CI actions moved
> off Node 20 (`checkout@v5`, `setup-java@v5`, `upload-artifact@v6`; build runs 40256341543, 40257236337); **mid-stream kill
> made deterministic**: on 60e888bc26ec the timed kills (60-90 % of the baseline) missed 4/4 again in push run 40256341545
> (3 servers, partial; answers exact, flight3's streams had not delivered yet), the second such miss in three runs. Attempt 1
> of `kill_mid_stream` now kills flight3 once it has sent `KILL_STREAM_TX_MB` (8) MB since the query started (its
> `/proc/<pid>/net/dev` tx bytes, polled every 0.1 s, capped at 95 % of the baseline); the timed delays follow as attempts
> 2-5 (`KILL_STREAM_DELAYS` still overrides all). Hit after data on attempt 1 in partial (push run 40258066925, `tx8M@5.0`)
> **and complete mode** (run 40258071118, `tx8M@5.4`; complete mode had needed attempts 3-4 or missed before) = clean
> errors, 0 wrong answers. Complete mode still reports a miss as INFO; could become FAIL there too after a few more green
> runs. Not covered: GitHub's `ubuntu-latest` → Ubuntu 26 label migration (announced from 2026-10-19; runs still green).

> **New server build `server-1.0.0-60e888bc26ec`** (2026-10-26, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` in this repo's assets release) = 20b887839edf + **`duckDbThreads` default 4** (was 1 in code; maintainer
> decision after the perf campaign: SF10 ~1.8x, SF1 ~1.3x faster, small queries unchanged). No protocol change. Run the
> suite once on it and make it the CI default if green; note timings if they change noticeably.
>
> **Done (session 54): green, now the e2e default** (`e2e.yml`, `run-e2e.sh`; 20b887839edf, 4eaab052deaf, c3b276054cc3, int7 stay
> selectable). Trino 455, local Iceberg, server started unchanged: partial 1+3 servers run 40249518015, complete 3 servers run
> 40249520307 — TPC-H 22/22, types, projection, TopN, aggmerge, dynfilter, splits, Iceberg incl. deletes/commit/**time travel OK**,
> failover (before, during a read on tpch/ice, restarted exact with 0 backup reads, two down, coordinator-down 13.9 / 14.5 s). The
> partial 3-server job failed once only on the mid-stream kill **timing** (4/4 kills landed before flight3 had sent data; answers
> exact, all clean failovers to a backup = the known miss of session 50); its re-run (attempt 2) hit on attempt 3 (clean error
> after data) and is green. **Timings got slower on the CI runner, not faster:** job wall time 1 server 12.4 min (20b887839edf:
> 8.8), 3 servers partial 13.7 → 19.0 min, complete 16.6 → 17.9 min; mid-stream baseline (lineitem checksum scan, 3 servers)
> 7.8 / 11.2 s vs 6.5 s. Likely CPU oversubscription: the 4-vCPU runner hosts up to 3 servers × 4 DuckDB threads + Trino;
> not a stand concern (one server per node). If CI time matters, start the CI servers with `-DduckDbThreads=1` (or 2) — see
> Open questions.


> **New server build `server-1.0.0-20b887839edf`** (2026-10-26, local coordinator; jar, plugins tar, libs tar + merged
> `SHA256SUMS` attached to this repo's assets release). = c3b276054cc3 + **Iceberg time travel** (`FOR VERSION AS OF` /
> `FOR TIMESTAMP AS OF`; c3b276054cc3 did **not** contain it — that earlier note was wrong, which is why the forwarded
> clause was rejected) + fix for parameterized prepared statements with `duckDbStatementCacheSize` 0/1 + Hive metastore
> Kerberos client + local catalog roots under a non-local `fs.defaultFS`. Run the suite once on it, retry the
> time-travel item, and make it the CI default if green. Server bugs → the server repo's Next steps via the coordinator.
>
> **Done (session 54): green, time travel works, now the e2e default.** Trino 455, local Iceberg warehouse, server started unchanged:
> partial 1+3 servers run 40246658737 and complete 3 servers run 40246660859 all green — TPC-H 22/22, types 2/2, projection, TopN,
> aggmerge, dynfilter, splits, Iceberg (copies, `orders_del` deletes, rename, TPC-H 8/8, commit), failover (node lost before, kill
> during a read on `tpch` and `ice`, mid-stream kill hit after data = clean error on attempt 3 (partial) / 4 (complete), `restarted`
> exact with 0 backup reads (14.2 / 16.7 s restart), two nodes down, `coordinator-down` 7.9 / 13.7 s). **`time-travel` OK in all
> three jobs:** `FOR VERSION AS OF` first/last snapshot of `ice.commits` = 5000/6000 rows, `FOR TIMESTAMP AS OF` the first commit
> time = 5000, versioned scan = Parquet rows (4900); the clause on a Parquet table fails cleanly (server: "Time travel (FOR
> VERSION/TIMESTAMP AS OF) is not …"). No regression against 4eaab052deaf/c3b276054cc3. The push/dispatch default `server-build`
> is now `20b887839edf` (`e2e.yml`, `run-e2e.sh`); 4eaab052deaf, c3b276054cc3 and int7 stay selectable. Still warned: the
> `START-NOTES.md` asset does not match `SHA256SUMS` (jar/tarballs verified). **Stand:** Iceberg time travel through Trino
> (`SELECT … FROM flight.<ns>.<t> FOR VERSION AS OF <snapshot>` / `FOR TIMESTAMP AS OF TIMESTAMP '… UTC'`) on 20b887839edf.


> **New server build `server-1.0.0-c3b276054cc3`** (2026-10-25, local coordinator): = 4eaab052deaf + Iceberg time travel (`FOR VERSION AS OF <snapshot>` / `FOR TIMESTAMP AS OF TIMESTAMP '… UTC'` after an Iceberg table name) + ZooKeeper client and lz4-java back in the runtime libs set (production HA with ZooKeeper). Assets `arrow-flight-server-1.0.0-c3b276054cc3-linux.jar`, `flight-plugins-1.0.0-c3b276054cc3.tar.zst`, `flight-server-libs-1.0.0-c3b276054cc3.tar.zst` (sums in SHA256SUMS). Run the suite once against it; if green make it the default. Connector follow-up: forward the time-travel clause (Trino `FOR VERSION/TIMESTAMP AS OF`, StarRocks own syntax if any) to the server — additive, no approval needed.
>
> **Session 50: suite run, time travel forwarded; server bug blocks it.** e2e on c3b276054cc3 (Trino 455, partial): 1 server run
> 40223996061/40225756304/40226815637 and 3 servers 40223996061 — TPC-H 22/22, types, projection, TopN, dynfilter, splits, aggmerge,
> Iceberg (copies, `orders_del` deletes, rename, TPC-H 8/8, commit), failover incl. restarted (0 backup reads), two down,
> coordinator-down 13.8 s: all as on 4eaab052deaf; ZooKeeper libs not exercised (CI has no ZooKeeper). The mid-stream kill missed on
> all 4 delays in both 3-server runs today (c3b276054cc3 **and** 4eaab052deaf push run 40223988267; answers exact every time,
> flight3's streams had not delivered data yet: timing, baseline 10.5-13.5 s; hit on attempt 1 in session 47). Complete mode 3
> servers run 40226837583: everything green incl. the mid-stream kill (hit on attempt 4), except time travel. **Connector (done, commit 4497299):** Trino `FOR VERSION AS OF <snapshot id>` / `FOR
> TIMESTAMP AS OF <timestamp [with time zone]>` (the end version; a start version / range is refused) becomes a `TableVersion` in
> the table handle and is written after the table name in every server query of that scan: schema lookup, statistics (cache key
> includes the version), range splits, scans, pushed aggregations (`… FROM ice.t FOR VERSION AS OF 42 WHERE …`, `… FOR TIMESTAMP
> AS OF TIMESTAMP '2026-10-25 10:15:30.123000 UTC'`; a timestamp without zone is read in the session zone like Trino's Iceberg
> connector). Branch/tag names refused with a clear error. Unit tests `TableVersionTest` (6, build 40223988406; all versions
> 40224120237: 455/483 green, 477 failed once in the untouched `FlightSqlSessionFailoverTest.staleConnectionToANodeThatStaysDown…`
> (port race, passed on re-run). e2e `iceberg.sh` `time-travel` check (on for builds other than int*/4eaab052deaf): VERSION AS OF
> first/last snapshot of `ice.commits` = 5000/6000 rows, TIMESTAMP AS OF the first commit time = 5000, versioned scan = Parquet;
> on failure it prints the server log and probes the server directly (`e2e/ci/flight_probe.py`, ADBC Flight SQL). **Result: the
> server refuses the clause on every path** — `ParquetQueryParser` (jOOQ) fails with `ParserException: Feature only supported in
> pro edition: [1:31] SELECT * FROM ice.commits FOR [*]VERSION AS OF …`, in `getExecuteSchema` and in GetFlightInfo (prepare)
> alike, for VERSION and TIMESTAMP, partial mode; in complete mode the execute path hands the SQL to DuckDB unchanged ("Parser
> Error: syntax error at or near \"VERSION\"", run 40226837583); clean errors, never rows. Reported as KNOWN for c3b276054cc3 only (see Open questions).
> Next: when a server build parses the clause, run the e2e on it (`time-travel` must be OK), then make that build the default.
> StarRocks: not in this repo (native connector lives elsewhere).

> **New server build `server-1.0.0-4eaab052deaf`** (2026-10-25, local coordinator; built by the server repo CI from main 4eaab05 = int7 + hadoopfs timeout defaults 3 s / 10 s + dependency security upgrades (Jackson 2.21, Netty 4.2.19, Parquet codecs, Hive libthrift, upstream-sync items) + a fresh, much smaller runtime libs set): assets `arrow-flight-server-1.0.0-4eaab052deaf-linux.jar`, `flight-plugins-1.0.0-4eaab052deaf.tar.zst`, `flight-server-libs-1.0.0-4eaab052deaf.tar.zst` (replaces flight-server-libs.tar.zst for this build), START-NOTES.md (sums in SHA256SUMS). Not stand-verified yet: run your full suite once against it (keep int7 as the fallback default until it is green); report regressions under Open questions.
>
> **CI suite green on 4eaab052deaf (session 47), stand check pending.** e2e now takes the server-CI asset names
> (`arrow-flight-server-1.0.0-<b>-linux.jar`, `flight-plugins-1.0.0-<b>.tar.zst`, its own `flight-server-libs-1.0.0-<b>.tar.zst`,
> `libs/SHA256SUMS` verified; the 520 MB int7 libs are not downloaded for it). Trino 455, local Iceberg warehouse, server started
> unchanged (same `-D` options, httpfs only): partial 1+3 servers run 40167126909 and complete 3 servers run 40167715652 all green —
> TPC-H 22/22, types, projection, TopN, dynfilter, splits, aggmerge, Iceberg incl. `orders_del` deletes and commit, failover (node
> lost before, kill during a read on `tpch` and `ice`, mid-stream kill hit after data = clean error, `restarted` exact with 0 backup
> reads, two nodes down, `coordinator-down` 14.1 / 14.3 s). No regression against int7. The push/dispatch default `server-build` is
> now `4eaab052deaf`; `int7` stays selectable (and in the release) as the fallback.

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
> 4 GB heap now).
>
> (b) **Done (session 41), pending stand check.** A pooled data-stream connection to a node that restarted may have died
> unnoticed (no FIN reached Trino); its next stream then failed unreachable before any data and was read from the backup.
> Now (`FlightDataStream` + `FlightSqlSession.openAt`) a stream on a **reused** pooled connection that fails unreachable
> before any data is retried **once on a new connection to the same location** before the next location — only while no other
> stream has retired that location's connections since the lease (generation unchanged), so of a burst of streams to a dead
> node only the first pays (at most one connect timeout, 5 s, for a black-holed host; refused/unresolved is immediate).
> Pooled channels gRPC already reports TRANSIENT_FAILURE/SHUTDOWN are not leased again. Server errors are never retried;
> after data nothing changes. Log line: `Flight stream at … lost its reused connection before any data (…); retrying on a new
> one`. Unit tests (`FlightSqlSessionFailoverTest`, 15): `staleConnectionToARestartedNodeIsRetriedOnANewOne` (proxy resets
> the old connections, like a restarted node; fails over to the backup without the fix — verified),
> `staleConnectionToANodeThatStaysDownIsRetriedOnceThenFailedOver`, `serverErrorOnAReusedConnectionIsNotRetried`. e2e block
> `restarted` (3 servers): warm pool, flight2+flight3 restarted (~17-18 s), lineitem stream checksum = baseline; CI's
> `docker rm -f` closes connections cleanly, so it showed 0 stale retries / 0 backup reads (the stale case does not
> reproduce there; the retry path did fire in the kill checks, with clean failover after). Runs: build 40144727653, e2e
> 40145935916 green (1+3 servers). **Stand:** restart one node, run a query within 60 s, and check the Trino log: expected
> `lost its reused connection … retrying on a new one` and no read from a backup (no +~1 s). Next: (c) a list of
> coordinator hosts (`arrow-flight.hosts=a,b,c`).
>
> (c) **Done (session 42), pending stand check.** New catalog property `arrow-flight.hosts=a,b:port,[ipv6]:port` (instead of
> `arrow-flight.host`; exactly one of the two; default port `arrow-flight.port`). `FlightSqlSession.ensureConnected` tries the
> coordinators starting with the one in use and moves to the next only when the current one is unreachable (refused,
> unresolved, UNAVAILABLE, or TIMED_OUT of the handshake, which gets a deadline of 2 x `connect-timeout-millis` only when
> several hosts are configured); credentials/server errors are thrown as before, never retried elsewhere. Sticky: the session
> stays on the new coordinator. Endpoints without locations are read from the coordinator in use; any configured coordinator
> named in a location is normalized as before. One `arrow-flight.host` = old behaviour (no handshake deadline). Log lines:
> `Flight coordinator a:32010 cannot be reached (…); trying b:32010`, `Using Flight coordinator b:32010`. Unit tests:
> `FlightSqlSessionFailoverTest` (21; new: refused first host, lost coordinator moves and stays, silent host given up within the
> handshake deadline, UNAUTHENTICATED not failed over, location naming, list parsing), `ArrowFlightConfigTest` (3). e2e block
> `coordinator-down` (3 servers, catalog `flighth` = `flight1,flight2:32010,flight3`): warm-up q06 on flight1, flight1 killed,
> q01 q03 q05 q06 q18 all exact, switch to flight2 logged. Runs: build 40148691957, e2e 40148691975 green (1+3 servers).
> **Follow-up found:** the first query after the kill switched only after ~31 s (query 01:13:38, switch 01:14:09): the
> first coordinator call hangs on the existing connection to the dead host (no keepalive, no deadline on coordinator calls;
> `docker kill` drops packets, no RST) until gRPC gives up. Same delay with a single host (there it then fails). Next: give
> the coordinator connection keepalive pings (like data connections; needs a channel built with `NettyChannelBuilder` +
> the bearer middleware, or a deadline on metadata calls) so a dead coordinator is noticed within ~`connect-timeout-millis`.
> **Stand:** catalog with `arrow-flight.hosts=<n1>,<n2>,<n3>`, stop n1, run TPC-H q01/q18: exact, and the time to the switch.
>
> (c') **Done (session 43), pending stand check.** With `arrow-flight.hosts` (several coordinators) the coordinator connection is now
> built like a data connection (`NettyChannelBuilder`: connect timeout, keepalive pings every `keepalive-millis` answered within
> `connect-timeout-millis`; BasicAuth's bearer captured via Arrow's `ClientInterceptorAdapter` + the same middleware). A coordinator
> that vanishes during a call fails that call within ~keepalive + connect timeout (≤ ~15 s with defaults), then the next coordinator is
> used. A single `arrow-flight.host` keeps Arrow's builder (no pings: old servers close pinging connections). Unit tests
> (`FlightSqlSessionFailoverTest`, 23): `coordinatorThatGoesSilentDuringACallIsDetectedByKeepaliveAndTheNextOneIsUsed` (11.6 s;
> hangs past 40 s without the fix — verified), `severalCoordinatorsWithBasicAuthUseTheBearerTokenTheServerReturns` (fails without the
> interceptor — verified). e2e `coordinator-down` now times the first query after the kill and FAILs above
> `COORDINATOR_SWITCH_MAX_SECONDS` (25): run 40152506314 **13.5 s** (switch 11 s after the query started; was ~31 s), 5/5 exact.
> Build 40152506378 green. **Stand:** as (c), plus the time of the first query after stopping n1 (expected ≤ ~15 s + query).

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
- **Server (not the plugin path): direct ADBC q04 fails on a 3-node cluster (seen session 68, also on 27f583adccd7).** The
  profile's direct baseline (`flight_time.py`, plain ADBC to flight1, no partial-mode header) gets for TPC-H q04 (`EXISTS`
  subquery on `lineitem`) `all 2 DoGet location(s) failed: DoGet("grpc+tcp://flight2|flight3:32010") ... Catalog Error: Table
  with name "tpch.lineitem" does not exist because schema "tpch" does not exist` (jobs 122004793999, 122018291122,
  122034850840); on 1 server it answers. Through Trino q04 is exact (the plugin sends single-table scans). Looks like a
  non-coordinator member running a statement whose subquery table it never registered. Worth a look on the server side?
- **`flight.split_count` default 4 vs 1 (session 65).** In CI (SF1, 4 vCPUs) one split is ~27 % faster on TPC-H (Trino
  elapsed 30-35 s vs 42-56 s, 1 and 3 servers) because every range split re-reads the Parquet files on the server (4 range
  statements in sequence = 1.5x the work of one); the plugin and its Flight client add almost nothing. The stand's 2026-10-23
  numbers (SF10, int7) favoured 4 for a Trino-CPU-heavy scan and for q03/q18. Keep 4, change to 1, or split by endpoint only
  (partial mode already returns one endpoint per node; a single server could use `endpointsPerServer`) instead of key ranges?
  Needs the stand's SF10 split_count 1/4 run first (Next steps, Stage 2 stand item (b)).
  Session 66: with server 27f583adccd7 (DuckDB 1.5.6) the gap is smaller in CI (1 server 27.7 vs 36.1 s, 3 servers 24.5 vs
  28.9 s; 4 range splits now ~1.35x one statement on the server, was 1.7x); run the stand comparison on that build.
  Session 67: option (c) exists now as opt-in `endpoint_splits_enabled` (one split per endpoint of one coordinator-planned
  statement). On 3 servers in CI it is as fast as split 1 on TPC-H and as fast as split 4 on the CPU-heavy checksum scan. Make it
  the default for clusters once the stand confirms it (and that tickets stay valid for long SF10 scans)?
  Session 69: with endpoint splits on, a single-endpoint plan now falls back to the `split_count` key ranges, so the option can
  be switched on for single servers and clusters alike (1 server behaves like today's default). Proposal after the stand run:
  `arrow-flight.endpoint-splits.enabled=true` as the default, `split-count` 4 kept for the single-server fallback.
- **Answered (session 63; moved to Next steps). Server build 4d707e2aedfc (session 59):** uploaded to `e2e-assets` with no note in STATUS. What does it change vs 7fbe2d75827c,
  and should it become the e2e default? (Session 61: superseded as default by 57d252b1437f = 4d707e2aedfc + shared outbound byte
  budget; only the 4d707e2aedfc delta itself is still undocumented.) The CI suite is green on it in both modes (runs 40295372006, 40295376402).
  *Coordinator, 2026-10-26:* 4d707e2aedfc = 7fbe2d75827c + DuckDB retries S3 connect errors for ~6 s (the stand saw
  every complete-mode query fail with "Could not establish connection" on S3 at `duckDbThreads=8`); the note was skipped
  because this repo was locked at upload time. No protocol change.
- **CI e2e on 60e888bc26ec is ~40 % slower (session 54)** (duckDbThreads default 4; 1 server 8.8 → 12.4 min, 3 servers 13.7 →
  19.0 min): CI runs up to 3 servers + Trino on 4 vCPUs. Should the e2e start its servers with `-DduckDbThreads=1` (the old
  default; keeps CI minutes and the mid-stream kill timing stable) or keep the production default 4 so CI tests what production
  runs? Kept the default 4 for now. (Session 55: the mid-stream kill no longer depends on timing, so only CI minutes are at
  stake.)
- **Resolved in 20b887839edf (session 54: time travel OK in CI, now the e2e default); the START-NOTES.md mismatch remains.** **Server c3b276054cc3: Iceberg time travel unusable (server bug, session 50).** Every statement with `FOR VERSION AS OF <id>` or
  `FOR TIMESTAMP AS OF TIMESTAMP '… UTC'` after an Iceberg table name fails in the server's `ParquetQueryParser.parseChecked`
  (jOOQ OSS parser): `org.jooq.impl.ParserException: Feature only supported in pro edition: [1:31] SELECT * FROM ice.commits FOR
  [*]VERSION AS OF 5871184891042262909` — on `getExecuteSchema` and on GetFlightInfo/prepare (direct ADBC probe, run 40226815637);
  in complete mode GetFlightInfo reaches DuckDB with the clause unchanged: `Parser Error: syntax error at or near "VERSION"`.
  The clause must be stripped/handled before jOOQ parses the SQL (or which exact syntax does the build expect?). The connector
  already sends it (commit 4497299). Also: `SHA256SUMS` in `e2e-assets` lists a START-NOTES.md (a48ceac3…2ae8) that differs from
  the uploaded one (50a61fd5…24a4) — please upload the new START-NOTES.md (e2e now only warns on it). Should c3b276054cc3 become the
  e2e default despite the time-travel bug (all other checks equal to 4eaab052deaf)? Kept at 4eaab052deaf for now.
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
    **Done (session 71):** VARCHAR keys are pushed in TopN now; this relies on the server keeping no collation and
    lockdown on — tell us if a deployment could ever set `default_collation`.
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

- **Result compression on the stand? (session 84; replaces the session-82 question, answered 2026-10-28 and done).** The plugin
  has `arrow-flight.result-compression=NONE|ZSTD|LZ4` (off by default). In CI ZSTD wins only on a shaped 1 Gbit link (+23 %),
  LZ4 never (pure-Java decoder). Could the stand run TPC-H SF10 and the full-scan checksum with `ZSTD` vs off (its real
  network) and say whether ZSTD should be the suggested value in `flight.properties.example`? A faster LZ4 decoder (lz4-java
  or aircompressor instead of commons-compress) is possible if LZ4 matters on the stand - worth it?
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
- 2026-10-25 (cloud session 41): stand follow-up (b), stale pooled connection after a node restart: a stream on a reused
  pooled connection that fails unreachable before any data is retried once on a new connection to the same location; failing
  pooled channels are not leased (details in Next steps). 3 unit tests; e2e block `restarted`. Build 40144727653 green; e2e
  40144727674 red only on the partial-mode mid-stream kill (all four 30-75 % delays landed before flight3's first batch,
  baseline 9.1 s; answers exact) → delays 60-90 % of the baseline; e2e 40145935916 green 1+3 servers (mid-stream hit after
  data at 8.8 s = clean error; `restarted` exact). ~65 Actions minutes. Invariants hold (retry only before any data, only on
  unreachable errors; server errors and auth unchanged). Next: (c) list of coordinator hosts.
- 2026-10-25 (cloud session 42): stand follow-up (c), list of coordinator hosts: `arrow-flight.hosts`, failover to the next
  coordinator only when the current one is unreachable, sticky (details in Next steps). 6 + 3 unit tests (local run of the
  common module 21/21 too); e2e block `coordinator-down`. Build 40148691957 green, e2e 40148691975 green 1+3 servers
  (`coordinator-down` 5/5 exact after flight1 killed). ~25 Actions minutes. Invariants hold (single host unchanged; auth and
  server errors are not retried on another host; data-stream failover untouched). Found: ~31 s until the switch because the
  coordinator connection has no keepalive → next step. `gh` in the session rejects its token (`GH_TOKEN` invalid); CI was
  followed through the GitHub MCP tools and the public API.
- 2026-10-25 (cloud session 43): follow-up of (c): keepalive on the coordinator connection when several hosts are configured
  (details in Next steps, item c'). 2 new unit tests (local run 23/23 with JDK 21 via `-Dmaven.compiler.release=21`; both fail
  without the change). Build 40152506378 green, e2e 40152506314 green 1+3 servers; `coordinator-down` first query after the kill
  13.5 s (was ~31 s). ~27 Actions minutes. Invariants hold (single host unchanged; auth and server errors not retried elsewhere;
  data-stream failover untouched). Stand-note items (a)-(c') all wait on stand checks; no new code item queued.
- 2026-10-25 (cloud sessions 44, 45, 46, 48, 49): no code item open (stand-note items (a)-(c') wait on stand checks, no new maintainer answer
  or item since session 43); lock taken and released, no CI runs.
- 2026-10-25 (cloud session 47): first Next-steps item, new server build `4eaab052deaf`: e2e learned the server-CI asset names
  (`[skip ci]` commit, runs dispatched); Trino 455 partial 1+3 servers run 40167126909 and complete 3 servers run 40167715652 green
  (every check incl. Iceberg, failover, mid-stream kill hit after data, coordinator-down 14 s). e2e default `server-build` switched to
  `4eaab052deaf` (int7 selectable). ~55 Actions minutes. No plugin code changed; invariants hold. Stand-note items (a)-(c') and the
  new build still wait on the stand.
- 2026-10-25 (cloud session 50): first Next-steps item, server build c3b276054cc3: e2e asset check failed at once (SHA256SUMS names a
  newer START-NOTES.md than the uploaded one; runs 40223244144, 40223246425) — the job now only warns on START-NOTES.md. Suite on
  c3b276054cc3, Trino 455: partial 1+3 servers 40223996061, complete 3 servers 40226837583 — every existing check as on 4eaab052deaf
  (mid-stream kill missed in the partial 3-server runs of both builds, exact answers; push run 40223988267 on 4eaab052deaf). Connector:
  Iceberg time travel forwarded (`TableVersion`, commit 4497299; build 40223988406 green, all versions 40224120237 green after one
  re-run of a port-race flake in `FlightSqlSessionFailoverTest` on 477). New e2e `time-travel` check + diagnostics + direct ADBC probe
  (`e2e/ci/flight_probe.py`; runs 40225756304, 40226815637): the server refuses the clause (jOOQ "Feature only supported in pro
  edition"; complete mode: DuckDB syntax error) — KNOWN for c3b276054cc3, Open question filed. e2e default stays 4eaab052deaf.
  ~110 Actions minutes. Invariants hold (unversioned queries unchanged; time-travel errors are clean server errors).
- 2026-10-25 (cloud sessions 51, 52, 53): first Next-steps item (time travel) still blocked on the server (no build newer than
  c3b276054cc3 in `e2e-assets`, no new maintainer answer); lock taken and released, no CI runs.
- 2026-10-26 (cloud session 54): e2e on new server build 20b887839edf (Trino 455): partial 1+3 servers run 40246658737, complete
  3 servers run 40246660859 — all green incl. Iceberg time travel (`time-travel` OK in all three jobs; the server bug of
  c3b276054cc3 is fixed). Made 20b887839edf the e2e default (`e2e.yml`, `run-e2e.sh`; push e2e run 40248099474 on that commit green, 1+3 servers). ~75 Actions
  minutes. Invariants hold (failover/kill checks exact or clean errors).
- 2026-10-26 (cloud session 54, cont.): coordinator added server build 60e888bc26ec (duckDbThreads 4) during the session; lock
  re-taken. e2e partial 1+3 servers run 40249518015 (3-server job failed once on the mid-stream kill timing only, exact answers;
  re-run attempt 2 green), complete 3 servers run 40249520307 green. Made 60e888bc26ec the e2e default (push e2e run 40252652443
  started on that commit; green, 1 server 9.4 min, 3 servers 18.9 min — confirmed in session 55). CI wall time up ~40 % (Open question). ~70 more
  Actions minutes.
- 2026-10-26 (cloud session 55): confirmed push e2e run 40252652443 green. Actions off Node 20 (checkout/setup-java v5,
  upload-artifact v6): build runs 40256341543, 40257236337 green; e2e push run 40256341545 failed only on the mid-stream kill
  timing (4/4 misses, exact answers). `kill_mid_stream` attempt 1 now triggered by flight3's sent bytes (8 MB): push e2e run
  40258066925 (partial 1+3 servers) and complete 3 servers run 40258071118 green, hit after data on attempt 1 in both.
  ~95 Actions minutes. Invariants hold (failover checks exact or clean errors).
- 2026-10-26 (cloud session 56): nothing to do — Next steps queue empty (rest waits on the stand / Open questions), no new
  server build in `e2e-assets` since 60e888bc26ec, last runs (40258066925, 40258071118, 40257236337) green. Stopped early;
  0 Actions minutes.
- 2026-10-26 (cloud session 57): new server build 7fbe2d75827c (planning with a node down, s3a roots): e2e partial 1+3 servers
  run 40282449340 and complete 3 servers run 40282451851 green; made it the e2e default (the push of that change starts a push
  e2e run — next session: confirm it). ~46 Actions minutes. Invariants hold.
- 2026-10-26 (cloud session 58): stand finding "q18 heap with several endpoints" fixed in CI. New e2e heap probe (input
  `heap-probe`); probe runs 40288988533 (script bug), 40289618273 (cause: plugin hash merge of all groups on 3 servers),
  40290830857 (first fix: NPE + diagnosis "single endpoint was expected"), 40291859743 (green: default q18 OK at 1G on 3 servers).
  Plugin: sturdier several-endpoints hint, accounted incremental hash merge, INFO line; arrow-flight-common: a closed pooled
  connection (gRPC UNKNOWN "channel closed", ClosedChannelException) counts as unreachable before any data — it failed
  `FlightSqlSessionFailoverTest` in build run 40290807373. Build runs 40291029063, 40291834596 green; push e2e run 40291834562
  green (1+3 servers, full suite incl. the new 1G heap check, failover, aggmerge sorted = hash). ~75 Actions minutes. Invariants: failover still only before data; server errors still surface.
- 2026-10-26 (cloud session 59): q18 heap check extended to one server with `endpointsPerServer=4` (1-server e2e job): push e2e run
  40295577297 green (eps4 default q18 OK at 1G, 4 endpoints confirmed by the hash run's INFO line). Unannounced server build
  4d707e2aedfc: e2e partial 1+3 servers run 40295372006 and complete 3 servers run 40295376402 green; not made the default (Open
  question). ~85 Actions minutes. Invariants hold (failover checks exact or clean errors).
- 2026-10-26 (cloud session 60): sorted aggregation merge converts server batches in 8192-row slices and reports its held pages
  in `getMemoryUsage()`; Arrow off-heap flags for the plugin's test JVM. Build run 40303409490 green (40303169371 failed only on
  the missing test-JVM flags); e2e run 40303631295 green (push runs 40303169395 / 40303409359 cancelled: concurrency group).
  CI heap peaks unchanged within noise. ~60 Actions minutes. Invariants hold (failover exact or clean errors, server errors surface).
- 2026-10-26 (cloud session 61): server build 57d252b1437f (shared outbound byte budget) green in both modes — dispatch runs
  40310816951 (partial, 1+3 servers) and 40310820425 (complete, 3 servers) — and made the e2e default (push run 40313494859 green).
  New e2e check `concurrent` (4 lineitem checksum scans + q01/q06/q12/q14 at once, exact + servers alive): green on 7fbe2d75827c
  (push run 40311008965) and 57d252b1437f; CI does not reproduce the stand's failure, so the stand rerun stays open. No plugin
  code changed. ~100 Actions minutes. Invariants hold (failover exact or clean errors).
- 2026-10-26 (cloud session 63): coordinator answered what 4d707e2aedfc changes (S3 connect retries, no protocol change); recorded
  in Next steps, open question closed. Queue otherwise unchanged: every item waits on the stand (57d252b1437f 8-concurrent-scan
  rerun, q18 SF10 heap, earlier stand checks) or a maintainer answer. No code change, no CI run, 0 Actions minutes.
- 2026-10-27 (cloud session 64): Stage 2 (plugin throughput). CI profile mode `profile=only` (TPC-H + raw scan per setting,
  Trino query stats, JFR CPU samples, direct ADBC times): plugin CPU is negligible (conversion <= 1.4 % of samples, Trino operators
  ~75 %), scans wait on the server streams; raw 6M-row scan through Trino 2.4-4.4 s vs 1.84 s for one direct ADBC stream. Plugin:
  bulk Arrow->Block copies + read-ahead of the next batch (`read_ahead_enabled`, default on); no CI speed-up measurable. Build run
  40435458428 green (49 tests); e2e push run 40435458521 green (1+3 servers); profile runs 40435218497 (baseline), 40436253774
  (1 srv; its 3-srv job cancelled by the next dispatch's concurrency group), 40437349225, 40438299101 (1+3 srv) — the first two
  later ones "failed" only on the new scan check (missing `;` in scan.sql, fixed). ~75 Actions minutes. Invariants hold
  (failover checks exact or clean errors, server errors surface; read-ahead keeps each stream's failover rules).
- 2026-10-27 (cloud session 65): Stage 2. Plugin-only read benchmark in CI (`profile=bench`: `FlightReadBench.java` through the
  plugin's own Flight client vs ADBC vs Trino split_count 1/2/4): the Java client = ADBC speed; range splits cost server re-scans
  (4 serial range statements 1.5x one statement); Trino with one split = one direct stream. Profile now uses Trino's elapsed
  time (session 64's per-query times included ~1.4 s CLI start). TPC-H SF1 with `split_count=1` ~27 % faster than default 4 on
  1 and 3 servers (22/22 exact) — default unchanged, Open question + stand item (b). Runs 40441072747, 40441623193, 40442318786
  (bench, 1+3 srv), 40442908161 (profile, 1+3 srv), all green. No plugin code changed (scripts only, `[skip ci]` pushes).
  ~70 Actions minutes. Invariants untouched.
- 2026-10-27 (cloud session 66): new server build 27f583adccd7 (DuckDB 1.5.6). `run-e2e.sh` chooses the DuckDB extension
  version per build (v1.5.6 after 57d252b1437f); full suite green: partial 1+3 servers run 40446519100, complete 3 servers run
  40446521466 (22/22, types, Iceberg, heap, concurrent, failover). Now the e2e default. Profile run 40448316888 vs 40442908161:
  TPC-H SF1 sum 1 server 41.5 → 36.1 s, 3 servers 45.5 → 28.9 s; small queries 0.1-0.7 s faster; range splits much cheaper on the
  server. No plugin code changed (`[skip ci]` pushes). ~60 Actions minutes. Invariants untouched (failover checks exact or
  clean errors).
- 2026-10-27 (cloud session 67): Stage 2. Opt-in endpoint splits (`arrow-flight.endpoint-splits.enabled`, session
  `endpoint_splits_enabled`, default off): a coordinator-planned scan with one split per Flight endpoint, so there are no range re-scans.
  Unit tests 52 green (build 40452390354; 455/477/483 run 40452515733). e2e push run 40452390252 green on 1 and 3 servers with
  the new `ep1` splits setting exact. Profile run 40452517908 (3 servers): TPC-H SF1 endpoint splits 30.1 s vs split 1 29.5 s vs
  default 36.9 s, CPU-heavy checksum scan 7.80 s (= split 4, vs split 1 13.38 s). Default unchanged; stand check + Open question.
  ~35 Actions minutes. Invariants untouched (off by default; same per-endpoint failover; missing column = error, not NULLs).
- 2026-10-27 (cloud session 68): first Next step (server build 775100bf923f). Suite green on it: partial 1+3 servers run
  40457717079, complete 3 servers run 40457720101 (22/22, types, Iceberg, failover, heap, concurrent, splits incl. ep1);
  profile run 40457722482: TPC-H SF1 sums and small queries unchanged within CI noise (1 server 36.6 / 27.8 s default /
  split 1). Made 775100bf923f the e2e default (`e2e.yml`, `run-e2e.sh`; commit `[skip ci]`, no plugin code changed and the
  same build was just run green). New Open question: direct-ADBC q04 catalog error on cluster members (server side,
  pre-existing). ~47 Actions minutes. Invariants untouched.
- 2026-10-27 (cloud session 69): Stage 2. Endpoint splits fall back to `split_count` key ranges when the coordinator plan has a
  single endpoint (large scans only; planned directly once the client expects one endpoint). Unit tests 56 green (build
  40463892150); e2e push run 40463892049 green on 1 and 3 servers (ep1 full scan 1 server 4.40 s = s4p1 4.52, was = s1p1 7.75);
  profile run 40463937582: TPC-H SF1 sums 1 server default / split 1 / endpoint splits 37.7 / 28.5 / 35.2 s, 3 servers 37.3 /
  29.3 / 30.0 s. Default unchanged (off). ~60 Actions minutes. Invariants untouched (off by default; same per-endpoint failover).
- 2026-10-27 (cloud session 70): first Next step (server build 45fc0a9e6e02, per-query overhead cuts). Suite green: partial 1+3
  servers run 40469746196, complete 3 servers run 40469753759; profile run 40469761749: 3 servers TPC-H SF1 37.9 / 29.8 / 30.3 s
  (default / split 1 / endpoint splits), unchanged; the 1-server profile looked ~5 s slower but its runner streamed raw scans at
  half speed, and back-to-back bench runs 40472082623 (new) / 40472093615 (old) show equal read speed. Made 45fc0a9e6e02 the e2e
  default (`[skip ci]`, no plugin code changed). ~75 Actions minutes. Invariants untouched (failover checks exact or clean errors).
- 2026-10-27 (cloud session 71): queue had nothing CI-actionable except the stand's collation finding; TopN pushdown now pushes
  VARCHAR sort keys (off by default as before; aggregation sorted merge unchanged). First push failed build 40476803699
  (AggregationReaderTest: the merge shared the predicate; e2e 40476803533 cancelled), fixed: build 40477030899 green, e2e push run
  40477030751 green on 1+3 servers incl. 3 new VARCHAR TopN checks (on = off, rows 1.5M → 150). ~45 Actions minutes. Invariants
  untouched (TopN off by default; answers compared on vs off).
- 2026-10-27 (cloud session 72): nothing actionable — no new server build in `e2e-assets` since 45fc0a9e6e02, no new maintainer
  answers; every Next steps item waits on the stand or an Open question. No CI run, 0 Actions minutes. Stopped early.
- 2026-10-27 (cloud session 73): nothing actionable, same as session 72 — no new `e2e-assets` upload since 45fc0a9e6e02
  (07:09 UTC), no new maintainer/stand answers; every Next steps item waits on the stand or an Open question. 0 Actions minutes.
- 2026-10-27 (cloud session 74): nothing actionable, same as sessions 72-73 — no new `e2e-assets` upload since 45fc0a9e6e02
  (07:09 UTC), no new maintainer/stand answers; every Next steps item waits on the stand or an Open question. 0 Actions minutes.
- 2026-10-27 (cloud sessions 75-76): nothing actionable, same as sessions 72-74 — no new `e2e-assets` upload since 45fc0a9e6e02
  (07:09 UTC), no new maintainer/stand answers; every Next steps item waits on the stand or an Open question. 0 Actions minutes.
- 2026-10-27 (cloud session 77): new server build 4e77c6bda59a appeared in `e2e-assets` at 20:53 UTC (no change note). Full suite
  green: partial 1+3 servers 40582176690, complete 3 servers 40582181234; profile run 40582216959: TPC-H SF1 sums 3 servers
  26.0 / 22.0 / 20.3 s (was 37.9 / 29.8 / 30.3 on 45fc0a9e6e02), 1 server 27.6 / 23.8 / 25.8 s. Made 4e77c6bda59a the e2e
  default (`[skip ci]`, no plugin code changed). ~75 Actions minutes. Invariants untouched (failover checks exact or clean errors).
- 2026-10-27 (cloud session 78): no new server build or maintainer answer; checked session 77's open point (4e77c6bda59a timing
  gain not repeated back to back). Two same-time profile pairs vs 45fc0a9e6e02: 40590270774 / 40590274657 (1+3 servers) and
  40592375074 / 40592379356 (3 servers), all green. The ~30 % 3-server gain was mostly runner hardware (`ubuntu-latest` mixes
  AMD EPYC 9V45 and 7763; Trino CPU up to 1.7x apart); matched by runner class the new build is ~3-7 % faster on 3 servers,
  1 server 14-28 % (one pair). `profile.sh` now logs vCPU count + CPU model (`[skip ci]`, profile output only). ~55 Actions
  minutes. No plugin code changed; Invariants untouched.
- 2026-10-27/28 (cloud sessions 79-81): nothing actionable — no new `e2e-assets` upload since 4e77c6bda59a (20:53 UTC), no new
  maintainer/stand answers; every Next steps item waits on the stand or an Open question. 0 Actions minutes.
- 2026-10-28 (cloud session 82): Next steps item 1 (new server build cadd1aca9c23, opt-in result compression). Full e2e green:
  partial 1+3 servers run 40617506171, complete 3 servers run 40617513105; profile run 40617519351 (3 servers 34.2 / 27.1 /
  26.6 s on EPYC 7763 = 4e77c6bda59a on the same CPU model within noise; 1 server 30.6 / 25.5 / 27.8 s on EPYC 9V74). Made it
  the e2e default (`e2e.yml`, `run-e2e.sh`, `[skip ci]`). New Open question: plugin opt-in for result compression. ~70 Actions
  minutes. No plugin code changed; Invariants untouched.
- 2026-10-28 (cloud session 83): nothing actionable — no new `e2e-assets` upload since cadd1aca9c23 (01:31 UTC), no new
  maintainer/stand answers (compression opt-in question still open); every Next steps item waits on the stand or an Open
  question. 0 Actions minutes.
- 2026-10-28 (cloud session 84): Next steps item 1 (maintainer: result compression as connector opt-in). New catalog property
  `arrow-flight.result-compression` (NONE default / ZSTD / LZ4): `arrow-compression` header on data-stream DoGets only, compressed
  IPC bodies decoded through a codec-aware `VectorLoader` swapped into the FlightStream (`StreamDecompression`); bundle +5 jars.
  Unit tests `FlightSqlSessionCompressionTest` (11) + config test; build 40670140762 green on 455/477/483 (failed intermediate
  runs 40669497835: a test with a wrong premise - Arrow's default loader finds codecs via ServiceLoader on the TCCL, now a test
  that decoding does not depend on it; 40669864811: bundle lists). e2e: zstd full suite 40670144631 green (1 + 3 servers),
  default off 40671042992 green (1 + 3 servers), slow-link check 40670149146 green (ZSTD 7.22 s vs off 9.41 s on netem 1 Gbit
  + 10 ms, 245 vs 1052 MB; LZ4 slower than off). New e2e inputs `result-compression`, `slow-link` (`e2e/ci/slowlink.sh`).
  New Open question: stand off vs ZSTD. ~85 Actions minutes. Invariants untouched (off by default, nothing sent).
- 2026-10-28 (cloud session 85): nothing actionable — no new `e2e-assets` upload since cadd1aca9c23 (01:31 UTC), no new
  maintainer/stand answers since session 84 (result compression done; stand off-vs-ZSTD and faster-LZ4 questions open); every
  Next steps item waits on the stand or an Open question. 0 Actions minutes.
- 2026-10-28 (cloud session 86): first Next steps item (server build a9e43ca5e296). Suite: partial 1+3 servers run 40683586005
  (3 green; 1 red only on the eps4 q18 heap guard, 305.7 of 307.2 MB per-node limit), complete 3 servers 40683589412 green,
  profile 40683593135; a9e43ca5e296 1 server again 40687927967 + 40690539517 green; cadd1aca9c23 baseline 40687878656 green.
  Cause: q18 builds the lineitem hash table unless the IN-subquery side finishes first (EXPLAIN via new `heap_plan` in
  `e2e/ci/heap-probe.sh`, commits 6418a96, 2fbaaae). Default stays cadd1aca9c23 until the guard is deterministic. No plugin
  code changed; Invariants untouched. ~95 Actions minutes.
