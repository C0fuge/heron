# Connector comparison: this repo vs upstream `flight-trino`

Maintainer's request of 2026-11-02 (STATUS "CONNECTOR COMPARISON"): the two native Trino connectors to the same Arrow
Flight SQL server, same runner, TPC-H SF1 and SF10, answers checked, timings, memory, pushdown.

## Method

- **Job:** `.github/workflows/connector-compare.yml` (workflow_dispatch) → `e2e/compare/run-compare.sh`, report by
  `e2e/compare/report.py` (printed at the end of the job's "Compare" step). One GitHub-hosted runner per
  (scale factor, server profile); **every connector of a table below ran on the same runner, one after the other**,
  each against a freshly started server and a freshly started Trino.
- **Connectors:**
  - **ours** = this repo (`trino-arrow-flight`, `-P trino-455`) on `trinodb/trino:455`;
  - **up** = `VadimSurpin/arrow-flight-server` branch `feat/trino-flight-connector`, module `flight-trino`, built with
    its own pom (`mvn -pl flight-trino -am package`, JDK 21, tests/checkstyle/spotbugs skipped) on `trinodb/trino:446`;
  - **ours446** = this repo built against the Trino 446 SPI (`-Ddep.trino.version=446 -Dmaven.compiler.release=21`, no
    source change) on `trinodb/trino:446`: same Trino as upstream (default-profile jobs only).
  - Catalogs: only host, port and bearer token (`arrow-flight.*` resp. `flight.*`); every other property at its default.
- **Server:** kestrel pre-release `server-1.0.0-f06b5834e76e` from `e2e-assets` (SHA256SUMS checked), one server,
  token auth, Parquet on S3 (`rclone serve s3` on the same runner), heap 3 GB. Default settings (only data location,
  S3 endpoint and plugin/extension directories are set; no `resultMode`, so the server default applies; ours still asks
  for partial results per call as always); the **speed** jobs add only `-DresourceProfile=speed`.
- **Trino:** same `config.properties` for every pass: single node, `-Xmx8G`, `query.max-memory(-per-node)=4GB`, 900 s cap
  per query.
- **Data:** SF1 = the e2e Parquet files (`test-data-sf1-types`), reference = `e2e/ci/reference/sf1` (DuckDB 1.4.1).
  SF10 = DuckDB 1.4.1 `dbgen(sf=10)` written as Parquet (snappy, files ≤ 256 MB), reference generated in the job by
  `e2e/compare/tpch_data.py ref` from the same files (that generator reproduces the committed SF1 reference 22/22 with
  identical checksums).
- **Queries:** `e2e/ci/queries/q01..q22.sql` through the Trino CLI inside the container (so each time includes ~1.5 s CLI
  start for both connectors). Round 1 = cold (fresh server + Trino), rounds 2-3 warm; **warm = median of rounds 2-3**.
  An answer counts as correct only if all 3 rounds equal the reference (`e2e/ci/compare.py`); the checksum column is
  `tpch_data.py sum` (rows sorted unless ORDER BY, numbers rounded to 2 places, sha256/12).
- **Memory:** container peaks (cgroup v2 `memory.peak`) of Trino and of the server over the whole pass (server = JVM +
  DuckDB native memory); per query Trino's peak user memory and the rows/bytes it read from the connector (REST API).
- **Pushdown:** `EXPLAIN` of q01, q03, q06, q14, q18 after the timed rounds; the `table = ...` lines show what each
  connector sends to the server.

## Versions

| | commit | Trino | Trino JVM |
|---|---|---|---|
| ours | this repo `ab563613037b` (plugin source = v10 line + sessions 1-92) | 455 | 22.0.2 |
| up | upstream `821f627ba757` (branch `feat/trino-flight-connector`, committed 2026-11-02) | 446 | 22.0.1 |
| ours446 | same as ours, built against the 446 SPI: **builds and runs unchanged** | 446 | 22.0.1 |

Server `f06b5834e76e` (`-Xmx3g`, JDK 21 container), runner: GitHub-hosted `ubuntu-latest`, 4 vCPU, 16 GB.
Run [41452015076](https://github.com/C0fuge/heron/actions/runs/41452015076).

## SF1

**Before** (session 94, ours with the old default `split-count=4`; the numbers with the new default are in
"After: proof with `split-count=1` as the default" at the end).

Every pass: **22/22 answers equal to the reference in all 3 rounds, 22/22 checksums equal**.

| job (runner CPU) | pass | sum cold s | **sum warm medians s** | Trino peak MB | server peak MB |
|---|---|---|---|---|---|
| default profile (EPYC 7763) | ours (455) | 78.5 | **63.3** | 5182 | 1182 |
| | up (446) | 89.6 | **71.7** | 4230 | 997 |
| | ours446 (446) | 76.7 | **62.2** | 3739 | 1141 |
| `resourceProfile=speed` (other runner) | ours (455) | 53.5 | **43.7** | 3850 | 1311 |
| | up (446) | 57.3 | **46.7** | 4405 | 876 |

Each query time includes ~1.5 s of Trino CLI start (q22, the cheapest query, takes 1.7 s), i.e. ~33 s of each warm sum
are fixed cost for every connector. The two profile jobs ran on different runners (the speed job's runner was
clearly faster), so compare connectors **within** a job, not the profiles across jobs. Container peaks are dominated
by JVM heap sizing (Trino `-Xmx8G` grows lazily), they say little about per-query need; Trino's per-query peak user
memory is in the table below.

Per query, default profile (cold / warm s; rows = rows the connector handed to Trino; mem = Trino peak user memory MB):

| q | ours cold | ours warm | up cold | up warm | ours446 warm | ours rows | up rows | ours mem | up mem |
|---|---|---|---|---|---|---|---|---|---|
| q01 | 4.99 | 2.08 | 7.92 | 3.98 | 1.99 | 4 | 5916591 | 0 | 9 |
| q02 | 4.65 | 2.22 | 4.92 | 2.46 | 2.02 | 1620799 | 1623959 | 3 | 31 |
| q03 | 4.55 | 3.17 | 4.12 | 2.92 | 3.20 | 3999223 | 3999223 | 114 | 135 |
| q04 | 2.52 | 2.24 | 3.06 | 2.38 | 2.15 | 1432583 | 6058433 | 4 | 45 |
| q05 | 4.82 | 3.69 | 5.31 | 3.93 | 3.59 | 6388838 | 6388838 | 329 | 299 |
| q06 | 2.42 | 2.22 | 2.52 | 2.19 | 2.16 | 1 | 114160 | 0 | 0 |
| q07 | 3.95 | 3.24 | 5.19 | 4.22 | 2.97 | 3488454 | 3488454 | 74 | 209 |
| q08 | 3.88 | 3.20 | 4.84 | 4.19 | 3.27 | 6619980 | 6619980 | 32 | 339 |
| q09 | 4.02 | 3.54 | 6.55 | 5.15 | 3.30 | 8321904 | 8511240 | 109 | 461 |
| q10 | 3.53 | 2.84 | 3.18 | 2.84 | 2.75 | 1685964 | 1685964 | 65 | 75 |
| q11 | 2.18 | 1.91 | 2.57 | 2.19 | 1.80 | 1620002 | 1620002 | 1 | 4 |
| q12 | 2.80 | 2.53 | 2.71 | 2.54 | 2.58 | 1530988 | 1759560 | 2 | 32 |
| q13 | 2.33 | 2.13 | 2.79 | 2.46 | 2.09 | 1634298 | 1650000 | 5 | 75 |
| q14 | 2.77 | 2.53 | 2.56 | 2.52 | 2.54 | 275983 | 275983 | 13 | 12 |
| q15 | 3.61 | 3.29 | 3.27 | 3.26 | 3.33 | 30000 | 461908 | 2 | 9 |
| q16 | 2.65 | 2.07 | 2.53 | 2.12 | 2.35 | 829585 | 841826 | 2 | 7 |
| q17 | 4.49 | 4.18 | 4.26 | 3.88 | 4.14 | 12002634 | 12002634 | 66 | 87 |
| q18 | 4.42 | 3.84 | 5.42 | 4.85 | 3.71 | 9151215 | 13652430 | 215 | 321 |
| q19 | 2.99 | 2.68 | 2.66 | 2.48 | 2.63 | 414377 | 414377 | 9 | 16 |
| q20 | 2.90 | 2.60 | 2.87 | 2.52 | 2.57 | 1721583 | 1919456 | 85 | 50 |
| q21 | 6.16 | 5.37 | 8.34 | 6.81 | 5.22 | 14327221 | 18743059 | 288 | 590 |
| q22 | 1.92 | 1.74 | 1.97 | 1.75 | 1.84 | 386304 | 1786308 | 1 | 1 |

Speed profile, warm s (ours / up): q01 1.54 / 2.44, q07 2.13 / 3.00, q09 2.39 / 3.12, q18 2.73 / 3.17,
q21 3.57 / 4.87; q03 2.23 / 1.90, q17 2.95 / 2.62, q20 1.94 / 1.74 (all other queries within ±0.2 s).

## SF10

**Before** (session 94, ours with the old default `split-count=4`; see "After" at the end).

Data: DuckDB `dbgen(sf=10)` Parquet, 2.85 GB (cached as `tpch-sf10-duckdb141-v1`). q11 has 0 rows at SF10 (its
`0.0001` fraction is the SF1 constant), same in the reference and in both connectors.

### `resourceProfile=speed` (job 125299281747)

| pass | correct (all rounds) | checksum = ref | sum cold s | **sum warm medians s** | **warm, 19 common queries s** | Trino peak MB | server peak MB |
|---|---|---|---|---|---|---|---|
| ours (455) | **22/22** | 22/22 | 382.4 | 362.8 | **277.6** | 9194 | 1901 |
| up (446) | **19/22** | 19/22 | 383.4 | 381.2 (3 failed) | **313.9** | 9576 | 1011 |

Upstream fails q09, q18, q21 in every round with `Query exceeded per-node memory limit of 4GB` (Trino's own limit, same
config for both; Trino peak user memory 4.0-4.2 GB). Ours answers them with 0.9 / 2.5 / 4.1 GB.

| q | ours cold | ours warm | up cold | up warm | ours rows | up rows | ours MB read | up MB read | ours mem MB | up mem MB |
|---|---|---|---|---|---|---|---|---|---|---|
| q01 | 11.31 | 9.57 | 25.33 | 25.05 | 4 | 59142609 | 0 | 4421 | 0 | 13 |
| q02 | 7.41 | 4.54 | 7.96 | 5.45 | 16207906 | 16239627 | 450 | 538 | 66 | 443 |
| q03 | 19.10 | 18.43 | 19.18 | 19.52 | 39923968 | 39923968 | 1080 | 1487 | 1584 | 1584 |
| q04 | 10.98 | 11.83 | 12.56 | 13.14 | 14327145 | 60559723 | 137 | 994 | 379 | 680 |
| q05 | 21.31 | 21.25 | 23.98 | 23.37 | 63861997 | 63861997 | 2223 | 2966 | 3322 | 3328 |
| q06 | 12.07 | 11.81 | 10.92 | 14.67 | 1 | 1139264 | 0 | 37 | 0 | 3 |
| q07 | 21.44 | 19.92 | 32.52 | 29.56 | 34830329 | 34830329 | 1040 | 1223 | 749 | 1671 |
| q08 | 16.69 | 20.20 | 27.89 | 26.69 | 66157068 | 66157068 | 2827 | 3509 | 252 | 4015 |
| q09 | 19.13 | 20.37 | 24.48 | FAIL (memory) | 83194859 | 85086077 | 3668 | 4898 | 930 | 4244 |
| q10 | 21.72 | 22.25 | 15.90 | 18.70 | 16881365 | 16881365 | 670 | 870 | 906 | 925 |
| q11 | 3.77 | 3.84 | 5.04 | 4.33 | 16200002 | 16200002 | 507 | 585 | 22 | 44 |
| q12 | 16.26 | 13.34 | 15.09 | 15.77 | 15310803 | 17600101 | 342 | 384 | 80 | 87 |
| q13 | 8.28 | 7.62 | 11.20 | 13.91 | 16345369 | 16500000 | 281 | 1045 | 204 | 663 |
| q14 | 17.54 | 17.12 | 12.60 | 17.09 | 2749223 | 2749223 | 89 | 96 | 120 | 127 |
| q15 | 28.79 | 26.57 | 23.99 | 26.24 | 300000 | 4631428 | 12 | 191 | 12 | 25 |
| q16 | 4.33 | 3.79 | 3.80 | 3.96 | 8296880 | 8419686 | 160 | 153 | 187 | 187 |
| q17 | 30.61 | 24.79 | 24.62 | 26.35 | 119974148 | 119974148 | 2699 | 3877 | 174 | 200 |
| q18 | 28.35 | 26.71 | 16.62 | FAIL (memory) | 91486052 | 128390564 | 1998 | 3306 | 2502 | 4035 |
| q19 | 13.54 | 17.14 | 18.15 | 14.12 | 4141904 | 4141904 | 156 | 193 | 171 | 129 |
| q20 | 25.90 | 20.53 | 12.32 | 12.31 | 17220717 | 19199166 | 469 | 586 | 611 | 581 |
| q21 | 40.89 | 38.14 | 32.45 | FAIL (memory) | 143253933 | 160521893 | 2938 | 3357 | 4146 | 4221 |
| q22 | 3.03 | 3.04 | 6.84 | 3.64 | 3863596 | 17863614 | 106 | 235 | 20 | 61 |

(For a failed query the "cold" time is the time to the error.)

### Default profile (job 125299282076)

| pass | correct (all rounds) | checksum = ref | sum cold s | **sum warm medians s** | **warm, 19 common queries s** | Trino peak MB | server peak MB |
|---|---|---|---|---|---|---|---|
| ours (455) | **22/22** | 22/22 | 366.5 | 334.6 | **258.6** | 8750 | 2345 |
| up (446) | **19/22** | 19/22 | 336.6 | 329.9 (3 failed) | **269.3** | 9778 | 1463 |
| ours446 (446) | **22/22** | 22/22 | 361.1 | 337.4 | **260.9** | 8691 | 2293 |

Same three upstream failures (q09, q18, q21: Trino 4 GB per-node limit, every round). Rows read and Trino memory per
query are the same as in the speed job (same plans); warm seconds (ours / up / ours446):

| q | ours | up | ours446 | | q | ours | up | ours446 |
|---|---|---|---|---|---|---|---|---|
| q01 | 9.34 | 21.47 | 9.24 | | q12 | 15.08 | 11.84 | 16.43 |
| q02 | 4.88 | 5.43 | 4.61 | | q13 | 7.50 | 10.02 | 8.22 |
| q03 | 19.22 | 13.04 | 18.55 | | q14 | 13.12 | 12.63 | 13.56 |
| q04 | 10.29 | 12.50 | 10.20 | | q15 | 26.20 | 23.33 | 27.26 |
| q05 | 18.56 | 23.64 | 20.25 | | q16 | 3.65 | 3.27 | 3.56 |
| q06 | 11.65 | 13.06 | 12.64 | | q17 | 27.99 | 22.11 | 23.38 |
| q07 | 19.48 | 28.63 | 20.25 | | q18 | 22.46 | FAIL | 22.31 |
| q08 | 17.73 | 23.23 | 17.74 | | q19 | 12.38 | 12.37 | 12.08 |
| q09 | 17.80 | FAIL | 18.07 | | q20 | 18.75 | 11.93 | 19.12 |
| q10 | 16.39 | 12.84 | 17.11 | | q21 | 35.72 | FAIL | 36.16 |
| q11 | 3.65 | 4.21 | 3.99 | | q22 | 2.80 | 3.75 | 2.66 |

## Pushdown (EXPLAIN, identical in every job)

What each connector sends to the server (the `table = ...` of the scans):

| query | ours | up |
|---|---|---|
| q01 | whole aggregation: `lineitem groupBy=[l_returnflag, l_linestatus] aggs=[SUM x4, AVG x3, COUNT(*)]` + the `l_shipdate` filter → **4 rows** come back | projection (4 columns) + `l_shipdate <= 1998-09-02` → 5.9 M rows (SF1), Trino aggregates |
| q03 | projection + filters (`c_mktsegment = 'BUILDING'`, date ranges; our handle's `toString` does not print the pushed WHERE, the equal row counts show it); joins in Trino | same: projection + `constraint` ranges (varchar equality, date ranges) |
| q06 | whole aggregation `aggs=[SUM(...)]` with the filter → **1 row** | projection + 3 range constraints → 114 k rows (SF1), Trino sums |
| q14 | projection + date filter; join in Trino | same |
| q18 | `lineitem groupBy=[l_orderkey] aggs=[SUM(l_quantity)]` for the IN-subquery (HAVING stays in Trino), other scans projection-only | projection only on all scans (`constraint=ALL`), the subquery's GROUP BY over 6 M / 60 M lineitem rows runs in Trino |

Beyond EXPLAIN (from the sources): ours also pushes LIMIT, TopN (opt-in), dynamic filters (opt-in) and answers
`getTableStatistics` with row count + per-column min/max (`SELECT count(*)`, min/max, answered by the server from Parquet footers); upstream pushes
projection and column domains only (integer / decimal / date / boolean ranges, UTF-8 string equality and IN, null checks;
string ranges and REAL/DOUBLE stay in Trino), no aggregation / LIMIT / TopN, and reports only a row count
(`FlightInfo.totalRecords` of the planned scan). Both read one split per Flight endpoint.

## Verdict

- **Correctness:** both connectors return the reference answers for everything they finish (every round, every job).
  Ours finishes 22/22 at SF1 and SF10; upstream finishes 22/22 at SF1 but **19/22 at SF10 under a 4 GB per-node Trino
  limit** (q09, q18, q21 exceed it), because it hands Trino every lineitem row of those joins and aggregations.
- **Where ours is faster and why:** queries whose heavy part is an aggregation over a big scan: q01 (SF10 9.6 s vs 25.1 s,
  4 rows instead of 59 M), q07, q08, q09, q13, q04, q22 and q18/q21 (which upstream cannot finish). The server does the
  aggregation in DuckDB and only groups cross the wire; Trino also needs far less memory (q08 0.25 GB vs 4.0 GB at SF10).
  At SF1 the margin is ~12 % of the warm sum (63.3 vs 71.7 s), of which ~33 s is fixed CLI start for both.
- **Where upstream is faster:** join-heavy queries whose scans both connectors send identically (same rows read): at SF10
  default q03 (13.0 vs 19.2 s), q10 (12.8 vs 16.4), q12 (11.8 vs 15.1), q17 (22.1 vs 28.0), q20 (11.9 vs 18.8), q15; in
  the speed job q10, q20 and roughly even q03/q14/q17. On the 19 queries both finish at SF10 ours is only 4 % faster
  (default: 258.6 vs 269.3 s) resp. 12 % (speed: 277.6 vs 313.9 s): aggregation wins and join losses nearly cancel. Ours on Trino 446 (`ours446`) is as fast as
  ours on 455, so it is not the Trino version; likely candidates are our planning-time statistics calls (one `count(*)` /
  min-max query per table and plan, which upstream does not make) and the different join orders those statistics produce
  (q20: upstream reads 10 % more rows but finishes 40 % sooner). Not investigated further in this item.
- **Server profile `speed`:** does not change the picture (SF1 43.7 vs 46.7 s, SF10 common 277.6 vs 313.9 s, other runners
  than the default jobs, so no profile-vs-profile conclusion).
- **Worth a follow-up on our side:** the join-query gap (q03/q10/q12/q17/q20) - compare the plans (join order and
  distribution) of ours and upstream for those queries and the cost of our statistics calls.
  **Done in session 96 (section below): the cause is the 4 key-range splits per scan, not the statistics or the plans.**
  **Session 97: `split-count` default is now 1; with it ours is 22 % faster than upstream on the 19 common SF10 queries
  (243.8 vs 313.8 s, run 41579205198) and faster or within 4 % on every one except q04 / q12 (+10-12 %).**
  **Session 98 (proof with the shipped default, run 41586926044): SF1 60.8 vs 74.8 s, SF10 19 common 252.1 vs 320.9 s; the
  five join queries are now equal or faster than upstream (see "After").**
- **Memory:** server peak is higher with ours (1.2-1.9 GB vs 0.9-1.0 GB: DuckDB does the aggregations); Trino's per-query
  memory is much lower with ours on aggregation queries and about equal on pure joins.

## Join-query gap: cause (session 96, 2026-11-03)

Maintainer's follow-up item: why is ours slower than upstream on q03/q10/q12/q17/q20 at SF10? Diagnostic run
[41574777687](https://github.com/C0fuge/heron/actions/runs/41574777687) (job 125715635108; SF10, default server profile,
server `f06b5834e76e`, the 5 queries x 3 rounds, plans printed to the job log). The job now accepts a query subset,
extra passes of ours with catalog session properties (`variants`), and prints EXPLAIN plus Trino planning / execution time.

Warm medians, s (every answer exact in every round):

| q | ours (default) | up | ours, `statistics_enabled=false` | ours, `split_count=1` |
|---|---|---|---|---|
| q03 | 21.69 | 15.53 | 21.78 | 15.95 |
| q10 | 22.37 | 16.27 | 21.96 | **15.38** |
| q12 | 17.96 | 14.44 | 17.62 | 15.09 |
| q17 | 30.75 | 28.93 | 28.95 | **18.55** |
| q20 | 22.57 | 16.73 | 24.11 | **15.05** |
| **sum** | **115.3** | **91.9** | **114.4** | **80.0** |

Trino planning time per query (warm, includes our metadata and statistics calls): ours 0.05-0.13 s, upstream 0.08-0.21 s.

- **Not the statistics.** Their planning-time cost is ~0.1 s (the row count / min-max answers are cached per table,
  `arrow-flight.statistics.cache-ttl-seconds`, 600 s), and with statistics switched off the times stay the same although
  the plans change (q03 `c_custkey = o_custkey` REPLICATED with statistics, PARTITIONED without; q17 likewise). The join
  order and distribution do not decide these queries on a single node.
- **The cause is the key-range splitting** (`arrow-flight.split-count`, default 4): every big scan (lineitem, orders)
  becomes 4 server statements `... WHERE key >= a AND key < b`, each planned and run by DuckDB on its own and competing for
  the same cores. Upstream reads one split per Flight endpoint (one statement). With `split_count=1` ours is **30 % faster
  than its default and 13 % faster than upstream** on these five queries (q17: 18.6 vs 28.9 s), with the same rows read.
  This matches the CI finding of session 65/66 (SF1 suite ~27 % faster with one split; see STATUS Open questions).
- Verification of the whole suite: see the next section.

## Full suite with `split_count=1` (session 97, 2026-11-03) - the new default

Run [41579205198](https://github.com/C0fuge/heron/actions/runs/41579205198) (jobs 125729788996 SF1, 125729789112 SF10;
default server profile, server `f06b5834e76e`, ours `62cb748c436f` = old default 4 splits, upstream `2e86e98ad599` on
Trino 446, ours-split1 = same plugin with session `split_count=1`; 3 rounds, warm = median of rounds 2-3). Every answer
that came back is exact in every round; upstream again fails q09, q18, q21 at SF10 (Trino's 4 GB per-node limit).

| | ours (4 splits, before) | up | **ours-split1 (now the default)** |
|---|---|---|---|
| SF1 correct | 22/22 | 22/22 | 22/22 |
| SF1 sum warm medians s | 53.2 | 61.6 | **49.4** |
| SF1 Trino / server peak MB | 5212 / 1254 | 4057 / 1007 | 4284 / 1006 |
| SF10 correct | 22/22 | 19/22 | 22/22 |
| SF10 sum warm medians s, all 22 | 402.1 | - | **325.4** |
| SF10 sum warm medians s, 19 common | 304.2 | 313.8 | **243.8** |

SF10 warm medians, s (split1 vs 4 splits):

| q | ours 4 splits | up | ours-split1 | split1 vs 4 |
|---|---|---|---|---|
| q01 | 10.47 | 29.21 | 10.70 | +2 % |
| q02 | 6.28 | 7.18 | 5.45 | -13 % |
| q03 | 20.84 | 14.77 | 14.41 | -31 % |
| q04 | 12.47 | 10.14 | 11.33 | -9 % |
| q05 | 23.56 | 23.25 | 17.62 | -25 % |
| q06 | 12.32 | 15.02 | 12.16 | -1 % |
| q07 | 24.91 | 30.52 | 16.91 | -32 % |
| q08 | 21.63 | 27.45 | 14.71 | -32 % |
| q09 | 25.33 | fail | 18.75 | -26 % |
| q10 | 20.06 | 15.60 | 16.09 | -20 % |
| q11 | 5.19 | 5.91 | 3.90 | -25 % |
| q12 | 16.56 | 13.36 | 14.63 | -12 % |
| q13 | 9.63 | 10.45 | 7.38 | -23 % |
| q14 | 14.46 | 14.09 | 14.30 | -1 % |
| q15 | 29.45 | 30.05 | 29.82 | +1 % |
| q16 | 4.04 | 3.73 | 3.62 | -10 % |
| q17 | 29.64 | 30.52 | 18.50 | -38 % |
| q18 | 27.56 | fail | 24.91 | -10 % |
| q19 | 14.25 | 13.91 | 14.49 | +2 % |
| q20 | 24.29 | 13.97 | 13.71 | -44 % |
| q21 | 44.98 | fail | 37.94 | -16 % |
| q22 | 4.20 | 4.63 | 4.04 | -4 % |

- **SF10:** no query is more than 2 % slower with one split; the sum is 19 % lower. Against upstream, ours-split1 is
  faster or equal (within 4 %) on every common query except q04 (+12 %) and q12 (+10 %), and 22 % faster in sum.
- **SF1:** 7 % lower sum; one query is slower by more than 10 %: q04 2.08 vs 1.78 s warm (+0.3 s; cold 2.73 vs 2.31).
  At SF10 q04 is 9 % faster with one split, so this is taken as the small-table case where 4 parallel server statements
  still pay off, not a reason to keep 4. Other SF1 queries: -28 % (q17) to +4 %.
- **Decision (session 97):** `arrow-flight.split-count` default 4 -> **1**. Key-range splitting stays available
  (`split-count` > 1 or session `split_count`), e.g. for a scan whose Trino-side work dominates (the stand's 2026-10-23
  SF10 run favoured 4 for such a scan).
- Proof with the new default (ours vs up, no variants): next section.

## After: proof with `split-count=1` as the default (session 98, 2026-11-03)

Run [41586926044](https://github.com/C0fuge/heron/actions/runs/41586926044) (jobs 125754488670 SF1, 125754488418 SF10;
default server profile, server `f06b5834e76e`, ours = commit aa608f3 with the shipped default (no session properties) on
Trino 455, up = upstream branch head at run time on Trino 446; 3 rounds, warm = median of rounds 2-3). Every answer that
came back is exact in every round (checksums = reference); upstream again fails q09, q18, q21 at SF10 (Trino's 4 GB
per-node limit, every round). Runners differ from the "before" jobs, so compare within a row of this table.

| | ours (new default) | up |
|---|---|---|
| SF1 correct | 22/22 | 22/22 |
| SF1 sum cold / **warm medians** s | 77.5 / **60.8** | 94.9 / **74.8** |
| SF1 Trino / server peak MB | 3392 / 979 | 4543 / 1022 |
| SF10 correct | 22/22 | 19/22 |
| SF10 sum warm medians s, all 22 | 332.7 | - |
| SF10 sum warm medians s, **19 common** | **252.1** | **320.9** |
| SF10 Trino / server peak MB | 8333 / 1814 | 9637 / 1461 |

Ours is 19 % faster than upstream at SF1 and 21 % faster on the common SF10 queries (before: 12 % resp. 4 %).

The five join queries of the maintainer's item, SF10 warm medians s (before = session 94 default job, same server):

| q | before: ours | before: up | **after: ours** | **after: up** |
|---|---|---|---|---|
| q03 | 19.22 | 13.04 | **14.82** | 15.72 |
| q10 | 16.39 | 12.84 | **15.09** | 15.04 |
| q12 | 15.08 | 11.84 | **15.41** | 16.98 |
| q17 | 27.99 | 22.11 | **19.55** | 30.18 |
| q20 | 18.75 | 11.93 | **14.41** | 15.75 |

SF10 per query, warm medians s (ours / up): q01 10.85 / 28.98, q02 4.50 / 8.27, q04 11.32 / 10.84, q05 18.30 / 23.46,
q06 12.77 / 12.51, q07 17.51 / 30.95, q08 14.89 / 28.05, q09 16.23 / fail, q11 3.29 / 5.75, q13 6.94 / 10.90,
q14 14.51 / 14.75, q15 33.62 / 29.34, q16 3.88 / 4.33, q18 27.79 / fail, q19 16.85 / 14.79, q21 36.66 / fail,
q22 3.56 / 4.31. Planning time (includes our statistics calls) 0.02-0.25 s for ours, 0.02-0.21 s for upstream.

SF1 per query, warm medians s (ours / up): q01 2.17 / 4.50, q02 2.21 / 2.56, q03 2.85 / 3.01, q04 2.30 / 2.50,
q05 3.35 / 4.38, q06 2.19 / 2.45, q07 2.96 / 4.40, q08 2.90 / 4.53, q09 3.12 / 5.26, q10 2.97 / 2.84, q11 1.97 / 2.19,
q12 2.67 / 2.56, q13 2.25 / 2.47, q14 2.56 / 2.52, q15 3.46 / 3.42, q16 2.33 / 2.03, q17 2.91 / 4.05, q18 3.84 / 4.93,
q19 2.58 / 2.56, q20 2.69 / 2.67, q21 4.72 / 7.18, q22 1.79 / 1.81.

- **Remaining spots where upstream is faster (SF10):** q15 (+15 %) and q19 (+14 %), both with a noisy round in ours
  (q15 cold 28.8 s < warm 33.6 s; q19 cold 23.2 s) and both read the same or fewer rows than upstream; q04 +4 %; q10 and
  q14 equal. In the session-97 run q15/q19 were equal (29.8 / 30.1 and 14.5 / 13.9 s), so these are taken as runner noise,
  not a new gap. SF1: everything within 0.13 s except q16 (+0.3 s, 2.33 vs 2.03; same at SF10 is 10 % faster).
- **Verdict:** the join-query gap is closed by `split-count=1`; ours is now faster than upstream in sum at both scale
  factors and on every one of the five join queries except q10 (equal). Maintainer's item: **IMPROVEMENTS DONE**.
