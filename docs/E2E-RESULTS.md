# End-to-end results (CI)

Workflow `.github/workflows/e2e.yml` → `e2e/ci/run-e2e.sh`; answers compared by `e2e/ci/compare.py` with
`e2e/ci/reference/sf1` (DuckDB 1.4.1 on the same SF1 Parquet files). Per-run outputs (answers, timings,
server/Trino logs) are in the run's artifact `e2e-<trino>-<mode>-<n>srv` (kept 7 days).

Setup: GitHub runner, Docker network; S3 = `rclone serve s3` (MinIO is no longer distributed) serving the
release `e2e-assets` data at `s3://lake/parquet/{tpch,types}`; Flight server int4 (JDK 21, s3 plugin, httpfs
v1.4.5, token auth, `-Xmx2g`); Trino `trinodb/trino:<v>` with the plugin built in the same job; catalog `flight`
with `arrow-flight.bearer-token`.

Comparison: numeric cells equal after rounding to 4 places (or relative 1e-9); rows sorted unless the query
has ORDER BY. Trino keeps DECIMAL for `avg(decimal)` and decimal division where DuckDB returns DOUBLE (q01
avg columns, q17): such cells pass when the DuckDB value rounded to Trino's scale is equal, and are counted
in the detail ("N cells equal at Trino DECIMAL scale").

| Date | Run | Trino | Mode | Servers | TPC-H SF1 (+ types) | Failover | Job time |
|---|---|---|---|---|---|---|---|
| 2026-10-22 | 39829220892 | 455 | partial | 1 | 22/22 | – | 5 min |
| 2026-10-22 | 39830281255 | 455 | partial | 1 | 22/22 | – | 5 min |
| 2026-10-22 | 39831337458 | 455 | complete | 1 | 22/22 | – | 5 min |
| 2026-10-22 | 39830281255 | 455 | partial | 3 | 22/22 | killed before: 5/5 exact; killed during q18: exact answer | 6.5 min |
| 2026-10-22 | 39835656012 | 455 | partial | 1 | 22/22, types 2/2 | – | 4.5 min |
| 2026-10-22 | 39836376906 | 455 | complete | 3 | 22/22, types 2/2 | killed before 5/5 exact; killed during q18 exact | 7.5 min |
| 2026-10-22 | 39840087384 | 455 | partial | 1 | 22/22, types 2/2 | – | 5 min |
| 2026-10-22 | 39841317139 | 455 (server int5) | partial | 1 | 22/22, types 2/2 | – | 5 min |
| 2026-10-22 | 39840087384 | 455 | partial | 3 | 22/22, types 2/2 | lost before 5+5 exact; killed during q18 exact, 3 streams failed over to flight1; two killed 5/5 exact | 8 min |
| 2026-10-23 | 39874470743 | 455 (int6) | complete | 1 | 22/22, types 2/2, Iceberg (deletes KNOWN) | – | 8.5 min |
| 2026-10-23 | 39878210446 | 455 (int6) | partial | 3 | 22/22, types 2/2, Iceberg (deletes KNOWN) | lost before 5+5 exact; killed during q18 exact on tpch (attempt 1) and on ice (attempt 3, streams re-read from flight2); two killed 5/5 tpch + 3/3 ice exact | 11 min |
| 2026-10-23 | 39886319914 | 455 (int6) | complete | 3 | 22/22, types 2/2, Iceberg (deletes KNOWN) | lost before 5+5 exact; killed during q18 exact (tpch; ice attempt 3, streams re-read from flight1); two killed 5/5 + 3/3 exact | 14 min |
| 2026-10-23 | 39886487789 | 455 (int6) | partial | 3 | 22/22, types 2/2, Iceberg (deletes KNOWN) | killed during q18 (tpch): clean error, one flight3 stream "failed after delivering data; not retried" (first mid-stream hit); ice exact; mid-stream lineitem check 4 misses (INFO, delays too short), all exact; two killed exact | 15.5 min |
| 2026-10-23 | 39891124604 | 455 (int6) | partial | 3 | 22/22, types 2/2, Iceberg (deletes KNOWN) | lost before 5+5 exact; killed during q18 exact (tpch, ice); killed mid-stream: attempt 1 (9 s) hit a flight3 stream after data → clean error; two killed exact | 17 min |
| 2026-10-23 | 39941848259 | 455 (int6, S3 warehouse) | partial | 1 | 22/22, types 2/2, Iceberg all OK incl. deletes (1,285,715 rows) | – | 19 min |

Types (`e2e/ci/types_check.py`, every run): each table of schema `types` read with `SELECT` of all
exposed columns through Trino (Python client, raw protocol values) vs DuckDB 1.4.1 (pip, in the job) on the
same Parquet files; rows compared as sorted multisets, per column and whole rows; columns the plugin does not
map are listed as SKIPPED. Result since 2026-10-22: `alltypes` 5000 rows x 23 columns and `alltypes_full`
5000 rows x 25 columns equal (bool, signed/unsigned ints, hugeint-as-double, float, double, decimal 9/18/38,
varchar, blob, date, time, timestamp s/ms/us/ns, timestamptz, uuid); skipped: interval, list, struct, map.

Failover checks (3 servers, `--hosts flight1,flight2,flight3`, catalog points to flight1):
- *lost before*: flight2 cut off the network (`docker network disconnect`), q01 q03 q05 q06 q18; then
  flight2 killed, 10 s pause, the same queries → all must equal the reference. The server checks its members
  when planning and never hands out the lost node (0 plugin failovers to flight2 observed in both rounds), so
  this checks the server's re-planning, not the plugin.
- *killed during*: q18 started, `docker kill flight3` 2 s later (retried with delays 1, 4, 6 s after
  restarting flight2 and flight3 if missed — a server starts only when every `--hosts` member joins, so
  flight3 cannot come back alone while flight2 is down); with Iceberg on (int5+) the same check runs again on
  schema `ice` (catalog tickets; a miss there is INFO, it depends on how Spark split the files) → the query must fail cleanly or return the exact reference answer, and the
  kill must hit a read of flight3: a failed query or the plugin's WARN `Flight stream at grpc+tcp://flight3…
  failed before any data (…); reading it from grpc+tcp://flight1…` (observed: 4 streams re-read from flight1,
  exact answer). In partial mode a miss on every delay fails the job.
- *two killed*: flight2 and flight3 down, q01 q03 q05 q06 q18 → exact or clean error (observed: 5/5 exact,
  flight1 answers alone) With Iceberg on, also q01 q03 q18 on schema `ice`.

- *killed mid-stream*: `SELECT count(*), checksum(<col>) for each of the 16 lineitem columns FROM lineitem` (not pushed
  down, streams all of lineitem) run once on the healthy cluster as baseline, then
  with `docker kill flight3` after 9, 8, 10, 11 s (`KILL_STREAM_DELAYS`; restart of flight2+flight3 between
  attempts) → the answer must equal the baseline or the query fails cleanly; evidence of a hit is the plugin's
  WARN `Flight stream at …flight3… failed after delivering data (…); not retried` (added 2026-10-23; behaviour
  unchanged, such failures were always final). A miss on every delay is INFO. A wrong answer fails the job. (Observed: run 39891124604, hit at 9 s, clean error.)
  The *killed during* q18 check can also hit mid-stream (run 39886487789: clean error "Network closed for
  unknown reason" plus that WARN).

Server build: `SERVER_BUILD` / workflow input `server-build` (`int4` default, `int5`) picks
`arrow-flight-server-1.0.0-<build>.jar` + `<build>-plugins.tar.zst` from the release.
Iceberg warehouse: `ICEBERG_WAREHOUSE` / workflow input `iceberg-warehouse` = `local` (default, `/wh` mounted on every
node) or `s3` (Spark writes `s3://lake/parquet/wh` through S3A; first green run 39941848259, deletes OK).
