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
  restarting flight3 if missed) → the query must fail cleanly or return the exact reference answer, and the
  kill must hit a read of flight3: a failed query or the plugin's WARN `Flight stream at grpc+tcp://flight3…
  failed before any data (…); reading it from grpc+tcp://flight1…` (observed: 4 streams re-read from flight1,
  exact answer). In partial mode a miss on every delay fails the job.
- *two killed*: flight2 and flight3 down, q01 q03 q05 q06 q18 → exact or clean error (observed: 5/5 exact,
  flight1 answers alone).

Not covered yet: a kill in the middle of a stream that already delivered data (must be a clean error); every
kill so far landed before the first batch.

Server build: `SERVER_BUILD` / workflow input `server-build` (`int4` default, `int5`) picks
`arrow-flight-server-1.0.0-<build>.jar` + `<build>-plugins.tar.zst` from the release.
