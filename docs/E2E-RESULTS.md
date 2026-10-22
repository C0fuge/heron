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

| Date | Run | Trino | Mode | Servers | TPC-H SF1 | Failover | Job time |
|---|---|---|---|---|---|---|---|
| 2026-10-22 | 39829220892 | 455 | partial | 1 | 22/22 | – | 5 min |
| 2026-10-22 | 39830281255 | 455 | partial | 1 | 22/22 | – | 5 min |
| 2026-10-22 | 39830281255 | 455 | partial | 3 | 22/22 | killed before: 5/5 exact; killed during q18: exact answer | 6.5 min |

Failover checks (3 servers, `--hosts flight1,flight2,flight3`, catalog points to flight1):
- *killed before*: `docker kill flight2`, 10 s pause, then q01 q03 q05 q06 q18 → must equal the reference.
- *killed during*: q18 started, `docker kill flight3` 2 s later → the query must either fail cleanly or
  return the exact reference answer (never different rows).

Known limits of these checks (to tighten later): the job does not yet prove that the endpoints of the killed
node were really read from backup locations (vs. the server no longer handing them out), nor that the kill
in *killed during* hit an open stream of flight3. Both need a log marker from the plugin or server.
