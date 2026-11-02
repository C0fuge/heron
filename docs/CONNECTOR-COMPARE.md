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
