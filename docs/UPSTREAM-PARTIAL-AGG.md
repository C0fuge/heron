# Upstream partial aggregation pushdown: review (session 297, 2026-11-15)

Source: upstream server repo, branch `feat/trino-partial-aggregation-pushdown` (read only; fetched into the session,
nothing pushed there). Commits reviewed: `f1bb88c` (engine SPI patch), `f121311` (partial COUNT states), `c2d4093`
(partial numeric aggregates), `f96e771` (numeric MIN/MAX), `398affc` (COUNT input types); server-side `821f627` /
`2e86e98` (DuckDB result streaming + streaming buffer) are server changes and reach us with the next server build.

## What upstream does

- A **patched Trino engine** (fork of Trino 446, `flight-trino/trino-fork/flight-connector.patch`, pinned base
  `4377ea536fb4`): new SPI `ConnectorMetadata.applyPartialAggregation` (default empty), `ConnectorTableHandle
  .producesPartialAggregation()`, a planner rule `PushPartialAggregationIntoTableScan` that runs after normal
  PARTIAL/FINAL planning and replaces a PARTIAL (or a SINGLE, keeping a FINAL) directly over the scan by a scan of
  **native Trino intermediate states**, and a relaxed `ValidateAggregationsWithDefaultValues`.
- The connector then emits, **per split/endpoint**, DuckDB-computed partial states in Trino's exact intermediate
  representation (COUNT -> BIGINT; MIN/MAX -> input type; integral SUM -> checked BIGINT; DOUBLE SUM; AVG ->
  ROW(DOUBLE sum, BIGINT count) in Trino 446's field order); Trino's own FINAL aggregation merges them after a normal
  hash exchange, i.e. the final merge is distributed over the workers and memory-accounted by Trino.
- Session property `partial_aggregation_pushdown`, default **off**; "no effect on stock Trino".
- Correctness details upstream handled: global COUNT over zero endpoints (synthetic empty-state split), floating
  MAX via `MIN(-v)` (DuckDB orders NaN above everything, Trino's MAX skips NaN), decimal footer extrema precision.

## How ours differs

- Ours uses the **stock** SPI `applyAggregation` (works on unmodified Trino 455 / 477 / 483): the whole aggregation is
  pushed, every endpoint returns its partial groups (`partial` result mode), and the plugin merges them in one page
  source (`AggregationReader`: k-way sorted merge, hash merge fallback, memory reported to Trino). The final merge is
  therefore single-threaded per scan (one split for aggregations), not distributed.
- Upstream's gain would show on high-cardinality GROUP BY over several endpoints (q04/q18 `GROUP BY l_orderkey`), where
  our merge is one driver. Measured on SF10 so far ours is already faster than upstream's connector in sum (session
  98/135, `docs/CONNECTOR-COMPARE.md`), but upstream's partial mode was not part of those runs.

## Feasibility for our production (stock Trino 455, offline)

- The engine patch applies cleanly (`git apply --check`) to Trino **455** (and 446); it does **not** apply to **477**
  or **483** (`PlanOptimizers.java` hunk at line 989; on 483 also `ConnectorTableHandle.java`) - each version needs
  its own port. Not compiled or tested here (a Trino build is ~30+ min of Actions time per version).
- Adopting it means the maintainer deploys a **self-built Trino** on every node instead of the stock 455 image; the
  plugin would keep the stock `applyAggregation` path when the patched SPI is absent. This is a deployment decision,
  not a plugin change -> Open question in `docs/STATUS.md`.
- The maintainer's note also asks for before/after times on server `server-1.0.0-2c169a53db3b`; that build is not in
  `e2e-assets` yet (only deafc861cce3 is, uploaded 2026-11-06), so no measurement was possible.

## Finding applied to our stock path

Upstream's MAX/NaN note applies to ours too: our `applyAggregation` pushed `max(double)` to DuckDB, which returns NaN
when the column holds NaN, while Trino's `max` skips NaN. Session 297 stops pushing `max` on DOUBLE (min, sum, count,
avg on DOUBLE and max on every other type stay pushed) and adds the e2e block `nanagg` (pushed vs non-pushed
min/max/count/sum on every DOUBLE column of schema `types`, which contains NaN and +-Infinity). See STATUS Log.

## Verdict per upstream commit (session 300, 2026-11-16)

Maintainer's answers (2026-11-16 06:40): production stays on **stock Trino**; the rule is "ours vs theirs per feature,
adopt only what is missing or measured better, never weaken our own features". Hence:

| upstream commit | what | verdict |
|---|---|---|
| `f1bb88c` | engine SPI patch (`applyPartialAggregation`, planner rule) | NOT APPLICABLE (needs a patched engine) |
| `f121311` | partial COUNT states pushed to Flight | NOT APPLICABLE (depends on f1bb88c); its "global COUNT over zero endpoints" note taken as cd043ad |
| `c2d4093` | partial numeric aggregates (SUM/AVG states) | NOT APPLICABLE (depends on f1bb88c) |
| `f96e771` | numeric MIN/MAX states, floating MAX via MIN(-v) | NOT APPLICABLE (depends on f1bb88c); its NaN note taken as b6d92bf (no `max` pushdown on DOUBLE) |
| `398affc` | COUNT input types | NOT APPLICABLE (depends on f1bb88c); ours already pushes `count(x)` for every mapped type |
| `821f627` | server: DuckDB `jdbc_stream_results` | NOT NEEDED here (server side): server d2e27119fda4 has its own `duckDbStreamResults` (`arrowflight.duckdb.streamResults`) |
| `2e86e98` | server: DuckDB `streaming_buffer_size` 64MiB | NOT NEEDED here (server side): server d2e27119fda4 has its own validated `duckDbStreamingBufferSize` (`arrowflight.duckdb.streamingBuffer`) |

Checked by reading the strings of `server-1.0.0-d2e27119fda4`'s `DuckDbAdapter`/`AppConfig` classes (no code run).
Before/after measurement of the two taken fixes: see STATUS Log, session 300.
