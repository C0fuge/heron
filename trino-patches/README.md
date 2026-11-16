# Trino patch series

Engine patches for Trino that make the connector measurably faster (maintainer's decision 2026-11-16: a patched
Trino is allowed when it is more efficient; the stock-Trino path stays supported and tested).

| version | series | status |
|---|---|---|
| 483 | `483/0001-partial-aggregation-pushdown.patch` | ported, compiles against Trino 483; plugin side not yet implemented |

## 0001 partial aggregation pushdown

Port of upstream's `flight-trino/trino-fork/flight-connector.patch` (Trino 446 base `4377ea536fb4`, upstream server
repo branch `feat/trino-partial-aggregation-pushdown`, commit `f1bb88c`) to Trino tag 483 (`50b0b50b75ab`):

- SPI: `ConnectorMetadata.applyPartialAggregation(session, handle, aggregates, assignments, groupingSets,
  intermediateTypes)` (default: empty) and `ConnectorTableHandle.producesPartialAggregation()` (default: false).
- Engine: `Metadata`/`MetadataManager`/tracing wrappers forward the call; planner rule
  `PushPartialAggregationIntoTableScan`, run after `AddIntermediateAggregations`, replaces a PARTIAL (or a SINGLE,
  keeping a FINAL) aggregation directly over a table scan by a scan of native intermediate states;
  `ValidateAggregationsWithDefaultValues` accepts such a scan.
- Changes against the 446 patch: `IterativeOptimizer` takes a name; `AggregationNode` has no hash symbol any more;
  `ConnectorExpressionTranslator.translate` takes the symbol allocator; `ResolvedAggregationFunctionMetadata
  .intermediateTypes()` and `CatalogSchemaFunctionName.functionName()` are records now;
  `ConnectorTableHandle` is written `{}` in 483.

The SPI method uses only types that stock Trino has, so the plugin can implement it without `@Override` and one plugin
build serves stock (method never called) and patched Trino.

## Build

`trino-patches/build-overlay.sh <version>` (JDK 25): sparse checkout of the Trino tag, `git apply` of the series,
recompiles only the touched sources against the published `trino-main`/`trino-spi` jars of that version (same class
file version) and replaces those classes in copies of the stock jars. Output in `trino-patches/target/<version>/`:
the two jars, `Containerfile` (`FROM trinodb/trino:<version>` + the two jars), `classes.txt`, `PATCHES.sha256`.
A full Trino build would produce the same two jars plus byte-identical copies of every other jar, at ~30+ min per
version. Deployment = replace `lib/trino-spi-<v>.jar` and `lib/trino-main-<v>.jar` of the stock server on every node.

CI: `.github/workflows/patched-trino.yml` (build, cached by version + patch hash; smoke test vs stock image);
`e2e.yml` input `patched-trino=1` runs the full suite on the patched image.
