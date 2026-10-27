#!/usr/bin/env bash
# CI end-to-end run: S3 (rclone) + Flight SQL server(s) + Trino with this plugin, TPC-H SF1 22/22
# compared with e2e/ci/reference/sf1, and every table of schema 'types' compared with DuckDB
# (pip duckdb==1.4.1) reading the same Parquet files (e2e/ci/types_check.py).
# Also: an unpushable projection, TopN pushdown on vs off, dynamic filters on vs off (DF_QUERIES: answers
# vs the reference, rows read and time per setting in dynfilter-rows.tsv), and split count x endpoint read parallelism
# (PAR_QUERIES: answers vs the reference, times in splits-times.tsv).
#
#   ASSETS=<dir with the e2e-assets release files>  PLUGIN_ZIP=<plugin zip>  OUT=<results dir>
#   TRINO_VERSION=455  RESULT_MODE=partial  SERVERS=1|3  SERVER_BUILD=int4..int7|<server CI build id>  e2e/ci/run-e2e.sh
#
# SERVERS=3 forms a cluster (backup locations) and then checks endpoint failover: a node lost
# before the queries (partitioned, then killed: answers must still be exact), a node killed during a read (clean error or exact
# answer, never wrong rows; retried with other delays until the kill hits a read of that node),
# a node killed after its stream delivered data (clean error or the exact answer, never fewer rows),
# and two of three nodes down (exact or clean error).
#
# ICEBERG=1 (default for server builds after int4: int5, int6, int7, which carry the 'iceberg' catalog plugin): Spark writes
# Iceberg tables (schema 'ice') and they are checked against the Parquet tables; see e2e/ci/iceberg.sh.
#
# Docker on the host (GitHub runner); S3 = rclone serve s3; everything on the private network 'afe2e'.
# Credentials (S3, Flight token) are throwaway values generated per run.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="${ASSETS:?}"; PLUGIN_ZIP="${PLUGIN_ZIP:?}"; OUT="${OUT:?}"
TRINO_VERSION="${TRINO_VERSION:-455}"; RESULT_MODE="${RESULT_MODE:-partial}"
SERVERS="${SERVERS:-1}"; SERVER_HEAP="${SERVER_HEAP:-2g}"; SERVER_BUILD="${SERVER_BUILD:-4e77c6bda59a}"
FAILOVER_QUERIES="${FAILOVER_QUERIES:-q01 q03 q05 q06 q18}"
ICEBERG_FAILOVER_QUERIES="${ICEBERG_FAILOVER_QUERIES:-q01 q03 q18}"  # all in ICEBERG_QUERIES
WORK="${WORK:-$(mktemp -d)}"
NET=afe2e
# the server checks its DuckDB extension versions at startup: 27f583adccd7 and later run DuckDB 1.5.6
case "$SERVER_BUILD" in
  int*|4eaab052deaf|c3b276054cc3|20b887839edf|60e888bc26ec|7fbe2d75827c|4d707e2aedfc|57d252b1437f)
    DUCKDB_EXT_VERSION="${DUCKDB_EXT_VERSION:-v1.4.5}" ;;
  *) DUCKDB_EXT_VERSION="${DUCKDB_EXT_VERSION:-v1.5.6}" ;;
esac
case "$SERVER_BUILD" in int4) ICEBERG="${ICEBERG:-0}" ;; *) ICEBERG="${ICEBERG:-1}" ;; esac
mkdir -p "$OUT" "$WORK" "$WORK/wh"
# shellcheck source=e2e/ci/iceberg.sh
. "$REPO/e2e/ci/iceberg.sh"
# shellcheck source=e2e/ci/heap-probe.sh
. "$REPO/e2e/ci/heap-probe.sh"
# shellcheck source=e2e/ci/profile.sh
. "$REPO/e2e/ci/profile.sh"

S3_KEY="ci$(openssl rand -hex 8)"; S3_SECRET="$(openssl rand -hex 24)"; TOKEN="$(openssl rand -hex 24)"

log() { echo "== $(date -u +%T) $*"; }

log "unpack assets"
mkdir -p "$WORK/server" "$WORK/data" "$WORK/plugin"
# Asset names: intN builds = arrow-flight-server-1.0.0-intN.jar + intN-plugins.tar.zst + the shared
# flight-server-libs.tar.zst; server-repo CI builds (e.g. 4eaab052deaf) = arrow-flight-server-1.0.0-<b>-linux.jar +
# flight-plugins-1.0.0-<b>.tar.zst + their own flight-server-libs-1.0.0-<b>.tar.zst (never mixed with other libs).
case "$SERVER_BUILD" in
  int*) SERVER_JAR="arrow-flight-server-1.0.0-$SERVER_BUILD.jar"; SERVER_PLUGINS="$SERVER_BUILD-plugins.tar.zst"
        SERVER_LIBS="flight-server-libs.tar.zst" ;;
  *)    SERVER_JAR="arrow-flight-server-1.0.0-$SERVER_BUILD-linux.jar"; SERVER_PLUGINS="flight-plugins-1.0.0-$SERVER_BUILD.tar.zst"
        SERVER_LIBS="flight-server-libs-1.0.0-$SERVER_BUILD.tar.zst" ;;
esac
cp "$ASSETS/$SERVER_JAR" "$WORK/server/server.jar"
tar --zstd -xf "$ASSETS/$SERVER_PLUGINS" -C "$WORK/server"
if [ -f "$ASSETS/$SERVER_LIBS" ]; then
  tar --zstd -xf "$ASSETS/$SERVER_LIBS" -C "$WORK/server"
  if [ -f "$WORK/server/libs/SHA256SUMS" ]; then (cd "$WORK/server/libs" && sha256sum --quiet -c SHA256SUMS); fi
else
  log "$SERVER_LIBS not in the release: running the server jar alone"
fi
mkdir -p "$WORK/server/libs"
ls "$WORK/server" "$WORK/server/plugins" || true; ls "$WORK/server/libs" | wc -l
find "$WORK/server" "$ASSETS" -maxdepth 3 -name 'START-NOTES*' -exec sh -c 'echo "--- $1"; cat "$1"' _ {} \;
tar --zstd -xf "$ASSETS/test-data-sf1-types.tar.zst" -C "$WORK/data"
find "$WORK/data" -maxdepth 3 -type d
unzip -qj "$PLUGIN_ZIP" -d "$WORK/plugin"

log "httpfs $DUCKDB_EXT_VERSION"
EXT_DIR="$WORK/duckdb/extensions/$DUCKDB_EXT_VERSION/linux_amd64"
mkdir -p "$EXT_DIR"
curl -fsSL "http://extensions.duckdb.org/$DUCKDB_EXT_VERSION/linux_amd64/httpfs.duckdb_extension.gz" \
  | gunzip > "$EXT_DIR/httpfs.duckdb_extension"

docker network create $NET >/dev/null

log "s3 (rclone serve s3)"
# MinIO is no longer distributed (images not pullable anonymously, release binaries 410 Gone):
# rclone serves a local directory over the S3 API; directory 'lake' is bucket 'lake'.
SF1_TPCH="$(dirname "$(find "$WORK/data" -type d -name lineitem -print -quit)")"
TYPES="$(find "$WORK/data" -maxdepth 2 -type d -name types -print -quit)"
mkdir -p "$WORK/s3/lake/parquet"
mv "$SF1_TPCH" "$WORK/s3/lake/parquet/tpch"
[ -n "$TYPES" ] && mv "$TYPES" "$WORK/s3/lake/parquet/types"
docker run -d --name s3 --network $NET -p 127.0.0.1:9000:9000 -v "$WORK/s3":/data \
  rclone/rclone:1.68.2 serve s3 /data --addr :9000 --auth-key "$S3_KEY,$S3_SECRET" >/dev/null
export AWS_ACCESS_KEY_ID="$S3_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET" AWS_DEFAULT_REGION=us-east-1
for i in $(seq 1 30); do aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://lake/ >/dev/null 2>&1 && break; sleep 1; done
aws --endpoint-url http://127.0.0.1:9000 s3 ls --recursive s3://lake/parquet/ > "$WORK/s3-listing.txt"
head -5 "$WORK/s3-listing.txt"; wc -l < "$WORK/s3-listing.txt"

SERVER_EXTRA=()
if [ "$ICEBERG" = 1 ]; then
  iceberg_generate
  SERVER_EXTRA=("${ICEBERG_SERVER_ARGS[@]}")
  SERVER_CP_EXTRA=$ICEBERG_SERVER_CP
fi

HOSTS="$(seq -s, -f 'flight%g' 1 "$SERVERS")"
log "flight servers $HOSTS (result mode $RESULT_MODE)"
start_server() {  # name
  mkdir -p "$WORK/$1"  # writable cwd: DuckDB temp dir (.tmp)
  docker run -d --name "$1" --network $NET \
    -v "$WORK/server":/app:ro -v "$WORK/duckdb":/ext/duckdb-home:ro \
    -v "$WORK/$1":/work -w /work -v "$WORK/wh":/wh:ro \
    -e LANG=C.UTF-8 -e FLIGHT_S3_ACCESS_KEY="$S3_KEY" -e FLIGHT_S3_SECRET_KEY="$S3_SECRET" \
    -e FLIGHT_AUTH_PROVIDER=token -e FLIGHT_AUTH_TOKEN="$TOKEN" \
    eclipse-temurin:21-jdk \
    java -Xmx$SERVER_HEAP --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
         -DdataDir=s3://lake/parquet -Ds3Endpoint=http://s3:9000 -Ds3UrlStyle=path -Ds3Region=us-east-1 \
         -Darrowflight.plugins.dir=/app/plugins -Darrowflight.duckdb.extensionDir=/ext/duckdb-home/extensions \
         -DresultMode="$RESULT_MODE" "${SERVER_EXTRA[@]}" ${SERVER_ENDPOINTS:+-Darrowflight.cluster.endpointsPerServer=$SERVER_ENDPOINTS} \
         -cp "${SERVER_CP_EXTRA:-}/app/server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
         --port 32010 --localhost "$1" --hosts "$HOSTS" >/dev/null
}
wait_server() {  # name
  for i in $(seq 1 90); do
    grep -q 'Server Arrow Flight SQL started' <(docker logs "$1" 2>&1) && break
    docker ps -q -f name="$1" -f status=running | grep -q . || break
    sleep 2
  done
  docker logs "$1" > "$OUT/$1.log" 2>&1 || true
  grep -q 'Server Arrow Flight SQL started' "$OUT/$1.log" || { tail -50 "$OUT/$1.log"; exit 1; }
}
for h in ${HOSTS//,/ }; do start_server "$h"; done
for h in ${HOSTS//,/ }; do wait_server "$h"; done

log "trino $TRINO_VERSION"
mkdir -p "$WORK/etc/catalog"
cat > "$WORK/etc/catalog/flight.properties" <<CAT
connector.name=arrow_flight
arrow-flight.host=flight1
arrow-flight.port=32010
arrow-flight.bearer-token=$TOKEN
CAT
# Same servers, but a list of coordinators: used by the coordinator-down check (3 servers).
cat > "$WORK/etc/catalog/flighth.properties" <<CAT
connector.name=arrow_flight
arrow-flight.hosts=flight1,flight2:32010,flight3
arrow-flight.port=32010
arrow-flight.bearer-token=$TOKEN
CAT
# start_trino <xmx> [probe]: (re)starts the Trino container; 'probe' (heap-probe.sh) drops ExitOnOutOfMemoryError so a
# heap error fails the query instead of ending the JVM.
start_trino() {
  docker rm -f trino >/dev/null 2>&1 || true
  {
    printf '%s\n' -server "-Xmx$1" -XX:+UseG1GC
    [ "${2:-}" = probe ] || echo -XX:+ExitOnOutOfMemoryError
    printf '%s\n' -Djdk.attach.allowAttachSelf=true --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true
  } > "$WORK/etc/jvm.config"
  docker run -d --name trino --network $NET -p 127.0.0.1:8080:8080 \
    -v "$WORK/plugin":/usr/lib/trino/plugin/arrow-flight:ro \
    -v "$WORK/etc/catalog/flight.properties":/etc/trino/catalog/flight.properties:ro \
    -v "$WORK/etc/catalog/flighth.properties":/etc/trino/catalog/flighth.properties:ro \
    -v "$WORK/etc/jvm.config":/etc/trino/jvm.config:ro \
    -v "$REPO/e2e/ci/queries":/queries:ro -v "$REPO/e2e/ci/profile-queries":/profile-queries:ro \
    "trinodb/trino:$TRINO_VERSION" >/dev/null
  for i in $(seq 1 90); do
    docker exec trino trino --execute 'SELECT 1' >/dev/null 2>&1 && break
    docker ps -q -f name=trino -f status=running | grep -q . || break
    sleep 2
  done
}
start_trino 3G
docker exec trino trino --execute 'SHOW SCHEMAS FROM flight' | tee "$OUT/schemas.txt"

run_query() {  # query-name out-dir
  local start; start=$(date +%s.%N)
  if docker exec trino trino --catalog "${QUERY_CATALOG:-flight}" --schema "${QUERY_SCHEMA:-tpch}" --output-format TSV -f "/queries/$1.sql" \
       > "$2/$1.tsv" 2> "$2/$1.err"; then
    rm -f "$2/$1.err"
  else
    rm -f "$2/$1.tsv"
  fi
  printf '%s\t%s\t%.2f\n' "$(basename "$2")" "$1" "$(echo "$(date +%s.%N) - $start" | bc)" >> "$OUT/times.tsv"
}
save_logs() { for c in trino ${HOSTS//,/ }; do docker logs "$c" > "$OUT/$c.log" 2>&1 || true; done; }
COMPARE=(python3 "$REPO/e2e/ci/compare.py" "$REPO/e2e/ci/reference/sf1")
QUERIES="$REPO/e2e/ci/queries"

# HEAP_PROBE=only: just the heap probe (heap-probe.sh), then stop; HEAP_PROBE=1: the probe after the TPC-H suite
# (otherwise a heap check with HEAP_CHECK_XMX, default 1G, runs there).
HEAP_PROBE="${HEAP_PROBE:-0}"
status=0
if [ "$HEAP_PROBE" = only ]; then
  heap_probe
  save_logs
  exit $status
fi
# PROFILE=only: just the throughput profile (profile.sh), then stop; PROFILE=bench: only its read benchmark.
if [ "${PROFILE:-0}" = only ] || [ "${PROFILE:-0}" = bench ]; then
  if [ "$PROFILE" = bench ]; then read_bench; else profile_run; fi
  save_logs
  exit $status
fi

log "TPC-H SF1 ($SERVERS server(s))"
mkdir -p "$OUT/sf1"
for q in "$QUERIES"/q*.sql; do run_query "$(basename "$q" .sql)" "$OUT/sf1"; done
save_logs
"${COMPARE[@]}" "$OUT/sf1" "$QUERIES" "$OUT/sf1-summary.tsv" || status=1
# Heap check (every run): q18 with a small Trino heap must stay exact; on 3 servers every table has 3 endpoints, whose
# grouped aggregation the plugin merges (STATUS stand finding 2026-10-26). HEAP_PROBE=1: the full probe instead.
if [ "$HEAP_PROBE" = 1 ]; then
  heap_probe
else
  HEAP_PROBE_XMX="${HEAP_CHECK_XMX:-1G}" HEAP_PROBE_SETTINGS=default HEAP_PROBE_FAIL=1 heap_probe
fi
# The same with one server answering every table with HEAP_CHECK_ENDPOINTS endpoints (server option
# arrowflight.cluster.endpointsPerServer; the stand failed q18 with 2/4/8). 'hash' only reports: its "merged in memory"
# INFO line shows the endpoint count. flight1 (and Trino, whose pooled connections it held) are restarted plainly after.
HEAP_CHECK_ENDPOINTS="${HEAP_CHECK_ENDPOINTS:-4}"
if [ "$SERVERS" = 1 ] && [ "$HEAP_CHECK_ENDPOINTS" -gt 1 ]; then
  docker logs flight1 > "$OUT/flight1-run0.log" 2>&1 || true
  docker rm -f flight1 >/dev/null; SERVER_ENDPOINTS=$HEAP_CHECK_ENDPOINTS start_server flight1; wait_server flight1
  HEAP_PROBE_TAG="eps$HEAP_CHECK_ENDPOINTS" HEAP_PROBE_XMX="${HEAP_CHECK_XMX:-1G}" HEAP_PROBE_SETTINGS="default hash" \
    HEAP_PROBE_FAIL=1 heap_probe
  docker logs flight1 > "$OUT/flight1-eps$HEAP_CHECK_ENDPOINTS.log" 2>&1 || true
  docker rm -f flight1 >/dev/null; start_server flight1; wait_server flight1
  start_trino 3G
fi

# A projection the plugin cannot push (ROW constructor over every column) must plan and run: Trino hands
# applyProjection the columns in no stable order, which looped the optimizer until it hit its time limit
# (run 39889582040). The planning cap turns a regression into a quick failure.
log "projection: checksum(ROW(all lineitem columns))"
mkdir -p "$OUT/projection"
ROW_SQL='SELECT count(*), checksum(ROW(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)) FROM lineitem'
start=$(date +%s.%N)
if docker exec trino trino --catalog flight --schema tpch --session iterative_optimizer_timeout=60s --output-format TSV \
     --execute "$ROW_SQL" > "$OUT/projection/row.tsv" 2> "$OUT/projection/row.err" \
   && [ "$(cut -f1 "$OUT/projection/row.tsv")" = 6001215 ]; then
  result=OK
else
  result=FAIL; status=1; head -c 2000 "$OUT/projection/row.err"
fi
printf 'projection\trow\t%.2f\n' "$(echo "$(date +%s.%N) - $start" | bc)" >> "$OUT/times.tsv"
{ printf 'query\tresult\tdetail\n'
  printf 'checksum_row\t%s\t%s\n' "$result" "$(tr '\t' ' ' < "$OUT/projection/row.tsv" 2>/dev/null)"; } | tee "$OUT/projection-summary.tsv"

# Query statistics from Trino by client source (rows the plugin got from the server = physicalInputPositions).
df_stats() {  # source -> "<physicalInputPositions> <elapsed>"
  local id
  id=$(curl -fsS -H 'X-Trino-User: e2e' http://127.0.0.1:8080/v1/query \
    | jq -r --arg s "$1" '[.[] | select(.session.source == $s)] | sort_by(.queryStats.createTime) | last | .queryId // empty')
  [ -n "$id" ] || { echo "? ?"; return; }
  curl -fsS -H 'X-Trino-User: e2e' "http://127.0.0.1:8080/v1/query/$id" \
    | jq -r '"\(.queryStats.physicalInputPositions) \(.queryStats.elapsedTime)"' 2>/dev/null || echo "? ?"
}
df_ratio() { awk -v a="$1" -v b="$2" 'BEGIN { if (b + 0 > 0) printf "%.3f", a / b; else print "?" }'; }

# TopN pushdown (off by default): with flight.topn_pushdown_enabled every answer must equal the one without
# it (sort keys are unique, so the order is exact), and EXPLAIN must show the TopN in the table scan; q4-q6 sort
# by VARCHAR (comments with spaces/punctuation, mixed case: the server's byte order must equal Trino's). Rows read
# from the server with it off and on go to topn-rows.tsv (INFO: the measurement for STATUS item 3).
log "TopN pushdown: on vs off"
mkdir -p "$OUT/topn"
TOPN_QUERIES=(
  'SELECT l_orderkey, l_linenumber, l_extendedprice FROM lineitem ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber LIMIT 10'
  "SELECT o_orderkey, o_orderdate, o_totalprice FROM orders WHERE o_orderstatus = 'F' ORDER BY o_orderdate, o_orderkey LIMIT 20"
  'SELECT c_custkey, c_acctbal FROM customer ORDER BY c_acctbal DESC NULLS FIRST, c_custkey LIMIT 5'
  'SELECT l_shipdate, l_orderkey, l_linenumber FROM lineitem ORDER BY l_shipdate DESC, l_orderkey DESC, l_linenumber LIMIT 1000'
  'SELECT o_comment, o_orderkey FROM orders ORDER BY o_comment, o_orderkey LIMIT 50'
  'SELECT s_comment, s_suppkey FROM supplier ORDER BY s_comment DESC, s_suppkey LIMIT 30'
  'SELECT c_name, c_mktsegment, c_custkey FROM customer ORDER BY c_mktsegment DESC, c_name LIMIT 25'
)
topn_run() {  # on|off index sql
  local t0; t0=$(date +%s.%N)
  docker exec trino trino --catalog flight --schema tpch --source "e2e-topn-$2-$1" --session "flight.topn_pushdown_enabled=$([ "$1" = on ] && echo true || echo false)" \
    --output-format TSV --execute "$3" > "$OUT/topn/$2-$1.tsv" 2> "$OUT/topn/$2-$1.err" || rm -f "$OUT/topn/$2-$1.tsv"
  printf 'topn-%s\tq%s\t%.2f\n' "$1" "$2" "$(echo "$(date +%s.%N) - $t0" | bc)" >> "$OUT/times.tsv"
}
printf 'query\tresult\tdetail\n' > "$OUT/topn-summary.tsv"
printf 'query\trows_off\trows_on\tratio_on\telapsed_off\telapsed_on\n' > "$OUT/topn-rows.tsv"
for i in "${!TOPN_QUERIES[@]}"; do
  sql=${TOPN_QUERIES[$i]}
  topn_run off "$i" "$sql"
  topn_run on "$i" "$sql"
  docker exec trino trino --catalog flight --schema tpch --session flight.topn_pushdown_enabled=true \
    --execute "EXPLAIN $sql" > "$OUT/topn/$i-explain.txt" 2>&1 || true
  pushed=$(grep -c 'topN=' "$OUT/topn/$i-explain.txt" || true)
  if [ ! -f "$OUT/topn/$i-off.tsv" ] || [ ! -f "$OUT/topn/$i-on.tsv" ]; then
    result=FAIL; detail="error: $(cat "$OUT/topn/$i-on.err" "$OUT/topn/$i-off.err" 2>/dev/null | grep -m1 -E 'failed|ERROR' | cut -c1-300 || true)"
  elif ! cmp -s "$OUT/topn/$i-off.tsv" "$OUT/topn/$i-on.tsv"; then
    result=FAIL; detail="answers differ ($(wc -l < "$OUT/topn/$i-off.tsv") vs $(wc -l < "$OUT/topn/$i-on.tsv") rows)"
  elif [ "$pushed" = 0 ]; then
    result=FAIL; detail="equal, but EXPLAIN shows no pushed TopN"
  else
    result=OK; detail="$(wc -l < "$OUT/topn/$i-on.tsv") rows equal, TopN in the scan"
  fi
  [ "$result" = OK ] || status=1
  printf 'q%s\t%s\t%s\n' "$i" "$result" "$detail" | tee -a "$OUT/topn-summary.tsv"
  read -r off_rows off_t <<< "$(df_stats "e2e-topn-$i-off")"
  read -r on_rows on_t <<< "$(df_stats "e2e-topn-$i-on")"
  printf 'q%s\t%s\t%s\t%s\t%s\t%s\n' "$i" "$off_rows" "$on_rows" "$(df_ratio "$on_rows" "$off_rows")" "$off_t" "$on_t" \
    | tee -a "$OUT/topn-rows.tsv"
done
grep -m1 'topN=' "$OUT/topn/0-explain.txt" | cut -c1-300 || true

# Grouped aggregation merge: with several endpoints the plugin merges their partial groups. Sorted merge (default:
# the server orders the groups, the plugin merges streams) and the old hash merge must give the same answers;
# times per setting go to aggmerge-times.tsv (INFO: STATUS "q04 slow on 3 nodes"). EXPLAIN must show the
# aggregation in the scan, otherwise the check proves nothing.
log "aggregation merge: sorted vs hash"
mkdir -p "$OUT/aggmerge"
AGGMERGE_QUERIES=(q04 q18 groups)
AGGMERGE_GROUPS_SQL='SELECT count(*), sum(c), checksum(s) FROM (SELECT l_orderkey, count(*) AS c, sum(l_quantity) AS s FROM lineitem GROUP BY l_orderkey)'
aggmerge_run() {  # sorted|hash name
  local t0 args; t0=$(date +%s.%N)
  if [ "$2" = groups ]; then args=(--execute "$AGGMERGE_GROUPS_SQL"); else args=(-f "/queries/$2.sql"); fi
  docker exec trino trino --catalog flight --schema tpch --source "e2e-aggmerge-$2-$1" \
    --session "flight.aggregation_sorted_merge_enabled=$([ "$1" = sorted ] && echo true || echo false)" \
    --output-format TSV "${args[@]}" > "$OUT/aggmerge/$2-$1.tsv" 2> "$OUT/aggmerge/$2-$1.err" || rm -f "$OUT/aggmerge/$2-$1.tsv"
  printf '%.2f' "$(echo "$(date +%s.%N) - $t0" | bc)"
}
printf 'query\tresult\tdetail\n' > "$OUT/aggmerge-summary.tsv"
printf 'query\thash_s\tsorted_s\thash2_s\tsorted2_s\n' > "$OUT/aggmerge-times.tsv"
docker exec trino trino --catalog flight --schema tpch --execute "EXPLAIN $AGGMERGE_GROUPS_SQL" > "$OUT/aggmerge/groups-explain.txt" 2>&1 || true
for name in "${AGGMERGE_QUERIES[@]}"; do
  h1=$(aggmerge_run hash "$name"); s1=$(aggmerge_run sorted "$name")
  h2=$(aggmerge_run hash "$name"); s2=$(aggmerge_run sorted "$name")
  if [ ! -f "$OUT/aggmerge/$name-hash.tsv" ] || [ ! -f "$OUT/aggmerge/$name-sorted.tsv" ]; then
    result=FAIL; detail="error: $(cat "$OUT/aggmerge/$name-sorted.err" "$OUT/aggmerge/$name-hash.err" 2>/dev/null | grep -m1 -E 'failed|ERROR' | cut -c1-300 || true)"
  elif ! cmp -s "$OUT/aggmerge/$name-hash.tsv" "$OUT/aggmerge/$name-sorted.tsv"; then
    result=FAIL; detail="answers differ"
  elif [ "$name" = groups ] && ! grep -q 'groupBy=' "$OUT/aggmerge/groups-explain.txt"; then
    result=FAIL; detail="equal, but EXPLAIN shows no pushed aggregation"
  else
    result=OK; detail="$(wc -l < "$OUT/aggmerge/$name-sorted.tsv") rows equal: $(head -c 120 "$OUT/aggmerge/$name-sorted.tsv" | tr '\t\n' '  ')"
  fi
  [ "$result" = OK ] || status=1
  printf '%s\t%s\t%s\n' "$name" "$result" "$detail" | tee -a "$OUT/aggmerge-summary.tsv"
  printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$h1" "$s1" "$h2" "$s2" | tee -a "$OUT/aggmerge-times.tsv"
done
grep -m1 'groupBy=' "$OUT/aggmerge/groups-explain.txt" | cut -c1-300 || true

# Dynamic filters (off by default): TPC-H joins with flight.dynamic_filtering_enabled must still equal the DuckDB
# reference, with the default IN-list cap (on: 1000 values, more become the [min, max] span) and a large one
# (big: DF_BIG_VALUES, plus Trino's enable_large_dynamic_filters so Trino itself keeps up to ~100k distinct keys). Rows the scans read (Trino physicalInputPositions = rows the plugin got from the server) and
# the elapsed time per setting go to dynfilter-rows.tsv (INFO: the measurement for STATUS item 4).
DF_QUERIES="${DF_QUERIES:-q02 q03 q05 q08 q09 q10 q17 q18 q20 q21}"
DF_BIG_VALUES="${DF_BIG_VALUES:-100000}"
log "dynamic filters: $DF_QUERIES off / on / big ($DF_BIG_VALUES values)"
mkdir -p "$OUT/dynfilter/on" "$OUT/dynfilter/off" "$OUT/dynfilter/big"
# enable_large_dynamic_filters is gone in later Trino releases (large filters are the default there);
# an unknown session property would fail every 'big' query.
LARGE_DF=()
large_df_row=$(docker exec trino trino --output-format TSV \
  --execute "SHOW SESSION LIKE 'enable_large_dynamic_filters'" 2>/dev/null || true)
[ -n "$large_df_row" ] && LARGE_DF=(--session enable_large_dynamic_filters=true)
log "dynfilter: large dynamic filters session flag: ${LARGE_DF[*]:-not available}"
printf 'query\trows_off\trows_on\trows_big\tratio_on\tratio_big\telapsed_off\telapsed_on\telapsed_big\n' > "$OUT/dynfilter-rows.tsv"
for q in $DF_QUERIES; do
  for mode in off on big; do
    case $mode in
      off) sess=(--session flight.dynamic_filtering_enabled=false) ;;
      on) sess=(--session flight.dynamic_filtering_enabled=true) ;;
      big) sess=(--session flight.dynamic_filtering_enabled=true --session "flight.dynamic_filtering_max_values=$DF_BIG_VALUES"
                 "${LARGE_DF[@]}") ;;
    esac
    t0=$(date +%s.%N)
    docker exec trino trino --catalog flight --schema tpch --source "e2e-df-$q-$mode" "${sess[@]}" \
      --output-format TSV -f "/queries/$q.sql" > "$OUT/dynfilter/$mode/$q.tsv" 2> "$OUT/dynfilter/$mode/$q.err" \
      || rm -f "$OUT/dynfilter/$mode/$q.tsv"
    printf 'dynfilter-%s\t%s\t%.2f\n' "$mode" "$q" "$(echo "$(date +%s.%N) - $t0" | bc)" >> "$OUT/times.tsv"
  done
  read -r off_rows off_t <<< "$(df_stats "e2e-df-$q-off")"
  read -r on_rows on_t <<< "$(df_stats "e2e-df-$q-on")"
  read -r big_rows big_t <<< "$(df_stats "e2e-df-$q-big")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$q" "$off_rows" "$on_rows" "$big_rows" \
    "$(df_ratio "$on_rows" "$off_rows")" "$(df_ratio "$big_rows" "$off_rows")" "$off_t" "$on_t" "$big_t" \
    | tee -a "$OUT/dynfilter-rows.tsv"
done
"${COMPARE[@]}" "$OUT/dynfilter/on" "$QUERIES" "$OUT/dynfilter-summary.tsv" "$DF_QUERIES" || status=1
"${COMPARE[@]}" "$OUT/dynfilter/big" "$QUERIES" "$OUT/dynfilter-big-summary.tsv" "$DF_QUERIES" || status=1

# Splits and endpoint reads (STATUS item 5): range splits per scan (flight.split_count 1 or 4) x endpoints of a split
# read one after another or PAR_PARALLELISM at a time (flight.endpoint_read_parallelism; with one server a split has one
# endpoint, so only split_count changes anything). Every answer must equal the reference (the full-scan checksum: equal in
# every setting); elapsed times (best of PAR_REPEAT) go to splits-times.tsv (INFO: the measurement).
PAR_QUERIES="${PAR_QUERIES:-q03 q12 q14 q18 q19}"
PAR_PARALLELISM="${PAR_PARALLELISM:-3}"
PAR_REPEAT="${PAR_REPEAT:-2}"
# Setting "ep1": flight.endpoint_splits_enabled (one split per endpoint of one coordinator-planned statement; 3 servers =
# 3 splits; 1 server = one endpoint, so a large scan falls back to split_count=4 key ranges).
PAR_SETTINGS="s1p1 s1p$PAR_PARALLELISM s4p1 s4p$PAR_PARALLELISM ep1"
SCAN_SQL='SELECT count(*), checksum(ROW(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)) FROM lineitem'
log "splits: $PAR_SETTINGS on $PAR_QUERIES + full lineitem scan"
par_run() {  # setting name out-file (-f file | --execute sql)
  local setting=$1 name=$2 outf=$3; shift 3
  local splits=${setting#s}; splits=${splits%p*} endpoint=false
  local par=${setting#*p} best="" i t0 t
  if [ "${setting%%p*}" = e ]; then splits=4; endpoint=true; fi
  for i in $(seq 1 "$PAR_REPEAT"); do
    t0=$(date +%s.%N)
    docker exec trino trino --catalog flight --schema tpch --session "flight.split_count=$splits" \
      --session "flight.endpoint_read_parallelism=$par" --session "flight.endpoint_splits_enabled=$endpoint" --output-format TSV "$@" > "$outf" 2> "${outf%.tsv}.err" \
      || { rm -f "$outf"; break; }
    t=$(echo "$(date +%s.%N) - $t0" | bc)
    best=$(awk -v a="$best" -v b="$t" 'BEGIN { if (a == "" || b + 0 < a + 0) printf "%.2f", b; else print a }')
  done
  printf 'splits-%s\t%s\t%s\n' "$setting" "$name" "${best:-?}" >> "$OUT/times.tsv"
  echo "${best:-error}"
}
printf 'query\t%s\n' "$(echo "$PAR_SETTINGS" | tr ' ' '\t')" > "$OUT/splits-times.tsv"
for setting in $PAR_SETTINGS; do mkdir -p "$OUT/splits/$setting"; done
for q in $PAR_QUERIES scan; do
  row="$q"
  for setting in $PAR_SETTINGS; do
    if [ "$q" = scan ]; then
      row+="\t$(par_run "$setting" scan "$OUT/splits/$setting/scan.tsv" --execute "$SCAN_SQL")"
    else
      row+="\t$(par_run "$setting" "$q" "$OUT/splits/$setting/$q.tsv" -f "/queries/$q.sql")"
    fi
  done
  printf "$row\n" | tee -a "$OUT/splits-times.tsv"
done
printf 'setting\tresult\tdetail\n' > "$OUT/splits-summary.tsv"
base="$OUT/splits/s1p1/scan.tsv"
for setting in $PAR_SETTINGS; do
  "${COMPARE[@]}" "$OUT/splits/$setting" "$QUERIES" "$OUT/splits/$setting/compare.tsv" "$PAR_QUERIES" || status=1
  if [ -f "$base" ] && [ -f "$OUT/splits/$setting/scan.tsv" ] && cmp -s "$base" "$OUT/splits/$setting/scan.tsv" \
     && [ "$(cut -f1 "$base")" = 6001215 ]; then
    result=OK; detail="scan $(tr '\t' ' ' < "$base")"
  else
    result=FAIL; status=1; detail="scan differs or failed: $(cat "$OUT/splits/$setting/scan.tsv" "$OUT/splits/$setting/scan.err" 2>/dev/null | head -c 300)"
  fi
  printf '%s\t%s\t%s; %s\n' "$setting" "$result" "$detail" \
    "$(awk -F'\t' 'NR > 1 { n++; if ($2 == "OK") ok++ } END { printf "%d/%d queries = reference", ok, n }' "$OUT/splits/$setting/compare.tsv")" \
    | tee -a "$OUT/splits-summary.tsv"
done

# Concurrent scans (stand 2026-10-26: 8 concurrent partial-mode scans on 3 nodes gave CANCELLED streams and a dead node;
# server 57d252b1437f shares one outbound byte budget across streams). CONC_QUERIES run at the same time ('scan' = the full
# lineitem checksum, the most bytes per query); every answer must be exact (the scan = the projection check's answer) and
# every server must still run afterwards. Wall time of the whole batch to times.tsv.
CONC_QUERIES="${CONC_QUERIES:-scan scan scan scan q01 q06 q12 q14}"
log "concurrent: $CONC_QUERIES"
mkdir -p "$OUT/concurrent"
start=$(date +%s.%N); i=0; pids=()
for q in $CONC_QUERIES; do
  i=$((i + 1))
  if [ "$q" = scan ]; then
    ( docker exec trino trino --catalog flight --schema tpch --output-format TSV --execute "$SCAN_SQL" \
        > "$OUT/concurrent/scan$i.tsv" 2> "$OUT/concurrent/scan$i.err" || rm -f "$OUT/concurrent/scan$i.tsv" ) &
  else
    ( docker exec trino trino --catalog flight --schema tpch --output-format TSV -f "/queries/$q.sql" \
        > "$OUT/concurrent/$q.tsv" 2> "$OUT/concurrent/$q.err" || rm -f "$OUT/concurrent/$q.tsv" ) &
  fi
  pids+=($!)
done
for p in "${pids[@]}"; do wait "$p" || true; done
printf 'concurrent\tall\t%.2f\n' "$(echo "$(date +%s.%N) - $start" | bc)" >> "$OUT/times.tsv"
printf 'query\tresult\tdetail\n' > "$OUT/concurrent-summary.tsv"
i=0; conc_tpch=()
for q in $CONC_QUERIES; do
  i=$((i + 1))
  if [ "$q" != scan ]; then conc_tpch+=("$q"); continue; fi
  if [ -s "$OUT/concurrent/scan$i.tsv" ] && cmp -s "$OUT/projection/row.tsv" "$OUT/concurrent/scan$i.tsv"; then
    printf 'scan%s\tOK\t%s\n' "$i" "$(tr '\t' ' ' < "$OUT/concurrent/scan$i.tsv")"
  else
    status=1
    printf 'scan%s\tFAIL\t%s\n' "$i" "$(cat "$OUT/concurrent/scan$i.tsv" "$OUT/concurrent/scan$i.err" 2>/dev/null | tr '\t\n' '  ' | head -c 300)"
  fi >> "$OUT/concurrent-summary.tsv"
done
conc_ok=()
for q in "${conc_tpch[@]}"; do
  if [ -f "$OUT/concurrent/$q.tsv" ]; then conc_ok+=("$q"); continue; fi
  status=1; printf '%s\tFAIL\terror: %s\n' "$q" "$(grep -m1 -E 'Query .* failed|ERROR' "$OUT/concurrent/$q.err" | cut -c1-200)" \
    >> "$OUT/concurrent-summary.tsv"
done
if [ ${#conc_ok[@]} -gt 0 ]; then
  "${COMPARE[@]}" "$OUT/concurrent" "$QUERIES" "$OUT/concurrent/compare.tsv" "${conc_ok[*]}" || status=1
  tail -n +2 "$OUT/concurrent/compare.tsv" >> "$OUT/concurrent-summary.tsv"
fi
for c in ${HOSTS//,/ }; do
  if docker ps -q -f name="^$c\$" -f status=running | grep -q .; then
    printf '%s\tOK\tstill running\n' "$c"
  else
    status=1; printf '%s\tFAIL\tnot running after the concurrent queries\n' "$c"
  fi >> "$OUT/concurrent-summary.tsv"
done
cat "$OUT/concurrent-summary.tsv"
save_logs

if [ -n "$TYPES" ]; then
  log "types: SELECT * per table vs DuckDB on the same Parquet"
  python3 -m venv "$WORK/venv"
  "$WORK/venv/bin/pip" install -q --disable-pip-version-check duckdb==1.4.1 trino==0.333.0
  "$WORK/venv/bin/python" "$REPO/e2e/ci/types_check.py" "$WORK/s3/lake/parquet/types" 127.0.0.1:8080 \
    "$OUT/types-summary.tsv" || status=1
  save_logs
fi

if [ "$ICEBERG" = 1 ]; then
  iceberg_check
  save_logs
fi

# Trino log lines written since mark_log (plugin WARNs about failed streams name the location).
mark_log() { LOG_MARK=$(docker logs trino 2>&1 | wc -l); }
log_since_mark() { docker logs trino 2>&1 | tail -n +$((LOG_MARK + 1)); }
FAILOVER_MARKER='Flight stream at .* failed before any data'
# exact answers or clean errors for the given queries in dir $1 -> summary $2
check_exact_or_error() {  # dir summary queries...
  local dir=$1 summary=$2; shift 2
  local ok=() q
  for q in "$@"; do [ -f "$dir/$q.tsv" ] && ok+=("$q"); done
  printf 'query\tresult\tdetail\n' > "$summary"
  if [ ${#ok[@]} -gt 0 ]; then
    "${COMPARE[@]}" "$dir" "$QUERIES" "$dir/compare.tsv" "${ok[*]}" || status=1
    tail -n +2 "$dir/compare.tsv" >> "$summary"
  fi
  for q in "$@"; do
    [ -f "$dir/$q.tsv" ] && continue
    printf '%s\tOK\tclean error: %s\n' "$q" \
      "$(grep -m1 -E 'Query .* failed|ERROR' "$dir/$q.err" | cut -c1-200)" >> "$summary"
  done
  cat "$summary"
}

if [ "$SERVERS" -ge 3 ]; then
  # Node lost before a query: the endpoints it served must be read from their backup locations.
  # Round 'partitioned': flight2 cut off the network right before the queries; round 'killed':
  # flight2 killed, KILL_SETTLE_SECONDS later. Both must be exact. The int4 server checks its
  # members when planning and does not hand out a lost node (observed: 0 streams at flight2 in
  # both rounds), so the count of plugin failovers here is informational; the plugin's own
  # failover is proven by the kill during a read below.
  log "failover: flight2 lost before the queries"
  docker network disconnect $NET flight2
  n=0
  for round in partitioned killed; do
    if [ "$round" = killed ]; then docker kill flight2 >/dev/null; sleep "${KILL_SETTLE_SECONDS:-10}"; fi
    dir="$OUT/killed-before/$round"; mkdir -p "$dir"
    mark_log
    for q in $FAILOVER_QUERIES; do run_query "$q" "$dir"; done
    "${COMPARE[@]}" "$dir" "$QUERIES" "$dir/summary.tsv" "$FAILOVER_QUERIES" || status=1
    log_since_mark | grep -E 'flight2' > "$dir/flight2-lines.txt" || true
    m=$(grep -cE "$FAILOVER_MARKER" "$dir/flight2-lines.txt" || true)
    echo "killed-before/$round: $m stream(s) of flight2 re-read from a backup location;" \
      "$(wc -l < "$dir/flight2-lines.txt") Trino log line(s) name flight2:"
    head -5 "$dir/flight2-lines.txt" | cut -c1-400
    n=$((n + m))
  done
  evidence=INFO
  { tail -n +2 "$OUT/killed-before/partitioned/summary.tsv" | sed 's/^/partitioned:/'
    tail -n +2 "$OUT/killed-before/killed/summary.tsv" | sed 's/^/killed:/'
    printf 'backup-reads\t%s\t%s stream(s) at flight2 re-read from a backup location\n' "$evidence" "$n"
  } > "$OUT/killed-before-summary.tsv"
  [ "$evidence" != FAIL ] || status=1

  # Node lost while a query reads from it: a clean error or the exact answer, never wrong rows.
  # The kill must hit a read of flight3: evidence is a failed query or the plugin's "failed before
  # any data; reading it from <backup>" WARN for flight3 (the endpoints were handed out before the
  # kill); otherwise flight3 is restarted and the kill retried with the next delay. In partial
  # mode a miss on every delay fails the job (schema tpch; for the Iceberg schema a miss is INFO:
  # whether flight3 serves any of its data files depends on how Spark split them).
  # A server starts only once every --hosts member has joined, so flight3 cannot come back alone
  # while flight2 is down: both are restarted (old logs kept as <host>-runN.log).
  RESTARTS=0
  restart_lost() {
    local h
    RESTARTS=$((RESTARTS + 1))
    for h in flight2 flight3; do
      docker logs "$h" > "$OUT/$h-run$RESTARTS.log" 2>&1 || true
      docker rm -f "$h" >/dev/null 2>&1 || true
      start_server "$h"
    done
    for h in flight2 flight3; do wait_server "$h"; done
    sleep 5
  }
  kill_during() {  # schema out-name; flight3 must be running
    local schema=$1 name=$2 attempt=0 hit=no delay dir result evidence
    log "failover: flight3 killed during a read (schema $schema)"
    printf 'attempt\tdelay_s\tq18\tflight3_hit\n' > "$OUT/$name-attempts.tsv"
    for delay in ${KILL_DURING_DELAYS:-2 1 4 6}; do
      attempt=$((attempt + 1))
      dir="$OUT/$name/$attempt"; mkdir -p "$dir"
      [ "$attempt" -gt 1 ] && restart_lost
      mark_log
      QUERY_SCHEMA=$schema run_query q18 "$dir" &
      sleep "$delay"
      docker kill flight3 >/dev/null
      wait
      check_exact_or_error "$dir" "$dir/summary.tsv" q18
      result=$([ -f "$dir/q18.tsv" ] && echo answer || echo error)
      log_since_mark | grep -E 'flight3' > "$dir/flight3-lines.txt" || true
      if [ "$result" = error ] || grep -qE "$FAILOVER_MARKER" "$dir/flight3-lines.txt"; then hit=yes; fi
      head -5 "$dir/flight3-lines.txt" | cut -c1-400
      printf '%s\t%s\t%s\t%s\n' "$attempt" "$delay" "$result" "$hit" | tee -a "$OUT/$name-attempts.tsv"
      [ "$hit" = yes ] && break
    done
    evidence=$([ "$hit" = yes ] && echo OK ||
      { [ "$RESULT_MODE" = partial ] && [ "$schema" = tpch ] && echo FAIL || echo INFO; })
    { printf 'query\tresult\tdetail\n'
      printf 'q18\t%s\t%s attempt(s) on schema %s, all exact or clean errors; kill hit a read of flight3: %s\n' \
        "$evidence" "$attempt" "$schema" "$hit"; } | tee "$OUT/$name-summary.tsv"
    [ "$evidence" != FAIL ] || status=1
  }
  kill_during tpch killed-during
  if [ "$ICEBERG" = 1 ]; then
    # Iceberg tables: endpoints carry catalog (snapshot-pinned) tickets
    restart_lost
    kill_during ice killed-during-ice
  fi

  # Node lost after its stream delivered data: the plugin must not retry (rows could be read twice),
  # so the query fails cleanly; an answer must equal the healthy baseline, never fewer rows. Evidence
  # of a hit is the plugin's "failed after delivering data" WARN for flight3; a miss on every delay
  # fails the job in partial mode (attempt 1 hit in every 3-server run since 39891124604, Trino 455/477/483)
  # and is INFO in complete mode (timing not established there). The query streams all of lineitem (checksum is not pushed down); its
  # streams open ~7-8 s after submission and one column took <1 s (runs 39886487789, 39888037652).
  # One checksum per column (written while checksum(ROW(...)) still looped the optimizer, run 39889582040;
  # the ROW form is now checked on its own above).
  STREAM_SQL='SELECT count(*), checksum(l_orderkey), checksum(l_partkey), checksum(l_suppkey), checksum(l_linenumber), checksum(l_quantity), checksum(l_extendedprice), checksum(l_discount), checksum(l_tax), checksum(l_returnflag), checksum(l_linestatus), checksum(l_shipdate), checksum(l_commitdate), checksum(l_receiptdate), checksum(l_shipinstruct), checksum(l_shipmode), checksum(l_comment) FROM lineitem'
  stream_query() {  # out-dir
    if docker exec trino trino --catalog flight --schema tpch --output-format TSV --execute "$STREAM_SQL" \
         > "$1/stream.tsv" 2> "$1/stream.err"; then rm -f "$1/stream.err"; else rm -f "$1/stream.tsv"; fi
  }
  flight3_tx() {  # bytes flight3 has sent on its non-loopback interfaces
    local pid
    pid=$(docker inspect -f '{{.State.Pid}}' flight3)
    { cat "/proc/$pid/net/dev" 2>/dev/null || sudo -n cat "/proc/$pid/net/dev"; } \
      | awk 'NR > 2 { sub(/^ +/, ""); split($0, f, /[: ]+/); if (f[1] != "lo") s += f[10] } END { print s + 0 }'
  }
  kill_after_tx() {  # bytes max-seconds: kill flight3 once it has sent that much since the call (or at the cap); prints when
    local start t0 now
    start=$(date +%s.%N); t0=$(flight3_tx)
    while :; do
      now=$(date +%s.%N)
      if [ "$(( $(flight3_tx) - t0 ))" -ge "$1" ] || [ "$(echo "$now - $start >= $2" | bc)" = 1 ]; then break; fi
      sleep 0.1
    done
    docker kill flight3 >/dev/null
    printf 'tx%dM@%.1f' "$(( $1 / 1048576 ))" "$(echo "$now - $start" | bc)"
  }
  kill_mid_stream() {
    local dir="$OUT/killed-mid-stream" attempt=0 hit=no wrong=0 delay delays base_s adir result evidence start
    log "failover: flight3 killed in the middle of a stream"
    restart_lost
    mkdir -p "$dir/base"
    start=$(date +%s.%N); stream_query "$dir/base"
    base_s=$(echo "$(date +%s.%N) - $start" | bc)
    printf 'killed-mid-stream\tbase\t%.2f\n' "$base_s" >> "$OUT/times.tsv"
    log "mid-stream baseline took ${base_s} s"
    if [ ! -f "$dir/base/stream.tsv" ]; then
      cat "$dir/base/stream.err"
      printf 'query\tresult\tdetail\nstream\tFAIL\tbaseline failed on a healthy cluster\n' | tee "$OUT/killed-mid-stream-summary.tsv"
      status=1; return
    fi
    cat "$dir/base/stream.tsv"
    printf 'attempt\tdelay_s\tresult\tflight3_hit\n' > "$OUT/killed-mid-stream-attempts.tsv"
    # Complete mode: a stream's first batch comes only after the server has the whole answer, so a kill
    # at 8-11 s found flight3's streams open but still empty (run 39920606099, 4/4 "before any data").
    # Kill there at 50/65/80/90 % of the healthy baseline instead.
    # Partial mode: streams deliver from the start, so kill well inside the query. Fixed 8-11 s delays missed 4/4
    # once the server (int7 re-upload 2026-10-24) answered the baseline in 7.5 s (run 40140961985); 30-75 % then
    # missed 4/4 with a 9.1 s baseline (run 40144727674: flight3's streams had not opened at 5.5 s): 60-90 %.
    if [ -n "${KILL_STREAM_DELAYS:-}" ]; then
      delays=$KILL_STREAM_DELAYS
    elif [ "$RESULT_MODE" = complete ]; then
      delays=$(awk -v b="$base_s" 'BEGIN { printf "%d %d %d %d", b*0.5, b*0.65, b*0.8, b*0.9 }')
    else
      delays=$(awk -v b="$base_s" 'BEGIN { printf "%.1f %.1f %.1f %.1f", b*0.8, b*0.7, b*0.9, b*0.6 }')
    fi
    # Timed kills keep missing when flight3's streams start late (60e888bc26ec, 4 DuckDB threads per server on the
    # 4-vCPU runner: 4/4 "before any data" at 60-90 % of the baseline, runs 40249518015, 40256341545). So attempt 1
    # kills flight3 once it has sent KILL_STREAM_TX_MB (8) MB since the query started -- its streams are delivering
    # then -- capped at 95 % of the baseline; the timed delays follow as further attempts.
    [ -n "${KILL_STREAM_DELAYS:-}" ] || delays="tx $delays"
    log "mid-stream kill delays: $delays"
    for delay in $delays; do
      attempt=$((attempt + 1))
      adir="$dir/$attempt"; mkdir -p "$adir"
      [ "$attempt" -gt 1 ] && restart_lost
      mark_log
      stream_query "$adir" &
      if [ "$delay" = tx ]; then
        delay=$(kill_after_tx $(( ${KILL_STREAM_TX_MB:-8} * 1048576 )) "$(echo "$base_s * 0.95" | bc)")
      else
        sleep "$delay"
        docker kill flight3 >/dev/null
      fi
      wait
      if [ -f "$adir/stream.tsv" ]; then
        if cmp -s "$dir/base/stream.tsv" "$adir/stream.tsv"; then result=exact; else result=WRONG; wrong=1; cat "$adir/stream.tsv"; fi
      else
        result="error: $(grep -m1 -E 'Query .* failed|ERROR' "$adir/stream.err" | cut -c1-200)"
      fi
      log_since_mark | grep -E 'flight3' > "$adir/flight3-lines.txt" || true
      grep -q 'failed after delivering data' "$adir/flight3-lines.txt" && hit=yes
      head -5 "$adir/flight3-lines.txt" | cut -c1-400
      printf '%s\t%s\t%s\t%s\n' "$attempt" "$delay" "$result" "$hit" | tee -a "$OUT/killed-mid-stream-attempts.tsv"
      [ "$wrong" = 1 ] && break
      [ "$hit" = yes ] && break
    done
    evidence=$([ "$wrong" = 1 ] && echo FAIL || { [ "$hit" = yes ] && echo OK || { [ "$RESULT_MODE" = partial ] && echo FAIL || echo INFO; }; })
    { printf 'query\tresult\tdetail\n'
      printf 'stream\t%s\t%s attempt(s), wrong answers: %s; kill hit a stream of flight3 after data: %s\n' \
        "$evidence" "$attempt" "$wrong" "$hit"; } | tee "$OUT/killed-mid-stream-summary.tsv"
    [ "$evidence" != FAIL ] || status=1
  }
  kill_mid_stream

  # Nodes restarted between two queries: the plugin's pooled connections to them are stale. The next
  # query must equal the healthy baseline; reads of flight3 should not fall back to a backup (a stale
  # pooled connection is retried once on a new one: "lost its reused connection before any data … retrying on a new one").
  # Pooled connections idle for 60 s are dropped anyway, so the restart is timed.
  restarted_nodes() {
    local dir="$OUT/restarted" result evidence start restart_s backups retries
    log "failover: flight2 and flight3 restarted between two queries"
    mkdir -p "$dir/warm" "$dir/after"
    restart_lost
    stream_query "$dir/warm"
    start=$(date +%s.%N); restart_lost; restart_s=$(echo "$(date +%s.%N) - $start" | bc)
    mark_log
    stream_query "$dir/after"
    if [ -f "$dir/after/stream.tsv" ]; then
      cmp -s "$OUT/killed-mid-stream/base/stream.tsv" "$dir/after/stream.tsv" && result=exact || result=WRONG
    else
      result="error: $(grep -m1 -E 'Query .* failed|ERROR' "$dir/after/stream.err" | cut -c1-200)"
    fi
    log_since_mark | grep -E 'flight[23]' > "$dir/lines.txt" || true
    backups=$(grep -cE "$FAILOVER_MARKER" "$dir/lines.txt" || true)
    retries=$(grep -c 'lost its reused connection before any data' "$dir/lines.txt" || true)
    head -5 "$dir/lines.txt" | cut -c1-400
    evidence=$(case "$result" in exact) echo OK ;; WRONG) echo FAIL ;; *) echo INFO ;; esac)
    { printf 'query\tresult\tdetail\n'
      printf 'stream\t%s\t%s after a %.1f s restart; stale-connection retries %s, reads from a backup %s\n' \
        "$evidence" "$result" "$restart_s" "$retries" "$backups"; } | tee "$OUT/restarted-summary.tsv"
    [ "$evidence" != FAIL ] || status=1
  }
  [ -f "$OUT/killed-mid-stream/base/stream.tsv" ] && restarted_nodes

  # Two of three nodes lost (flight2 and flight3): every answer exact, or a clean error.
  log "failover: flight2 and flight3 down"
  docker kill flight2 flight3 >/dev/null 2>&1 || true  # back up if kill_during restarted them
  mkdir -p "$OUT/two-killed"
  for q in $FAILOVER_QUERIES; do run_query "$q" "$OUT/two-killed"; done
  check_exact_or_error "$OUT/two-killed" "$OUT/two-killed-summary.tsv" $FAILOVER_QUERIES
  if [ "$ICEBERG" = 1 ]; then
    mkdir -p "$OUT/two-killed-ice"
    for q in $ICEBERG_FAILOVER_QUERIES; do QUERY_SCHEMA=ice run_query "$q" "$OUT/two-killed-ice"; done
    check_exact_or_error "$OUT/two-killed-ice" "$OUT/two-killed-ice-summary.tsv" $ICEBERG_FAILOVER_QUERIES
  fi

  # The catalog's first coordinator lost: catalog flighth (arrow-flight.hosts=flight1,flight2,flight3)
  # connected to flight1 by a first query, then flight1 is killed. Every answer must be exact (a
  # clean error fails too: flight2 and flight3 are up); the Trino log must show the switch.
  log "failover: coordinator flight1 down, catalog with a host list"
  restart_lost
  mkdir -p "$OUT/coordinator-down/warm" "$OUT/coordinator-down/after"
  QUERY_CATALOG=flighth run_query q06 "$OUT/coordinator-down/warm"
  check_exact_or_error "$OUT/coordinator-down/warm" "$OUT/coordinator-down-warm-summary.tsv" q06
  mark_log
  docker kill flight1 >/dev/null
  sleep "${KILL_SETTLE_SECONDS:-10}"
  # The first query after the kill pays for noticing the dead coordinator: its connection pings every
  # 10 s and gives up after the 5 s connect timeout (was ~31 s without keepalive, session 42).
  first_start=$(date +%s.%N)
  first_done=
  for q in $FAILOVER_QUERIES; do
    QUERY_CATALOG=flighth run_query "$q" "$OUT/coordinator-down/after"
    [ -n "$first_done" ] || first_done=$(echo "$(date +%s.%N) - $first_start" | bc)
  done
  check_exact_or_error "$OUT/coordinator-down/after" "$OUT/coordinator-down-summary.tsv" $FAILOVER_QUERIES
  printf 'coordinator-down: first query after the kill took %.1f s\n' "$first_done" | tee "$OUT/coordinator-down/first.txt"
  if [ "$(echo "$first_done > ${COORDINATOR_SWITCH_MAX_SECONDS:-25}" | bc)" = 1 ]; then
    echo "coordinator-down: FAIL first query took more than ${COORDINATOR_SWITCH_MAX_SECONDS:-25} s"; status=1
  fi
  log_since_mark | grep -E 'Flight coordinator' | head -5 | cut -c1-400 | tee "$OUT/coordinator-down/lines.txt" || true
  [ -f "$OUT/coordinator-down/warm/q06.tsv" ] || { echo "coordinator-down: warm-up q06 failed"; status=1; }
  for q in $FAILOVER_QUERIES; do
    [ -f "$OUT/coordinator-down/after/$q.tsv" ] \
      || { echo "coordinator-down: $q failed: $(head -c 300 "$OUT/coordinator-down/after/$q.err")"; status=1; }
  done
  grep -q 'Using Flight coordinator flight2' "$OUT/coordinator-down/lines.txt" \
    || { echo "coordinator-down: no switch to flight2 in the Trino log"; status=1; }
  save_logs
fi
exit $status
