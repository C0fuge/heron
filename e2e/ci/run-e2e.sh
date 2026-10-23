#!/usr/bin/env bash
# CI end-to-end run: S3 (rclone) + Flight SQL server(s) + Trino with this plugin, TPC-H SF1 22/22
# compared with e2e/ci/reference/sf1, and every table of schema 'types' compared with DuckDB
# (pip duckdb==1.4.1) reading the same Parquet files (e2e/ci/types_check.py).
#
#   ASSETS=<dir with the e2e-assets release files>  PLUGIN_ZIP=<plugin zip>  OUT=<results dir>
#   TRINO_VERSION=455  RESULT_MODE=partial  SERVERS=1|3  SERVER_BUILD=int4|int5|int6  e2e/ci/run-e2e.sh
#
# SERVERS=3 forms a cluster (backup locations) and then checks endpoint failover: a node lost
# before the queries (partitioned, then killed: answers must still be exact), a node killed during a read (clean error or exact
# answer, never wrong rows; retried with other delays until the kill hits a read of that node),
# a node killed after its stream delivered data (clean error or the exact answer, never fewer rows),
# and two of three nodes down (exact or clean error).
#
# ICEBERG=1 (default for server builds after int4: int5, int6, which carry the 'iceberg' catalog plugin): Spark writes
# Iceberg tables (schema 'ice') and they are checked against the Parquet tables; see e2e/ci/iceberg.sh.
#
# Docker on the host (GitHub runner); S3 = rclone serve s3; everything on the private network 'afe2e'.
# Credentials (S3, Flight token) are throwaway values generated per run.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="${ASSETS:?}"; PLUGIN_ZIP="${PLUGIN_ZIP:?}"; OUT="${OUT:?}"
TRINO_VERSION="${TRINO_VERSION:-455}"; RESULT_MODE="${RESULT_MODE:-partial}"
SERVERS="${SERVERS:-1}"; SERVER_HEAP="${SERVER_HEAP:-2g}"; SERVER_BUILD="${SERVER_BUILD:-int4}"
FAILOVER_QUERIES="${FAILOVER_QUERIES:-q01 q03 q05 q06 q18}"
ICEBERG_FAILOVER_QUERIES="${ICEBERG_FAILOVER_QUERIES:-q01 q03 q18}"  # all in ICEBERG_QUERIES
WORK="${WORK:-$(mktemp -d)}"
NET=afe2e
DUCKDB_EXT_VERSION=v1.4.5
case "$SERVER_BUILD" in int4) ICEBERG="${ICEBERG:-0}" ;; *) ICEBERG="${ICEBERG:-1}" ;; esac
mkdir -p "$OUT" "$WORK" "$WORK/wh"
# shellcheck source=e2e/ci/iceberg.sh
. "$REPO/e2e/ci/iceberg.sh"

S3_KEY="ci$(openssl rand -hex 8)"; S3_SECRET="$(openssl rand -hex 24)"; TOKEN="$(openssl rand -hex 24)"

log() { echo "== $(date -u +%T) $*"; }

log "unpack assets"
mkdir -p "$WORK/server" "$WORK/data" "$WORK/plugin"
cp "$ASSETS/arrow-flight-server-1.0.0-$SERVER_BUILD.jar" "$WORK/server/server.jar"
tar --zstd -xf "$ASSETS/$SERVER_BUILD-plugins.tar.zst" -C "$WORK/server"
if [ -f "$ASSETS/flight-server-libs.tar.zst" ]; then
  tar --zstd -xf "$ASSETS/flight-server-libs.tar.zst" -C "$WORK/server"
else
  log "flight-server-libs.tar.zst not in the release: running the server jar alone"
fi
mkdir -p "$WORK/server/libs"
find "$WORK/server" -maxdepth 3 -name 'START-NOTES*' -exec sh -c 'echo "--- $1"; cat "$1"' _ {} \;
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
         -DresultMode="$RESULT_MODE" "${SERVER_EXTRA[@]}" \
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
cat > "$WORK/etc/jvm.config" <<JVM
-server
-Xmx3G
-XX:+UseG1GC
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
--add-opens=java.base/java.nio=ALL-UNNAMED
-Darrow.enable_unsafe_memory_access=true
JVM
docker run -d --name trino --network $NET -p 127.0.0.1:8080:8080 \
  -v "$WORK/plugin":/usr/lib/trino/plugin/arrow-flight:ro \
  -v "$WORK/etc/catalog/flight.properties":/etc/trino/catalog/flight.properties:ro \
  -v "$WORK/etc/jvm.config":/etc/trino/jvm.config:ro \
  -v "$REPO/e2e/ci/queries":/queries:ro \
  "trinodb/trino:$TRINO_VERSION" >/dev/null
for i in $(seq 1 90); do
  docker exec trino trino --execute 'SELECT 1' >/dev/null 2>&1 && break
  docker ps -q -f name=trino -f status=running | grep -q . || break
  sleep 2
done
docker exec trino trino --execute 'SHOW SCHEMAS FROM flight' | tee "$OUT/schemas.txt"

run_query() {  # query-name out-dir
  local start; start=$(date +%s.%N)
  if docker exec trino trino --catalog flight --schema "${QUERY_SCHEMA:-tpch}" --output-format TSV -f "/queries/$1.sql" \
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

log "TPC-H SF1 ($SERVERS server(s))"
mkdir -p "$OUT/sf1"
for q in "$QUERIES"/q*.sql; do run_query "$(basename "$q" .sql)" "$OUT/sf1"; done
save_logs
status=0
"${COMPARE[@]}" "$OUT/sf1" "$QUERIES" "$OUT/sf1-summary.tsv" || status=1

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
  # is INFO (timing-dependent). The query streams all of lineitem (checksum is not pushed down); its
  # streams open ~7-8 s after submission and one column took <1 s (runs 39886487789, 39888037652).
  # One checksum per column (written while checksum(ROW(...)) still looped the optimizer, run 39889582040;
  # the ROW form is now checked on its own above).
  STREAM_SQL='SELECT count(*), checksum(l_orderkey), checksum(l_partkey), checksum(l_suppkey), checksum(l_linenumber), checksum(l_quantity), checksum(l_extendedprice), checksum(l_discount), checksum(l_tax), checksum(l_returnflag), checksum(l_linestatus), checksum(l_shipdate), checksum(l_commitdate), checksum(l_receiptdate), checksum(l_shipinstruct), checksum(l_shipmode), checksum(l_comment) FROM lineitem'
  stream_query() {  # out-dir
    if docker exec trino trino --catalog flight --schema tpch --output-format TSV --execute "$STREAM_SQL" \
         > "$1/stream.tsv" 2> "$1/stream.err"; then rm -f "$1/stream.err"; else rm -f "$1/stream.tsv"; fi
  }
  kill_mid_stream() {
    local dir="$OUT/killed-mid-stream" attempt=0 hit=no wrong=0 delay adir result evidence start
    log "failover: flight3 killed in the middle of a stream"
    restart_lost
    mkdir -p "$dir/base"
    start=$(date +%s.%N); stream_query "$dir/base"
    printf 'killed-mid-stream\tbase\t%.2f\n' "$(echo "$(date +%s.%N) - $start" | bc)" >> "$OUT/times.tsv"
    if [ ! -f "$dir/base/stream.tsv" ]; then
      cat "$dir/base/stream.err"
      printf 'query\tresult\tdetail\nstream\tFAIL\tbaseline failed on a healthy cluster\n' | tee "$OUT/killed-mid-stream-summary.tsv"
      status=1; return
    fi
    cat "$dir/base/stream.tsv"
    printf 'attempt\tdelay_s\tresult\tflight3_hit\n' > "$OUT/killed-mid-stream-attempts.tsv"
    for delay in ${KILL_STREAM_DELAYS:-9 8 10 11}; do
      attempt=$((attempt + 1))
      adir="$dir/$attempt"; mkdir -p "$adir"
      [ "$attempt" -gt 1 ] && restart_lost
      mark_log
      stream_query "$adir" &
      sleep "$delay"
      docker kill flight3 >/dev/null
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
    evidence=$([ "$wrong" = 1 ] && echo FAIL || { [ "$hit" = yes ] && echo OK || echo INFO; })
    { printf 'query\tresult\tdetail\n'
      printf 'stream\t%s\t%s attempt(s), wrong answers: %s; kill hit a stream of flight3 after data: %s\n' \
        "$evidence" "$attempt" "$wrong" "$hit"; } | tee "$OUT/killed-mid-stream-summary.tsv"
    [ "$evidence" != FAIL ] || status=1
  }
  kill_mid_stream

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
  save_logs
fi
exit $status
