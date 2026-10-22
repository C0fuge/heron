#!/usr/bin/env bash
# CI end-to-end run: S3 (rclone) + Flight SQL server(s) + Trino with this plugin, TPC-H SF1 22/22
# compared with e2e/ci/reference/sf1, and every table of schema 'types' compared with DuckDB
# (pip duckdb==1.4.1) reading the same Parquet files (e2e/ci/types_check.py).
#
#   ASSETS=<dir with the e2e-assets release files>  PLUGIN_ZIP=<plugin zip>  OUT=<results dir>
#   TRINO_VERSION=455  RESULT_MODE=partial  SERVERS=1|3  SERVER_BUILD=int4|int5  e2e/ci/run-e2e.sh
#
# SERVERS=3 forms a cluster (backup locations) and then checks endpoint failover: a node killed
# before the queries (answers must still be exact) and a node killed during a read (clean error or
# exact answer, never wrong rows).
#
# Docker on the host (GitHub runner); S3 = rclone serve s3; everything on the private network 'afe2e'.
# Credentials (S3, Flight token) are throwaway values generated per run.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="${ASSETS:?}"; PLUGIN_ZIP="${PLUGIN_ZIP:?}"; OUT="${OUT:?}"
TRINO_VERSION="${TRINO_VERSION:-455}"; RESULT_MODE="${RESULT_MODE:-partial}"
SERVERS="${SERVERS:-1}"; SERVER_HEAP="${SERVER_HEAP:-2g}"; SERVER_BUILD="${SERVER_BUILD:-int4}"
FAILOVER_QUERIES="${FAILOVER_QUERIES:-q01 q03 q05 q06 q18}"
WORK="${WORK:-$(mktemp -d)}"
NET=afe2e
DUCKDB_EXT_VERSION=v1.4.5
mkdir -p "$OUT" "$WORK"

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

HOSTS="$(seq -s, -f 'flight%g' 1 "$SERVERS")"
log "flight servers $HOSTS (result mode $RESULT_MODE)"
start_server() {  # name
  mkdir -p "$WORK/$1"  # writable cwd: DuckDB temp dir (.tmp)
  docker run -d --name "$1" --network $NET \
    -v "$WORK/server":/app:ro -v "$WORK/duckdb":/ext/duckdb-home:ro \
    -v "$WORK/$1":/work -w /work \
    -e LANG=C.UTF-8 -e FLIGHT_S3_ACCESS_KEY="$S3_KEY" -e FLIGHT_S3_SECRET_KEY="$S3_SECRET" \
    -e FLIGHT_AUTH_PROVIDER=token -e FLIGHT_AUTH_TOKEN="$TOKEN" \
    eclipse-temurin:21-jdk \
    java -Xmx$SERVER_HEAP --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
         -DdataDir=s3://lake/parquet -Ds3Endpoint=http://s3:9000 -Ds3UrlStyle=path -Ds3Region=us-east-1 \
         -Darrowflight.plugins.dir=/app/plugins -Darrowflight.duckdb.extensionDir=/ext/duckdb-home/extensions \
         -DresultMode="$RESULT_MODE" \
         -cp "/app/server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
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
  if docker exec trino trino --catalog flight --schema tpch --output-format TSV -f "/queries/$1.sql" \
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

if [ -n "$TYPES" ]; then
  log "types: SELECT * per table vs DuckDB on the same Parquet"
  python3 -m venv "$WORK/venv"
  "$WORK/venv/bin/pip" install -q --disable-pip-version-check duckdb==1.4.1 trino==0.333.0
  "$WORK/venv/bin/python" "$REPO/e2e/ci/types_check.py" "$WORK/s3/lake/parquet/types" 127.0.0.1:8080 \
    "$OUT/types-summary.tsv" || status=1
  save_logs
fi

if [ "$SERVERS" -ge 3 ]; then
  # Node lost before a query: the endpoints it served must be read from their backup locations.
  log "failover: flight2 killed before the queries"
  docker kill flight2 >/dev/null
  sleep "${KILL_SETTLE_SECONDS:-10}"
  mkdir -p "$OUT/killed-before"
  for q in $FAILOVER_QUERIES; do run_query "$q" "$OUT/killed-before"; done
  "${COMPARE[@]}" "$OUT/killed-before" "$QUERIES" "$OUT/killed-before-summary.tsv" "$FAILOVER_QUERIES" || status=1

  # Node lost while a query reads from it: a clean error or the exact answer, never wrong rows.
  log "failover: flight3 killed during a read"
  mkdir -p "$OUT/killed-during"
  run_query q18 "$OUT/killed-during" &
  sleep "${KILL_DURING_DELAY_SECONDS:-2}"
  docker kill flight3 >/dev/null
  wait
  if [ -f "$OUT/killed-during/q18.tsv" ]; then
    "${COMPARE[@]}" "$OUT/killed-during" "$QUERIES" "$OUT/killed-during-summary.tsv" q18 || status=1
  else
    printf 'query\tresult\tdetail\nq18\tOK\tclean error: %s\n' \
      "$(grep -m1 -E 'Query .* failed|ERROR' "$OUT/killed-during/q18.err" | cut -c1-200)" | tee "$OUT/killed-during-summary.tsv"
  fi
  save_logs
fi
exit $status
