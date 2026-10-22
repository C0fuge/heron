#!/usr/bin/env bash
# CI end-to-end run: MinIO + Flight SQL server(s) + Trino with this plugin, TPC-H SF1 22/22
# compared with e2e/ci/reference/sf1.
#
#   ASSETS=<dir with the e2e-assets release files>  PLUGIN_ZIP=<plugin zip>  OUT=<results dir>
#   TRINO_VERSION=455  RESULT_MODE=partial  e2e/ci/run-e2e.sh
#
# Docker on the host (GitHub runner); everything on the private network 'afe2e'.
# Credentials (MinIO, Flight token) are throwaway values generated per run.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="${ASSETS:?}"; PLUGIN_ZIP="${PLUGIN_ZIP:?}"; OUT="${OUT:?}"
TRINO_VERSION="${TRINO_VERSION:-455}"; RESULT_MODE="${RESULT_MODE:-partial}"
WORK="${WORK:-$(mktemp -d)}"
NET=afe2e
DUCKDB_EXT_VERSION=v1.4.5
mkdir -p "$OUT" "$WORK"

S3_KEY="ci$(openssl rand -hex 8)"; S3_SECRET="$(openssl rand -hex 24)"; TOKEN="$(openssl rand -hex 24)"

log() { echo "== $(date -u +%T) $*"; }

log "unpack assets"
mkdir -p "$WORK/server" "$WORK/data" "$WORK/plugin"
cp "$ASSETS"/arrow-flight-server-*.jar "$WORK/server/server.jar"
tar --zstd -xf "$ASSETS/int4-plugins.tar.zst" -C "$WORK/server"
if [ -f "$ASSETS/flight-server-libs.tar.zst" ]; then
  tar --zstd -xf "$ASSETS/flight-server-libs.tar.zst" -C "$WORK/server"
else
  log "flight-server-libs.tar.zst not in the release: running the server jar alone"
fi
mkdir -p "$WORK/server/libs"
find "$WORK/server" -maxdepth 3 -name 'START-NOTES*' -exec sh -c 'echo "--- $1"; cat "$1"' _ {} \;
tar --zstd -xf "$ASSETS/test-data-sf1-types.tar.zst" -C "$WORK/data"
find "$WORK/data" -maxdepth 3 -type d | head -20
unzip -qj "$PLUGIN_ZIP" -d "$WORK/plugin"

log "httpfs $DUCKDB_EXT_VERSION"
EXT_DIR="$WORK/duckdb/extensions/$DUCKDB_EXT_VERSION/linux_amd64"
mkdir -p "$EXT_DIR"
curl -fsSL "http://extensions.duckdb.org/$DUCKDB_EXT_VERSION/linux_amd64/httpfs.duckdb_extension.gz" \
  | gunzip > "$EXT_DIR/httpfs.duckdb_extension"

docker network create $NET >/dev/null

log "minio"
# The MinIO images are no longer pullable anonymously (Docker Hub repo gone, quay.io unauthorized):
# run the release binary in a plain container.
MINIO_URL=https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2025-04-22T22-12-26Z
mkdir -p "$WORK/minio"
curl -fsSL -o "$WORK/minio/minio" "$MINIO_URL"
echo "$(curl -fsSL "$MINIO_URL.sha256sum" | cut -d' ' -f1)  $WORK/minio/minio" | sha256sum --check
chmod +x "$WORK/minio/minio"
docker run -d --name minio --network $NET -p 127.0.0.1:9000:9000 -v "$WORK/minio":/opt/minio:ro \
  -e MINIO_ROOT_USER="$S3_KEY" -e MINIO_ROOT_PASSWORD="$S3_SECRET" \
  ubuntu:24.04 /opt/minio/minio server /data >/dev/null
for i in $(seq 1 60); do curl -fs http://127.0.0.1:9000/minio/health/ready && break; sleep 1; done
export AWS_ACCESS_KEY_ID="$S3_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET" AWS_DEFAULT_REGION=us-east-1
S3="aws --endpoint-url http://127.0.0.1:9000 s3"
$S3 mb s3://lake >/dev/null
SF1_TPCH="$(dirname "$(find "$WORK/data" -type d -name lineitem | head -1)")"
TYPES="$(find "$WORK/data" -maxdepth 2 -type d -name types | head -1)"
$S3 cp --recursive --only-show-errors "$SF1_TPCH" s3://lake/parquet/tpch
[ -n "$TYPES" ] && $S3 cp --recursive --only-show-errors "$TYPES" s3://lake/parquet/types
$S3 ls --recursive s3://lake/parquet/ | head -5

log "flight server (result mode $RESULT_MODE)"
docker run -d --name flight1 --network $NET \
  -v "$WORK/server":/app:ro -v "$WORK/duckdb":/ext/duckdb-home:ro -w /app \
  -e LANG=C.UTF-8 -e FLIGHT_S3_ACCESS_KEY="$S3_KEY" -e FLIGHT_S3_SECRET_KEY="$S3_SECRET" \
  -e FLIGHT_AUTH_PROVIDER=token -e FLIGHT_AUTH_TOKEN="$TOKEN" \
  eclipse-temurin:21-jdk \
  java --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
       -DdataDir=s3://lake/parquet -Ds3Endpoint=http://minio:9000 -Darrowflight.plugins.dir=/app/plugins \
       -Darrowflight.duckdb.extensionDir=/ext/duckdb-home/extensions \
       -DresultMode="$RESULT_MODE" \
       -cp "/app/server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
       --port 32010 --localhost flight1 --hosts flight1 >/dev/null
for i in $(seq 1 90); do
  docker logs flight1 2>&1 | grep -q 'Server Arrow Flight SQL started' && break
  docker ps -q -f name=flight1 -f status=running | grep -q . || break
  sleep 2
done
docker logs flight1 > "$OUT/flight1.log" 2>&1 || true
grep -q 'Server Arrow Flight SQL started' "$OUT/flight1.log" || { tail -50 "$OUT/flight1.log"; exit 1; }

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
docker run -d --name trino --network $NET \
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

log "TPC-H SF1"
mkdir -p "$OUT/sf1"
for q in "$REPO"/e2e/ci/queries/q*.sql; do
  n="$(basename "$q" .sql)"; start=$(date +%s.%N)
  if docker exec trino trino --catalog flight --schema tpch --output-format TSV -f "/queries/$n.sql" \
       > "$OUT/sf1/$n.tsv" 2> "$OUT/sf1/$n.err"; then
    rm -f "$OUT/sf1/$n.err"
  else
    rm -f "$OUT/sf1/$n.tsv"
  fi
  printf '%s\t%.2f\n' "$n" "$(echo "$(date +%s.%N) - $start" | bc)" >> "$OUT/sf1-times.tsv"
done
docker logs trino > "$OUT/trino.log" 2>&1 || true
docker logs flight1 > "$OUT/flight1.log" 2>&1 || true
python3 "$REPO/e2e/ci/compare.py" "$REPO/e2e/ci/reference/sf1" "$OUT/sf1" "$REPO/e2e/ci/queries" "$OUT/sf1-summary.tsv"
