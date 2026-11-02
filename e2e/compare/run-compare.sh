#!/usr/bin/env bash
# Connector comparison (STATUS "CONNECTOR COMPARISON", docs/CONNECTOR-COMPARE.md): this repo's Trino connector vs the
# upstream server repo's flight-trino connector, against the same Flight server on the same runner, TPC-H at scale SF.
#
#   ASSETS=<e2e-assets dir> WORK=<dir> OUT=<dir> SF=1|10 PROFILE=default|speed \
#   PASSES="ours:455:<zip> up:446:<zip> [ours446:446:<zip>]" e2e/compare/run-compare.sh
#
# Per pass (connector x Trino version): fresh server (PROFILE=speed adds -DresourceProfile=speed, nothing else is set
# beyond data location / S3 / plugin dirs), fresh Trino (TRINO_XMX, same config for every pass), EXPLAIN of
# EXPLAIN_QUERIES (what reaches the server), then ROUNDS rounds of q01..q22 (round 1 = cold). Each answer is checked
# with e2e/ci/compare.py against the DuckDB reference (SF1: e2e/ci/reference/sf1; SF10: generated in the job from the
# same Parquet files by tpch_data.py, which reproduces the SF1 reference 22/22). Container memory peaks from cgroup v2
# memory.peak (fallback: 1 s docker stats samples); per-query Trino stats (rows/bytes read, peak memory) from its REST API.
# The report (report.py) is printed at the end of the job log: artifacts cannot be read from the dev sessions.
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="${ASSETS:?}"; WORK="${WORK:?}"; OUT="${OUT:?}"; SF="${SF:-1}"; PROFILE="${PROFILE:-default}"
PASSES="${PASSES:?}"; SERVER_BUILD="${SERVER_BUILD:-f06b5834e76e}"; ROUNDS="${ROUNDS:-3}"
TRINO_XMX="${TRINO_XMX:-8G}"; TRINO_QUERY_MEM="${TRINO_QUERY_MEM:-4GB}"; SERVER_HEAP="${SERVER_HEAP:-3g}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-900}"  # seconds per query
EXPLAIN_QUERIES="${EXPLAIN_QUERIES:-q01 q03 q06 q14 q18}"
DATA_CACHE="${DATA_CACHE:-$WORK/tpch-sf$SF}"   # SF10: Parquet + reference (cached by the workflow)
NET=afcmp
QDIR="$REPO/e2e/ci/queries"
mkdir -p "$OUT" "$WORK"
log() { echo "== $(date -u +%T) $*"; }
S3_KEY="ci$(openssl rand -hex 8)"; S3_SECRET="$(openssl rand -hex 24)"; TOKEN="$(openssl rand -hex 24)"

log "server $SERVER_BUILD"
mkdir -p "$WORK/server"
cp "$ASSETS/arrow-flight-server-1.0.0-$SERVER_BUILD-linux.jar" "$WORK/server/server.jar"
tar --zstd -xf "$ASSETS/flight-plugins-1.0.0-$SERVER_BUILD.tar.zst" -C "$WORK/server"
tar --zstd -xf "$ASSETS/flight-server-libs-1.0.0-$SERVER_BUILD.tar.zst" -C "$WORK/server"
find "$WORK/server" -maxdepth 3 -name 'START-NOTES*' -exec grep -n -i -A3 'resourceProfile' {} \; | head -40 || true
EXT_DIR="$WORK/duckdb/extensions/v1.5.6/linux_amd64"; mkdir -p "$EXT_DIR"
curl -fsSL http://extensions.duckdb.org/v1.5.6/linux_amd64/httpfs.duckdb_extension.gz | gunzip > "$EXT_DIR/httpfs.duckdb_extension"

log "data sf$SF"
mkdir -p "$WORK/s3/lake/parquet"
if [ "$SF" = 1 ]; then
  mkdir -p "$WORK/data"; tar --zstd -xf "$ASSETS/test-data-sf1-types.tar.zst" -C "$WORK/data"
  mv "$(dirname "$(find "$WORK/data" -type d -name lineitem -print -quit)")" "$WORK/s3/lake/parquet/tpch"
  REF="$REPO/e2e/ci/reference/sf1"
else
  if [ ! -f "$DATA_CACHE/ref/MANIFEST.tsv" ]; then
    python3 "$REPO/e2e/compare/tpch_data.py" gen "$SF" "$DATA_CACHE/tpch"
    python3 "$REPO/e2e/compare/tpch_data.py" ref "$DATA_CACHE/tpch" "$QDIR" "$DATA_CACHE/ref"
  fi
  cp -al "$DATA_CACHE/tpch" "$WORK/s3/lake/parquet/tpch" 2>/dev/null || cp -r "$DATA_CACHE/tpch" "$WORK/s3/lake/parquet/tpch"
  REF="$DATA_CACHE/ref"
fi
du -sh "$WORK/s3/lake/parquet/tpch"/* | tee "$OUT/data-sizes.txt"
python3 "$REPO/e2e/compare/tpch_data.py" sum "$QDIR" "$REF" > "$OUT/reference-sums.tsv"

docker network create $NET >/dev/null
docker run -d --name s3 --network $NET -v "$WORK/s3":/data \
  rclone/rclone:1.68.2 serve s3 /data --addr :9000 --auth-key "$S3_KEY,$S3_SECRET" >/dev/null

start_server() {
  docker rm -f flight1 >/dev/null 2>&1 || true
  SERVER_CWD="$WORK/flight1-$(date +%s%N)"; mkdir -p "$SERVER_CWD"  # fresh cwd; the old one holds root-owned DuckDB temp files
  local extra=(); [ "$PROFILE" = default ] || extra=(-DresourceProfile="$PROFILE")
  docker run -d --name flight1 --network $NET \
    -v "$WORK/server":/app:ro -v "$WORK/duckdb":/ext/duckdb-home:ro -v "$SERVER_CWD":/work -w /work \
    -e LANG=C.UTF-8 -e FLIGHT_S3_ACCESS_KEY="$S3_KEY" -e FLIGHT_S3_SECRET_KEY="$S3_SECRET" \
    -e FLIGHT_AUTH_PROVIDER=token -e FLIGHT_AUTH_TOKEN="$TOKEN" \
    eclipse-temurin:21-jdk \
    java -Xmx"$SERVER_HEAP" --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
         -DdataDir=s3://lake/parquet -Ds3Endpoint=http://s3:9000 -Ds3UrlStyle=path -Ds3Region=us-east-1 \
         -Darrowflight.plugins.dir=/app/plugins -Darrowflight.duckdb.extensionDir=/ext/duckdb-home/extensions \
         "${extra[@]}" \
         -cp "/app/server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
         --port 32010 --localhost flight1 --hosts flight1 >/dev/null
  for _ in $(seq 1 90); do
    docker logs flight1 2>&1 | grep -q 'Server Arrow Flight SQL started' && return 0
    docker ps -q -f name=flight1 -f status=running | grep -q . || break
    sleep 2
  done
  docker logs flight1 2>&1 | tail -50; return 1
}

start_trino() {  # version plugin-dir catalog-file
  docker rm -f trino >/dev/null 2>&1 || true
  mkdir -p "$WORK/etc"
  printf '%s\n' -server "-Xmx$TRINO_XMX" -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Djdk.attach.allowAttachSelf=true \
    --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true > "$WORK/etc/jvm.config"
  cat > "$WORK/etc/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
query.max-memory=$TRINO_QUERY_MEM
query.max-memory-per-node=$TRINO_QUERY_MEM
EOF
  docker run -d --name trino --network $NET -p 127.0.0.1:8080:8080 \
    -v "$2":/usr/lib/trino/plugin/arrow-flight:ro -v "$3":/etc/trino/catalog/flight.properties:ro \
    -v "$WORK/etc/jvm.config":/etc/trino/jvm.config:ro -v "$WORK/etc/config.properties":/etc/trino/config.properties:ro \
    -v "$QDIR":/queries:ro "trinodb/trino:$1" >/dev/null
  for _ in $(seq 1 90); do
    docker exec trino trino --execute 'SELECT 1' >/dev/null 2>&1 && return 0
    docker ps -q -f name=trino -f status=running | grep -q . || break
    sleep 2
  done
  docker logs trino 2>&1 | tail -80; return 1
}

# Peak memory of both containers: cgroup v2 memory.peak, else the largest 1 s docker stats sample.
sampler() {
  while :; do docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' trino flight1 2>/dev/null; sleep 1; done
}
to_bytes() { numfmt --from=iec-i "$(echo "$1" | sed 's/B$//; s/iB$/Ki/; s/KiKi/Ki/')" 2>/dev/null || echo 0; }
mem_peak() {  # container samples-file
  local p; p=$(docker exec "$1" cat /sys/fs/cgroup/memory.peak 2>/dev/null || true)
  if [ -n "$p" ]; then echo "$p cgroup"; return; fi
  awk -v c="$1" '$1 == c { print $2 }' "$2" | while read -r v; do to_bytes "$v"; done | sort -n | tail -1 | sed 's/$/ stats/'
}

query_stats() {  # source -> "rows bytes peak_user_mem_bytes"
  local id
  id=$(curl -fsS -H 'X-Trino-User: cmp' http://127.0.0.1:8080/v1/query \
    | jq -r --arg s "$1" '[.[] | select(.session.source == $s)] | last | .queryId // empty' 2>/dev/null || true)
  [ -n "$id" ] || { echo "? ? ?"; return; }
  curl -fsS -H 'X-Trino-User: cmp' "http://127.0.0.1:8080/v1/query/$id" \
    | jq -r '.queryStats | "\(.physicalInputPositions) \(.physicalInputDataSize) \(.peakUserMemoryReservation)"' 2>/dev/null \
    || echo "? ? ?"
}

run_pass() {  # name trino-version plugin-zip
  local name=$1 version=$2 zip=$3 dir="$OUT/$1" catalog="$WORK/$1.properties"
  mkdir -p "$dir" "$WORK/plugin-$name"
  rm -rf "${WORK:?}/plugin-$name"/*; unzip -qj "$zip" -d "$WORK/plugin-$name"
  case "$name" in
    up*) printf 'connector.name=arrow_flight\nflight.host=flight1\nflight.port=32010\nflight.bearer-token=%s\n' "$TOKEN" > "$catalog" ;;
    *)   printf 'connector.name=arrow_flight\narrow-flight.host=flight1\narrow-flight.port=32010\narrow-flight.bearer-token=%s\n' "$TOKEN" > "$catalog" ;;
  esac
  log "pass $name: Trino $version, profile $PROFILE, sf$SF"
  { echo "pass=$name"; echo "trino=$version"; echo "profile=$PROFILE"; echo "sf=$SF"; echo "server=$SERVER_BUILD"
    echo "zip=$(basename "$zip") $(sha256sum "$zip" | cut -c1-12)"; } > "$dir/meta.txt"
  if ! start_server; then echo "server did not start" >> "$dir/meta.txt"; return 0; fi
  if ! start_trino "$version" "$WORK/plugin-$name" "$catalog"; then
    echo "trino did not start" >> "$dir/meta.txt"; docker logs trino > "$dir/trino.log" 2>&1 || true; return 0
  fi
  docker exec trino java -version 2>&1 | head -1 | sed 's/^/trino_jvm=/' >> "$dir/meta.txt" || true
  sampler > "$dir/mem-samples.txt" & local spid=$!
  printf 'query\tround\tseconds\tstatus\n' > "$dir/times.tsv"
  printf 'query\tround\trows_read\tbytes_read\tpeak_user_mem\n' > "$dir/stats.tsv"
  for r in $(seq 1 "$ROUNDS"); do
    mkdir -p "$dir/r$r"
    for f in "$QDIR"/q*.sql; do
      local q; q=$(basename "$f" .sql)
      local src="cmp-$name-$q-r$r" t0 t1 st=ok
      t0=$(date +%s.%N)
      if timeout "$QUERY_TIMEOUT" docker exec trino trino --catalog flight --schema tpch --output-format TSV \
           --source "$src" --session "query_max_run_time=${QUERY_TIMEOUT}s" -f "/queries/$q.sql" \
           > "$dir/r$r/$q.tsv" 2> "$dir/r$r/$q.err"; then
        rm -f "$dir/r$r/$q.err"
      else
        st=error; rm -f "$dir/r$r/$q.tsv"; tail -c 300 "$dir/r$r/$q.err" | tr '\n' ' '; echo
        docker ps -q -f name=trino -f status=running | grep -q . || { st=trino-died; }
      fi
      t1=$(date +%s.%N)
      printf '%s\t%s\t%.2f\t%s\n' "$q" "$r" "$(echo "$t1 - $t0" | bc)" "$st" >> "$dir/times.tsv"
      printf '%s\t%s\t%s\n' "$q" "$r" "$(query_stats "$src" | tr ' ' '\t')" >> "$dir/stats.tsv"
      if [ "$st" = trino-died ]; then
        docker logs trino > "$dir/trino-died-$q-r$r.log" 2>&1 || true
        start_trino "$version" "$WORK/plugin-$name" "$catalog" || break 2
      fi
    done
    python3 "$REPO/e2e/ci/compare.py" "$REF" "$dir/r$r" "$QDIR" "$dir/answers-r$r.tsv" > /dev/null || true
    python3 "$REPO/e2e/compare/tpch_data.py" sum "$QDIR" "$dir/r$r" > "$dir/sums-r$r.tsv"
    log "$name round $r: $(grep -c $'\tOK\t' "$dir/answers-r$r.tsv") OK, $(awk -F'\t' -v r="$r" '$2 == r { s += $3 } END { printf "%.1f", s }' "$dir/times.tsv") s"
  done
  kill "$spid" 2>/dev/null || true
  { echo "trino $(mem_peak trino "$dir/mem-samples.txt")"; echo "flight1 $(mem_peak flight1 "$dir/mem-samples.txt")"; } > "$dir/memory.txt"
  # EXPLAIN after the timed rounds (the cold round stays cold): the plan fragment that reaches the server
  mkdir -p "$dir/explain"
  for q in $EXPLAIN_QUERIES; do
    { echo "EXPLAIN"; cat "$QDIR/$q.sql"; } > "$WORK/explain-$q.sql"
    docker cp "$WORK/explain-$q.sql" trino:/tmp/explain.sql >/dev/null
    docker exec trino trino --catalog flight --schema tpch -f /tmp/explain.sql > "$dir/explain/$q.txt" 2>&1 || true
  done
  docker logs trino > "$dir/trino.log" 2>&1 || true; docker logs flight1 > "$dir/flight1.log" 2>&1 || true
  docker rm -f trino flight1 >/dev/null 2>&1 || true
}

for p in $PASSES; do
  IFS=: read -r name version zip <<< "$p"
  if [ -n "$zip" ] && [ -f "$zip" ]; then run_pass "$name" "$version" "$zip"
  else mkdir -p "$OUT/$name"; printf 'pass=%s\ntrino=%s\nnot built\n' "$name" "$version" > "$OUT/$name/meta.txt"; fi
done
python3 "$REPO/e2e/compare/report.py" "$OUT" "$REF" | tee "$OUT/report.md"
