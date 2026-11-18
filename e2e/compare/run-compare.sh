#!/usr/bin/env bash
# Connector comparison (STATUS "CONNECTOR COMPARISON", docs/CONNECTOR-COMPARE.md): this repo's Trino connector vs the
# upstream server repo's flight-trino connector, against the same Flight server on the same runner, TPC-H at scale SF.
#
#   ASSETS=<e2e-assets dir> WORK=<dir> OUT=<dir> SF=1|10 PROFILE=default|speed \
#   PASSES="ours:455:<zip> up:446:<zip> [ours446:446:<zip>] [name:455:<zip>:prop=value,prop=value] [p:483p:<zip>]" \
#   [QUERIES="q03 q10"] [QUERY_SET=tpch|agg] [SERVERS=1|3] e2e/compare/run-compare.sh
#
# A 5th pass field names the Flight server build for that pass (default SERVER_BUILD), e.g.
# "old-deafc861cce3:455:<zip>::deafc861cce3": server A vs server B in one job on the same runner (its assets must be in
# ASSETS too).
#
# SERVERS=3 starts a cluster flight1..flight3 (--hosts flight1,flight2,flight3, backup locations, every server reads the
# same S3 bucket) per pass instead of one server; the catalog still points at flight1 (the coordinator for the plugin).
# SERVER_HEAP applies to each server; memory.txt lists every server's peak.
#
# QUERY_SET=agg times e2e/compare/queries-agg/qa*.sql instead of TPC-H: non-decimal count/sum/min/max/avg directly over
# a scan, i.e. the aggregations the patched Trino's partial aggregation pushdown takes (TPC-H's scan aggregations are all
# decimal or have no aggregates, so its plans are the same on stock and patched Trino). Their DuckDB reference is
# generated in the job from the same Parquet files; EXPLAIN_QUERIES defaults to all of them.
#
# A 4th pass field sets catalog session properties for every query of that pass (e.g. ours-nostats with
# statistics_enabled=false); a pass whose name starts with "up" uses the upstream catalog properties. QUERIES limits the
# timed queries (default: all). PLAN_LOG=1 prints the EXPLAIN of EXPLAIN_QUERIES (without Layout lines) to the job log,
# and stats.tsv also records Trino's planning / analysis / execution time per query.
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
PASSES="${PASSES:?}"; SERVER_BUILD="${SERVER_BUILD:-36f3302321ba}"; ROUNDS="${ROUNDS:-3}"
TRINO_XMX="${TRINO_XMX:-8G}"; TRINO_QUERY_MEM="${TRINO_QUERY_MEM:-4GB}"; SERVER_HEAP="${SERVER_HEAP:-3g}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-900}"  # seconds per query
QUERY_SET="${QUERY_SET:-tpch}"; SERVERS="${SERVERS:-1}"
HOSTS="$(seq -s, -f 'flight%g' 1 "$SERVERS")"; HOST_LIST="${HOSTS//,/ }"
if [ "$QUERY_SET" = agg ]; then
  EXPLAIN_QUERIES="${EXPLAIN_QUERIES:-qa01 qa02 qa03 qa04 qa05 qa06}"
else
  EXPLAIN_QUERIES="${EXPLAIN_QUERIES:-q01 q03 q06 q14 q18}"
fi
QUERIES="${QUERIES:-}"; PLAN_LOG="${PLAN_LOG:-0}"
DATA_CACHE="${DATA_CACHE:-$WORK/tpch-sf$SF}"   # SF10: Parquet + reference (cached by the workflow)
NET=afcmp
QDIR="$REPO/e2e/ci/queries"; [ "$QUERY_SET" != agg ] || QDIR="$REPO/e2e/compare/queries-agg"
mkdir -p "$OUT" "$WORK"
log() { echo "== $(date -u +%T) $*"; }
S3_KEY="ci$(openssl rand -hex 8)"; S3_SECRET="$(openssl rand -hex 24)"; TOKEN="$(openssl rand -hex 24)"

setup_server() {  # build -> $WORK/server-<build> (jar, plugins, libs), once per build
  local d="$WORK/server-$1"
  [ -d "$d" ] && return 0
  log "server $1"
  mkdir -p "$d"
  cp "$ASSETS/arrow-flight-server-1.0.0-$1-linux.jar" "$d/server.jar"
  tar --zstd -xf "$ASSETS/flight-plugins-1.0.0-$1.tar.zst" -C "$d"
  tar --zstd -xf "$ASSETS/flight-server-libs-1.0.0-$1.tar.zst" -C "$d"
  sha256sum "$d/server.jar" | cut -c1-12 | sed "s/^/server $1 jar sha256 /"
}
setup_server "$SERVER_BUILD"
find "$WORK/server-$SERVER_BUILD" -maxdepth 3 -name 'START-NOTES*' -exec grep -n -i -A3 'resourceProfile' {} \; | head -40 || true
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
    python3 "$REPO/e2e/compare/tpch_data.py" ref "$DATA_CACHE/tpch" "$REPO/e2e/ci/queries" "$DATA_CACHE/ref"
  fi
  cp -al "$DATA_CACHE/tpch" "$WORK/s3/lake/parquet/tpch" 2>/dev/null || cp -r "$DATA_CACHE/tpch" "$WORK/s3/lake/parquet/tpch"
  REF="$DATA_CACHE/ref"
fi
du -sh "$WORK/s3/lake/parquet/tpch"/* | tee "$OUT/data-sizes.txt"
if [ "$QUERY_SET" = agg ]; then
  REF="$WORK/ref-agg"; python3 "$REPO/e2e/compare/tpch_data.py" ref "$WORK/s3/lake/parquet/tpch" "$QDIR" "$REF"
fi
python3 "$REPO/e2e/compare/tpch_data.py" sum "$QDIR" "$REF" > "$OUT/reference-sums.tsv"

docker network create $NET >/dev/null
docker run -d --name s3 --network $NET -v "$WORK/s3":/data \
  rclone/rclone:1.68.2 serve s3 /data --addr :9000 --auth-key "$S3_KEY,$S3_SECRET" >/dev/null

start_server() {  # build; all of $HOSTS; a cluster member starts only once every --hosts member has joined
  local h extra=() srv="$WORK/server-$1"; [ "$PROFILE" = default ] || extra=(-DresourceProfile="$PROFILE")
  for h in $HOST_LIST; do
    docker rm -f "$h" >/dev/null 2>&1 || true
    local cwd="$WORK/$h-$(date +%s%N)"; mkdir -p "$cwd"  # fresh cwd; the old one holds root-owned DuckDB temp files
    docker run -d --name "$h" --network $NET \
      -v "$srv":/app:ro -v "$WORK/duckdb":/ext/duckdb-home:ro -v "$cwd":/work -w /work \
      -e LANG=C.UTF-8 -e FLIGHT_S3_ACCESS_KEY="$S3_KEY" -e FLIGHT_S3_SECRET_KEY="$S3_SECRET" \
      -e FLIGHT_AUTH_PROVIDER=token -e FLIGHT_AUTH_TOKEN="$TOKEN" \
      eclipse-temurin:21-jdk \
      java -Xmx"$SERVER_HEAP" --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
           -DdataDir=s3://lake/parquet -Ds3Endpoint=http://s3:9000 -Ds3UrlStyle=path -Ds3Region=us-east-1 \
           -Darrowflight.plugins.dir=/app/plugins -Darrowflight.duckdb.extensionDir=/ext/duckdb-home/extensions \
           "${extra[@]}" \
           -cp "/app/server.jar:/app/libs/*" pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
           --port 32010 --localhost "$h" --hosts "$HOSTS" >/dev/null
  done
  for h in $HOST_LIST; do
    local up=0
    for _ in $(seq 1 90); do
      docker logs "$h" 2>&1 | grep -q 'Server Arrow Flight SQL started' && { up=1; break; }
      docker ps -q -f name="$h" -f status=running | grep -q . || break
      sleep 2
    done
    [ "$up" = 1 ] || { echo "server $h did not start:"; docker logs "$h" 2>&1 | tail -50; return 1; }
  done
}

# Trino version "483p" = image trino-patched:483 (trino-patches/build-overlay.sh, built by connector-compare.yml)
trino_image() { case "$1" in *p) echo "trino-patched:${1%p}" ;; *) echo "trinodb/trino:$1" ;; esac; }

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
    -v "$QDIR":/queries:ro "$(trino_image "$1")" >/dev/null
  for _ in $(seq 1 90); do
    docker exec trino trino --execute 'SELECT 1' >/dev/null 2>&1 && return 0
    docker ps -q -f name=trino -f status=running | grep -q . || break
    sleep 2
  done
  docker logs trino 2>&1 | tail -80; return 1
}

# Peak memory of both containers: cgroup v2 memory.peak, else the largest 1 s docker stats sample.
sampler() {
  while :; do docker stats --no-stream --format '{{.Name}} {{.MemUsage}}' trino $HOST_LIST 2>/dev/null; sleep 1; done
}
to_bytes() { numfmt --from=iec-i "$(echo "$1" | sed 's/B$//; s/iB$/Ki/; s/KiKi/Ki/')" 2>/dev/null || echo 0; }
mem_peak() {  # container samples-file
  local p; p=$(docker exec "$1" cat /sys/fs/cgroup/memory.peak 2>/dev/null || true)
  if [ -n "$p" ]; then echo "$p cgroup"; return; fi
  awk -v c="$1" '$1 == c { print $2 }' "$2" | while read -r v; do to_bytes "$v"; done | sort -n | tail -1 | sed 's/$/ stats/'
}

query_stats() {  # source -> "rows bytes peak_user_mem planning analysis execution"
  local id
  id=$(curl -fsS -H 'X-Trino-User: cmp' http://127.0.0.1:8080/v1/query \
    | jq -r --arg s "$1" '[.[] | select(.session.source == $s)] | last | .queryId // empty' 2>/dev/null || true)
  [ -n "$id" ] || { echo "? ? ? ? ? ?"; return; }
  curl -fsS -H 'X-Trino-User: cmp' "http://127.0.0.1:8080/v1/query/$id" \
    | jq -r '.queryStats | "\(.physicalInputPositions) \(.physicalInputDataSize) \(.peakUserMemoryReservation) \(.planningTime) \(.analysisTime) \(.executionTime)"' 2>/dev/null \
    || echo "? ? ? ? ? ?"
}

run_pass() {  # name trino-version plugin-zip [session props a=b,c=d] [server build]
  local name=$1 version=$2 zip=$3 props=${4:-} build=${5:-$SERVER_BUILD} dir="$OUT/$1" catalog="$WORK/$1.properties"
  local sess=() kv
  for kv in ${props//,/ }; do sess+=(--session "flight.$kv"); done
  mkdir -p "$dir" "$WORK/plugin-$name"
  rm -rf "${WORK:?}/plugin-$name"/*; unzip -qj "$zip" -d "$WORK/plugin-$name"
  case "$name" in
    up*) printf 'connector.name=arrow_flight\nflight.host=flight1\nflight.port=32010\nflight.bearer-token=%s\n' "$TOKEN" > "$catalog" ;;
    *)   printf 'connector.name=arrow_flight\narrow-flight.host=flight1\narrow-flight.port=32010\narrow-flight.bearer-token=%s\n' "$TOKEN" > "$catalog" ;;
  esac
  log "pass $name: Trino $version, profile $PROFILE, sf$SF"
  { echo "pass=$name"; echo "trino=$version"; echo "profile=$PROFILE"; echo "sf=$SF"; echo "server=$build"; echo "servers=$SERVERS"
    echo "zip=$(basename "$zip") $(sha256sum "$zip" | cut -c1-12)"; [ -z "$props" ] || echo "note=session $props"
  } > "$dir/meta.txt"
  setup_server "$build"
  if ! start_server "$build"; then echo "server did not start" >> "$dir/meta.txt"; return 0; fi
  if ! start_trino "$version" "$WORK/plugin-$name" "$catalog"; then
    echo "trino did not start" >> "$dir/meta.txt"; docker logs trino > "$dir/trino.log" 2>&1 || true; return 0
  fi
  docker exec trino java -version 2>&1 | head -1 | sed 's/^/trino_jvm=/' >> "$dir/meta.txt" || true
  sampler > "$dir/mem-samples.txt" & local spid=$!
  printf 'query\tround\tseconds\tstatus\n' > "$dir/times.tsv"
  printf 'query\tround\trows_read\tbytes_read\tpeak_user_mem\tplanning\tanalysis\texecution\n' > "$dir/stats.tsv"
  for r in $(seq 1 "$ROUNDS"); do
    mkdir -p "$dir/r$r"
    for f in "$QDIR"/q*.sql; do
      local q; q=$(basename "$f" .sql)
      [ -z "$QUERIES" ] || [[ " $QUERIES " == *" $q "* ]] || continue
      local src="cmp-$name-$q-r$r" t0 t1 st=ok
      t0=$(date +%s.%N)
      if timeout "$QUERY_TIMEOUT" docker exec trino trino --catalog flight --schema tpch --output-format TSV \
           --source "$src" --session "query_max_run_time=${QUERY_TIMEOUT}s" "${sess[@]}" -f "/queries/$q.sql" \
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
  { echo "trino $(mem_peak trino "$dir/mem-samples.txt")"
    for h in $HOST_LIST; do echo "$h $(mem_peak "$h" "$dir/mem-samples.txt")"; done; } > "$dir/memory.txt"
  # EXPLAIN after the timed rounds (the cold round stays cold): the plan fragment that reaches the server
  mkdir -p "$dir/explain"
  for q in $EXPLAIN_QUERIES; do
    { echo "EXPLAIN"; cat "$QDIR/$q.sql"; } > "$WORK/explain-$q.sql"
    docker cp "$WORK/explain-$q.sql" trino:/tmp/explain.sql >/dev/null
    docker exec trino trino --catalog flight --schema tpch "${sess[@]}" -f /tmp/explain.sql > "$dir/explain/$q.txt" 2>&1 || true
    if [ "$PLAN_LOG" = 1 ]; then
      echo "---- PLAN $name $q ----"
      grep -v -e 'Layout:' -e '^\s*│\?\s*\$hashvalue' "$dir/explain/$q.txt" | sed 's/^"//; s/"$//' | cut -c1-260 || true
    fi
  done
  docker logs trino > "$dir/trino.log" 2>&1 || true
  for h in $HOST_LIST; do docker logs "$h" > "$dir/$h.log" 2>&1 || true; done
  docker rm -f trino $HOST_LIST >/dev/null 2>&1 || true
}

for p in $PASSES; do
  IFS=: read -r name version zip props build <<< "$p"
  if [ -n "$zip" ] && [ -f "$zip" ]; then run_pass "$name" "$version" "$zip" "$props" "$build"
  else mkdir -p "$OUT/$name"; printf 'pass=%s\ntrino=%s\nnot built\n' "$name" "$version" > "$OUT/$name/meta.txt"; fi
done
python3 "$REPO/e2e/compare/report.py" "$OUT" "$REF" | tee "$OUT/report.md"
