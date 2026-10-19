#!/usr/bin/env bash
# Runs the plugin against real Trino *server tarballs* (the way a bare-metal / offline host
# runs Trino, not the Docker image), one Trino line at a time, on JDK 25, against one Flight
# SQL server. Everything is podman containers on a private network; nothing is left behind.
#
#   TARBALLS=/dir/with/trino-server-<v>.tar.gz      (455, 477, 483, ...)
#   PLUGINS=/dir/with/trino-arrow-flight-plugin-trino-<v>.zip
#   FLIGHT_SRC=/path/to/arrow-flight-server         (target/arrow-flight-server-1.0.0.jar + target/libs/)
#   DATA=/dir/containing/demo/t/data.parquet
#   ./version-matrix.sh 455 477 483
#
# Extra check per version: the plugin built for a DIFFERENT line is tried too, to show it fails
# loudly (that is the "versions don't match" symptom) — pass CROSS=1 to enable.
set -euo pipefail

TARBALLS="${TARBALLS:?}"; PLUGINS="${PLUGINS:?}"; FLIGHT_SRC="${FLIGHT_SRC:?}"; DATA="${DATA:?}"
WORK="${WORK:-$(mktemp -d)}"
NET=afc-matrix
JDK_IMAGE=docker.io/library/eclipse-temurin:25-jdk

cleanup() { podman rm -f afc-flight afc-trino >/dev/null 2>&1 || true; podman network rm $NET >/dev/null 2>&1 || true; }
[ "${KEEP:-0}" = 1 ] || trap cleanup EXIT
cleanup
podman network create $NET >/dev/null

echo "== flight server"
podman run -d --name afc-flight --network $NET \
  -v "$FLIGHT_SRC/target":/app:ro,z -v "$DATA":/parquet:ro,z -w /app \
  docker.io/library/eclipse-temurin:21-jdk \
  java --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
       -DnumServers=1 -cp "/app/arrow-flight-server-1.0.0.jar:/app/libs/*" \
       pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
       --data-dir /parquet --port 32010 --hosts 127.0.0.1 --localhost afc-flight --metrics-enabled false >/dev/null
until podman logs afc-flight 2>&1 | grep -q 'Server Arrow Flight SQL started'; do sleep 2; done

mkdir -p "$WORK/etc/catalog"
cat > "$WORK/etc/catalog/flight.properties" <<EOF
connector.name=arrow_flight
arrow-flight.host=afc-flight
arrow-flight.port=32010
arrow-flight.bearer-token=ignored-by-server
EOF
cat > "$WORK/etc/config.properties" <<EOF
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
EOF
# the same flags the offline bundle's jvm.config.snippet adds
JVM=(-Xmx2G -XX:+UseG1GC --add-modules=jdk.incubator.vector -Djdk.attach.allowAttachSelf=true -XX:+EnableDynamicAgentLoading
     --add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED
     --sun-misc-unsafe-memory-access=allow -Darrow.enable_unsafe_memory_access=true)

run_one() {  # <trino version> <plugin line>
  local v=$1 line=$2 home="$WORK/trino-server-$1"
  if [ ! -d "$home/lib" ]; then
    mkdir -p "$home"
    tar xzf "$TARBALLS/trino-server-$v.tar.gz" -C "$WORK" "trino-server-$v/lib" "trino-server-$v/plugin/tpch"
  fi
  rm -rf "$WORK/plugin-$line"; mkdir -p "$WORK/plugin-$line"
  unzip -q -j "$PLUGINS/trino-arrow-flight-plugin-trino-$line.zip" -d "$WORK/plugin-$line"

  podman rm -f afc-trino >/dev/null 2>&1 || true
  podman run -d --name afc-trino --network $NET \
    -v "$home/lib":/trino/lib:ro,z -v "$home/plugin/tpch":/trino/plugin/tpch:ro,z \
    -v "$WORK/plugin-$line":/trino/plugin/arrow-flight:ro,z -v "$WORK/etc":/trino/etc:ro,z \
    -w /trino $JDK_IMAGE \
    java "${JVM[@]}" -Dnode.environment=matrix -Dnode.id=matrix-1 -Dnode.data-dir=/tmp/trino-data \
         -Dconfig=etc/config.properties -Dcatalog.config-dir=etc/catalog \
         -Dplugin.dir=plugin -cp '/trino/lib/*' io.trino.server.TrinoServer >/dev/null
  until podman logs afc-trino 2>&1 | grep -qE 'SERVER STARTED|Error injecting|ERROR.*main'; do
    podman inspect -f '{{.State.Running}}' afc-trino | grep -q true || break; sleep 3; done
  if ! podman logs afc-trino 2>&1 | grep -q 'SERVER STARTED'; then
    echo "   !! Trino $v did not start with plugin-$line:"; podman logs afc-trino 2>&1 | grep -E 'Exception|Error' | head -5; return 1; fi

  local cli="$WORK/trino-cli.jar"
  q() { podman exec afc-trino java --enable-native-access=ALL-UNNAMED -jar /cli.jar --server localhost:8080 --output-format TSV --execute "$1" 2>&1 | grep -v -E "^WARNING|jline|^\s*$"; }
  podman cp "$cli" afc-trino:/cli.jar
  local fail=0
  check() {  # <label> <sql> <expected substring>
    local out; out=$(q "$2") || true
    if grep -qxF -- "$3" <<<"$out"; then echo "   ok   $1"; else echo "   FAIL $1"; sed 's/^/        /' <<<"$out" | head -6; fail=1; fi
  }
  # expected values are for the e2e demo table (50000 rows, see README)
  check "show schemas"        "SHOW SCHEMAS FROM flight"                                   "demo"
  check "describe"            "DESCRIBE flight.demo.t"                                     "string_col	varchar		"
  check "count(*)"            "SELECT count(*) FROM flight.demo.t"                          "50000"
  check "filter pushdown"     "SELECT count(*) FROM flight.demo.t WHERE id < 100 AND bool_col" "50"
  check "projection+order"    "SELECT id, string_col FROM flight.demo.t WHERE id BETWEEN 3 AND 4 ORDER BY id" "4	str_0004"
  check "aggregates"          "SELECT count(DISTINCT string_col), sum(bigint_col), min(date_col), max(double_col) FROM flight.demo.t" "1000	225000	2020-01-01	0.9999843152608262"
  local plan; plan=$(q "EXPLAIN SELECT id FROM flight.demo.t WHERE id = 7") || true
  if grep -q 'TableScan' <<<"$plan" && ! grep -q 'Filter' <<<"$plan"; then echo "   ok   predicate pushed (no Filter node)"
  else echo "   FAIL predicate pushdown"; sed 's/^/        /' <<<"$plan" | head -12; fail=1; fi
  return $fail
}

[ -f "$WORK/trino-cli.jar" ] || cp "${TRINO_CLI:?set TRINO_CLI to trino-cli-<v>-executable.jar}" "$WORK/trino-cli.jar"

rc=0
for v in "$@"; do
  echo "== Trino $v + plugin built for $v"
  run_one "$v" "$v" || rc=1
  if [ "${CROSS:-0}" = 1 ]; then
    for other in "$@"; do [ "$other" = "$v" ] && continue
      echo "== Trino $v + plugin built for $other (expected to FAIL)"
      if run_one "$v" "$other" >/dev/null 2>&1; then echo "   unexpected: works"; else echo "   fails, as expected"; fi
    done
  fi
done
exit $rc
