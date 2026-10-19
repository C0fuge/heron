#!/bin/sh
# Runs INSIDE the lab container. Uses the lab's own podman to run the Flight server and
# Trino as nested containers, then queries them. Nothing here touches the host podman.
set -e

echo "== lab: podman info =="
podman version --format '{{.Client.Version}}' || true

echo "== load images from tar (no network) =="
podman load -i /lab/trino.tar
podman load -i /lab/temurin21.tar

podman rm -f flight-server trino-e2e 2>/dev/null || true
podman network rm flightnet 2>/dev/null || true
podman network create flightnet

mkdir -p /lab/conf
cat > /lab/conf/flight.properties <<EOF
connector.name=arrow_flight
arrow-flight.host=flight-server
arrow-flight.port=32010
arrow-flight.bearer-token=ignored-by-server
arrow-flight.default-schema=demo
EOF
cat > /lab/conf/jvm.config <<'EOF'
-server
-XX:MaxRAMPercentage=45
-Djdk.attach.allowAttachSelf=true
-XX:+EnableDynamicAgentLoading
--add-opens=java.base/java.nio=ALL-UNNAMED
--enable-native-access=ALL-UNNAMED
-Darrow.enable_unsafe_memory_access=true
EOF
cat > /lab/conf/config.properties <<'EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
EOF

echo "== start flight-server (nested) =="
podman run -d --name flight-server --network flightnet \
  -v /data/flightjar:/app:ro -v /data/parquet:/parquet:ro -w /app \
  docker.io/library/eclipse-temurin:21-jdk \
  java --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.enable_unsafe_memory_access=true \
       -DnumServers=1 -cp "/app/arrow-flight-server-1.0.0.jar:/app/libs/*" \
       pro.surpin.data.arrowflight.server.HadoopArrowFlightServer \
       --data-dir /parquet --port 32010 --hosts 127.0.0.1 --localhost flight-server --metrics-enabled false
until podman logs flight-server 2>&1 | grep -q 'Server Arrow Flight SQL started'; do sleep 2; done
echo "   flight-server up"

echo "== start trino-e2e (nested) =="
podman run -d --name trino-e2e --network flightnet \
  -v /data/plugin:/usr/lib/trino/plugin/arrow-flight:ro \
  -v /lab/conf/flight.properties:/etc/trino/catalog/flight.properties:ro \
  -v /lab/conf/jvm.config:/etc/trino/jvm.config:ro \
  -v /lab/conf/config.properties:/etc/trino/config.properties:ro \
  docker.io/trinodb/trino:455
until podman exec trino-e2e curl -s http://localhost:8080/v1/info 2>/dev/null | grep -q '"starting":false'; do sleep 4; done
echo "   trino up"

echo "== queries =="
run() { echo "--- $1"; podman exec -i trino-e2e trino --output-format ALIGNED --execute "$1"; echo; }
run "SHOW SCHEMAS FROM flight"
run "SHOW TABLES FROM flight.demo"
run "DESCRIBE flight.demo.t"
run "SELECT count(*) n FROM flight.demo.t"
run "SELECT id, bool_col, bigint_col, string_col, date_col FROM flight.demo.t WHERE id < 5 ORDER BY id"
run "SELECT string_col, count(*) c, sum(bigint_col) s FROM flight.demo.t WHERE bigint_col >= 6 GROUP BY string_col ORDER BY c DESC, string_col LIMIT 5"
run "EXPLAIN (TYPE LOGICAL) SELECT id, string_col FROM flight.demo.t WHERE id BETWEEN 10 AND 20 AND bool_col = true"
echo "== nested containers still running inside lab: =="
podman ps --format '{{.Names}} {{.Status}}'
