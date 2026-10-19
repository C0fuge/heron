#!/usr/bin/env bash
# End-to-end test as a "lab": one privileged outer container runs its OWN podman, and the
# Flight SQL server + Trino run as nested containers inside it. The host podman only ever
# sees the `lab` container.
#
#   host podman
#   └── lab  (quay.io/podman/stable, --privileged)          ← nested podman
#       ├── flight-server  (eclipse-temurin:21-jdk + arrow-flight-server jar)
#       └── trino-e2e      (trinodb/trino:455 + this plugin)
#
# Prereqs on the host:
#   - podman
#   - this repo built:   ../trino-arrow-flight/target/trino-arrow-flight-*.zip
#   - $FLIGHT_SRC = a checkout of https://github.com/VadimSurpin/arrow-flight-server with
#     target/arrow-flight-server-*.jar and target/libs/ built (JDK 21), e.g.:
#       podman run --rm -v "$FLIGHT_SRC":/s:z -w /s docker.io/library/maven:3.9-eclipse-temurin-21 sh -c '\
#         mvn -q -DskipTests -Dcheckstyle.skip -Dspotbugs.skip -Denforcer.skip=true package && \
#         mvn -q -o dependency:copy-dependencies -DincludeScope=test -DoutputDirectory=target/libs'
#   - a Parquet table at $DATA/demo/t/data.parquet (schema: id int, bool_col bool,
#     bigint_col bigint, double_col double, string_col varchar, date_col date)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
FLIGHT_SRC="${FLIGHT_SRC:?set FLIGHT_SRC to your arrow-flight-server checkout}"
DATA="${DATA:?set DATA to a dir containing demo/t/data.parquet}"
LAB="$HERE/lab-work"

mkdir -p "$LAB/plugin/arrow-flight"

echo "== unpack the plugin bundle (host) =="
rm -rf "$LAB/plugin/arrow-flight"; mkdir -p "$LAB/plugin/arrow-flight"
podman run --rm -v "$REPO/trino-arrow-flight/target":/t:ro -v "$LAB/plugin/arrow-flight":/o:z \
  docker.io/library/eclipse-temurin:21-jdk \
  bash -c 'cd /o && jar xf /t/trino-arrow-flight-0.1.0-SNAPSHOT.zip && mv trino-arrow-flight-0.1.0-SNAPSHOT/* . && rmdir trino-arrow-flight-0.1.0-SNAPSHOT'

echo "== export the images the lab will run, from the host store (no re-download inside) =="
[ -f "$LAB/trino.tar" ]     || podman save -o "$LAB/trino.tar"     docker.io/trinodb/trino:455
[ -f "$LAB/temurin21.tar" ] || podman save -o "$LAB/temurin21.tar" docker.io/library/eclipse-temurin:21-jdk

cp "$HERE/lab-inside.sh" "$LAB/inside.sh"; chmod +x "$LAB/inside.sh"

echo "== start the lab (podman-in-podman) =="
podman rm -f lab >/dev/null 2>&1 || true
podman run -d --name lab --privileged --device /dev/fuse \
  -v "$LAB":/lab:z \
  -v "$LAB/plugin/arrow-flight":/data/plugin:z,ro \
  -v "$DATA":/data/parquet:z,ro \
  -v "$FLIGHT_SRC/target":/data/flightjar:z,ro \
  quay.io/podman/stable sleep infinity

echo "== run the nested stack + queries inside the lab =="
podman exec lab sh /lab/inside.sh

cat <<EOF

Nested containers live inside 'lab'. To poke at them:
  podman exec lab podman exec -i trino-e2e trino --execute "SELECT * FROM flight.demo.t LIMIT 5"
Tear down:
  podman rm -f lab
EOF
