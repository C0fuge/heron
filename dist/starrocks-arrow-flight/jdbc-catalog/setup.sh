#!/usr/bin/env bash
# Set up a StarRocks JDBC external catalog that talks to the Arrow Flight SQL server
# through the Apache Arrow Flight SQL JDBC driver. No StarRocks source changes needed.
#
# Usage: ./setup.sh [FLIGHT_HOST] [FLIGHT_PORT]
set -euo pipefail

FLIGHT_HOST="${1:-localhost}"
FLIGHT_PORT="${2:-32010}"
DRIVER_VERSION="18.1.0"
DRIVER_JAR="flight-sql-jdbc-driver-${DRIVER_VERSION}.jar"
DEST_DIR="${DEST_DIR:-$PWD}"

url="https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/${DRIVER_VERSION}/${DRIVER_JAR}"
echo "Downloading $DRIVER_JAR ..."
curl -fL -o "${DEST_DIR}/${DRIVER_JAR}" "$url"
echo "Saved to ${DEST_DIR}/${DRIVER_JAR}"
echo

cat <<EOF
Next steps
----------
1. Put the driver where every FE node can read it (a local path or an http(s) URL):
     ${DEST_DIR}/${DRIVER_JAR}

2. Create the catalog (adjust user/token; drop useEncryption=false for TLS):

CREATE EXTERNAL CATALOG flight_jdbc PROPERTIES (
    "type"         = "jdbc",
    "user"         = "test-user",
    "password"     = "test-token",
    "jdbc_uri"     = "jdbc:arrow-flight-sql://${FLIGHT_HOST}:${FLIGHT_PORT}?useEncryption=false",
    "driver_url"   = "file://${DEST_DIR}/${DRIVER_JAR}",
    "driver_class" = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
);

3. Use it:
   SET CATALOG flight_jdbc;
   SHOW DATABASES;
   SELECT * FROM <schema>.<table> LIMIT 10;

Trade-off: correctness now, but one JDBC stream per query — no per-endpoint
parallelism that a native connector would give.
EOF
