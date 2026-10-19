-- StarRocks: reach the Arrow Flight SQL server via the JDBC catalog + Arrow Flight SQL JDBC driver.
-- Run ./setup.sh first to fetch flight-sql-jdbc-driver-18.1.0.jar, then edit the paths below.

CREATE EXTERNAL CATALOG flight_jdbc PROPERTIES (
    "type"         = "jdbc",
    "user"         = "test-user",
    "password"     = "test-token",                       -- bearer token goes in "password"
    "jdbc_uri"     = "jdbc:arrow-flight-sql://localhost:32010?useEncryption=false",
    "driver_url"   = "file:///opt/starrocks/fe/lib/flight-sql-jdbc-driver-18.1.0.jar",
    "driver_class" = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
);

SET CATALOG flight_jdbc;
SHOW DATABASES;
-- SELECT * FROM test_schema.test_table LIMIT 10;

-- DROP CATALOG flight_jdbc;
