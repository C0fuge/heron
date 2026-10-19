# Dead end — do not use

Tested live against a real StarRocks 4.1.4 instance: `CREATE EXTERNAL CATALOG ... type="jdbc"`
with the Arrow Flight SQL JDBC driver **does not work**. StarRocks' JDBC catalog hardcodes
a fixed allow-list of driver classes (MySQL/MariaDB, PostgreSQL, Oracle, SQL Server,
ClickHouse) in `JDBCMetadata.createSchemaResolver()` and rejects anything else:

```
ERROR 1064 (HY000): org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver not support yet
```

This directory (`setup.sh`, `catalog.sql`) is kept only as a record of that finding — see
`../DESIGN.md` for what actually works (`../direct-parquet-access/`).
