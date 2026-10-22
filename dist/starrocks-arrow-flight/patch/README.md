# StarRocks FE patch

StarRocks has no connector plugin SPI — connectors are hardcoded in the
`com.starrocks.connector.ConnectorType` enum. To add the `arrow_flight` catalog type you
must patch the FE source and rebuild.

```bash
SR=<path to your StarRocks checkout>

# 1. drop the FE-side connector classes into the source tree
mkdir -p "$SR/fe/fe-core/src/main/java/com/starrocks/connector/arrowflight"
cp ../src/starrocks/java/com/starrocks/connector/arrowflight/*.java \
   "$SR/fe/fe-core/src/main/java/com/starrocks/connector/arrowflight/"

# 2. register the type in the ConnectorType enum
git -C "$SR" apply "$(pwd)/ConnectorType.java.patch"

# 3. make arrow-flight-common + the Arrow jars visible to fe-core
#    (add them as dependencies of fe/fe-core/pom.xml, or mvn install arrow-flight-common
#     and add <dependency>pro.surpin:arrow-flight-common</dependency> plus
#     org.apache.arrow:flight-sql there)

# 4. build the FE (needs JDK 17 + thrift + protoc)
cd "$SR/fe" && mvn -q -DskipTests install
```

After that a catalog can be created:

```sql
CREATE EXTERNAL CATALOG flight PROPERTIES (
  "type" = "arrow_flight",
  "arrow_flight.host" = "localhost",
  "arrow_flight.port" = "32010",
  "arrow_flight.user" = "test-user",
  "arrow_flight.bearer_token" = "test-token"
);
SHOW DATABASES FROM flight;          -- works (metadata)
SELECT * FROM flight.test_schema.t;  -- fails: no BE scan path yet, see ../DESIGN.md
```

For a setup that can actually read rows today, use the JDBC route in `../jdbc-catalog/`.
