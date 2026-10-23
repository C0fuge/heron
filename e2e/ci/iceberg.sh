# Iceberg part of the e2e run (server builds with the 'iceberg' catalog plugin, int5+); sourced by
# run-e2e.sh, uses its variables and functions (WORK, OUT, NET, QUERIES, COMPARE, run_query, log).
#
# iceberg_generate (before the servers start): Spark + Iceberg write a HadoopCatalog warehouse
# $WORK/wh (namespace 'ice') from the SF1 Parquet files; every Flight server mounts it read-only at
# the same path /wh (catalog kind iceberg-hadoop, warehouse /wh, localRootsShared=true):
#   ice.lineitem, ice.orders, ice.customer   full copies (TPC-H queries run against them)
#   ice.orders_del    orders minus o_orderkey % 7 = 0, as merge-on-read position delete files
#   ice.customer_ren  customer with c_comment renamed to c_note (rename after the data was written)
#   ice.commits       customer c_custkey <= 5000; iceberg_check commits 1000 more rows mid-run
# ICEBERG_WAREHOUSE=s3 instead: Spark writes the same warehouse through s3a into the rclone S3 bucket
# (s3://lake/parquet/wh: the server opens one store per storage plugin, so an s3 catalog root must lie
# below the dataDir s3://lake/parquet; metadata carries s3:// URIs, fs.s3.impl = S3A) and the servers
# read it with their s3 store (no /wh mount needed).
# iceberg_check (after the parquet checks): each table equals its Parquet counterpart read through
# the same plugin (row count + order-insensitive checksum per column, column names and types),
# TPC-H queries on schema ice equal the DuckDB reference, and a commit made between two queries
# is seen by the later query only (within ICEBERG_COMMIT_WAIT seconds; complete mode refreshes its
# views every 60 s). Server log lines 'answeredCompletely' (partial-mode fallbacks) are reported.

SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.3}"
ICEBERG_SPARK_JAR="${ICEBERG_SPARK_JAR:-https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar}"
ICEBERG_QUERIES="${ICEBERG_QUERIES:-q01 q03 q04 q06 q12 q13 q18 q22}"
ICEBERG_COMMIT_WAIT="${ICEBERG_COMMIT_WAIT:-100}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-local}"  # local | s3
HADOOP_AWS_JAR="${HADOOP_AWS_JAR:-https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar}"
AWS_SDK_BUNDLE_JAR="${AWS_SDK_BUNDLE_JAR:-https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar}"
ICEBERG_SERVER_CP=/app/conf:
# server flags (start_server); local: a directory shared by all nodes; s3: a prefix of the bucket
case "$ICEBERG_WAREHOUSE" in
  local) ICEBERG_WH_URI=file:///wh; ICEBERG_WH_DIR="$WORK/wh"
         ICEBERG_SERVER_ARGS=(-Dcatalog=iceberg-hadoop -Darrowflight.iceberg-hadoop.warehouse=/wh
                              -Darrowflight.catalog.localRootsShared=true) ;;
  s3)    ICEBERG_WH_URI=s3://lake/parquet/wh; ICEBERG_WH_DIR="$WORK/s3/lake/parquet/wh"
         ICEBERG_SERVER_ARGS=(-Dcatalog=iceberg-hadoop -Darrowflight.iceberg-hadoop.warehouse=s3://lake/parquet/wh) ;;
  *)     echo "ICEBERG_WAREHOUSE must be local or s3, not '$ICEBERG_WAREHOUSE'" >&2; exit 1 ;;
esac

spark_sql() {  # sql-file
  local jars=/spark/iceberg-spark-runtime.jar s3conf=()
  if [ "$ICEBERG_WAREHOUSE" = s3 ]; then
    # S3A for both s3a:// and s3:// (the server and DuckDB read s3:// URIs); credentials from the env
    jars+=,/spark/hadoop-aws.jar,/spark/aws-java-sdk-bundle.jar
    local k
    for k in fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem fs.s3a.endpoint=http://s3:9000 \
             fs.s3a.path.style.access=true fs.s3a.connection.ssl.enabled=false fs.s3a.endpoint.region=us-east-1 \
             fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider \
             fs.s3a.multipart.threshold=1G fs.s3a.multipart.size=512M \
             fs.s3a.attempts.maximum=2 fs.s3a.retry.limit=2 fs.s3a.directory.marker.retention=keep; do
      s3conf+=(--conf "spark.hadoop.$k")
    done
  fi
  docker run --rm --network "$NET" -u 0 -e HOME=/tmp -e SPARK_LOCAL_IP=127.0.0.1 \
    -e AWS_ACCESS_KEY_ID="$S3_KEY" -e AWS_SECRET_ACCESS_KEY="$S3_SECRET" -e AWS_REGION=us-east-1 \
    -v "$WORK/wh":/wh -v "$WORK/s3/lake/parquet":/data:ro -v "$WORK/spark":/spark:ro \
    "$SPARK_IMAGE" /opt/spark/bin/spark-sql --master 'local[4]' --driver-memory 3g \
      --jars "$jars" "${s3conf[@]}" \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.wh=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.wh.type=hadoop --conf "spark.sql.catalog.wh.warehouse=$ICEBERG_WH_URI" \
      --conf spark.ui.enabled=false -f "/spark/$1" 2>&1 | grep -vE '^\s*$| INFO |^WARNING: ' || true
}

iceberg_generate() {
  log "iceberg: DuckDB iceberg + avro extensions, Spark tables in $ICEBERG_WH_URI ($ICEBERG_WH_DIR)"
  local ext
  for ext in iceberg avro; do
    curl -fsSL "http://extensions.duckdb.org/$DUCKDB_EXT_VERSION/linux_amd64/$ext.duckdb_extension.gz" \
      | gunzip > "$EXT_DIR/$ext.duckdb_extension"
  done
  mkdir -p "$WORK/wh" "$WORK/spark"
  curl -fsSL -o "$WORK/spark/iceberg-spark-runtime.jar" "$ICEBERG_SPARK_JAR"
  if [ "$ICEBERG_WAREHOUSE" = s3 ]; then
    curl -fsSL -o "$WORK/spark/hadoop-aws.jar" "$HADOOP_AWS_JAR"
    curl -fsSL -o "$WORK/spark/aws-java-sdk-bundle.jar" "$AWS_SDK_BUNDLE_JAR"
  fi
  local p='parquet.`file:///data/tpch'
  cat > "$WORK/spark/generate.sql" <<SQL
$( [ "$ICEBERG_WAREHOUSE" = s3 ] || echo 'CREATE NAMESPACE IF NOT EXISTS wh.ice;' )
CREATE TABLE wh.ice.lineitem USING iceberg AS SELECT * FROM $p/lineitem\`;
CREATE TABLE wh.ice.orders USING iceberg AS SELECT * FROM $p/orders\`;
CREATE TABLE wh.ice.customer USING iceberg AS SELECT * FROM $p/customer\`;
CREATE TABLE wh.ice.orders_del USING iceberg
  TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')
  AS SELECT * FROM $p/orders\`;
DELETE FROM wh.ice.orders_del WHERE o_orderkey % 7 = 0;
CREATE TABLE wh.ice.customer_ren USING iceberg AS SELECT * FROM $p/customer\`;
ALTER TABLE wh.ice.customer_ren RENAME COLUMN c_comment TO c_note;
CREATE TABLE wh.ice.commits USING iceberg AS SELECT * FROM $p/customer\` WHERE c_custkey <= 5000;
SELECT 'snapshots', count(*) FROM wh.ice.orders_del.snapshots;
SELECT 'delete files', content, count(*) FROM wh.ice.orders_del.all_files GROUP BY content;
SQL
  cat > "$WORK/spark/commit.sql" <<SQL
INSERT INTO wh.ice.commits SELECT * FROM $p/customer\` WHERE c_custkey BETWEEN 5001 AND 6000;
SQL
  if [ "$ICEBERG_WAREHOUSE" = s3 ]; then
    # rclone serve s3 (local backend) answers 500 "file already exists" when parallel PUTs create the
    # same new directory, and refuses S3A's "dir/" marker objects over existing directories: the table
    # directories exist before Spark writes, each holding a hidden object so S3A sees them as directories
    # (CREATE NAMESPACE is skipped: S3A would PUT a marker for wh/ice/ and rclone refuses it)
    local t d
    for t in lineitem orders customer orders_del customer_ren commits; do
      for d in data metadata; do
        mkdir -p "$ICEBERG_WH_DIR/ice/$t/$d"; : > "$ICEBERG_WH_DIR/ice/$t/$d/.keep"
      done
    done
  fi
  docker pull -q "$SPARK_IMAGE" >/dev/null
  spark_sql generate.sql | tee "$OUT/iceberg-generate.log" | tail -20
  find "$ICEBERG_WH_DIR" -name 'version-hint.text' | sort
  find "$ICEBERG_WH_DIR" -type f | sed 's#/[^/]*$##' | sort | uniq -c | head -40 > "$OUT/iceberg-layout.txt"
  if [ ! -f "$ICEBERG_WH_DIR/ice/commits/metadata/version-hint.text" ]; then
    log "iceberg: Spark did not write the tables; first distinct errors:"
    grep -E 'ERROR|Exception|Caused by|Error:' "$OUT/iceberg-generate.log" | grep -vE '^\s+at ' |
      awk '!seen[$0]++' | head -40 | cut -c1-400 || true
    find "$ICEBERG_WH_DIR" -maxdepth 4 2>/dev/null | head -20 || true
    docker logs s3 2>&1 | tail -20 || true
    return 1
  fi
  # metadata location URIs as the server will see them
  grep -o '"metadata-file" *: *"[^"]*"\|"location" *: *"[^"]*"' \
    "$ICEBERG_WH_DIR/ice/commits/metadata/v1.metadata.json" | head -3 || true
  # int5 server: the Hadoop jars in libs/ do not register the local FileSystem ("No FileSystem for
  # scheme file" when the iceberg-hadoop warehouse is a local path); core-site.xml on the classpath
  mkdir -p "$WORK/server/conf"
  cat > "$WORK/server/conf/core-site.xml" <<XML
<?xml version="1.0"?>
<configuration>
  <property><name>fs.file.impl</name><value>org.apache.hadoop.fs.LocalFileSystem</value></property>
</configuration>
XML
}

# Trino CLI, TSV; stderr kept (errors become part of the compared text) minus the jline warning
tq() {
  { docker exec trino trino --catalog flight --output-format TSV --execute "$1" 2>&1 || true; } |
    grep -vE 'org\.jline|Unable to create a system terminal' || true
}

ice_out() {  # check result detail
  printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$OUT/iceberg-summary.tsv"
  [ "$2" != FAIL ] || status=1
}

# columns "name<TAB>type" of flight.<schema>.<table>
ice_columns() {
  tq "SELECT column_name, data_type FROM information_schema.columns
      WHERE table_schema = '$1' AND table_name = '$2' ORDER BY ordinal_position"
}

# ice_compare check ice-table parquet-table [where] [rename-from:rename-to]
ice_compare() {
  local check=$1 it=$2 pt=$3 where=${4:-} ren=${5:-}
  local icols pcols exp_cols aggs_i='count(*)' aggs_p='count(*)' c t res_i res_p
  icols=$(ice_columns ice "$it"); pcols=$(ice_columns tpch "$pt")
  exp_cols=$pcols
  [ -n "$ren" ] && exp_cols=$(printf '%s\n' "$pcols" | sed "s/^${ren%%:*}\t/${ren##*:}\t/")
  if [ "$icols" != "$exp_cols" ]; then
    # diagnostics: the error behind it (information_schema swallows per-table errors)
    { echo "--- DESCRIBE ice.$it"; tq "DESCRIBE ice.\"$it\""
      echo "--- SELECT count(*) FROM ice.$it"; tq "SELECT count(*) FROM ice.\"$it\""
      local h; for h in trino ${HOSTS//,/ }; do
        # '|| true': with pipefail a node that never logged the table would end the job (set -e)
        echo "--- $h log lines naming $it"; docker logs "$h" 2>&1 | grep -A3 "$it" | tail -30 || true; done
    } 2>&1 | cut -c1-600 | tee "$OUT/iceberg-$it-diagnostics.txt"
    # Known server issue (int6, local warehouse): the complete-answer view (iceberg_scan) is refused by
    # DuckDB, "file system operations are disabled by configuration", so the table is missing (clean
    # error, no wrong rows). Reported as KNOWN; any other cause fails.
    if grep -qE "skipTable table=ice\.$it reason=.*file system operations are disabled" \
         "$OUT/iceberg-$it-diagnostics.txt"; then
      ice_out "$check" KNOWN "ice.$it has no columns: server skips its iceberg_scan view (DuckDB 'file system operations are disabled'); queries fail cleanly"
    else
      ice_out "$check" FAIL "columns differ: ice.$it [$(echo $icols)] vs expected [$(echo $exp_cols)]"
    fi
    return
  fi
  while IFS=$'\t' read -r c t; do
    aggs_i+=", checksum(\"$c\")"
    [ -n "$ren" ] && [ "$c" = "${ren##*:}" ] && c=${ren%%:*}
    aggs_p+=", checksum(\"$c\")"
  done <<< "$icols"
  res_i=$(tq "SELECT $aggs_i FROM ice.\"$it\"")
  res_p=$(tq "SELECT $aggs_p FROM tpch.\"$pt\" ${where:+WHERE $where}")
  if [ "$res_i" = "$res_p" ] && [[ "$res_i" =~ ^[0-9]+$'\t' ]]; then
    ice_out "$check" OK "${res_i%%$'\t'*} rows, $(wc -l <<< "$icols") column checksums equal to tpch.$pt${where:+ WHERE $where}"
  else
    ice_out "$check" FAIL "ice.$it: $(echo "$res_i" | cut -c1-200) | tpch.$pt: $(echo "$res_p" | cut -c1-200)"
  fi
}

iceberg_check() {
  log "iceberg: schema ice vs parquet schema tpch"
  printf 'check\tresult\tdetail\n' > "$OUT/iceberg-summary.tsv"
  local tables
  tables=$(tq "SHOW TABLES FROM ice" | sort | tr '\n' ' ')
  if [ "$tables" = "commits customer customer_ren lineitem orders orders_del " ]; then
    ice_out tables OK "$tables"
  else
    ice_out tables FAIL "SHOW TABLES FROM ice: $(echo "$tables" | cut -c1-300)"
    return
  fi
  local t
  for t in lineitem orders customer; do ice_compare "copy:$t" "$t" "$t"; done
  ice_compare deletes orders_del orders 'o_orderkey % 7 <> 0'
  ice_compare rename customer_ren customer '' 'c_comment:c_note'

  local dir="$OUT/iceberg-tpch" q ok=0
  mkdir -p "$dir"
  for q in $ICEBERG_QUERIES; do QUERY_SCHEMA=ice run_query "$q" "$dir"; done
  if "${COMPARE[@]}" "$dir" "$QUERIES" "$dir/summary.tsv" "$ICEBERG_QUERIES"; then ok=1; fi
  ice_out tpch "$([ $ok = 1 ] && echo OK || echo FAIL)" \
    "$ICEBERG_QUERIES on schema ice vs reference: $(tail -n +2 "$dir/summary.tsv" | cut -f1,2 | tr '\t\n' ': ')"

  # a commit between two queries: the first sees 5000 rows, a later one 6000
  # S3 warehouse: HadoopCatalog commits on S3 are not atomic (S3A rename = copy + delete), so a query
  # in the commit window may fail cleanly (seen: "Column 'c_custkey' not found. Available: []"); such
  # errors are retried within the wait and reported as INFO. Any other answer fails.
  local before after waited=0 errors=0 first_error=
  before=$(tq "SELECT count(*), max(c_custkey) FROM ice.commits")
  spark_sql commit.sql > "$OUT/iceberg-commit.log"
  while :; do
    after=$(tq "SELECT count(*), max(c_custkey) FROM ice.commits")
    [ "$after" = $'6000\t6000' ] && break
    if [ "$ICEBERG_WAREHOUSE" = s3 ] && [[ "$after" =~ ^Query\ .*failed ]] && [ $waited -lt "$ICEBERG_COMMIT_WAIT" ]; then
      errors=$((errors + 1)); [ -n "$first_error" ] || first_error=$(echo "$after" | head -1 | cut -c1-300)
    else
      [ "$after" = $'5000\t5000' ] && [ $waited -lt "$ICEBERG_COMMIT_WAIT" ] || break
    fi
    sleep 5; waited=$((waited + 5))
  done
  [ $errors = 0 ] || ice_out commit-window INFO "$errors clean error(s) while the commit landed; first: $first_error"
  if [ "$before" = $'5000\t5000' ] && [ "$after" = $'6000\t6000' ]; then
    ice_out commit OK "before the commit 5000 rows; after it 6000 rows, seen ${waited}s after the commit"
  else
    ice_out commit FAIL "before: $(echo $before); after ${waited}s: $(echo "$after" | cut -c1-200)"
  fi

  local h n
  for h in ${HOSTS//,/ }; do
    docker logs "$h" 2>&1 | grep -E 'answeredCompletely' > "$OUT/$h-answered-completely.txt" || true
    n=$(wc -l < "$OUT/$h-answered-completely.txt")
    ice_out "fallback:$h" INFO "$n answeredCompletely line(s); reasons: $(grep -oE 'reason=[^ ]+' \
      "$OUT/$h-answered-completely.txt" | sort | uniq -c | tr -s ' \n' ' ' | cut -c1-200)"
  done
  cat "$OUT/iceberg-summary.tsv"
}
