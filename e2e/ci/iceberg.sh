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
# iceberg_check (after the parquet checks): each table equals its Parquet counterpart read through
# the same plugin (row count + order-insensitive checksum per column, column names and types),
# TPC-H queries on schema ice equal the DuckDB reference, and a commit made between two queries
# is seen by the later query only (within ICEBERG_COMMIT_WAIT seconds; complete mode refreshes its
# views every 60 s). Server log lines 'answeredCompletely' (partial-mode fallbacks) are reported.

SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.3}"
ICEBERG_SPARK_JAR="${ICEBERG_SPARK_JAR:-https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar}"
ICEBERG_QUERIES="${ICEBERG_QUERIES:-q01 q03 q04 q06 q12 q13 q18 q22}"
ICEBERG_COMMIT_WAIT="${ICEBERG_COMMIT_WAIT:-100}"
# server flags (start_server); the warehouse is a local directory shared by all nodes
ICEBERG_SERVER_ARGS=(-Dcatalog=iceberg-hadoop -Darrowflight.iceberg-hadoop.warehouse=/wh
                     -Darrowflight.catalog.localRootsShared=true)

spark_sql() {  # sql-file
  docker run --rm --network none -u 0 -e HOME=/tmp \
    -v "$WORK/wh":/wh -v "$WORK/s3/lake/parquet":/data:ro -v "$WORK/spark":/spark:ro \
    "$SPARK_IMAGE" /opt/spark/bin/spark-sql --master 'local[4]' --driver-memory 3g \
      --jars /spark/iceberg-spark-runtime.jar \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.wh=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.wh.type=hadoop --conf spark.sql.catalog.wh.warehouse=file:///wh \
      --conf spark.ui.enabled=false -f "/spark/$1" 2>&1 | grep -vE '^\s*$| INFO |^WARNING: ' || true
}

iceberg_generate() {
  log "iceberg: DuckDB iceberg + avro extensions, Spark tables in $WORK/wh"
  local ext
  for ext in iceberg avro; do
    curl -fsSL "http://extensions.duckdb.org/$DUCKDB_EXT_VERSION/linux_amd64/$ext.duckdb_extension.gz" \
      | gunzip > "$EXT_DIR/$ext.duckdb_extension"
  done
  mkdir -p "$WORK/wh" "$WORK/spark"
  curl -fsSL -o "$WORK/spark/iceberg-spark-runtime.jar" "$ICEBERG_SPARK_JAR"
  local p='parquet.`file:///data/tpch'
  cat > "$WORK/spark/generate.sql" <<SQL
CREATE NAMESPACE IF NOT EXISTS wh.ice;
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
  docker pull -q "$SPARK_IMAGE" >/dev/null
  spark_sql generate.sql | tee "$OUT/iceberg-generate.log" | tail -20
  find "$WORK/wh" -name 'version-hint.text' | sort
  find "$WORK/wh" -type f | sed 's#/[^/]*$##' | sort | uniq -c | head -40 > "$OUT/iceberg-layout.txt"
  [ -f "$WORK/wh/ice/commits/metadata/version-hint.text" ] || { log "iceberg: Spark did not write the tables"; return 1; }
}

tq() { docker exec trino trino --catalog flight --output-format TSV --execute "$1" 2>&1; }

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
    ice_out "$check" FAIL "columns differ: ice.$it [$(echo $icols)] vs expected [$(echo $exp_cols)]"
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
  local before after waited=0
  before=$(tq "SELECT count(*), max(c_custkey) FROM ice.commits")
  spark_sql commit.sql > "$OUT/iceberg-commit.log"
  while :; do
    after=$(tq "SELECT count(*), max(c_custkey) FROM ice.commits")
    [ "$after" = $'6000\t6000' ] && break
    [ "$after" = $'5000\t5000' ] && [ $waited -lt "$ICEBERG_COMMIT_WAIT" ] || break
    sleep 5; waited=$((waited + 5))
  done
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
