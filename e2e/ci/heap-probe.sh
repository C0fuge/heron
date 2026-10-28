# Trino heap probe (STATUS stand finding 2026-10-26: q18 SF10 fails "Java heap space" with a 6 GB Trino heap whenever a
# table has more than one endpoint, passes with one endpoint). Sourced by run-e2e.sh; uses its variables and functions
# (WORK, OUT, QUERIES, COMPARE, run_query, start_trino, log, status).
#
# heap_probe: for each heap in HEAP_PROBE_XMX Trino is restarted with that -Xmx (no ExitOnOutOfMemoryError, so the
# query fails with the error instead of the JVM exiting) and each query of HEAP_PROBE_QUERIES runs once per setting of
# HEAP_PROBE_SETTINGS:
#   default   plugin defaults
#   hash      flight.aggregation_sorted_merge_enabled=false (old hash merge of the endpoints' groups)
#   nopush    flight.aggregation_pushdown_enabled=false (Trino aggregates the plain scan itself)
# While a query runs, the live heap is sampled every HEAP_PROBE_SAMPLE seconds (jcmd GC.class_histogram = live objects
# after a full GC); the largest sample's total and top classes go to heap-probe/<heap>-<setting>-<query>.histo.
# Results (answer vs the reference, error, seconds, peak live MB) are appended to heap-probe.tsv; HEAP_PROBE_TAG (e.g. eps4)
# prefixes the run directories and is added to the servers column. HEAP_PROBE_FAIL=1 makes a wrong
# answer or an error in a 'default' run fail the job (the regression check); otherwise the probe only reports.
# Trino is restarted with the normal heap afterwards.

HEAP_PROBE_XMX="${HEAP_PROBE_XMX:-640M 1G 1536M}"
HEAP_PROBE_SETTINGS="${HEAP_PROBE_SETTINGS:-default hash nopush}"
HEAP_PROBE_QUERIES="${HEAP_PROBE_QUERIES:-q18}"
HEAP_PROBE_SAMPLE="${HEAP_PROBE_SAMPLE:-2}"
HEAP_PROBE_FAIL="${HEAP_PROBE_FAIL:-0}"

heap_session() {  # setting -> trino CLI --session args
  case "$1" in
    default) ;;
    hash) echo "--session flight.aggregation_sorted_merge_enabled=false" ;;
    nopush) echo "--session flight.aggregation_pushdown_enabled=false" ;;
    *) echo "--session $1" ;;  # raw catalog.property=value
  esac
}

trino_pid() { { docker exec trino jcmd -l 2>&1 || true; } | awk '/TrinoServer/ { print $1; exit }'; }

# Samples the live heap until $1 (a file) exists; keeps the largest histogram in $2.
heap_sampler() {  # stop-file histo-file
  local pid best=0 total
  pid=$(trino_pid)
  [ -n "$pid" ] || { echo "no TrinoServer pid: $(docker exec trino jcmd -l 2>&1 | head -3 | tr '\n' ' ')" > "$2"; return; }
  while [ ! -f "$1" ]; do
    if timeout 30 docker exec trino jcmd "$pid" GC.class_histogram > "$2.tmp" 2>&1; then
      total=$(awk '/^Total/ { print $3 }' "$2.tmp" || true)
      if [ -n "$total" ] && [ "$total" -gt "$best" ]; then best=$total; head -25 "$2.tmp" > "$2"; echo "Total $total" >> "$2"; fi
    fi
    sleep "$HEAP_PROBE_SAMPLE"
  done
  rm -f "$2.tmp"
}

# Writes EXPLAIN of query $1 and SHOW STATS of the TPC-H tables to $2/$1.plan; prints joins, estimates and pushed queries.
heap_plan() {  # query dir
  local t
  { echo "EXPLAIN"; cat "$QUERIES/$1.sql"; } | docker exec -i trino trino --catalog flight --schema tpch --output-format TSV_HEADER \
    > "$2/$1.plan" 2>&1 || true
  for t in customer orders lineitem; do
    echo "--- SHOW STATS FOR $t" >> "$2/$1.plan"
    docker exec trino trino --catalog flight --schema tpch --output-format TSV --execute "SHOW STATS FOR $t" \
      2>&1 | awk -F'\t' '$1 == "" || $1 ~ /key$/' >> "$2/$1.plan" || true
  done
  echo "--- plan of $1 (${HEAP_PROBE_TAG:-plain}):"
  grep -E 'Join\[|SemiJoin|Estimates|Fragment|query=|SHOW STATS|^\t|^[a-z_]+key\t' "$2/$1.plan" | grep -v 'cpu:\|Layout' | cut -c1-260 | head -60 || true
}

heap_probe() {
  local xmx setting q dir sess stop histo t0 secs result detail peak
  log "heap probe: Trino -Xmx $HEAP_PROBE_XMX x settings $HEAP_PROBE_SETTINGS x $HEAP_PROBE_QUERIES ($SERVERS server(s)${HEAP_PROBE_TAG:+, $HEAP_PROBE_TAG})"
  mkdir -p "$OUT/heap-probe"
  [ -f "$OUT/heap-probe.tsv" ] ||
    printf 'servers\txmx\tsetting\tquery\tresult\tseconds\tpeak_live_mb\tdetail\n' > "$OUT/heap-probe.tsv"
  for xmx in $HEAP_PROBE_XMX; do
    for setting in $HEAP_PROBE_SETTINGS; do
      for q in $HEAP_PROBE_QUERIES; do
        # a fresh JVM per run: a heap error can leave the old one in any state
        start_trino "$xmx" probe
        dir="$OUT/heap-probe/${HEAP_PROBE_TAG:+$HEAP_PROBE_TAG-}$xmx-$setting"; mkdir -p "$dir"
        stop="$dir/$q.stop"; histo="$dir/$q.histo"; rm -f "$stop"
        heap_sampler "$stop" "$histo" &
        local sampler=$!
        sess=$(heap_session "$setting")
        t0=$(date +%s.%N)
        # shellcheck disable=SC2086
        if docker exec trino trino --catalog flight --schema tpch $sess --output-format TSV -f "/queries/$q.sql" \
             > "$dir/$q.tsv" 2> "$dir/$q.err"; then
          rm -f "$dir/$q.err"
        else
          rm -f "$dir/$q.tsv"
        fi
        secs=$(printf '%.1f' "$(echo "$(date +%s.%N) - $t0" | bc)")
        touch "$stop"; wait "$sampler" || true
        peak=$(awk '/^Total/ { printf "%.0f", $2 / 1048576 }' "$histo" 2>/dev/null || true)
        if [ -f "$dir/$q.tsv" ] && "${COMPARE[@]}" "$dir" "$QUERIES" "$dir/compare-$q.tsv" "$q" >/dev/null; then
          result=OK; detail=""
        elif [ -f "$dir/$q.tsv" ]; then
          result=WRONG; detail="answer differs from the reference"
        else
          result=ERROR; detail="$(grep -m1 -E 'Query .* failed|ERROR|Exception' "$dir/$q.err" | cut -c1-200 || true)"
          docker logs trino 2>&1 | grep -m3 -E 'OutOfMemoryError|heap space' | cut -c1-300 >> "$dir/$q.err" || true
        fi
        if [ "$setting" = default ] && [ "$result" != OK ] && [ "$HEAP_PROBE_FAIL" = 1 ]; then status=1; fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$SERVERS${HEAP_PROBE_TAG:+/$HEAP_PROBE_TAG}" "$xmx" "$setting" "$q" "$result" "$secs" "${peak:-?}" "$detail" \
          | tee -a "$OUT/heap-probe.tsv"
        echo "--- live heap peak $xmx $setting $q:"; head -14 "$histo" 2>/dev/null || true
        [ -f "$dir/$q.err" ] && head -c 600 "$dir/$q.err"
        docker logs trino 2>&1 | grep -m3 'merged in memory' | cut -c1-300 || true
        # Join order and estimates decide which side Trino builds a hash of: print them for tagged runs and failures
        if [ "$setting" = default ] && { [ -n "${HEAP_PROBE_TAG:-}" ] || [ "$result" != OK ]; }; then
          heap_plan "$q" "$dir"
        fi
      done
    done
  done
  start_trino 3G
}
