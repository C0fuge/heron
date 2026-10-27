# Throughput profile (STATUS Stage 2: plugin throughput). Sourced by run-e2e.sh; uses its variables and functions
# (WORK, OUT, QUERIES, COMPARE, NET, TOKEN, REPO, log, status, trino_pid from heap-probe.sh).
#
# profile_run: TPC-H SF1 q01..q22 through Trino, once to warm up, then PROFILE_REPEAT timed passes per setting of
# PROFILE_SETTINGS ('default' = plugin defaults, otherwise comma-separated catalog.property=value session settings, e.g.
# flight.split_count=16). The first timed pass of 'default' runs under Java Flight Recorder (settings=profile); for each
# query Trino's query statistics are saved (profile/<setting>/<q>.json) and summarised by profile_report.py stats (elapsed,
# CPU, the plugin's read time = Flight read + Arrow->Block conversion, scan blocked time). The recording's CPU samples are
# grouped by profile_report.py jfr (conversion / Arrow decode / gRPC / Trino operators ...). The same query files are also
# timed straight against flight1 (flight_time.py, ADBC, DuckDB runs the whole query) = the server's share. Answers are
# compared with the reference (a wrong answer fails the job); times are INFO.
# Results: profile-times.tsv (best seconds per query x setting + direct), profile-stats-<setting>.tsv, profile-jfr.txt.

PROFILE_SETTINGS="${PROFILE_SETTINGS:-default flight.split_count=16}"
PROFILE_REPEAT="${PROFILE_REPEAT:-2}"

profile_query() {  # setting-dir query session-args... -> seconds; Trino query info to <dir>/<q>.json
  local dir=$1 q=$2 t0 t id; shift 2
  t0=$(date +%s.%N)
  if docker exec trino trino --catalog flight --schema tpch --source "prof-$q" "$@" --output-format TSV \
       -f "/$( [ "$q" = scan ] && echo profile-queries || echo queries)/$q.sql" > "$dir/$q.tsv" 2> "$dir/$q.err"; then
    rm -f "$dir/$q.err"
  else
    rm -f "$dir/$q.tsv"
  fi
  t=$(echo "$(date +%s.%N) - $t0" | bc)
  id=$(curl -fsS -H 'X-Trino-User: e2e' http://127.0.0.1:8080/v1/query \
    | jq -r --arg s "prof-$q" '[.[] | select(.session.source == $s)] | sort_by(.queryStats.createTime) | last | .queryId // empty')
  [ -z "$id" ] || curl -fsS -H 'X-Trino-User: e2e' "http://127.0.0.1:8080/v1/query/$id" > "$dir/$q.json" || true
  printf '%.2f' "$t"
}

profile_run() {
  local setting tag dir q pass sess t pid jfr names
  log "profile: settings $PROFILE_SETTINGS, $PROFILE_REPEAT timed pass(es), $SERVERS server(s)"
  mkdir -p "$OUT/profile/warm"
  names=$(for q in "$QUERIES"/q*.sql; do basename "$q" .sql; done)
  for q in $names; do profile_query "$OUT/profile/warm" "$q" > /dev/null; done
  pid=$(trino_pid)
  jfr=""
  for setting in $PROFILE_SETTINGS; do
    tag=${setting//flight./}; tag=${tag//[^a-z0-9_]/}
    sess=()
    [ "$setting" = default ] || for s in ${setting//,/ }; do sess+=(--session "$s"); done
    for pass in $(seq 1 "$PROFILE_REPEAT"); do
      dir="$OUT/profile/$tag-$pass"; mkdir -p "$dir"
      if [ "$setting" = default ] && [ "$pass" = 1 ] && [ -n "$pid" ]; then
        docker exec trino jcmd "$pid" JFR.start name=prof settings=profile filename=/tmp/prof.jfr > /dev/null && jfr=1
      fi
      for q in $names; do
        printf '%s\t%s\t%s\n' "$tag" "$q" "$(profile_query "$dir" "$q" "${sess[@]}")" >> "$OUT/profile/times-raw.tsv"
      done
      if [ -n "$jfr" ] && [ "$setting" = default ] && [ "$pass" = 1 ]; then
        docker exec trino jcmd "$pid" JFR.stop name=prof > /dev/null || jfr=""
      fi
      # raw scan rate: 6M lineitem rows x 8 columns through the plugin, aggregated by Trino (pushdown off)
      printf '%s\t%s\t%s\n' "$tag" scan "$(profile_query "$dir" scan "${sess[@]}" --session flight.aggregation_pushdown_enabled=false)" \
        >> "$OUT/profile/times-raw.tsv"
      "${COMPARE[@]}" "$dir" "$QUERIES" "$dir/compare.tsv" || status=1
      cmp -s "$dir/scan.tsv" "$OUT/profile/$(echo "$PROFILE_SETTINGS" | awk '{ s = $1; gsub(/flight\./, "", s); gsub(/[^a-z0-9_]/, "", s); print s }')-1/scan.tsv" \
        || { echo "profile: scan answer of $tag-$pass differs from the first setting's"; status=1; }
      python3 "$REPO/e2e/ci/profile_report.py" stats "$dir" > "$OUT/profile-stats-$tag-$pass.tsv" || true
    done
  done
  log "profile: direct to flight1 (ADBC, whole query in DuckDB)"
  docker run --rm --network "$NET" -e FLIGHT_TOKEN="$TOKEN" -v "$REPO/e2e/ci":/probe:ro -v "$QUERIES":/queries:ro -v "$REPO/e2e/ci/profile-queries":/pq:ro \
    python:3.12-slim sh -c 'pip install -q --disable-pip-version-check adbc-driver-flightsql pyarrow >/dev/null 2>&1 &&
      python /probe/flight_time.py grpc://flight1:32010 tpch "$@"' probe "$PROFILE_REPEAT" $(for q in $names; do echo "/queries/$q.sql"; done) /pq/scan-direct.sql \
    | tee "$OUT/profile/direct.tsv" || true
  # best time per query x setting, plus the direct time
  python3 - "$OUT/profile/times-raw.tsv" "$OUT/profile/direct.tsv" > "$OUT/profile-times.tsv" <<'PY' || true
import sys
from collections import defaultdict
best, tags = defaultdict(dict), []
for line in open(sys.argv[1]):
    tag, q, t = line.rstrip("\n").split("\t")
    if tag not in tags:
        tags.append(tag)
    best[q][tag] = min(float(t), best[q].get(tag, float("inf")))
direct = {l.split("\t")[0].replace("scan-direct", "scan"): l.split("\t")[1] for l in open(sys.argv[2]) if "\t" in l}
print("query\t" + "\t".join(tags) + "\tdirect")
for q in sorted(best):
    print(q + "\t" + "\t".join(f"{best[q].get(t, 0):.2f}" for t in tags) + "\t" + direct.get(q, "?"))
print("TOTAL\t" + "\t".join(f"{sum(best[q].get(t, 0) for q in best):.2f}" for t in tags) + "\t"
      + f"{sum(float(v) for v in direct.values() if v.replace('.', '', 1).isdigit()):.2f}")
PY
  echo "--- profile: best seconds per query (direct = straight to flight1)"; cat "$OUT/profile-times.tsv"
  for f in "$OUT"/profile-stats-*.tsv; do echo "--- $(basename "$f")"; grep -E '^(q0[1-9]|q1[0-9]|q2[0-2]|scan|TOTAL|query)' "$f"; done
  if [ -n "$jfr" ]; then
    docker cp trino:/tmp/prof.jfr "$OUT/profile/prof.jfr" > /dev/null
    "${JAVA_HOME:+$JAVA_HOME/bin/}jfr" print --json --stack-depth 64 --events jdk.ExecutionSample "$OUT/profile/prof.jfr" \
      > "$WORK/prof.json" && python3 "$REPO/e2e/ci/profile_report.py" jfr "$WORK/prof.json" > "$OUT/profile-jfr.txt" || true
    rm -f "$WORK/prof.json"
    echo "--- profile: JFR CPU samples (default settings, first timed pass)"; cat "$OUT/profile-jfr.txt" 2>/dev/null || true
  else
    echo "profile: no JFR recording (no TrinoServer pid?)"
  fi
}
