# Slow-link check of result compression (maintainer 2026-10-28). Sourced by run-e2e.sh; uses its variables and
# functions (OUT, log, status, trino_seconds from profile.sh).
#
# slow_link_run (the whole run with SLOW_LINK=only): the lineitem scan checksum(ROW(all 16 columns)) - nothing is pushed
# down, every row crosses the link - through catalogs flight (compression off), flight_lz4 and flight_zstd
# (arrow-flight.result-compression), SLOW_LINK_REPEAT times each, interleaved: first on the plain Docker link, then with
# netem (SLOW_LINK_NETEM, default 'delay 10ms rate 1gbit') on the host side of Trino's veth, i.e. on everything the Flight
# servers send to Trino. Per link x setting: best Trino elapsed, bytes on the link (the veth's counter, best run), wire MB/s,
# and effective MB/s = the plain Arrow bytes (the 'off' run's link bytes) / elapsed. Every answer must equal the 'off' one
# (else the job fails); times are INFO. Result: slowlink.tsv.

SLOW_LINK_NETEM="${SLOW_LINK_NETEM:-delay 10ms rate 1gbit}"
SLOW_LINK_REPEAT="${SLOW_LINK_REPEAT:-2}"
SLOW_LINK_SETTINGS="${SLOW_LINK_SETTINGS:-off:flight lz4:flight_lz4 zstd:flight_zstd}"
SLOW_LINK_SQL='SELECT count(*), checksum(ROW(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)) FROM lineitem'

trino_veth() {  # host-side interface of the Trino container's eth0
  local idx; idx=$(docker exec trino cat /sys/class/net/eth0/iflink)
  ip -o link | awk -F': ' -v i="$idx" '$1 == i { sub(/@.*/, "", $2); print $2 }'
}

slow_link_query() {  # catalog source veth -> "seconds bytes" on stdout; answer in $OUT/slowlink/<source>.tsv
  local catalog=$1 source=$2 veth=$3 b0 b1 t0 t id
  b0=$(cat "/sys/class/net/$veth/statistics/tx_bytes"); t0=$(date +%s.%N)
  docker exec trino trino --catalog "$catalog" --schema tpch --source "$source" --output-format TSV \
    --execute "$SLOW_LINK_SQL" > "$OUT/slowlink/$source.tsv" 2> "$OUT/slowlink/$source.err" || true
  t=$(echo "$(date +%s.%N) - $t0" | bc)
  b1=$(cat "/sys/class/net/$veth/statistics/tx_bytes")
  [ -s "$OUT/slowlink/$source.err" ] || rm -f "$OUT/slowlink/$source.err"
  id=$(curl -fsS -H 'X-Trino-User: e2e' http://127.0.0.1:8080/v1/query \
    | jq -r --arg s "$source" '[.[] | select(.session.source == $s)] | last | .queryId // empty')
  if [ -n "$id" ]; then
    t=$(trino_seconds "$(curl -fsS -H 'X-Trino-User: e2e' "http://127.0.0.1:8080/v1/query/$id" | jq -r '.queryStats.elapsedTime // empty')" "$t")
  fi
  echo "$t $((b1 - b0))"
}

slow_link_run() {
  local veth link setting tag catalog round r secs bytes answer expected off_bytes
  mkdir -p "$OUT/slowlink"
  veth=$(trino_veth)
  [ -n "$veth" ] || { echo "::error::slow link: no veth found for the Trino container"; status=1; return; }
  sudo modprobe sch_netem 2>/dev/null || true
  log "slow link: Trino veth $veth; settings $SLOW_LINK_SETTINGS; netem '$SLOW_LINK_NETEM'"
  # warm-up (plans, connections, server caches) on every catalog
  for setting in $SLOW_LINK_SETTINGS; do slow_link_query "${setting#*:}" "slow-warm-${setting%%:*}" "$veth" >/dev/null; done
  printf 'link\tsetting\trun\tseconds\tlink_bytes\n' > "$OUT/slowlink/runs.tsv"
  for link in plain shaped; do
    if [ "$link" = shaped ]; then
      # a deep queue: netem's default 1000 packets drops at 1 Gbit/s with 10 ms delay
      # shellcheck disable=SC2086
      sudo tc qdisc replace dev "$veth" root netem $SLOW_LINK_NETEM limit 100000 || { status=1; return; }
      tc qdisc show dev "$veth"
    fi
    for round in $(seq 1 "$SLOW_LINK_REPEAT"); do
      for setting in $SLOW_LINK_SETTINGS; do
        tag=${setting%%:*}; catalog=${setting#*:}
        read -r secs bytes < <(slow_link_query "$catalog" "slow-$link-$tag-$round" "$veth")
        printf '%s\t%s\t%s\t%s\t%s\n' "$link" "$tag" "$round" "$secs" "$bytes" >> "$OUT/slowlink/runs.tsv"
        answer=$(cat "$OUT/slowlink/slow-$link-$tag-$round.tsv" 2>/dev/null || true)
        expected=$(cat "$OUT/slowlink/slow-plain-off-1.tsv" 2>/dev/null || true)
        if [ -z "$answer" ] || [ "$answer" != "$expected" ]; then
          echo "::error::slow link: $link $tag run $round answered '$answer', compression off answered '$expected'"
          cat "$OUT/slowlink/slow-$link-$tag-$round.err" 2>/dev/null | tail -20 || true
          status=1
        fi
      done
    done
  done
  sudo tc qdisc del dev "$veth" root 2>/dev/null || true
  # best run per link x setting; effective MB/s counts the plain Arrow bytes (setting off) per second
  awk -F'\t' 'NR > 1 {
      k = $1 "\t" $2
      if (!(k in best) || $4 < best[k]) { best[k] = $4; bytes[k] = $5 }
      if (!(k in order)) { order[k] = ++n; keys[n] = k }
    }
    END {
      print "link\tsetting\tbest_s\tlink_MB\twire_MB_s\teffective_MB_s"
      for (i = 1; i <= n; i++) {
        k = keys[i]; split(k, p, "\t"); off = bytes[p[1] "\toff"]
        printf "%s\t%.2f\t%.1f\t%.1f\t%.1f\n", k, best[k], bytes[k] / 1e6, bytes[k] / 1e6 / best[k], off / 1e6 / best[k]
      }
    }' "$OUT/slowlink/runs.tsv" | tee "$OUT/slowlink.tsv"
}
