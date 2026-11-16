-- ~10k groups (per supplier) under a filter
SELECT l_suppkey, count(*), sum(l_partkey), min(l_shipdate), max(l_linenumber)
FROM lineitem
WHERE l_shipdate >= DATE '1995-01-01'
GROUP BY l_suppkey
ORDER BY count(*) DESC, l_suppkey
LIMIT 50
