-- partsupp: one group per supplier, integer sum
SELECT ps_suppkey, count(*), sum(ps_availqty), min(ps_partkey), max(ps_partkey)
FROM partsupp
GROUP BY ps_suppkey
ORDER BY sum(ps_availqty) DESC, ps_suppkey
LIMIT 20
