-- high cardinality: one group per order (1.5M groups at SF1), aggregated again above
SELECT count(*), sum(c), max(c), min(s), max(m)
FROM (SELECT l_orderkey, count(*) c, sum(l_linenumber) s, max(l_shipdate) m FROM lineitem GROUP BY l_orderkey);
