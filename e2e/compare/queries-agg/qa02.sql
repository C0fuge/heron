-- 4 groups over the whole lineitem scan (q01-shaped, integer columns instead of decimals)
SELECT l_returnflag, l_linestatus, count(*), sum(l_linenumber), sum(l_orderkey), min(l_partkey), max(l_suppkey),
       round(avg(l_linenumber), 6)
FROM lineitem
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
