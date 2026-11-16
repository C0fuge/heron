-- global, non-decimal: count/min/max/int sum/avg directly over the lineitem scan
SELECT count(*), count(l_comment), sum(l_partkey), min(l_shipdate), max(l_receiptdate), round(avg(l_suppkey), 6)
FROM lineitem;
