-- orders: 5 groups, integer/date aggregates
SELECT o_orderpriority, count(*), sum(o_custkey), min(o_orderdate), max(o_orderdate), round(avg(o_shippriority), 6)
FROM orders
GROUP BY o_orderpriority
ORDER BY o_orderpriority;
