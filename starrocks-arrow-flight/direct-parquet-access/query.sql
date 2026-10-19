-- Query the Flight server's data from StarRocks by reading the same underlying Parquet
-- files directly (bypasses the Arrow Flight SQL protocol entirely — see ../DESIGN.md for
-- why the JDBC-catalog route doesn't work and this is the practical alternative).
--
-- Tested against StarRocks 4.1.4: returns rows identical to the Trino plugin querying the
-- same table over the real Flight protocol (count(*) = 50000, same sample rows).
--
-- Prereq: the Parquet directory the Flight server reads from (--data-dir) must be
-- reachable from StarRocks' BE node(s), e.g. bind-mounted into the BE container/host at
-- the same or a translated path.

-- ad-hoc, no catalog/table object needed:
SELECT count(*) FROM FILES(
  "path"   = "file:///path/to/flight-data/demo/t/*.parquet",
  "format" = "parquet"
);

SELECT id, bool_col, bigint_col, string_col, date_col
FROM FILES("path" = "file:///path/to/flight-data/demo/t/*.parquet", "format" = "parquet")
WHERE id < 5
ORDER BY id;

-- or materialize once as a native StarRocks table (fast repeated queries, no re-scan):
CREATE TABLE demo_t
LIKE FILES("path" = "file:///path/to/flight-data/demo/t/*.parquet", "format" = "parquet");

INSERT INTO demo_t
SELECT * FROM FILES("path" = "file:///path/to/flight-data/demo/t/*.parquet", "format" = "parquet");

SELECT count(*) FROM demo_t;
