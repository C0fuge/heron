package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlQueryBuilderTest
{
    private static final ArrowFlightColumnHandle ID = new ArrowFlightColumnHandle("id", BIGINT);
    private static final ArrowFlightColumnHandle NAME = new ArrowFlightColumnHandle("name", VARCHAR);

    private static ArrowFlightTableHandle table(String quote, ArrowFlightColumnHandle... cols)
    {
        return new ArrowFlightTableHandle("test_schema", "test_table", ImmutableList.copyOf(cols), quote);
    }

    @Test
    void unquotedByDefault()
    {
        assertEquals(
                "SELECT id, name FROM test_schema.test_table",
                SqlQueryBuilder.buildScanSql(table("", ID, NAME)));
    }

    @Test
    void quotedWhenConfigured()
    {
        assertEquals(
                "SELECT \"id\", \"name\" FROM \"test_schema\".\"test_table\"",
                SqlQueryBuilder.buildScanSql(table("\"", ID, NAME)));
    }

    @Test
    void rangeAndEqualityBecomeWhere()
    {
        Map<ColumnHandle, Domain> domains = Map.of(
                ID, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), false),
                NAME, Domain.singleValue(VARCHAR, Slices.utf8Slice("o'brien")));

        String sql = SqlQueryBuilder.buildScanSql(
                table("", ID, NAME).withConstraint(TupleDomain.withColumnDomains(domains)));

        assertTrue(sql.contains("id >= 10"), sql);
        assertTrue(sql.contains("name = 'o''brien'"), sql);
    }

    @Test
    void unconstrainedTableHasNoWhere()
    {
        ArrowFlightTableHandle handle = new ArrowFlightTableHandle("s", "t", ImmutableList.of(ID), "");
        assertEquals("SELECT id FROM s.t", SqlQueryBuilder.buildScanSql(handle));
    }

    @Test
    void emptyProjectionReadsNarrowestColumn()
    {
        ArrowFlightTableHandle handle = table("", NAME, ID).withProjectedColumns(List.of());
        assertEquals("SELECT id FROM test_schema.test_table", SqlQueryBuilder.buildScanSql(handle));
    }

    @Test
    void aggregationSqlPutsGroupKeysFirstAndSplitsAvg()
    {
        ArrowFlightColumnHandle price = new ArrowFlightColumnHandle("price", createDecimalType(15, 2));
        ArrowFlightColumnHandle discount = new ArrowFlightColumnHandle("discount", createDecimalType(15, 2));
        Expr revenue = Expr.call(Expr.Kind.MULTIPLY, createDecimalType(31, 4), List.of(
                Expr.column("price", createDecimalType(15, 2)),
                Expr.call(Expr.Kind.SUBTRACT, createDecimalType(16, 2), List.of(
                        Expr.constant("1", createDecimalType(1, 0)),
                        Expr.column("discount", createDecimalType(15, 2))))));
        ArrowFlightColumnHandle revenueColumn = new ArrowFlightColumnHandle("afexpr_1", createDecimalType(31, 4), Optional.of(revenue));
        Aggregation aggregation = new Aggregation(List.of(NAME), List.of(
                new AggregateColumn(AggregateColumn.Function.COUNT_STAR, Optional.empty(), new ArrowFlightColumnHandle("afagg0", BIGINT)),
                new AggregateColumn(AggregateColumn.Function.SUM, Optional.of(ID), new ArrowFlightColumnHandle("afagg1", BIGINT)),
                new AggregateColumn(AggregateColumn.Function.SUM, Optional.of(revenueColumn), new ArrowFlightColumnHandle("afagg2", createDecimalType(38, 4))),
                new AggregateColumn(AggregateColumn.Function.AVG, Optional.of(price), new ArrowFlightColumnHandle("afagg3", createDecimalType(15, 2)))));
        ArrowFlightTableHandle handle = table("", ID, NAME, price, discount).withAggregation(aggregation);

        assertEquals("SELECT name, count(*) AS afagg0, sum(CAST(id AS DECIMAL(38,0))) AS afagg1, "
                        + "sum((price * (1 - discount))) AS afagg2, sum(price) AS afagg3_s, count(price) AS afagg3_c "
                        + "FROM test_schema.test_table GROUP BY name",
                SqlQueryBuilder.buildScanSql(handle));
    }

    @Test
    void extraPredicatesAndSplitRangeAreAnded()
    {
        ArrowFlightTableHandle handle = table("", ID, NAME).withExtraPredicates(List.of("(name LIKE 'a%')"));
        assertEquals("SELECT id FROM test_schema.test_table WHERE (name LIKE 'a%') AND (id >= 5)",
                SqlQueryBuilder.buildScanSql(handle, List.of(ID), List.of("(id >= 5)")));
    }

    @Test
    void decimalAndTimestampLiterals()
    {
        assertEquals("12.34", SqlQueryBuilder.formatValue(createDecimalType(10, 2), 1234L));
        assertEquals("TIMESTAMP '1970-01-01 00:00:01.000002'", SqlQueryBuilder.formatValue(TIMESTAMP_MICROS, 1_000_002L));
        assertEquals("0.1000000000000000055511151231257827021181583404541015625", SqlQueryBuilder.formatValue(DOUBLE, 0.1));
    }

    @Test
    void expressionEvaluatesLikeTrino()
    {
        Expr revenue = Expr.call(Expr.Kind.MULTIPLY, createDecimalType(31, 4), List.of(
                Expr.column("p", createDecimalType(15, 2)),
                Expr.call(Expr.Kind.SUBTRACT, createDecimalType(16, 2), List.of(
                        Expr.constant("1", createDecimalType(1, 0)),
                        Expr.column("d", createDecimalType(15, 2))))));
        assertEquals(new BigDecimal("90.0000"), revenue.evaluate(Map.of("p", new BigDecimal("100.00"), "d", new BigDecimal("0.10"))));
    }

    @Test
    void unfilteredGlobalMinMaxForcesRealScan()
    {
        ArrowFlightColumnHandle day = new ArrowFlightColumnHandle("day", io.trino.spi.type.DateType.DATE);
        Aggregation minMax = new Aggregation(List.of(), List.of(
                new AggregateColumn(AggregateColumn.Function.MIN, Optional.of(day), new ArrowFlightColumnHandle("afagg0", io.trino.spi.type.DateType.DATE))));
        assertEquals("SELECT min(day) AS afagg0 FROM test_schema.test_table WHERE TRUE",
                SqlQueryBuilder.buildScanSql(table("", ID, day).withAggregation(minMax)));

        // grouped, filtered or count-only aggregations are left alone
        Aggregation countOnly = new Aggregation(List.of(), List.of(
                new AggregateColumn(AggregateColumn.Function.COUNT_STAR, Optional.empty(), new ArrowFlightColumnHandle("afagg0", BIGINT))));
        assertEquals("SELECT count(*) AS afagg0 FROM test_schema.test_table",
                SqlQueryBuilder.buildScanSql(table("", ID, day).withAggregation(countOnly)));
        Aggregation grouped = new Aggregation(List.of(ID), minMax.getAggregates());
        assertEquals("SELECT id, min(day) AS afagg0 FROM test_schema.test_table GROUP BY id",
                SqlQueryBuilder.buildScanSql(table("", ID, day).withAggregation(grouped)));
    }

    private static TupleDomain<ColumnHandle> idIn(long count)
    {
        List<Object> values = new java.util.ArrayList<>();
        for (long v = 1; v <= count; v++) {
            values.add(v * 10);
        }
        return TupleDomain.withColumnDomains(Map.of(ID, Domain.multipleValues(BIGINT, values)));
    }

    @Test
    void dynamicFilterUpToMaxValuesIsAnInList()
    {
        String where = SqlQueryBuilder.dynamicFilterClause(idIn(3), java.util.Set.of(ID), 3, "");
        assertEquals("id IN (10, 20, 30)", where);
    }

    @Test
    void dynamicFilterOverMaxValuesIsItsSpan()
    {
        String where = SqlQueryBuilder.dynamicFilterClause(idIn(1500), java.util.Set.of(ID), 1000, "");
        assertEquals("id >= 10 AND id <= 15000", where.replace("(", "").replace(")", ""));
        String list = SqlQueryBuilder.dynamicFilterClause(idIn(1500), java.util.Set.of(ID), 2000, "");
        assertTrue(list.startsWith("id IN (10, 20, ") && list.endsWith(", 15000)"), list);
    }

    @Test
    void dynamicFilterOnlyOnAllowedColumns()
    {
        TupleDomain<ColumnHandle> dynamic = TupleDomain.withColumnDomains(Map.of(
                ID, Domain.singleValue(BIGINT, 7L),
                NAME, Domain.singleValue(VARCHAR, Slices.utf8Slice("x"))));
        assertEquals("id = 7", SqlQueryBuilder.dynamicFilterClause(dynamic, java.util.Set.of(ID), 1000, ""));
        assertEquals("", SqlQueryBuilder.dynamicFilterClause(dynamic, java.util.Set.of(), 1000, ""));
    }
}
