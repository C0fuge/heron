package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Partial aggregation pushdown (patched Trino, trino-patches/): state types and state rows. */
class PartialAggregationTest
{
    private static final ArrowFlightColumnHandle KEY = new ArrowFlightColumnHandle("k", BIGINT);
    private static final ArrowFlightColumnHandle VALUE = new ArrowFlightColumnHandle("v", BIGINT);
    private static final ArrowFlightColumnHandle SCORE = new ArrowFlightColumnHandle("score", DOUBLE);
    private static final ArrowFlightColumnHandle PRICE = new ArrowFlightColumnHandle("price", createDecimalType(15, 2));
    private static final Map<String, ColumnHandle> SCAN = Map.of("k", KEY, "v", VALUE, "score", SCORE, "price", PRICE);
    private static final ArrowFlightTableHandle TABLE = new ArrowFlightTableHandle("s", "t", ImmutableList.of(KEY, VALUE, SCORE, PRICE), "");
    private static final RowType AVG_STATE = RowType.anonymous(List.of(DOUBLE, BIGINT));

    private static AggregateFunction function(String name, Type output, ArrowFlightColumnHandle... inputs)
    {
        List<ConnectorExpression> arguments = java.util.Arrays.stream(inputs)
                .map(c -> (ConnectorExpression) new Variable(c.getColumnName(), c.getColumnType())).toList();
        return new AggregateFunction(name, output, arguments, List.of(), false, Optional.empty());
    }

    @Test
    void statesHaveTrinosIntermediateTypes()
    {
        List<AggregateFunction> aggregates = List.of(function("count", BIGINT), function("sum", BIGINT, VALUE),
                function("min", BIGINT, VALUE), function("avg", DOUBLE, VALUE), function("avg", DOUBLE, SCORE), function("sum", DOUBLE, SCORE));
        // the types Trino 483 asks for (patched-trino.yml smoke test prints them)
        List<Type> states = List.of(BIGINT, BIGINT, BIGINT, AVG_STATE, AVG_STATE, DOUBLE);
        Aggregation aggregation = ArrowFlightMetadata.translatePartialAggregation(TABLE, aggregates, SCAN, List.of(List.of(KEY)), states)
                .orElseThrow();
        assertTrue(aggregation.isPartial());
        assertEquals(List.of(KEY), aggregation.getGroupingColumns());
        assertEquals(states, aggregation.getAggregates().stream().map(a -> a.getOutput().getColumnType()).toList());
        assertTrue(TABLE.withAggregation(aggregation).producesPartialAggregation());
        assertTrue(ArrowFlightMetadata.partialCandidate(ArrowFlightMetadata.translateAggregation(TABLE, aggregates, SCAN,
                List.of(List.of(KEY)), Optional.empty()).orElseThrow()));
    }

    @Test
    void partialResultAssignsTheGroupingColumnsStockResultDoesNot()
    {
        List<AggregateFunction> aggregates = List.of(function("count", BIGINT), function("sum", BIGINT, VALUE));
        Aggregation partial = ArrowFlightMetadata.translatePartialAggregation(TABLE, aggregates, SCAN, List.of(List.of(KEY)),
                List.of(BIGINT, BIGINT)).orElseThrow();
        // the patched planner builds the new scan from the result's assignments only
        assertEquals(List.of("k", "afagg0", "afagg1"), ArrowFlightMetadata.aggregationResult(TABLE, partial).getAssignments().stream()
                .map(io.trino.spi.connector.Assignment::getVariable).toList());
        assertEquals(2, ArrowFlightMetadata.aggregationResult(TABLE, partial).getProjections().size());
        Aggregation stock = ArrowFlightMetadata.translateAggregation(TABLE, aggregates, SCAN, List.of(List.of(KEY)), Optional.empty()).orElseThrow();
        assertEquals(List.of("afagg0", "afagg1"), ArrowFlightMetadata.aggregationResult(TABLE, stock).getAssignments().stream()
                .map(io.trino.spi.connector.Assignment::getVariable).toList());
    }

    @Test
    void groupByWithoutAggregatesStaysOnTheStockPath()
    {
        // the patched rule skips aggregations without aggregates: applyAggregation must keep them
        assertFalse(ArrowFlightMetadata.partialCandidate(ArrowFlightMetadata.translateAggregation(TABLE, List.of(), SCAN,
                List.of(List.of(KEY)), Optional.empty()).orElseThrow()));
    }

    @Test
    void declinesWholeRequestOnAnyStateItCannotProduce()
    {
        // engine asks for another state type (e.g. a Trino version with a different AVG state layout)
        assertTrue(ArrowFlightMetadata.translatePartialAggregation(TABLE, List.of(function("count", BIGINT), function("avg", DOUBLE, VALUE)),
                SCAN, List.of(List.of()), List.of(BIGINT, RowType.anonymous(List.of(BIGINT, DOUBLE)))).isEmpty());
        // DECIMAL sum state is Trino-internal VARBINARY: stays on the stock path, which keeps applyAggregation
        DecimalType sumType = createDecimalType(38, 2);
        List<AggregateFunction> decimalSum = List.of(function("count", BIGINT), function("sum", sumType, PRICE));
        assertTrue(ArrowFlightMetadata.translatePartialAggregation(TABLE, decimalSum, SCAN, List.of(List.of(KEY)),
                List.of(BIGINT, io.trino.spi.type.VarbinaryType.VARBINARY)).isEmpty());
        assertFalse(ArrowFlightMetadata.partialCandidate(ArrowFlightMetadata.translateAggregation(TABLE, decimalSum, SCAN,
                List.of(List.of(KEY)), Optional.empty()).orElseThrow()));
        // stock aggregation of the same request is unchanged (not partial, final output types)
        Aggregation stock = ArrowFlightMetadata.translateAggregation(TABLE, decimalSum, SCAN, List.of(List.of(KEY)), Optional.empty()).orElseThrow();
        assertFalse(stock.isPartial());
        assertFalse(TABLE.withAggregation(stock).producesPartialAggregation());
        assertEquals(sumType, stock.getAggregates().get(1).getOutput().getColumnType());
        // max(double) is never pushed (NaN order), partial or not
        assertTrue(ArrowFlightMetadata.translatePartialAggregation(TABLE, List.of(function("max", DOUBLE, SCORE)), SCAN,
                List.of(List.of()), List.of(DOUBLE)).isEmpty());
    }

    @Test
    void stockTrinoHasNoPartialSpi()
    {
        // the unit tests run against the stock SPI: applyAggregation must keep pushing everything there
        assertFalse(ArrowFlightMetadata.PARTIAL_AGGREGATION_SPI);
        assertFalse(TABLE.producesPartialAggregation());
    }

    @Test
    void statesOfOneEndpointAreNotMerged()
    {
        ArrowFlightColumnHandle count = new ArrowFlightColumnHandle("afagg0", BIGINT);
        ArrowFlightColumnHandle avg = new ArrowFlightColumnHandle("afagg1", AVG_STATE);
        AggregateColumn avgColumn = new AggregateColumn(AggregateColumn.Function.AVG, Optional.of(VALUE), avg);
        Aggregation aggregation = new Aggregation(List.of(KEY), List.of(
                new AggregateColumn(AggregateColumn.Function.COUNT_STAR, Optional.empty(), count), avgColumn), true);
        AggregationReader reader = new AggregationReader(aggregation, new ArrowTypeConverter());
        // raw: k, count, avg sum (DECIMAL(38,0) for integer input), avg count; two rows of the same key stay two rows
        DecimalType sumType = createDecimalType(38, 0);
        Page raw = new Page(
                block(BIGINT, 1L, 1L, 2L),
                block(BIGINT, 3L, 1L, 2L),
                block(sumType, ArrowTypeConverter.encodeDecimal(new BigDecimal(30), sumType), ArrowTypeConverter.encodeDecimal(new BigDecimal(30), sumType), null),
                block(BIGINT, 2L, 1L, 0L));
        Page states = reader.finishStates(raw, List.of(avg, KEY, count));
        assertEquals(3, states.getPositionCount());
        assertEquals(List.of(1L, 1L, 2L), longs(states.getBlock(1)));
        assertEquals(List.of(3L, 1L, 2L), longs(states.getBlock(2)));
        List<Block> fields = ((RowBlock) states.getBlock(0)).getFieldBlocks();
        // (30, 2) and (30, 1) merge to 20 in Trino's FINAL; no non-NULL input = (0.0, 0) like Trino's own state
        assertEquals(List.of(30.0, 30.0, 0.0), List.of(DOUBLE.getDouble(fields.get(0), 0), DOUBLE.getDouble(fields.get(0), 1), DOUBLE.getDouble(fields.get(0), 2)));
        assertEquals(List.of(2L, 1L, 0L), longs(fields.get(1)));
    }

    @Test
    void emptyGlobalStateRow()
    {
        ArrowFlightColumnHandle count = new ArrowFlightColumnHandle("afagg0", BIGINT);
        ArrowFlightColumnHandle sum = new ArrowFlightColumnHandle("afagg1", BIGINT);
        ArrowFlightColumnHandle avg = new ArrowFlightColumnHandle("afagg2", AVG_STATE);
        Aggregation aggregation = new Aggregation(List.of(), List.of(
                new AggregateColumn(AggregateColumn.Function.COUNT_STAR, Optional.empty(), count),
                new AggregateColumn(AggregateColumn.Function.SUM, Optional.of(VALUE), sum),
                new AggregateColumn(AggregateColumn.Function.AVG, Optional.of(SCORE), avg)), true);
        AggregationReader reader = new AggregationReader(aggregation, new ArrowTypeConverter());
        Page page = reader.finishStates(reader.emptyGlobalRawPage(), List.of(sum, avg, count));
        assertEquals(1, page.getPositionCount());
        assertNull(readNativeValue(BIGINT, page.getBlock(0), 0));
        List<Block> fields = ((RowBlock) page.getBlock(1)).getFieldBlocks();
        assertEquals(0.0, DOUBLE.getDouble(fields.get(0), 0));
        assertEquals(0L, BIGINT.getLong(fields.get(1), 0));
        assertEquals(0L, BIGINT.getLong(page.getBlock(2), 0));
    }

    private static Block block(Type type, Object... values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, values.length);
        for (Object value : values) {
            writeNativeValue(type, builder, value);
        }
        return builder.build();
    }

    private static List<Long> longs(Block block)
    {
        return java.util.stream.IntStream.range(0, block.getPositionCount()).mapToObj(i -> BIGINT.getLong(block, i)).toList();
    }
}
