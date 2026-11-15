package pro.surpin.trino.arrowflight;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowFlightMetadataAggregateTest
{
    private static final ArrowFlightColumnHandle SCORE = new ArrowFlightColumnHandle("score", DOUBLE);
    private static final ArrowFlightColumnHandle KEY = new ArrowFlightColumnHandle("k", BIGINT);
    private static final ArrowFlightColumnHandle PRICE = new ArrowFlightColumnHandle("price", createDecimalType(15, 2));
    private static final Map<String, ColumnHandle> SCAN = Map.of("score", SCORE, "k", KEY, "price", PRICE);

    private static Optional<AggregateColumn> translate(String function, ArrowFlightColumnHandle input, Type outputType)
    {
        AggregateFunction aggregate = new AggregateFunction(function, outputType,
                List.of(new Variable(input.getColumnName(), input.getColumnType())), List.of(), false, Optional.empty());
        return ArrowFlightMetadata.translateAggregate(aggregate, SCAN, "out");
    }

    @Test
    void maxOfDoubleStaysInTrinoBecauseDuckDbOrdersNanLast()
    {
        // Trino's max(double) skips NaN when another value exists; DuckDB's returns NaN.
        assertTrue(translate("max", SCORE, DOUBLE).isEmpty());
    }

    @Test
    void minOfDoubleAndOtherExtremaArePushed()
    {
        assertEquals(AggregateColumn.Function.MIN, translate("min", SCORE, DOUBLE).orElseThrow().getFunction());
        assertEquals(AggregateColumn.Function.MAX, translate("max", KEY, BIGINT).orElseThrow().getFunction());
        assertEquals(AggregateColumn.Function.MAX, translate("max", PRICE, createDecimalType(15, 2)).orElseThrow().getFunction());
        assertEquals(AggregateColumn.Function.SUM, translate("sum", SCORE, DOUBLE).orElseThrow().getFunction());
    }
}
