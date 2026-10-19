package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

/**
 * Reads the result of a pushed-down aggregation. The server returns "raw" columns (grouping
 * columns, then one column per aggregate, two for AVG = sum + count). With a single endpoint
 * those are finalised batch by batch; with several endpoints (data spread over several Flight
 * nodes) each endpoint aggregated only its own files, so the partial rows are merged here first.
 */
final class AggregationReader
{
    enum Role
    {
        GROUP, COUNT, SUM, MIN, MAX, AVG_SUM, AVG_COUNT
    }

    record Raw(String name, Type type, Role role) {}

    private final Aggregation aggregation;
    private final List<Raw> raws;
    private final ArrowTypeConverter converter;

    AggregationReader(Aggregation aggregation, ArrowTypeConverter converter)
    {
        this.aggregation = aggregation;
        this.converter = converter;
        ImmutableList.Builder<Raw> builder = ImmutableList.builder();
        for (ArrowFlightColumnHandle group : aggregation.getGroupingColumns()) {
            builder.add(new Raw(group.getColumnName(), group.getColumnType(), Role.GROUP));
        }
        for (AggregateColumn aggregate : aggregation.getAggregates()) {
            String out = aggregate.getOutput().getColumnName();
            Type outType = aggregate.getOutput().getColumnType();
            switch (aggregate.getFunction()) {
                case COUNT_STAR, COUNT -> builder.add(new Raw(out, BIGINT, Role.COUNT));
                case SUM -> builder.add(new Raw(out, outType, Role.SUM));
                case MIN -> builder.add(new Raw(out, outType, Role.MIN));
                case MAX -> builder.add(new Raw(out, outType, Role.MAX));
                case AVG -> {
                    builder.add(new Raw(aggregate.sumName(), avgSumType(aggregate.getInput().orElseThrow().getColumnType()), Role.AVG_SUM));
                    builder.add(new Raw(aggregate.countName(), BIGINT, Role.AVG_COUNT));
                }
            }
        }
        this.raws = builder.build();
    }

    static Type avgSumType(Type input)
    {
        if (input instanceof DecimalType decimal) {
            return createDecimalType(38, decimal.getScale());
        }
        if (input.equals(DOUBLE)) {
            return DOUBLE;
        }
        return createDecimalType(38, 0);
    }

    /** One server batch -> page of raw columns. */
    Page rawPage(VectorSchemaRoot root)
    {
        int positions = root.getRowCount();
        Block[] blocks = new Block[raws.size()];
        for (int i = 0; i < raws.size(); i++) {
            Raw raw = raws.get(i);
            FieldVector vector = root.getVector(raw.name());
            blocks[i] = vector == null ? converter.nullBlock(raw.type(), positions) : converter.toBlock(vector, raw.type(), positions);
        }
        return new Page(positions, blocks);
    }

    /** Raw page -> page with the columns Trino asked for, in its order. */
    Page finish(Page raw, List<ArrowFlightColumnHandle> requested)
    {
        Block[] blocks = new Block[requested.size()];
        for (int channel = 0; channel < requested.size(); channel++) {
            ArrowFlightColumnHandle column = requested.get(channel);
            AggregateColumn aggregate = aggregation.aggregateFor(column).orElse(null);
            if (aggregate != null && aggregate.getFunction() == AggregateColumn.Function.AVG) {
                blocks[channel] = average(raw, aggregate);
            }
            else {
                blocks[channel] = raw.getBlock(rawIndex(column.getColumnName()));
            }
        }
        return new Page(raw.getPositionCount(), blocks);
    }

    private Block average(Page raw, AggregateColumn aggregate)
    {
        int sumIndex = rawIndex(aggregate.sumName());
        int countIndex = rawIndex(aggregate.countName());
        Type sumType = raws.get(sumIndex).type();
        Type outType = aggregate.getOutput().getColumnType();
        Block sums = raw.getBlock(sumIndex);
        Block counts = raw.getBlock(countIndex);
        BlockBuilder builder = outType.createBlockBuilder(null, raw.getPositionCount());
        for (int position = 0; position < raw.getPositionCount(); position++) {
            Object sum = readNativeValue(sumType, sums, position);
            Object count = readNativeValue(BIGINT, counts, position);
            if (sum == null || count == null || (Long) count == 0) {
                builder.appendNull();
                continue;
            }
            long n = (Long) count;
            Object value;
            if (outType.equals(DOUBLE)) {
                double total = sumType.equals(DOUBLE) ? (Double) sum : ArrowTypeConverter.decodeDecimal(sum, (DecimalType) sumType).doubleValue();
                value = total / n;
            }
            else {
                DecimalType decimal = (DecimalType) outType;
                BigDecimal total = ArrowTypeConverter.decodeDecimal(sum, (DecimalType) sumType);
                value = ArrowTypeConverter.encodeDecimal(total.divide(BigDecimal.valueOf(n), decimal.getScale(), RoundingMode.HALF_UP), decimal);
            }
            writeNativeValue(outType, builder, value);
        }
        return builder.build();
    }

    private int rawIndex(String name)
    {
        for (int i = 0; i < raws.size(); i++) {
            if (raws.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("No raw aggregation column " + name);
    }

    Merger merger()
    {
        return new Merger();
    }

    /** Combines partial aggregation rows from several endpoints. */
    final class Merger
    {
        private final Map<List<Object>, Object[]> groups = new LinkedHashMap<>();

        void add(Page raw)
        {
            for (int position = 0; position < raw.getPositionCount(); position++) {
                List<Object> key = new ArrayList<>();
                Object[] values = new Object[raws.size()];
                for (int i = 0; i < raws.size(); i++) {
                    Object value = readNativeValue(raws.get(i).type(), raw.getBlock(i), position);
                    if (raws.get(i).role() == Role.GROUP) {
                        key.add(value);
                    }
                    values[i] = value;
                }
                Object[] state = groups.get(key);
                if (state == null) {
                    groups.put(key, values);
                }
                else {
                    combine(state, values);
                }
            }
        }

        private void combine(Object[] state, Object[] values)
        {
            for (int i = 0; i < raws.size(); i++) {
                Raw raw = raws.get(i);
                Object a = state[i];
                Object b = values[i];
                if (raw.role() == Role.GROUP || b == null) {
                    continue;
                }
                if (a == null) {
                    state[i] = b;
                    continue;
                }
                state[i] = switch (raw.role()) {
                    case COUNT, AVG_COUNT, SUM, AVG_SUM -> add(raw.type(), a, b);
                    case MIN -> compare(a, b) <= 0 ? a : b;
                    case MAX -> compare(a, b) >= 0 ? a : b;
                    case GROUP -> a;
                };
            }
        }

        private Object add(Type type, Object a, Object b)
        {
            if (type.equals(DOUBLE)) {
                return (Double) a + (Double) b;
            }
            if (type instanceof DecimalType decimal) {
                return ArrowTypeConverter.encodeDecimal(
                        ArrowTypeConverter.decodeDecimal(a, decimal).add(ArrowTypeConverter.decodeDecimal(b, decimal)), decimal);
            }
            try {
                return Math.addExact((Long) a, (Long) b);
            }
            catch (ArithmeticException e) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "bigint addition overflow");
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private int compare(Object a, Object b)
        {
            return ((Comparable) a).compareTo(b);
        }

        Page result()
        {
            int positions = groups.size();
            Block[] blocks = new Block[raws.size()];
            for (int i = 0; i < raws.size(); i++) {
                Type type = raws.get(i).type();
                BlockBuilder builder = type.createBlockBuilder(null, positions);
                for (Object[] values : groups.values()) {
                    writeNativeValue(type, builder, values[i]);
                }
                blocks[i] = builder.build();
            }
            return new Page(positions, blocks);
        }

        @Override
        public String toString()
        {
            return "Merger" + Arrays.toString(raws.toArray());
        }
    }
}
