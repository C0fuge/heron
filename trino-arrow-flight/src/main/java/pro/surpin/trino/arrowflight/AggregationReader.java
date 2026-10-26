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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
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

    private static final long BYTES_PER_GROUP = 128;
    private static final long BYTES_PER_RAW_VALUE = 40;

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
    private static int compare(Object a, Object b)
    {
        return ((Comparable) a).compareTo(b);
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

        Page result()
        {
            return toPage(new ArrayList<>(groups.values()));
        }

        /**
         * Next page of at most {@code maxRows} merged rows, removed from the merger as they are emitted (the groups
         * leave memory page by page); null once every group was emitted.
         */
        Page nextPage(int maxRows)
        {
            if (groups.isEmpty()) {
                return null;
            }
            List<Object[]> out = new ArrayList<>(Math.min(maxRows, groups.size()));
            Iterator<Object[]> iterator = groups.values().iterator();
            while (iterator.hasNext() && out.size() < maxRows) {
                out.add(iterator.next());
                iterator.remove();
            }
            return toPage(out);
        }

        int groupCount()
        {
            return groups.size();
        }

        /**
         * Rough heap held by the groups: per group a map entry, the key list, the value array and one boxed value
         * per raw column (measured ~210 bytes per group for GROUP BY bigint + one decimal sum, TPC-H q18).
         */
        long estimatedBytes()
        {
            return groups.size() * (BYTES_PER_GROUP + BYTES_PER_RAW_VALUE * raws.size());
        }

        private Page toPage(List<Object[]> rows)
        {
            int positions = rows.size();
            Block[] blocks = new Block[raws.size()];
            for (int i = 0; i < raws.size(); i++) {
                Type type = raws.get(i).type();
                BlockBuilder builder = type.createBlockBuilder(null, positions);
                for (Object[] values : rows) {
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
    /**
     * True if the groups of this aggregation can be merged from streams sorted by the server:
     * every grouping column has a type whose server order equals Trino's (see
     * {@link SqlQueryBuilder#isTopNSortable}).
     */
    boolean canMergeSorted()
    {
        return !aggregation.getGroupingColumns().isEmpty()
                && aggregation.getGroupingColumns().stream().allMatch(c -> SqlQueryBuilder.isTopNSortable(c.getColumnType()));
    }

    SortedMerger sortedMerger(List<PageStream> streams, int maxRows)
    {
        return new SortedMerger(streams, maxRows);
    }

    /** Raw pages of one endpoint, in the order the server sent them; null at the end. */
    interface PageStream
    {
        Page next();

        void close();
    }

    /**
     * Merges partial groups from several endpoints whose rows are sorted by the grouping columns
     * ({@code ORDER BY g1 NULLS FIRST, ...}): equal groups meet at the head of the streams, so
     * only one group is held at a time and merged rows are emitted page by page. A stream that is
     * not sorted fails the query (merging it would return a group twice).
     */
    final class SortedMerger
    {
        private final List<PageStream> streams;
        private final int maxRows;
        private final int[] groupIndexes;
        private final PriorityQueue<Cursor> heads;
        private boolean started;
        private Object[] pending;

        private SortedMerger(List<PageStream> streams, int maxRows)
        {
            this.streams = List.copyOf(streams);
            this.maxRows = maxRows;
            this.groupIndexes = java.util.stream.IntStream.range(0, raws.size()).filter(i -> raws.get(i).role() == Role.GROUP).toArray();
            this.heads = new PriorityQueue<>(Math.max(1, streams.size()), (a, b) -> compareKeys(a.values, b.values));
        }

        /** Next page of merged raw rows, or null when every stream is exhausted. */
        Page nextPage()
        {
            if (!started) {
                started = true;
                for (PageStream stream : streams) {
                    Cursor cursor = new Cursor(stream);
                    if (cursor.advance()) {
                        heads.add(cursor);
                    }
                }
            }
            List<Object[]> out = new ArrayList<>();
            while (!heads.isEmpty() && out.size() < maxRows) {
                Cursor cursor = heads.poll();
                Object[] values = cursor.values;
                if (pending != null && compareKeys(pending, values) == 0) {
                    combine(pending, values);
                }
                else {
                    if (pending != null) {
                        out.add(pending);
                    }
                    pending = values;
                }
                if (cursor.advance()) {
                    heads.add(cursor);
                }
            }
            if (heads.isEmpty() && pending != null && out.size() < maxRows) {
                out.add(pending);
                pending = null;
            }
            if (out.isEmpty()) {
                return null;
            }
            Block[] blocks = new Block[raws.size()];
            for (int i = 0; i < raws.size(); i++) {
                Type type = raws.get(i).type();
                BlockBuilder builder = type.createBlockBuilder(null, out.size());
                for (Object[] values : out) {
                    writeNativeValue(type, builder, values[i]);
                }
                blocks[i] = builder.build();
            }
            return new Page(out.size(), blocks);
        }

        void close()
        {
            streams.forEach(PageStream::close);
        }

        private int compareKeys(Object[] a, Object[] b)
        {
            for (int index : groupIndexes) {
                Object x = a[index];
                Object y = b[index];
                int c = x == null ? (y == null ? 0 : -1) : (y == null ? 1 : compare(x, y));
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }

        private final class Cursor
        {
            private final PageStream stream;
            private Page page;
            private int position;
            private Object[] values;

            Cursor(PageStream stream)
            {
                this.stream = stream;
            }

            /** Moves to the next row; false at the end of the stream. */
            boolean advance()
            {
                while (page == null || position >= page.getPositionCount()) {
                    page = stream.next();
                    position = 0;
                    if (page == null) {
                        values = null;
                        return false;
                    }
                }
                Object[] next = new Object[raws.size()];
                for (int i = 0; i < raws.size(); i++) {
                    next[i] = readNativeValue(raws.get(i).type(), page.getBlock(i), position);
                }
                position++;
                if (values != null && compareKeys(values, next) > 0) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR,
                            "Arrow Flight server returned aggregation groups out of GROUP BY order; cannot merge them "
                                    + "(set flight.aggregation_sorted_merge_enabled = false)");
                }
                values = next;
                return true;
            }
        }
    }
}
