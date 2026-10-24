package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregationReaderTest
{
    private static final ArrowFlightColumnHandle KEY = new ArrowFlightColumnHandle("k", BIGINT);
    private static final ArrowFlightColumnHandle DAY = new ArrowFlightColumnHandle("d", DATE);
    private static final ArrowFlightColumnHandle VALUE = new ArrowFlightColumnHandle("v", BIGINT);
    private static final ArrowFlightColumnHandle COUNT = new ArrowFlightColumnHandle("afagg0", BIGINT);
    private static final ArrowFlightColumnHandle SUM = new ArrowFlightColumnHandle("afagg1", BIGINT);
    private static final ArrowFlightColumnHandle MIN = new ArrowFlightColumnHandle("afagg2", BIGINT);

    /** GROUP BY k: count(*), sum(v), min(v) -> raw columns k, afagg0, afagg1, afagg2. */
    private static final Aggregation AGGREGATION = new Aggregation(List.of(KEY), List.of(
            new AggregateColumn(AggregateColumn.Function.COUNT_STAR, Optional.empty(), COUNT),
            new AggregateColumn(AggregateColumn.Function.SUM, Optional.of(VALUE), SUM),
            new AggregateColumn(AggregateColumn.Function.MIN, Optional.of(VALUE), MIN)));
    private static final List<Type> RAW_TYPES = List.of(BIGINT, BIGINT, BIGINT, BIGINT);

    private static AggregationReader reader(Aggregation aggregation)
    {
        return new AggregationReader(aggregation, new ArrowTypeConverter());
    }

    @Test
    void mergesEqualGroupsOfSeveralSortedStreams()
    {
        // rows: k, count, sum, min
        TestStream a = stream(List.of(row(1L, 1L, 10L, 10L), row(3L, 2L, 30L, 5L)), List.of(row(7L, 1L, 70L, 70L)));
        TestStream b = stream(List.of(row(null, 4L, 4L, 1L), row(3L, 1L, 3L, 3L), row(4L, 1L, 40L, 40L)));
        TestStream c = stream(List.of(), List.of(row(1L, 2L, 5L, 2L), row(7L, 3L, 7L, 1L)));

        List<Object[]> merged = drain(reader(AGGREGATION).sortedMerger(List.of(a, b, c), 1000));

        assertRows(List.of(
                row(null, 4L, 4L, 1L),
                row(1L, 3L, 15L, 2L),
                row(3L, 3L, 33L, 3L),
                row(4L, 1L, 40L, 40L),
                row(7L, 4L, 77L, 1L)), merged);
        assertTrue(a.closed && b.closed && c.closed);
    }

    @Test
    void emitsPagesOfAtMostMaxRowsWithoutSplittingAGroup()
    {
        List<Object[]> rowsA = new ArrayList<>();
        List<Object[]> rowsB = new ArrayList<>();
        for (long k = 0; k < 100; k++) {
            rowsA.add(row(k, 1L, k, k));
            rowsB.add(row(k, 1L, k, k));
        }
        AggregationReader.SortedMerger merger = reader(AGGREGATION).sortedMerger(
                List.of(stream(rowsA.subList(0, 37), rowsA.subList(37, 100)), stream(rowsB)), 7);
        List<Object[]> all = new ArrayList<>();
        Page page;
        while ((page = merger.nextPage()) != null) {
            assertTrue(page.getPositionCount() <= 7, "page of " + page.getPositionCount());
            all.addAll(rows(page));
        }
        assertEquals(100, all.size());
        for (int k = 0; k < 100; k++) {
            assertEquals(List.of((long) k, 2L, 2L * k, (long) k), Arrays.asList(all.get(k)));
        }
    }

    @Test
    void streamOutOfOrderFailsInsteadOfReturningAGroupTwice()
    {
        TestStream unsorted = stream(List.of(row(5L, 1L, 1L, 1L), row(2L, 1L, 1L, 1L)));
        TestStream other = stream(List.of(row(2L, 1L, 1L, 1L)));
        AggregationReader.SortedMerger merger = reader(AGGREGATION).sortedMerger(List.of(unsorted, other), 1000);
        TrinoException e = assertThrows(TrinoException.class, () -> drain(merger));
        assertTrue(e.getMessage().contains("out of GROUP BY order"), e.getMessage());
    }

    @Test
    void sameResultAsHashMergeOnRandomPartialGroups()
    {
        Random random = new Random(42);
        for (int round = 0; round < 20; round++) {
            int streamCount = 1 + random.nextInt(4);
            List<TestStream> sorted = new ArrayList<>();
            AggregationReader.Merger hash = reader(AGGREGATION).merger();
            for (int s = 0; s < streamCount; s++) {
                // one endpoint's partial result: distinct keys (some NULL), ascending NULLS FIRST
                Set<Long> keys = new HashSet<>();
                int n = random.nextInt(300);
                for (int i = 0; i < n; i++) {
                    keys.add((long) random.nextInt(500));
                }
                List<Long> ordered = new ArrayList<>(keys);
                ordered.sort(null);
                List<Object[]> rows = new ArrayList<>();
                if (random.nextBoolean()) {
                    rows.add(row(null, 1L + random.nextInt(5), (long) random.nextInt(100), (long) random.nextInt(100)));
                }
                for (Long k : ordered) {
                    rows.add(row(k, 1L + random.nextInt(5), (long) random.nextInt(100) - 50, (long) random.nextInt(100)));
                }
                hash.add(page(RAW_TYPES, rows));
                // cut into batches of random sizes, some empty
                List<List<Object[]>> batches = new ArrayList<>();
                int from = 0;
                while (from < rows.size()) {
                    int to = Math.min(rows.size(), from + random.nextInt(50));
                    batches.add(rows.subList(from, to));
                    from = to;
                }
                sorted.add(new TestStream(RAW_TYPES, batches));
            }
            List<Object[]> expected = rows(hash.result());
            expected.sort((x, y) -> x[0] == null ? (y[0] == null ? 0 : -1) : (y[0] == null ? 1 : Long.compare((Long) x[0], (Long) y[0])));
            List<Object[]> actual = drain(reader(AGGREGATION).sortedMerger(new ArrayList<>(sorted), 1 + random.nextInt(64)));
            assertRows(expected, actual);
        }
    }

    @Test
    void sortedMergeOnlyForGroupedAggregationsOnServerOrderedTypes()
    {
        assertTrue(reader(AGGREGATION).canMergeSorted());
        assertTrue(reader(new Aggregation(List.of(KEY, DAY), List.of())).canMergeSorted());
        assertFalse(reader(new Aggregation(List.of(), AGGREGATION.getAggregates())).canMergeSorted());
        assertFalse(reader(new Aggregation(List.of(KEY, new ArrowFlightColumnHandle("s", VARCHAR)), List.of())).canMergeSorted());
        assertFalse(reader(new Aggregation(List.of(new ArrowFlightColumnHandle("x", DOUBLE)), List.of())).canMergeSorted());
    }

    @Test
    void closeCancelsStreamsNotYetFinished()
    {
        TestStream a = stream(List.of(row(1L, 1L, 1L, 1L)), List.of(row(2L, 1L, 1L, 1L)), List.of(row(3L, 1L, 1L, 1L)));
        TestStream b = stream(List.of(row(1L, 1L, 1L, 1L)), List.of(row(5L, 1L, 1L, 1L)));
        AggregationReader.SortedMerger merger = reader(AGGREGATION).sortedMerger(List.of(a, b), 1);
        assertEquals(1, merger.nextPage().getPositionCount());
        assertFalse(a.closed || b.closed);
        merger.close();
        assertTrue(a.closed && b.closed);
    }

    // ------------------------------------------------------------------ helpers

    private static Object[] row(Object... values)
    {
        return values;
    }

    @SafeVarargs
    private static TestStream stream(List<Object[]>... batches)
    {
        return new TestStream(RAW_TYPES, List.of(batches));
    }

    private static Page page(List<Type> types, List<Object[]> rows)
    {
        Block[] blocks = new Block[types.size()];
        for (int i = 0; i < types.size(); i++) {
            BlockBuilder builder = types.get(i).createBlockBuilder(null, rows.size());
            for (Object[] row : rows) {
                writeNativeValue(types.get(i), builder, row[i]);
            }
            blocks[i] = builder.build();
        }
        return new Page(rows.size(), blocks);
    }

    private static List<Object[]> rows(Page page)
    {
        List<Object[]> out = new ArrayList<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            Object[] row = new Object[page.getChannelCount()];
            for (int i = 0; i < row.length; i++) {
                row[i] = readNativeValue(RAW_TYPES.get(i), page.getBlock(i), position);
            }
            out.add(row);
        }
        return out;
    }

    private static List<Object[]> drain(AggregationReader.SortedMerger merger)
    {
        List<Object[]> out = new ArrayList<>();
        Page page;
        while ((page = merger.nextPage()) != null) {
            out.addAll(rows(page));
        }
        assertNull(merger.nextPage());
        return out;
    }

    private static void assertRows(List<Object[]> expected, List<Object[]> actual)
    {
        assertEquals(expected.stream().map(Arrays::asList).toList(), actual.stream().map(Arrays::asList).toList());
    }

    private static final class TestStream
            implements AggregationReader.PageStream
    {
        private final Deque<Page> pages = new ArrayDeque<>();
        private boolean closed;

        TestStream(List<Type> types, List<List<Object[]>> batches)
        {
            batches.forEach(batch -> pages.add(page(types, ImmutableList.copyOf(batch))));
        }

        @Override
        public Page next()
        {
            Page page = pages.poll();
            if (page == null) {
                closed = true;
            }
            return page;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
