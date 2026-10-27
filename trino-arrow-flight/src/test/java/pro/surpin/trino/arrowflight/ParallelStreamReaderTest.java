package pro.surpin.trino.arrowflight;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParallelStreamReaderTest
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final AtomicInteger open = new AtomicInteger();
    private final AtomicInteger maxOpen = new AtomicInteger();

    @AfterEach
    void tearDown()
    {
        executor.shutdownNow();
    }

    /** Batches of the given row counts; next() may wait on a latch first. */
    private class FakeStream
            implements ParallelStreamReader.BatchStream
    {
        final int[] rows;
        final VectorSchemaRoot root = new VectorSchemaRoot(List.of(), List.of(), 0);
        final AtomicInteger inNext = new AtomicInteger();
        volatile CountDownLatch gate;
        volatile RuntimeException failAt;
        int index = -1;
        volatile boolean overlapped;
        volatile boolean cancelled;
        volatile boolean closed;
        volatile boolean closedDuringNext;

        FakeStream(int... rows)
        {
            this.rows = rows;
        }

        @Override
        public boolean next()
        {
            if (inNext.incrementAndGet() > 1) {
                overlapped = true;
            }
            try {
                if (gate != null) {
                    gate.await(10, TimeUnit.SECONDS);
                }
                if (failAt != null) {
                    throw failAt;
                }
                index++;
                if (index >= rows.length) {
                    return false;
                }
                root.setRowCount(rows[index]);
                return true;
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finally {
                if (closed) {
                    closedDuringNext = true;
                }
                inNext.decrementAndGet();
            }
        }

        @Override
        public VectorSchemaRoot getRoot()
        {
            return root;
        }

        @Override
        public void cancel(String message)
        {
            cancelled = true;
        }

        @Override
        public void close()
        {
            closed = true;
            open.decrementAndGet();
        }
    }

    private Supplier<ParallelStreamReader.BatchStream> opener(FakeStream stream)
    {
        return () -> {
            maxOpen.accumulateAndGet(open.incrementAndGet(), Math::max);
            return stream;
        };
    }

    private ParallelStreamReader reader(int parallelism, FakeStream... streams)
    {
        List<Supplier<ParallelStreamReader.BatchStream>> openers = new ArrayList<>();
        for (FakeStream stream : streams) {
            openers.add(opener(stream));
        }
        return new ParallelStreamReader(openers, parallelism, executor);
    }

    /** Sums the rows of every batch, checking that a handed-out batch is not overwritten meanwhile. */
    private static long drain(ParallelStreamReader reader)
            throws Exception
    {
        long rows = 0;
        while (!reader.isFinished()) {
            VectorSchemaRoot root = reader.poll();
            if (root == null) {
                reader.isBlocked().get(10, TimeUnit.SECONDS);
                continue;
            }
            int count = root.getRowCount();
            assertTrue(count > 0);
            Thread.sleep(1);
            assertEquals(count, root.getRowCount(), "batch changed while handed out");
            rows += count;
        }
        return rows;
    }

    @Test
    void readsEveryBatchOfEveryStreamWithBoundedParallelism()
            throws Exception
    {
        FakeStream a = new FakeStream(5, 0, 7);
        FakeStream b = new FakeStream(11);
        FakeStream c = new FakeStream();
        FakeStream d = new FakeStream(1, 2, 3);
        ParallelStreamReader reader = reader(2, a, b, c, d);
        assertEquals(29, drain(reader));
        assertNull(reader.poll());
        assertEquals(2, maxOpen.get());
        for (FakeStream stream : List.of(a, b, c, d)) {
            assertTrue(stream.closed);
            assertFalse(stream.cancelled, "a stream read to its end is not cancelled");
            assertFalse(stream.overlapped);
        }
        assertEquals(0, open.get());
        reader.close();
    }

    @Test
    void parallelismOneReadsOneStreamAtATime()
            throws Exception
    {
        ParallelStreamReader reader = reader(1, new FakeStream(1), new FakeStream(2), new FakeStream(3));
        assertEquals(6, drain(reader));
        assertEquals(1, maxOpen.get());
    }

    @Test
    void releaseStartsTheNextReadBeforeTheNextPoll()
            throws Exception
    {
        // Read-ahead: once the caller has converted a batch, the stream loads the next one while the caller is busy.
        CountDownLatch secondRead = new CountDownLatch(2);
        FakeStream stream = new FakeStream(3, 4, 5)
        {
            @Override
            public boolean next()
            {
                secondRead.countDown();
                return super.next();
            }
        };
        ParallelStreamReader reader = reader(1, stream);
        VectorSchemaRoot first = null;
        while (first == null) {
            first = reader.poll();
            if (first == null) {
                reader.isBlocked().get(10, TimeUnit.SECONDS);
            }
        }
        assertEquals(3, first.getRowCount());
        assertEquals(1, secondRead.getCount(), "no read ahead before release()");
        reader.release();
        assertTrue(secondRead.await(10, TimeUnit.SECONDS), "release() did not start the next read");
        reader.release();  // nothing handed out: no-op
        assertEquals(9, drain(reader));
        assertFalse(stream.overlapped);
        assertTrue(stream.closed);
        reader.close();
    }

    @Test
    void streamsAreReadAtTheSameTime()
            throws Exception
    {
        // Each next() waits until all three streams are inside next(): only parallel reads get past it.
        CountDownLatch all = new CountDownLatch(3);
        List<FakeStream> streams = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            streams.add(new FakeStream(4)
            {
                boolean first = true;

                @Override
                public boolean next()
                {
                    if (first) {
                        first = false;
                        all.countDown();
                        try {
                            assertTrue(all.await(10, TimeUnit.SECONDS), "streams were not read in parallel");
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return super.next();
                }
            });
        }
        ParallelStreamReader reader = reader(3, streams.toArray(FakeStream[]::new));
        assertEquals(12, drain(reader));
    }

    @Test
    void failureOfOneStreamSurfacesAndCloseCancelsTheOthers()
            throws Exception
    {
        FakeStream ok = new FakeStream(1, 1, 1);
        ok.gate = new CountDownLatch(1);   // still inside next() when the other one fails
        FakeStream bad = new FakeStream(1);
        RuntimeException failure = new RuntimeException("node lost");
        bad.failAt = failure;
        ParallelStreamReader reader = reader(2, ok, bad);
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> drain(reader));
        assertSame(failure, thrown);

        reader.close();
        assertTrue(ok.cancelled);
        assertFalse(ok.closed, "not closed while inside next()");
        ok.gate.countDown();
        for (int i = 0; i < 100 && !ok.closed; i++) {
            Thread.sleep(10);
        }
        assertTrue(ok.closed, "closed once next() returned");
        assertFalse(ok.closedDuringNext);
        assertTrue(bad.closed);
        assertTrue(reader.isFinished());
        assertNull(reader.poll());
    }

    @Test
    void closeBeforeTheEndCancelsOpenStreamsAndSkipsUnopenedOnes()
            throws Exception
    {
        FakeStream a = new FakeStream(3, 3);
        FakeStream b = new FakeStream(3, 3);
        FakeStream unopened = new FakeStream(3);
        ParallelStreamReader reader = reader(2, a, b, unopened);
        VectorSchemaRoot root;
        while ((root = reader.poll()) == null) {
            reader.isBlocked().get(10, TimeUnit.SECONDS);
        }
        assertEquals(3, root.getRowCount());
        reader.close();   // e.g. LIMIT reached
        for (int i = 0; i < 100 && !(a.closed && b.closed); i++) {
            Thread.sleep(10);
        }
        assertTrue(a.cancelled && a.closed);
        assertTrue(b.cancelled && b.closed);
        assertFalse(unopened.closed);
        assertEquals(2, maxOpen.get());
        assertEquals(0, open.get());
    }
}
