package pro.surpin.trino.arrowflight;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Reads the endpoints of one split several at a time. The server answers a query with one
 * endpoint per Flight node; read one after another, a split of a 3-node cluster keeps only one
 * node busy at a time. Here up to {@code parallelism} streams are open at once: each stream's
 * {@code next()} runs on a background thread, and the driver takes whichever batch is ready.
 * <p>
 * Each stream keeps its own failover rules (it is still read by one thread at a time, and a
 * batch is handed out only after its {@code next()} returned). A stream's next batch is asked
 * for only once the caller is done with the previous one, because a stream reuses its root.
 */
final class ParallelStreamReader
        implements AutoCloseable
{
    /** One Arrow stream, as {@link pro.surpin.arrowflight.common.FlightDataStream} offers it. */
    interface BatchStream
    {
        boolean next();

        VectorSchemaRoot getRoot();

        void cancel(String message);

        void close();
    }

    private static final AtomicInteger THREADS = new AtomicInteger();
    // Threads only wait on the network; the stream count is bounded by Trino's drivers x parallelism.
    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "arrow-flight-read-" + THREADS.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    });

    private final class Slot
    {
        final BatchStream stream;
        CompletableFuture<Boolean> next;

        Slot(BatchStream stream)
        {
            this.stream = stream;
        }

        void requestNext()
        {
            next = CompletableFuture.supplyAsync(stream::next, executor);
        }
    }

    private final Deque<Supplier<BatchStream>> pending;
    private final int parallelism;
    private final ExecutorService executor;
    private final List<Slot> active = new ArrayList<>();
    private Slot handedOut;
    private boolean closed;

    /**
     * @param streams opens each stream, in order (called on the caller's thread)
     */
    ParallelStreamReader(List<Supplier<BatchStream>> streams, int parallelism)
    {
        this(streams, parallelism, EXECUTOR);
    }

    ParallelStreamReader(List<Supplier<BatchStream>> streams, int parallelism, ExecutorService executor)
    {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism < 1");
        }
        this.pending = new ArrayDeque<>(requireNonNull(streams, "streams is null"));
        this.parallelism = parallelism;
        this.executor = requireNonNull(executor, "executor is null");
    }

    /**
     * A non-empty batch that is ready, or null when none is ready yet (see {@link #isBlocked()})
     * or all streams ended (see {@link #isFinished()}). The batch is valid until the next call.
     */
    VectorSchemaRoot poll()
    {
        if (closed) {
            return null;
        }
        if (handedOut != null) {
            handedOut.requestNext();
            handedOut = null;
        }
        while (active.size() < parallelism && !pending.isEmpty()) {
            Slot slot = new Slot(pending.poll().get());
            active.add(slot);
            slot.requestNext();
        }
        for (int i = 0; i < active.size(); i++) {
            Slot slot = active.get(i);
            if (!slot.next.isDone()) {
                continue;
            }
            boolean more;
            try {
                more = slot.next.join();
            }
            catch (CompletionException e) {
                throw e.getCause() instanceof RuntimeException r ? r : e;
            }
            if (!more) {
                active.remove(i);
                slot.stream.close();
                return poll();
            }
            VectorSchemaRoot root = slot.stream.getRoot();
            if (root.getRowCount() == 0) {
                slot.requestNext();
                continue;
            }
            handedOut = slot;
            return root;
        }
        return null;
    }

    /**
     * The caller is done with the batch {@link #poll()} returned: its stream starts loading the next one now (in the
     * background) instead of at the next {@code poll()}. Optional; the root must not be used afterwards.
     */
    void release()
    {
        if (handedOut != null && !closed) {
            handedOut.requestNext();
            handedOut = null;
        }
    }

    boolean isFinished()
    {
        return closed || (active.isEmpty() && pending.isEmpty());
    }

    /** Done when some stream has a batch (or failed) or nothing is left to wait for. */
    CompletableFuture<?> isBlocked()
    {
        if (closed || handedOut != null || active.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        // A failed read completes it normally: poll() throws the failure.
        return CompletableFuture.anyOf(active.stream()
                .map(slot -> slot.next.handle((more, error) -> null))
                .toArray(CompletableFuture[]::new));
    }

    /** Cancels every open stream; a stream still inside {@code next()} is closed when that call returns. */
    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        pending.clear();
        RuntimeException failure = null;
        for (Slot slot : active) {
            try {
                // An active stream has not reached its end (LIMIT reached or query cancelled).
                try {
                    slot.stream.cancel("closed");
                }
                catch (RuntimeException ignored) {
                    // best effort
                }
                if (slot.next.isDone()) {
                    slot.stream.close();
                }
                else {
                    slot.next.whenComplete((ignored, error) -> slot.stream.close());
                }
            }
            catch (RuntimeException e) {
                failure = e;
            }
        }
        active.clear();
        handedOut = null;
        if (failure != null) {
            throw failure;
        }
    }
}
