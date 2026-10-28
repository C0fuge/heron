package pro.surpin.arrowflight.common;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Lets a {@link FlightStream} read record batches whose IPC bodies the server compressed (ZSTD or LZ4 frame).
 *
 * <p>Arrow Java 18's FlightStream loads every batch with a {@code VectorLoader} that has no codec, so a
 * compressed body fails with "compression type not supported". It has no way to pass one in either, so
 * {@link #install} replaces the stream's loader with one that has the codecs of {@code arrow-compression},
 * once the schema arrived and before the caller reads the first batch. Plain bodies are loaded as before
 * by that loader, so a server that ignores the request is read unchanged.
 */
final class StreamDecompression
{
    /** Call header the server reads the codec from ({@code zstd}, {@code lz4}; absent = plain bodies). */
    static final String HEADER = "arrow-compression";

    private static final Field LOADER;
    private static final Field ROOT;
    private static final Field ENDED;
    private static final RuntimeException UNAVAILABLE;

    static {
        Field loader = null;
        Field root = null;
        Field ended = null;
        RuntimeException unavailable = null;
        try {
            loader = accessible("loader", VectorLoader.class);
            root = accessible("root", Future.class);
            // completed by gRPC's onCompleted / onError: the call ended, with or without a schema
            ended = accessible("cancelled", CompletableFuture.class);
            CommonsCompressionFactory.INSTANCE.getClass();
        }
        catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            unavailable = new IllegalStateException("Result compression is not supported with this Arrow Flight version: " + e, e);
        }
        LOADER = loader;
        ROOT = root;
        ENDED = ended;
        UNAVAILABLE = unavailable;
    }

    private StreamDecompression() {}

    private static Field accessible(String name, Class<?> type)
            throws NoSuchFieldException
    {
        Field field = FlightStream.class.getDeclaredField(name);
        if (!type.isAssignableFrom(field.getType())) {
            throw new IllegalStateException("FlightStream." + name + " is a " + field.getType().getName());
        }
        field.setAccessible(true);
        return field;
    }

    /** Fails when streams could not be decompressed, so a catalog asking for compression fails at start. */
    static void checkAvailable()
    {
        if (UNAVAILABLE != null) {
            throw UNAVAILABLE;
        }
    }

    /**
     * Waits for the stream's schema (or its end) and gives it a loader that decompresses. Must be called before
     * the first {@link FlightStream#next()}, on the thread that reads the stream. A stream that failed throws its
     * error here, as {@code next()} would.
     */
    static void install(FlightStream stream)
    {
        checkAvailable();
        try {
            Future<?> root = (Future<?>) ROOT.get(stream);
            CompletableFuture<?> ended = (CompletableFuture<?>) ENDED.get(stream);
            // gRPC sets the root on the schema message, before any batch and before the call ends
            while (!root.isDone() && !ended.isDone()) {
                try {
                    ended.get(10, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException | ExecutionException e) {
                    // wait on
                }
            }
            if (!root.isDone()) {
                // ended without a schema: no batch to load; next() reports the end or the error
                return;
            }
            VectorSchemaRoot schemaRoot = stream.getRoot();
            if (schemaRoot != null) {
                LOADER.set(stream, new VectorLoader(schemaRoot, CommonsCompressionFactory.INSTANCE));
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for the Flight stream's schema", e);
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}
