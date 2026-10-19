package pro.surpin.arrowflight.common;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * One Arrow data stream that has a connection to itself, see {@link FlightSqlSession#openStream}.
 * Closing the stream hands the connection back for the next stream.
 */
public final class FlightDataStream
        implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(FlightDataStream.class);

    private final FlightStream stream;
    private final Consumer<RuntimeException> onFailure;
    private final Consumer<Boolean> release;
    private boolean failed;
    private boolean closed;

    /**
     * @param onFailure told about a failed read
     * @param release called once on close; true if the connection is fit for another stream
     */
    FlightDataStream(FlightStream stream, Consumer<RuntimeException> onFailure, Consumer<Boolean> release)
    {
        this.stream = requireNonNull(stream, "stream is null");
        this.onFailure = requireNonNull(onFailure, "onFailure is null");
        this.release = requireNonNull(release, "release is null");
    }

    /** Loads the next batch into {@link #getRoot()}; false at the end of the stream. */
    public boolean next()
    {
        try {
            return stream.next();
        }
        catch (RuntimeException e) {
            failed = true;
            onFailure.accept(e);
            throw e;
        }
    }

    public VectorSchemaRoot getRoot()
    {
        return stream.getRoot();
    }

    /** Asks the server to stop producing; the stream still has to be closed. */
    public void cancel(String message)
    {
        stream.cancel(message, null);
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            stream.close();
        }
        catch (Exception e) {
            failed = true;
            LOG.warn("Error closing Flight stream: {}", e.toString());
        }
        finally {
            release.accept(!failed);
        }
    }
}
