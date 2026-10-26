package pro.surpin.arrowflight.common;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.UnresolvedAddressException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * One Arrow data stream that has a connection to itself, see {@link FlightSqlSession#openStream}.
 * Closing the stream hands the connection back for the next stream.
 *
 * <p>The endpoint's locations are tried in order: when a location cannot be reached
 * (UNAVAILABLE: connection refused, connect timeout, unknown host, connection lost) before the
 * stream handed any batch to the caller, the same ticket is read from the next location. Once a
 * batch (or the end of the stream) was returned, a failure is final, so no row is read twice.
 *
 * <p>A connection reused from the pool may have died unnoticed (the node restarted since): when
 * such a stream fails unreachable before any data, the same location is tried once more on a new
 * connection before the next location.
 */
public final class FlightDataStream
        implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(FlightDataStream.class);

    /** Opens the ticket's stream at one location. */
    @FunctionalInterface
    interface Opener
    {
        Opened open(String location);
    }

    /**
     * A stream at one location.
     *
     * @param release called once when the stream is closed; true if the connection is fit for another stream
     * @param staleConnection asked when the stream failed unreachable before any data: true if the failure may be
     *         the reused connection's own, so a new connection to the same location is worth one try
     */
    record Opened(FlightStream stream, Consumer<Boolean> release, BooleanSupplier staleConnection) {}

    private final List<String> locations;
    private final Opener opener;
    private final BiConsumer<String, RuntimeException> onFailure;
    private int index;
    private Opened current;
    // A next() call returned normally: data (or the end) reached the caller, retrying could duplicate rows.
    private boolean delivered;
    // The current location was already retried on a new connection.
    private boolean retriedOnNewConnection;
    private boolean failed;
    private boolean cancelled;
    private boolean closed;

    /**
     * @param locations locations in the order to try, at least one
     * @param onFailure told about every failed read, with the location it failed at
     */
    FlightDataStream(List<String> locations, Opener opener, BiConsumer<String, RuntimeException> onFailure)
    {
        this.locations = List.copyOf(requireNonNull(locations, "locations is null"));
        if (this.locations.isEmpty()) {
            throw new IllegalArgumentException("No location to read the stream from");
        }
        this.opener = requireNonNull(opener, "opener is null");
        this.onFailure = requireNonNull(onFailure, "onFailure is null");
    }

    /** Loads the next batch into {@link #getRoot()}; false at the end of the stream. */
    public boolean next()
    {
        while (true) {
            try {
                boolean more = open().stream().next();
                delivered = true;
                return more;
            }
            catch (RuntimeException e) {
                failed = true;
                String location = locations.get(index);
                // asked before onFailure, which retires the location's pooled connections
                boolean retryHere = !delivered && !cancelled && !closed && !retriedOnNewConnection
                        && current != null && isUnreachable(e) && current.staleConnection().getAsBoolean();
                onFailure.accept(location, e);
                if (retryHere) {
                    LOG.warn("Flight stream at {} lost its reused connection before any data ({}); "
                            + "retrying on a new one", location, e.getMessage());
                    closeCurrent();
                    failed = false;
                    retriedOnNewConnection = true;
                    continue;
                }
                if (delivered && !cancelled && !closed) {
                    LOG.warn("Flight stream at {} failed after delivering data ({}); not retried", location, e.getMessage());
                }
                if (delivered || cancelled || closed || index + 1 >= locations.size() || !isUnreachable(e)) {
                    throw e;
                }
                LOG.warn("Flight stream at {} failed before any data ({}); reading it from {}",
                        location, e.getMessage(), locations.get(index + 1));
                closeCurrent();
                failed = false;
                retriedOnNewConnection = false;
                index++;
            }
        }
    }

    public VectorSchemaRoot getRoot()
    {
        return open().stream().getRoot();
    }

    /** Location the stream is read from now. */
    public String location()
    {
        return locations.get(index);
    }

    /** Asks the server to stop producing; the stream still has to be closed. */
    public void cancel(String message)
    {
        cancelled = true;
        if (current != null) {
            current.stream().cancel(message, null);
        }
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        closeCurrent();
    }

    private Opened open()
    {
        if (current == null) {
            if (closed) {
                throw new IllegalStateException("Flight stream is closed");
            }
            current = requireNonNull(opener.open(locations.get(index)), "opened stream is null");
        }
        return current;
    }

    private void closeCurrent()
    {
        Opened opened = current;
        current = null;
        if (opened == null) {
            return;
        }
        try {
            opened.stream().close();
        }
        catch (Exception e) {
            failed = true;
            LOG.warn("Error closing Flight stream: {}", e.toString());
        }
        finally {
            opened.release().accept(!failed);
        }
    }

    /**
     * Whether a failure means the location could not be reached (or dropped the connection),
     * as opposed to the server refusing or failing the query itself.
     */
    static boolean isUnreachable(Throwable e)
    {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof FlightRuntimeException f && f.status().code() == FlightStatusCode.UNAVAILABLE) {
                return true;
            }
            // ClosedChannelException: gRPC reports a pooled connection the peer closed as UNKNOWN "channel closed" when
            // the call's first write races the close (seen in FlightSqlSessionFailoverTest, build run 40290807373)
            if (t instanceof ConnectException || t instanceof NoRouteToHostException || t instanceof UnknownHostException
                    || t instanceof UnresolvedAddressException || t instanceof ClosedChannelException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }
}
