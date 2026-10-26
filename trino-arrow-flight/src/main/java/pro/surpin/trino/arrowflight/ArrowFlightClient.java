package pro.surpin.trino.arrowflight;

import com.google.inject.Inject;
import jakarta.annotation.PreDestroy;
import pro.surpin.arrowflight.common.FlightSqlSession;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Trino-scoped singleton over the shared {@link FlightSqlSession}. Guice builds it from the
 * catalog properties; {@link PreDestroy} ties its lifecycle to the connector shutdown.
 */
public class ArrowFlightClient
        extends FlightSqlSession
{
    static final long SEVERAL_ENDPOINTS_MEMORY_NANOS = TimeUnit.HOURS.toNanos(1);

    @Inject
    public ArrowFlightClient(ArrowFlightConfig config)
    {
        super(config.toFlightConfig());
    }

    // When a FlightInfo last had several endpoints (System.nanoTime(), valid once sawSeveralEndpoints).
    private final AtomicLong lastSeveralEndpointsNanos = new AtomicLong();
    private final AtomicBoolean sawSeveralEndpoints = new AtomicBoolean();
    private final AtomicBoolean sawFlightInfo = new AtomicBoolean();

    /**
     * Hint for planning a query before its FlightInfo is known: will it likely have more than one endpoint? True
     * until a FlightInfo was seen, and while one with several endpoints was seen in the last
     * {@link #SEVERAL_ENDPOINTS_MEMORY_NANOS}. A cluster that answers some queries with a single endpoint (a server
     * that answers one completely) must not turn the hint off for the queries that run next: the hint decides
     * whether a grouped aggregation is merged as sorted streams or in memory (TPC-H q18 on 3 nodes held every group
     * of lineitem in the heap). Only an install that answers with one endpoint for that long counts as single-node.
     */
    public boolean expectsSeveralEndpoints()
    {
        return expectsSeveralEndpoints(System.nanoTime());
    }

    boolean expectsSeveralEndpoints(long nowNanos)
    {
        return !sawFlightInfo.get()
                || (sawSeveralEndpoints.get() && nowNanos - lastSeveralEndpointsNanos.get() < SEVERAL_ENDPOINTS_MEMORY_NANOS);
    }

    public void recordEndpointCount(int count)
    {
        recordEndpointCount(count, System.nanoTime());
    }

    void recordEndpointCount(int count, long nowNanos)
    {
        if (count > 1) {
            lastSeveralEndpointsNanos.set(nowNanos);
            sawSeveralEndpoints.set(true);
        }
        sawFlightInfo.set(true);
    }

    @PreDestroy
    @Override
    public void close()
    {
        super.close();
    }
}
