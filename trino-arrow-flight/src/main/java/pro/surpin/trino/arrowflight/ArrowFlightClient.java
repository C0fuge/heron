package pro.surpin.trino.arrowflight;

import com.google.inject.Inject;
import jakarta.annotation.PreDestroy;
import pro.surpin.arrowflight.common.FlightSqlSession;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Trino-scoped singleton over the shared {@link FlightSqlSession}. Guice builds it from the
 * catalog properties; {@link PreDestroy} ties its lifecycle to the connector shutdown.
 */
public class ArrowFlightClient
        extends FlightSqlSession
{
    static final long SEVERAL_ENDPOINTS_MEMORY_NANOS = TimeUnit.HOURS.toNanos(1);
    static final int SINGLE_ENDPOINT_ANSWERS = 100;

    @Inject
    public ArrowFlightClient(ArrowFlightConfig config)
    {
        super(config.toFlightConfig());
    }

    // When a FlightInfo last had several endpoints (System.nanoTime(), valid once sawSeveralEndpoints).
    private final AtomicLong lastSeveralEndpointsNanos = new AtomicLong();
    private final AtomicBoolean sawSeveralEndpoints = new AtomicBoolean();
    // FlightInfos with a single endpoint while none with several was seen.
    private final AtomicInteger singleEndpointAnswers = new AtomicInteger();

    /**
     * Hint for planning a query before its FlightInfo is known: will it likely have more than one endpoint? It decides
     * whether a grouped aggregation is merged as sorted streams or in memory (TPC-H q18 on 3 nodes held every group of
     * lineitem in the heap), so it errs towards "several": true for {@link #SEVERAL_ENDPOINTS_MEMORY_NANOS} after a
     * FlightInfo with several endpoints, and until {@link #SINGLE_ENDPOINT_ANSWERS} FlightInfos in a row had one. A
     * cluster answers some queries with one endpoint (a key range whose files lie on one node, a query answered
     * completely), also as the very first query of a catalog; only an install that never answers with more counts as
     * single-node.
     */
    public boolean expectsSeveralEndpoints()
    {
        return expectsSeveralEndpoints(System.nanoTime());
    }

    boolean expectsSeveralEndpoints(long nowNanos)
    {
        if (sawSeveralEndpoints.get()) {
            return nowNanos - lastSeveralEndpointsNanos.get() < SEVERAL_ENDPOINTS_MEMORY_NANOS
                    || singleEndpointAnswers.get() < SINGLE_ENDPOINT_ANSWERS;
        }
        return singleEndpointAnswers.get() < SINGLE_ENDPOINT_ANSWERS;
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
            singleEndpointAnswers.set(0);
        }
        else if (count == 1) {
            singleEndpointAnswers.incrementAndGet();
        }
    }

    @PreDestroy
    @Override
    public void close()
    {
        super.close();
    }
}
