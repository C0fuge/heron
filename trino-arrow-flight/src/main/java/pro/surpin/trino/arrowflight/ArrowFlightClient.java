package pro.surpin.trino.arrowflight;

import com.google.inject.Inject;
import jakarta.annotation.PreDestroy;
import pro.surpin.arrowflight.common.FlightSqlSession;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Trino-scoped singleton over the shared {@link FlightSqlSession}. Guice builds it from the
 * catalog properties; {@link PreDestroy} ties its lifecycle to the connector shutdown.
 */
public class ArrowFlightClient
        extends FlightSqlSession
{
    @Inject
    public ArrowFlightClient(ArrowFlightConfig config)
    {
        super(config.toFlightConfig());
    }

    // Endpoints in the last FlightInfo (= Flight nodes answering in partial mode); 0 = none seen yet.
    private final AtomicInteger lastEndpointCount = new AtomicInteger();

    /** Hint for planning a query before its FlightInfo is known: will it likely have more than one endpoint? */
    public boolean expectsSeveralEndpoints()
    {
        return lastEndpointCount.get() != 1;
    }

    public void recordEndpointCount(int count)
    {
        lastEndpointCount.set(count);
    }

    @PreDestroy
    @Override
    public void close()
    {
        super.close();
    }
}
