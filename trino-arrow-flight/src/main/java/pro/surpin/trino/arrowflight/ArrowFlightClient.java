package pro.surpin.trino.arrowflight;

import com.google.inject.Inject;
import jakarta.annotation.PreDestroy;
import pro.surpin.arrowflight.common.FlightSqlSession;

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

    @PreDestroy
    @Override
    public void close()
    {
        super.close();
    }
}
