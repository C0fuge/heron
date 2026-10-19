package pro.surpin.trino.arrowflight;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ArrowFlightConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        // Referenced from the catalog file as: connector.name=arrow_flight
        return "arrow_flight";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(config, "config is null");

        Bootstrap app = new Bootstrap(new ArrowFlightModule(context.getTypeManager()));

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(ArrowFlightConnector.class);
    }
}
