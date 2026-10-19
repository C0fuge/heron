package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

/**
 * Entry point discovered by Trino through {@code META-INF/services/io.trino.spi.Plugin}.
 */
public class ArrowFlightPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new ArrowFlightConnectorFactory());
    }
}
