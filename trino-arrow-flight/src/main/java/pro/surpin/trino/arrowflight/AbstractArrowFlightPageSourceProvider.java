package pro.surpin.trino.arrowflight;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * The {@code createPageSource} signature differs between Trino SPI lines, so the concrete
 * {@code ArrowFlightPageSourceProvider} lives in a per-SPI source root and calls
 * {@link #createFlightPageSource}.
 */
public abstract class AbstractArrowFlightPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ArrowFlightClient client;
    private final ArrowTypeConverter typeConverter;

    protected AbstractArrowFlightPageSourceProvider(ArrowFlightClient client, ArrowTypeConverter typeConverter)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
    }

    protected ConnectorPageSource createFlightPageSource(
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        ImmutableList.Builder<ArrowFlightColumnHandle> projected = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            projected.add((ArrowFlightColumnHandle) column);
        }
        return new ArrowFlightPageSource(client, typeConverter, (ArrowFlightTableHandle) table, (ArrowFlightSplit) split,
                projected.build(),
                ArrowFlightSessionProperties.isDynamicFilteringEnabled(session) ? dynamicFilter : DynamicFilter.EMPTY,
                ArrowFlightSessionProperties.getDynamicFilteringWaitMillis(session),
                ArrowFlightSessionProperties.getDynamicFilteringMaxValues(session));
    }
}
