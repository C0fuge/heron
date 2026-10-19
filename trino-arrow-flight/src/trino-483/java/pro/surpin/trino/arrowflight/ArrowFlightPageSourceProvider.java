package pro.surpin.trino.arrowflight;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;

/**
 * Trino SPI 483: {@code createPageSource(..., Optional<ConnectorTableCredentials>, List<ColumnHandle>, DynamicFilter)}.
 * The MemoryContext overload's default delegates here.
 */
public final class ArrowFlightPageSourceProvider
        extends AbstractArrowFlightPageSourceProvider
{
    @Inject
    public ArrowFlightPageSourceProvider(ArrowFlightClient client, ArrowTypeConverter typeConverter)
    {
        super(client, typeConverter);
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> credentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        return createFlightPageSource(session, split, table, columns, dynamicFilter);
    }
}
