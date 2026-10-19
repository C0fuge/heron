package pro.surpin.trino.arrowflight;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;

/**
 * Trino SPI 477: {@code createPageSource(..., List<ColumnHandle>, DynamicFilter)}.
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
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        return createFlightPageSource(session, split, table, columns, dynamicFilter);
    }
}
