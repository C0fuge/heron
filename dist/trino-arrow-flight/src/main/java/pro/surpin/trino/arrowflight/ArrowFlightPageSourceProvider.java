package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ArrowFlightPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ArrowFlightClient client;
    private final ArrowTypeConverter typeConverter;

    @Inject
    public ArrowFlightPageSourceProvider(ArrowFlightClient client, ArrowTypeConverter typeConverter)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
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
        ImmutableList.Builder<ArrowFlightColumnHandle> projected = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            projected.add((ArrowFlightColumnHandle) column);
        }
        return new ArrowFlightPageSource(client, typeConverter, (ArrowFlightSplit) split, projected.build());
    }
}
