package pro.surpin.trino.arrowflight;

import io.trino.spi.Page;

import java.util.List;

/**
 * Trino SPI up to 455: the engine pulls {@link Page}s via {@code getNextPage()}.
 */
public final class ArrowFlightPageSource
        extends AbstractArrowFlightPageSource
{
    public ArrowFlightPageSource(
            ArrowFlightClient client,
            ArrowTypeConverter typeConverter,
            ArrowFlightTableHandle table,
            ArrowFlightSplit split,
            List<ArrowFlightColumnHandle> columns,
            io.trino.spi.connector.DynamicFilter dynamicFilter,
            int dynamicFilterWaitMillis,
            int dynamicFilterMaxValues,
            int endpointParallelism,
            boolean sortedAggregationMerge)
    {
        super(client, typeConverter, table, split, columns, dynamicFilter, dynamicFilterWaitMillis, dynamicFilterMaxValues,
                endpointParallelism, sortedAggregationMerge);
    }

    @Override
    public Page getNextPage()
    {
        return readNextPage();
    }
}
