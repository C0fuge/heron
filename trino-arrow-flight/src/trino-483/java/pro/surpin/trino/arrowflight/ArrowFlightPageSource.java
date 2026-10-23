package pro.surpin.trino.arrowflight;

import io.trino.spi.Page;
import io.trino.spi.connector.SourcePage;

import java.util.List;

/**
 * Trino SPI 477 and later (this copy: 483): {@code getNextPage()} is gone and the engine pulls
 * {@link SourcePage}s via {@code getNextSourcePage()}.
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
            int dynamicFilterMaxValues)
    {
        super(client, typeConverter, table, split, columns, dynamicFilter, dynamicFilterWaitMillis, dynamicFilterMaxValues);
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        Page page = readNextPage();
        return page == null ? null : SourcePage.create(page);
    }
}
