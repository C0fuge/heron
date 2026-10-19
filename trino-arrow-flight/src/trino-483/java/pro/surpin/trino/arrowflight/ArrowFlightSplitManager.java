package pro.surpin.trino.arrowflight;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;

import java.util.Set;

/**
 * Trino SPI 483: {@code getSplits(..., Set<ColumnHandle>, Constraint)} (no DynamicFilter).
 */
public final class ArrowFlightSplitManager
        extends AbstractArrowFlightSplitManager
{
    @Inject
    public ArrowFlightSplitManager(ArrowFlightStatistics statistics, ArrowFlightConfig config)
    {
        super(statistics, config);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> columns,
            Constraint constraint)
    {
        return planSplits(session, table);
    }
}
