package pro.surpin.trino.arrowflight;

import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * The connector is read-only and stateless, so a single transaction token is enough.
 */
public enum ArrowFlightTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE
}
