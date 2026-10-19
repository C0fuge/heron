package pro.surpin.trino.arrowflight;

import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Streams Arrow record batches for a single {@link ArrowFlightSplit} and converts each
 * batch to a Trino {@link Page}. One batch is held in memory at a time.
 */
public class ArrowFlightPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(ArrowFlightPageSource.class);

    private final ArrowFlightClient client;
    private final ArrowTypeConverter typeConverter;
    private final ArrowFlightSplit split;
    private final List<ArrowFlightColumnHandle> columns;

    private FlightStream stream;
    private boolean finished;
    private long completedBytes;
    private long readTimeNanos;

    public ArrowFlightPageSource(
            ArrowFlightClient client,
            ArrowTypeConverter typeConverter,
            ArrowFlightSplit split,
            List<ArrowFlightColumnHandle> columns)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
        this.split = requireNonNull(split, "split is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (stream == null) {
                stream = client.openStream(split.ticket(), split.getLocationUris());
            }
            if (!stream.next()) {
                finished = true;
                return null;
            }
            VectorSchemaRoot root = stream.getRoot();
            Page page = typeConverter.toPage(root, columns);
            completedBytes += page.getSizeInBytes();
            return page;
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading from Arrow Flight stream: " + e.getMessage(), e);
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        if (stream != null) {
            try {
                stream.close();
            }
            catch (Exception e) {
                log.warn(e, "Error closing Flight stream");
            }
            finally {
                stream = null;
            }
        }
    }
}
