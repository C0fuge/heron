package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * One slice of a table scan. The Flight query itself is planned by the page source on the
 * worker (so it can include join dynamic filters that only exist at run time); the split only
 * carries the extra WHERE conjunct that selects its key range, if the scan was range-split.
 */
public final class ArrowFlightSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(ArrowFlightSplit.class);

    private final Optional<String> rangePredicate;

    @JsonCreator
    public ArrowFlightSplit(@JsonProperty("rangePredicate") Optional<String> rangePredicate)
    {
        this.rangePredicate = requireNonNull(rangePredicate, "rangePredicate is null");
    }

    @JsonProperty
    public Optional<String> getRangePredicate()
    {
        return rangePredicate;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + rangePredicate.map(s -> estimatedSizeOf(s)).orElse(0L);
    }

    @Override
    public String toString()
    {
        return "ArrowFlightSplit" + rangePredicate.map(p -> "[" + p + "]").orElse("[all]");
    }
}
