package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * One slice of a table scan. Normally the Flight query itself is planned by the page source on the
 * worker (so it can include join dynamic filters that only exist at run time); the split only
 * carries the extra WHERE conjunct that selects its key range, if the scan was range-split.
 * <p>
 * With {@code endpoint_splits_enabled} the coordinator plans the scan once and every endpoint of
 * its FlightInfo becomes a split: then the split carries that endpoint's ticket and locations and
 * the names of the columns the planned query returns.
 */
public final class ArrowFlightSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(ArrowFlightSplit.class);

    private final Optional<String> rangePredicate;
    private final Optional<String> ticket;
    private final List<String> locations;
    private final List<String> plannedColumns;

    public ArrowFlightSplit(Optional<String> rangePredicate)
    {
        this(rangePredicate, Optional.empty(), List.of(), List.of());
    }

    @JsonCreator
    public ArrowFlightSplit(
            @JsonProperty("rangePredicate") Optional<String> rangePredicate,
            @JsonProperty("ticket") Optional<String> ticket,
            @JsonProperty("locations") List<String> locations,
            @JsonProperty("plannedColumns") List<String> plannedColumns)
    {
        this.rangePredicate = requireNonNull(rangePredicate, "rangePredicate is null");
        this.ticket = requireNonNull(ticket, "ticket is null");
        this.locations = List.copyOf(requireNonNull(locations, "locations is null"));
        this.plannedColumns = List.copyOf(requireNonNull(plannedColumns, "plannedColumns is null"));
    }

    /** A split that reads one endpoint of a scan planned on the coordinator. */
    public static ArrowFlightSplit forEndpoint(byte[] ticket, List<String> locations, List<String> plannedColumns)
    {
        return new ArrowFlightSplit(Optional.empty(), Optional.of(Base64.getEncoder().encodeToString(ticket)), locations, plannedColumns);
    }

    @JsonProperty
    public Optional<String> getRangePredicate()
    {
        return rangePredicate;
    }

    /** Base64 of the endpoint ticket, if the scan was planned on the coordinator. */
    @JsonProperty
    public Optional<String> getTicket()
    {
        return ticket;
    }

    @JsonProperty
    public List<String> getLocations()
    {
        return locations;
    }

    @JsonProperty
    public List<String> getPlannedColumns()
    {
        return plannedColumns;
    }

    public Optional<byte[]> ticketBytes()
    {
        return ticket.map(t -> Base64.getDecoder().decode(t));
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + rangePredicate.map(s -> estimatedSizeOf(s)).orElse(0L)
                + ticket.map(s -> estimatedSizeOf(s)).orElse(0L)
                + estimatedSizeOf(locations, s -> estimatedSizeOf(s))
                + estimatedSizeOf(plannedColumns, s -> estimatedSizeOf(s));
    }

    @Override
    public String toString()
    {
        if (ticket.isPresent()) {
            return "ArrowFlightSplit[endpoint " + locations + "]";
        }
        return "ArrowFlightSplit" + rangePredicate.map(p -> "[" + p + "]").orElse("[all]");
    }
}
