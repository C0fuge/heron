package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Base64;
import java.util.List;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * One split == one {@code FlightEndpoint} returned by GetFlightInfo. It carries the
 * self-contained ticket plus the locations that can serve it, so the split is
 * re-runnable without re-planning on the coordinator.
 */
public final class ArrowFlightSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(ArrowFlightSplit.class);

    private final byte[] ticket;
    private final List<String> locationUris;
    private final List<HostAddress> addresses;

    public ArrowFlightSplit(byte[] ticket, List<String> locationUris, List<HostAddress> addresses)
    {
        this.ticket = requireNonNull(ticket, "ticket is null").clone();
        this.locationUris = ImmutableList.copyOf(requireNonNull(locationUris, "locationUris is null"));
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @JsonCreator
    public static ArrowFlightSplit fromJson(
            @JsonProperty("ticketBase64") String ticketBase64,
            @JsonProperty("locationUris") List<String> locationUris,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        return new ArrowFlightSplit(Base64.getDecoder().decode(ticketBase64), locationUris, addresses);
    }

    @JsonProperty
    public String getTicketBase64()
    {
        return Base64.getEncoder().encodeToString(ticket);
    }

    public byte[] ticket()
    {
        return ticket.clone();
    }

    @JsonProperty
    public List<String> getLocationUris()
    {
        return locationUris;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(ticket);
        for (String uri : locationUris) {
            size += 48L + (2L * uri.length());
        }
        size += 64L * addresses.size();
        return size;
    }
}
