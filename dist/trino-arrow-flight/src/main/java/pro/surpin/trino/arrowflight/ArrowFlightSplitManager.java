package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ArrowFlightSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(ArrowFlightSplitManager.class);

    private final ArrowFlightClient client;

    @Inject
    public ArrowFlightSplitManager(ArrowFlightClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        String sql = SqlQueryBuilder.buildScanSql(handle);
        log.debug("Planning Flight scan: %s", sql);

        FlightInfo info = client.getFlightInfo(sql);

        ImmutableList.Builder<ArrowFlightSplit> splits = ImmutableList.builder();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            List<String> locationUris = new ArrayList<>();
            List<HostAddress> addresses = new ArrayList<>();
            for (Location location : endpoint.getLocations()) {
                URI uri = location.getUri();
                locationUris.add(uri.toString());
                if (uri.getHost() != null && uri.getPort() > 0) {
                    addresses.add(HostAddress.fromParts(uri.getHost(), uri.getPort()));
                }
            }
            splits.add(new ArrowFlightSplit(endpoint.getTicket().getBytes(), locationUris, addresses));
        }

        List<ArrowFlightSplit> result = splits.build();
        log.debug("Flight scan produced %s split(s)", result.size());
        return new FixedSplitSource(result);
    }
}
