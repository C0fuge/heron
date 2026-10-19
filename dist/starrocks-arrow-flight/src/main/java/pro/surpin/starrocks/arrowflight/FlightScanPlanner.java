package pro.surpin.starrocks.arrowflight;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.arrowflight.common.FlightSqlSession;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Engine-neutral half of the future StarRocks connector: given a schema/table and the
 * columns a query needs, it renders the {@code SELECT}, asks the Flight server for the
 * endpoints, and returns them as plain data the StarRocks scan node can turn into split
 * assignments. Everything StarRocks-specific (catalog wiring, type mapping to
 * {@code com.starrocks.catalog.Type}, {@code ConnectorScanRangeSource}) lives in
 * src/starrocks/java and is only compiled under the {@code -Pstarrocks} profile.
 */
public final class FlightScanPlanner
{
    private final FlightSqlSession session;

    public FlightScanPlanner(FlightSqlSession session)
    {
        this.session = session;
    }

    public FlightScan plan(String schema, String table, List<String> columns)
    {
        String projection = columns.isEmpty()
                ? "*"
                : columns.stream().map(FlightScanPlanner::quote).collect(Collectors.joining(", "));
        String sql = "SELECT " + projection + " FROM " + quote(schema) + "." + quote(table);

        Schema arrowSchema = session.resultSchema(sql);
        FlightInfo info = session.getFlightInfo(sql);

        List<FlightSplit> splits = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            List<String> uris = endpoint.getLocations().stream()
                    .map(Location::getUri).map(URI::toString).collect(Collectors.toList());
            splits.add(new FlightSplit(endpoint.getTicket().getBytes(), uris));
        }
        return new FlightScan(arrowSchema, splits);
    }

    private static String quote(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /** Result schema plus one entry per Flight endpoint. */
    public record FlightScan(Schema arrowSchema, List<FlightSplit> splits) {}

    /** A self-contained ticket and the locations that can serve it. */
    public record FlightSplit(byte[] ticket, List<String> locationUris) {}
}
