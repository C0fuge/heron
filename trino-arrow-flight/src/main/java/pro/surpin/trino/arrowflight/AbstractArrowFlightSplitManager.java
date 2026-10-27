package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.type.Type;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * The server answers any query with one endpoint per Flight node, i.e. one stream for the
 * whole table on a single-node install, which Trino would read and process on one thread.
 * Large plain scans are therefore cut into key ranges on an integer/date column
 * ({@code col >= a AND col < b}); each range is its own split, read and processed in parallel.
 * Parquet row-group statistics let the server skip row groups outside a range.
 * <p>
 * With {@code endpoint_splits_enabled} a plain scan is instead planned here, on the coordinator, as one
 * statement, and each endpoint of its FlightInfo (one per Flight node, or {@code endpointsPerServer} per
 * server) becomes a split: Trino still reads the parts in parallel, but the server runs the scan once
 * rather than once per key range. Join dynamic filters (applied on the worker) are then not used, so the
 * mode stays off while {@code dynamic_filtering_enabled} is on.
 * <p>
 * The {@code getSplits} signature differs between Trino SPI lines, so the concrete
 * {@code ArrowFlightSplitManager} lives in a per-SPI source root and calls {@link #planSplits}.
 */
public abstract class AbstractArrowFlightSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(AbstractArrowFlightSplitManager.class);

    private final ArrowFlightClient client;
    private final ArrowFlightStatistics statistics;
    private final long minRowsPerSplit;
    private final Map<String, String> splitColumns;

    protected AbstractArrowFlightSplitManager(ArrowFlightClient client, ArrowFlightStatistics statistics, ArrowFlightConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.minRowsPerSplit = config.getSplitMinRows();
        this.splitColumns = parseSplitColumns(config.getSplitColumns());
    }

    protected ConnectorSplitSource planSplits(ConnectorSession session, ConnectorTableHandle table)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        if (useEndpointSplits(session, handle)) {
            List<ArrowFlightSplit> splits = endpointSplits(handle);
            log.debug("%s: %s endpoint split(s)", handle, splits.size());
            return new FixedSplitSource(splits);
        }
        List<ArrowFlightSplit> splits = rangeSplits(session, handle)
                .orElseGet(() -> List.of(new ArrowFlightSplit(Optional.empty())));
        log.debug("%s: %s split(s)", handle, splits.size());
        return new FixedSplitSource(splits);
    }

    static boolean useEndpointSplits(ConnectorSession session, ArrowFlightTableHandle handle)
    {
        return ArrowFlightSessionProperties.isEndpointSplitsEnabled(session)
                && !ArrowFlightSessionProperties.isDynamicFilteringEnabled(session)
                && handle.getAggregation().isEmpty()
                && handle.getLimit().isEmpty();
    }

    /** One split per endpoint of the scan's FlightInfo; none if the server answers with no endpoint. */
    List<ArrowFlightSplit> endpointSplits(ArrowFlightTableHandle handle)
    {
        List<ArrowFlightColumnHandle> columns = SqlQueryBuilder.baseColumns(handle, handle.effectiveColumns());
        String sql = SqlQueryBuilder.buildScanSql(handle, columns, List.of());
        log.debug("Flight query (endpoint splits): %s", sql);
        FlightInfo info = client.getFlightInfo(sql);
        client.recordEndpointCount(info.getEndpoints().size());
        List<String> names = columns.stream().map(ArrowFlightColumnHandle::getColumnName).toList();
        ImmutableList.Builder<ArrowFlightSplit> splits = ImmutableList.builder();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            List<String> uris = endpoint.getLocations().stream().map(l -> l.getUri().toString()).toList();
            splits.add(ArrowFlightSplit.forEndpoint(endpoint.getTicket().getBytes(), uris, names));
        }
        return splits.build();
    }

    private Optional<List<ArrowFlightSplit>> rangeSplits(ConnectorSession session, ArrowFlightTableHandle handle)
    {
        int maxSplits = ArrowFlightSessionProperties.getSplitCount(session);
        // An aggregation must see all rows of a group in one place; a LIMIT is cheap anyway.
        if (maxSplits <= 1 || handle.getAggregation().isPresent() || handle.getLimit().isPresent()) {
            return Optional.empty();
        }
        Optional<ArrowFlightColumnHandle> column = splitColumn(handle);
        if (column.isEmpty()) {
            return Optional.empty();
        }
        OptionalLong rows = statistics.rowCount(handle);
        if (rows.isEmpty() || rows.getAsLong() < 2 * minRowsPerSplit) {
            return Optional.empty();
        }
        // Split on what the scan returns after its filters, not on the table size: a selective
        // scan is faster as one server query than as several.
        double expected = ScanEstimator.estimateRows(handle, statistics, rows.getAsLong()).orElse(rows.getAsLong());
        log.debug("%s: constraint=%s, ~%.0f of %s rows after filters", handle, handle.getConstraint().toString(), expected, rows.getAsLong());
        if (expected < 2 * minRowsPerSplit) {
            log.debug("%s: ~%.0f rows after filters, not splitting", handle, expected);
            return Optional.empty();
        }
        int count = (int) Math.min(maxSplits, (long) (expected / minRowsPerSplit));
        Optional<ArrowFlightStatistics.Range> range = statistics.range(handle, column.get().getColumnName());
        if (range.isEmpty()) {
            return Optional.empty();
        }
        long min = range.get().min();
        long max = range.get().max();
        // Width computed in double to avoid overflow on wide ranges.
        double width = ((double) max - (double) min + 1) / count;
        if (width < 1) {
            return Optional.empty();
        }
        Type type = column.get().getColumnType();
        String name = SqlQueryBuilder.quoteIdentifier(column.get().getColumnName(), handle.getIdentifierQuote());
        ImmutableList.Builder<ArrowFlightSplit> splits = ImmutableList.builder();
        long low = min;
        for (int i = 0; i < count; i++) {
            boolean last = i == count - 1;
            long high = last ? max : (long) Math.floor(min + width * (i + 1));
            if (!last && high <= low) {
                continue;
            }
            String predicate;
            if (i == 0 && last) {
                return Optional.empty();
            }
            if (i == 0) {
                predicate = "(" + name + " < " + literal(type, high) + " OR " + name + " IS NULL)";
            }
            else if (last) {
                predicate = "(" + name + " >= " + literal(type, low) + ")";
            }
            else {
                predicate = "(" + name + " >= " + literal(type, low) + " AND " + name + " < " + literal(type, high) + ")";
            }
            splits.add(new ArrowFlightSplit(Optional.of(predicate)));
            low = high;
        }
        return Optional.of(splits.build());
    }

    private Optional<ArrowFlightColumnHandle> splitColumn(ArrowFlightTableHandle handle)
    {
        String configured = splitColumns.get((handle.getSchemaName() + "." + handle.getTableName()).toLowerCase(Locale.ROOT));
        for (ArrowFlightColumnHandle column : handle.getColumns()) {
            if (configured != null ? column.getColumnName().equalsIgnoreCase(configured) : isSplittable(column.getColumnType())) {
                return isSplittable(column.getColumnType()) ? Optional.of(column) : Optional.empty();
            }
        }
        return Optional.empty();
    }

    private static boolean isSplittable(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT) || type.equals(DATE);
    }

    private static String literal(Type type, long value)
    {
        return SqlQueryBuilder.formatValue(type, value);
    }

    private static Map<String, String> parseSplitColumns(String spec)
    {
        Map<String, String> out = new HashMap<>();
        for (String entry : spec.split(",")) {
            int eq = entry.indexOf('=');
            if (eq > 0) {
                out.put(entry.substring(0, eq).trim().toLowerCase(Locale.ROOT), entry.substring(eq + 1).trim());
            }
        }
        return out;
    }
}
