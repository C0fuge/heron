package pro.surpin.trino.arrowflight;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import pro.surpin.arrowflight.common.FlightDataStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Table facts the server answers cheaply from Parquet footers / row-group statistics:
 * the row count ({@code count(*)}) and min/max of an integer or date column. Cached per
 * catalog for {@code arrow-flight.statistics.cache-ttl-seconds}.
 */
public class ArrowFlightStatistics
{
    private static final Logger log = Logger.get(ArrowFlightStatistics.class);

    public record Range(long min, long max) {}

    private record Cached<T>(T value, long expiresAtNanos) {}

    private final ArrowFlightClient client;
    private final long ttlNanos;
    private final String quote;
    private final Map<String, Cached<OptionalLong>> rowCounts = new ConcurrentHashMap<>();
    private final Map<String, Cached<Optional<Range>>> ranges = new ConcurrentHashMap<>();

    @Inject
    public ArrowFlightStatistics(ArrowFlightClient client, ArrowFlightConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.ttlNanos = config.getStatisticsCacheTtlSeconds() * 1_000_000_000L;
        this.quote = config.getIdentifierQuote();
    }

    public OptionalLong rowCount(ArrowFlightTableHandle table)
    {
        String from = SqlQueryBuilder.tableName(table);
        return cached(rowCounts, from, () -> {
            List<long[]> rows = query("SELECT count(*) AS c FROM " + from, 1);
            if (rows == null) {
                return null;   // failed: answer "unknown" now, ask again next time
            }
            long total = 0;
            for (long[] row : rows) {
                total += row[0];
            }
            return OptionalLong.of(total);
        }, OptionalLong.empty());
    }

    /** min/max of an integer or date column, empty if unknown or the table is empty. */
    public Optional<Range> range(ArrowFlightTableHandle table, String column)
    {
        String from = SqlQueryBuilder.tableName(table);
        String c = SqlQueryBuilder.quoteIdentifier(column, quote);
        return cached(ranges, from + "#" + column, () -> {
            String sql = "SELECT min(" + c + ") AS a, max(" + c + ") AS b FROM " + from;
            List<long[]> rows = query(sql, 2);
            if (rows != null && rows.stream().allMatch(r -> r[0] == Long.MIN_VALUE && r[1] == Long.MIN_VALUE)) {
                // The server's footer-statistics shortcut returns NULL min/max for DATE columns;
                // a WHERE clause makes it scan the column instead.
                rows = query(sql + " WHERE TRUE", 2);
            }
            if (rows == null) {
                return null;   // failed: answer "unknown" now, ask again next time
            }
            if (rows.isEmpty()) {
                return Optional.empty();
            }
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (long[] row : rows) {
                if (row[0] == Long.MIN_VALUE && row[1] == Long.MIN_VALUE) {
                    continue;
                }
                min = Math.min(min, row[0]);
                max = Math.max(max, row[1]);
            }
            return min > max ? Optional.empty() : Optional.of(new Range(min, max));
        }, Optional.empty());
    }

    /** {@code loader} returns null when the server could not be asked; that is not cached. */
    private <T> T cached(Map<String, Cached<T>> cache, String key, Supplier<T> loader, T unknown)
    {
        long now = System.nanoTime();
        Cached<T> entry = cache.get(key);
        if (entry != null && entry.expiresAtNanos() - now > 0) {
            return entry.value();
        }
        T value = loader.get();
        if (value == null) {
            cache.remove(key);
            return unknown;
        }
        cache.put(key, new Cached<>(value, now + ttlNanos));
        return value;
    }

    /** Runs a query returning {@code width} integer/date columns; null rows become Long.MIN_VALUE. Null on failure. */
    private List<long[]> query(String sql, int width)
    {
        long start = System.nanoTime();
        try {
            FlightInfo info = client.getFlightInfo(sql);
            List<long[]> rows = new ArrayList<>();
            for (FlightEndpoint endpoint : info.getEndpoints()) {
                List<String> uris = endpoint.getLocations().stream().map(l -> l.getUri().toString()).toList();
                try (FlightDataStream stream = client.openStream(endpoint.getTicket().getBytes(), uris)) {
                    while (stream.next()) {
                        List<FieldVector> vectors = stream.getRoot().getFieldVectors();
                        for (int position = 0; position < stream.getRoot().getRowCount(); position++) {
                            long[] row = new long[width];
                            for (int i = 0; i < width; i++) {
                                row[i] = value(vectors.get(i), position);
                            }
                            rows.add(row);
                        }
                    }
                }
            }
            log.debug("%s -> %s rows in %s ms", sql, rows.size(), (System.nanoTime() - start) / 1_000_000);
            return rows;
        }
        catch (Exception e) {
            log.warn("Statistics query failed (%s): %s", sql, e.getMessage());
            return null;
        }
    }

    private static long value(FieldVector vector, int position)
    {
        if (vector.isNull(position)) {
            return Long.MIN_VALUE;
        }
        if (vector instanceof BaseIntVector ints) {
            return ints.getValueAsLong(position);
        }
        if (vector instanceof DateDayVector dates) {
            return dates.get(position);
        }
        throw new IllegalStateException("Unexpected statistics vector " + vector.getClass().getSimpleName());
    }
}
