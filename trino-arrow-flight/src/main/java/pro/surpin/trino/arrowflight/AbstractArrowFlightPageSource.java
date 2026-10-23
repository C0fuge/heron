package pro.surpin.trino.arrowflight;

import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import pro.surpin.arrowflight.common.FlightDataStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Objects.requireNonNull;

/**
 * Reads one {@link ArrowFlightSplit}. The Flight query is planned here, on the worker, right
 * before the first read: by then the join dynamic filters (e.g. the keys of the small side of
 * a join) are usually known and are added to the WHERE clause, so the server sends only rows
 * that can match. Up to {@code dynamic_filtering_wait_timeout_ms} is spent waiting for them.
 * <p>
 * The endpoints (one per Flight node) of a plain scan are read one after another, or up to
 * {@code endpoint_read_parallelism} at a time (see {@link ParallelStreamReader}).
 * <p>
 * The engine-facing "next page" method differs between Trino SPI lines
 * ({@code getNextPage()} up to 455-ish, {@code getNextSourcePage()} from 477 on), so the
 * concrete {@code ArrowFlightPageSource} lives in a per-SPI source root and only adapts
 * {@link #readNextPage()}.
 */
public abstract class AbstractArrowFlightPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(AbstractArrowFlightPageSource.class);

    private final ArrowFlightClient client;
    private final ArrowTypeConverter converter;
    private final ArrowFlightTableHandle table;
    private final ArrowFlightSplit split;
    private final List<ArrowFlightColumnHandle> columns;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilterDeadlineNanos;
    private final int dynamicFilterMaxValues;
    private final int endpointParallelism;

    /** Plain scans: table columns fetched from the server (expressions are computed here). */
    private final List<ArrowFlightColumnHandle> serverColumns;
    private final AggregationReader aggregationReader;

    private List<FlightEndpoint> endpoints;
    private int nextEndpoint;
    private FlightDataStream stream;
    private ParallelStreamReader parallelReader;
    private boolean finished;
    private long remainingRows;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    protected AbstractArrowFlightPageSource(
            ArrowFlightClient client,
            ArrowTypeConverter converter,
            ArrowFlightTableHandle table,
            ArrowFlightSplit split,
            List<ArrowFlightColumnHandle> columns,
            DynamicFilter dynamicFilter,
            int dynamicFilterWaitMillis,
            int dynamicFilterMaxValues,
            int endpointParallelism)
    {
        this.client = requireNonNull(client, "client is null");
        this.converter = requireNonNull(converter, "converter is null");
        this.table = requireNonNull(table, "table is null");
        this.split = requireNonNull(split, "split is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilterDeadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(dynamicFilterWaitMillis);
        this.dynamicFilterMaxValues = dynamicFilterMaxValues;
        this.endpointParallelism = endpointParallelism;
        this.remainingRows = table.getLimit().orElse(Long.MAX_VALUE);
        this.aggregationReader = table.getAggregation().map(a -> new AggregationReader(a, converter)).orElse(null);
        this.serverColumns = aggregationReader == null ? SqlQueryBuilder.baseColumns(table, columns) : List.of();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public java.util.OptionalLong getCompletedPositions()
    {
        return java.util.OptionalLong.of(completedPositions);
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
    public CompletableFuture<?> isBlocked()
    {
        if (endpoints == null && waitingForDynamicFilter()) {
            long remaining = dynamicFilterDeadlineNanos - System.nanoTime();
            CompletableFuture<?> timeout = new CompletableFuture<>()
                    .completeOnTimeout(null, Math.max(remaining, 0), TimeUnit.NANOSECONDS);
            return CompletableFuture.anyOf(dynamicFilter.isBlocked(), timeout);
        }
        if (parallelReader != null) {
            return parallelReader.isBlocked();
        }
        return NOT_BLOCKED;
    }

    private boolean waitingForDynamicFilter()
    {
        return dynamicFilter.isAwaitable() && !dynamicFilter.isComplete() && dynamicFilterDeadlineNanos - System.nanoTime() > 0;
    }

    protected Page readNextPage()
    {
        if (finished) {
            return null;
        }
        if (endpoints == null && waitingForDynamicFilter()) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (endpoints == null && !plan()) {
                finished = true;
                return null;
            }
            Page page = aggregationReader != null ? nextAggregationPage() : nextScanPage();
            if (page == null) {
                if (parallelReader != null && !parallelReader.isFinished()) {
                    return null;   // no stream has a batch ready yet: see isBlocked()
                }
                finished = true;
                closeStream();
                return null;
            }
            if (page.getPositionCount() > remainingRows) {
                page = page.getRegion(0, (int) remainingRows);
            }
            remainingRows -= page.getPositionCount();
            if (remainingRows == 0) {
                finished = true;
                closeStream();
            }
            completedBytes += page.getSizeInBytes();
            completedPositions += page.getPositionCount();
            return page;
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            // gRPC reports any failure while decoding a message as "Failed to read message";
            // the real reason is at the bottom of the cause chain, so put it in the message.
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            String cause = root == e ? "" : " (cause: " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")";
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error reading from Arrow Flight: " + e.getMessage() + cause, e);
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    /** Builds the SQL and asks the server for endpoints. False if no row can qualify. */
    private boolean plan()
    {
        List<String> extra = new ArrayList<>();
        split.getRangePredicate().ifPresent(extra::add);

        // A dynamic filter must never narrow the rows a pushed TopN chooses from (Trino does not push one below
        // a TopN; this keeps it that way).
        TupleDomain<ColumnHandle> dynamic = table.getTopN().isPresent() ? TupleDomain.all() : dynamicFilter.getCurrentPredicate();
        if (dynamic.isNone()) {
            if (aggregationReader == null) {
                return false;
            }
        }
        else if (!dynamic.isAll()) {
            Set<ArrowFlightColumnHandle> allowed = aggregationReader != null
                    ? Set.copyOf(table.getAggregation().get().getGroupingColumns())
                    : Set.copyOf(table.getColumns());
            String where = SqlQueryBuilder.dynamicFilterClause(dynamic, allowed, dynamicFilterMaxValues, table.getIdentifierQuote());
            if (!where.isEmpty()) {
                extra.add(where);
            }
        }

        String sql = aggregationReader != null
                ? SqlQueryBuilder.buildAggregationSql(table, extra)
                : SqlQueryBuilder.buildScanSql(table, serverColumns, extra);
        log.debug("Flight query: %s", sql);
        FlightInfo info = client.getFlightInfo(sql);
        endpoints = info.getEndpoints();
        if (aggregationReader == null && endpointParallelism > 1 && endpoints.size() > 1) {
            List<Supplier<ParallelStreamReader.BatchStream>> streams = new ArrayList<>();
            for (FlightEndpoint endpoint : endpoints) {
                streams.add(() -> batchStream(openStream(endpoint)));
            }
            parallelReader = new ParallelStreamReader(streams, endpointParallelism);
        }
        return true;
    }

    /** Next non-empty batch over all endpoints, or null at the end. */
    private VectorSchemaRoot nextBatch()
    {
        while (true) {
            if (stream == null) {
                if (nextEndpoint >= endpoints.size()) {
                    return null;
                }
                stream = openStream(endpoints.get(nextEndpoint++));
            }
            if (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                if (root.getRowCount() > 0) {
                    return root;
                }
            }
            else {
                closeStream();
            }
        }
    }

    private FlightDataStream openStream(FlightEndpoint endpoint)
    {
        List<String> uris = endpoint.getLocations().stream().map(l -> l.getUri().toString()).toList();
        return client.openStream(endpoint.getTicket().getBytes(), uris);
    }

    private static ParallelStreamReader.BatchStream batchStream(FlightDataStream stream)
    {
        return new ParallelStreamReader.BatchStream()
        {
            @Override
            public boolean next()
            {
                return stream.next();
            }

            @Override
            public VectorSchemaRoot getRoot()
            {
                return stream.getRoot();
            }

            @Override
            public void cancel(String message)
            {
                stream.cancel(message);
            }

            @Override
            public void close()
            {
                stream.close();
            }
        };
    }

    private Page nextScanPage()
    {
        VectorSchemaRoot root = parallelReader != null ? parallelReader.poll() : nextBatch();
        if (root == null) {
            return null;
        }
        int positions = root.getRowCount();
        Map<String, Block> base = new HashMap<>();
        for (ArrowFlightColumnHandle column : serverColumns) {
            FieldVector vector = root.getVector(column.getColumnName());
            base.put(column.getColumnName(), vector == null
                    ? converter.nullBlock(column.getColumnType(), positions)
                    : converter.toBlock(vector, column.getColumnType(), positions));
        }
        Block[] blocks = new Block[columns.size()];
        for (int channel = 0; channel < columns.size(); channel++) {
            ArrowFlightColumnHandle column = columns.get(channel);
            blocks[channel] = column.getExpression().isPresent()
                    ? evaluate(column, base, positions)
                    : base.get(column.getColumnName());
        }
        return new Page(positions, blocks);
    }

    private Block evaluate(ArrowFlightColumnHandle column, Map<String, Block> base, int positions)
    {
        Expr expression = column.getExpression().orElseThrow();
        Set<String> inputs = new java.util.LinkedHashSet<>();
        expression.collectColumns(inputs);
        Map<String, Type> types = new HashMap<>();
        for (ArrowFlightColumnHandle c : serverColumns) {
            types.put(c.getColumnName(), c.getColumnType());
        }
        Type type = column.getColumnType();
        BlockBuilder builder = type.createBlockBuilder(null, positions);
        Map<String, Object> row = new HashMap<>();
        for (int position = 0; position < positions; position++) {
            for (String input : inputs) {
                Type inputType = types.get(input);
                row.put(input, ArrowTypeConverter.toNumber(readNativeValue(inputType, base.get(input), position), inputType));
            }
            writeNativeValue(type, builder, ArrowTypeConverter.fromNumber(expression.evaluate(row), type));
        }
        return builder.build();
    }

    private Page nextAggregationPage()
    {
        if (endpoints.size() <= 1) {
            VectorSchemaRoot root = nextBatch();
            return root == null ? null : aggregationReader.finish(aggregationReader.rawPage(root), columns);
        }
        if (nextEndpoint > 0 && stream == null && nextEndpoint >= endpoints.size()) {
            return null;
        }
        // Several Flight nodes each aggregated their own files: merge the partial groups.
        AggregationReader.Merger merger = aggregationReader.merger();
        VectorSchemaRoot root;
        while ((root = nextBatch()) != null) {
            merger.add(aggregationReader.rawPage(root));
        }
        nextEndpoint = endpoints.size();
        return aggregationReader.finish(merger.result(), columns);
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    private void closeStream()
    {
        if (parallelReader != null) {
            parallelReader.close();
        }
        if (stream != null) {
            stream.close();
            stream = null;
        }
    }

    @Override
    public void close()
    {
        if (stream != null && !finished) {
            // Stop the server from producing the rest (LIMIT reached or query cancelled).
            try {
                stream.cancel("closed");
            }
            catch (Exception ignored) {
                // best effort
            }
        }
        closeStream();
    }
}
