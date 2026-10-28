package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

public class ArrowFlightMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(ArrowFlightMetadata.class);

    private final ArrowFlightClient client;
    private final ArrowTypeConverter typeConverter;
    private final String identifierQuote;
    private final ArrowFlightStatistics statistics;

    @Inject
    public ArrowFlightMetadata(ArrowFlightClient client, ArrowTypeConverter typeConverter, ArrowFlightConfig config, ArrowFlightStatistics statistics)
    {
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.client = requireNonNull(client, "client is null");
        this.typeConverter = requireNonNull(typeConverter, "typeConverter is null");
        this.identifierQuote = requireNonNull(config, "config is null").getIdentifierQuote();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.listSchemas();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        List<String> schemas = schemaName.map(List::of).orElseGet(client::listSchemas);
        for (String schema : schemas) {
            for (String table : client.listTables(schema)) {
                tables.add(new SchemaTableName(schema, table));
            }
        }
        return tables.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new io.trino.spi.TrinoException(
                    io.trino.spi.StandardErrorCode.NOT_SUPPORTED, "This connector does not support reading a range of table versions");
        }
        // FOR VERSION / TIMESTAMP AS OF arrives as the end version; forwarded to the server (Iceberg tables)
        Optional<TableVersion> version = endVersion.map(v -> TableVersion.from(session, v));
        try {
            Schema arrowSchema = client.resultSchema(selectAll(tableName) + version.map(TableVersion::toSql).orElse(""));
            ImmutableList.Builder<ArrowFlightColumnHandle> columns = ImmutableList.builder();
            for (Field field : arrowSchema.getFields()) {
                Type type;
                try {
                    type = typeConverter.toTrinoType(field);
                }
                catch (io.trino.spi.TrinoException unsupported) {
                    // One column of a type Trino cannot show (interval, list, struct, map) must not
                    // make the whole table unusable: leave it out, like Trino's JDBC connectors do.
                    log.debug("%s: skipping column %s: %s", tableName, field.getName(), unsupported.getMessage());
                    continue;
                }
                columns.add(new ArrowFlightColumnHandle(field.getName(), type));
            }
            return new ArrowFlightTableHandle(tableName.getSchemaName(), tableName.getTableName(),
                    columns.build(), version, identifierQuote);
        }
        catch (RuntimeException e) {
            // Unknown table -> Trino expects null so it can raise "Table does not exist". But the
            // same call also fails when the server is down or restarting; reporting that as a
            // missing table hides the real problem, so confirm with the table list first.
            if (credentialsRejected(e)) {
                // Listing would only cost one more failed login.
                throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR,
                        e.getMessage(), e);
            }
            List<String> tables;
            try {
                tables = client.listTables(tableName.getSchemaName());
            }
            catch (RuntimeException listFailure) {
                e.addSuppressed(listFailure);
                throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR,
                        "Arrow Flight server unavailable while resolving " + tableName + ": " + e.getMessage(), e);
            }
            if (tables.stream().noneMatch(t -> t.equalsIgnoreCase(tableName.getTableName()))) {
                return null;
            }
            throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Arrow Flight server failed to describe " + tableName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (ArrowFlightColumnHandle column : handle.getColumns()) {
            columns.add(column.toColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.schemaTableName(), columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> handles = ImmutableMap.builder();
        for (ArrowFlightColumnHandle column : handle.getColumns()) {
            handles.put(column.getColumnName(), column);
        }
        return handles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ArrowFlightColumnHandle) columnHandle).toColumnMetadata();
    }

    // -------------------------------------------------------------------------
    // Pushdown
    // -------------------------------------------------------------------------

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        if (handle.getTopN().isPresent()) {
            // a filter above the TopN is not one the server may apply before choosing the top rows
            return Optional.empty();
        }
        Set<ArrowFlightColumnHandle> groupingColumns = handle.getAggregation()
                .map(a -> Set.copyOf(a.getGroupingColumns()))
                .orElse(null);

        Map<ColumnHandle, Domain> pushable = new LinkedHashMap<>();
        Map<ColumnHandle, Domain> remaining = new LinkedHashMap<>();
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isNone()) {
            return Optional.empty();
        }
        for (Map.Entry<ColumnHandle, Domain> entry : summary.getDomains().orElse(Map.of()).entrySet()) {
            ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) entry.getKey();
            // After an aggregation only filters on grouping keys may move below it (into WHERE).
            boolean allowed = column.getExpression().isEmpty()
                    && (groupingColumns == null ? handle.getColumns().contains(column) : groupingColumns.contains(column));
            if (allowed && SqlQueryBuilder.isPushdownSupported(column.getColumnType())) {
                pushable.put(column, entry.getValue());
                if (column.getColumnType().equals(DOUBLE)) {
                    // NaN orders differently in DuckDB; let the engine re-check doubles.
                    remaining.put(column, entry.getValue());
                }
            }
            else {
                remaining.put(column, entry.getValue());
            }
        }
        TupleDomain<ColumnHandle> newConstraint = handle.getConstraint().intersect(TupleDomain.withColumnDomains(pushable));

        List<String> extra = new ArrayList<>(handle.getExtraPredicates());
        List<ConnectorExpression> remainingExpressions = new ArrayList<>();
        boolean newExpression = false;
        for (ConnectorExpression conjunct : ExpressionTranslator.conjuncts(constraint.getExpression())) {
            Optional<String> sql = groupingColumns == null && ArrowFlightSessionProperties.isExpressionPushdownEnabled(session)
                    ? ExpressionTranslator.toPredicateSql(conjunct, constraint.getAssignments(), handle.getIdentifierQuote())
                    : Optional.empty();
            if (sql.isPresent()) {
                if (!extra.contains(sql.get())) {
                    extra.add(sql.get());
                    newExpression = true;
                }
            }
            else {
                remainingExpressions.add(conjunct);
            }
        }

        if (newConstraint.equals(handle.getConstraint()) && !newExpression) {
            // Nothing new was pushed; returning a result here would loop the optimizer.
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                handle.withConstraint(newConstraint).withExtraPredicates(extra),
                TupleDomain.withColumnDomains(remaining),
                ExpressionTranslator.and(remainingExpressions),
                false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        return applyProjection((ArrowFlightTableHandle) table, projections, assignments,
                ArrowFlightSessionProperties.isAggregationPushdownEnabled(session));
    }

    static Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ArrowFlightTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments,
            boolean expressionPushdown)
    {
        if (handle.getAggregation().isPresent()) {
            return Optional.empty();
        }

        List<ConnectorExpression> newProjections = new ArrayList<>();
        Map<String, ArrowFlightColumnHandle> newAssignments = new LinkedHashMap<>();
        boolean pushedExpression = false;
        for (ConnectorExpression projection : projections) {
            if (projection instanceof Variable || !expressionPushdown) {
                newProjections.add(projection);
                continue;
            }
            Optional<Expr> expr = projection instanceof Call ? ExpressionTranslator.toExpr(projection, assignments) : Optional.empty();
            if (expr.isEmpty()) {
                newProjections.add(projection);
                continue;
            }
            String name = "afexpr_" + Integer.toHexString(expr.get().hashCode());
            ArrowFlightColumnHandle column = new ArrowFlightColumnHandle(name, projection.getType(), expr);
            newAssignments.put(name, column);
            newProjections.add(new Variable(name, projection.getType()));
            pushedExpression = true;
        }
        Set<String> referenced = new LinkedHashSet<>();
        newProjections.forEach(p -> collectVariables(p, referenced));
        for (String name : referenced) {
            if (!newAssignments.containsKey(name)) {
                newAssignments.put(name, (ArrowFlightColumnHandle) assignments.get(name));
            }
        }

        List<ArrowFlightColumnHandle> newProjection = ImmutableList.copyOf(new LinkedHashSet<>(newAssignments.values()));
        // Trino passes the projections in no stable order (PartialTranslator collects them in a HashMap keyed by
        // node identity), so compare as sets: an order-only difference is not progress and would loop the optimizer.
        if (!pushedExpression && ImmutableSet.copyOf(newProjection).equals(ImmutableSet.copyOf(handle.effectiveColumns()))) {
            return Optional.empty();
        }

        ImmutableList.Builder<Assignment> result = ImmutableList.builder();
        newAssignments.forEach((name, column) -> result.add(new Assignment(name, column, column.getColumnType())));
        return Optional.of(new ProjectionApplicationResult<>(
                handle.withProjectedColumns(newProjection),
                newProjections,
                result.build(),
                false));
    }

    private static void collectVariables(ConnectorExpression expression, Set<String> out)
    {
        if (expression instanceof Variable variable) {
            out.add(variable.getName());
        }
        expression.getChildren().forEach(child -> collectVariables(child, out));
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        if (!ArrowFlightSessionProperties.isAggregationPushdownEnabled(session)
                || handle.getAggregation().isPresent() || handle.getLimit().isPresent() || handle.getTopN().isPresent()
                || groupingSets.size() != 1) {
            return Optional.empty();
        }

        List<ArrowFlightColumnHandle> groupBy = new ArrayList<>();
        for (ColumnHandle columnHandle : groupingSets.get(0)) {
            ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) columnHandle;
            if (column.getExpression().isPresent() || !handle.getColumns().contains(column) || !isGroupable(column.getColumnType())) {
                return Optional.empty();
            }
            groupBy.add(column);
        }

        List<AggregateColumn> pushed = new ArrayList<>();
        ImmutableList.Builder<ConnectorExpression> projections = ImmutableList.builder();
        ImmutableList.Builder<Assignment> resultAssignments = ImmutableList.builder();
        for (int i = 0; i < aggregates.size(); i++) {
            AggregateFunction aggregate = aggregates.get(i);
            Optional<AggregateColumn> translated = translateAggregate(aggregate, assignments, "afagg" + i);
            if (translated.isEmpty()) {
                return Optional.empty();
            }
            ArrowFlightColumnHandle output = translated.get().getOutput();
            pushed.add(translated.get());
            projections.add(new Variable(output.getColumnName(), output.getColumnType()));
            resultAssignments.add(new Assignment(output.getColumnName(), output, output.getColumnType()));
        }

        Map<ColumnHandle, ColumnHandle> groupingMapping = new LinkedHashMap<>();
        groupBy.forEach(column -> groupingMapping.put(column, column));
        return Optional.of(new AggregationApplicationResult<>(
                handle.withAggregation(new Aggregation(groupBy, pushed)),
                projections.build(),
                resultAssignments.build(),
                groupingMapping,
                false));
    }

    private static Optional<AggregateColumn> translateAggregate(AggregateFunction aggregate, Map<String, ColumnHandle> assignments, String outputName)
    {
        if (aggregate.isDistinct() || aggregate.getFilter().isPresent() || !aggregate.getSortItems().isEmpty()) {
            return Optional.empty();
        }
        Type outputType = aggregate.getOutputType();
        String function = aggregate.getFunctionName();
        List<ConnectorExpression> arguments = aggregate.getArguments();
        if (function.equals("count") && arguments.isEmpty()) {
            return Optional.of(new AggregateColumn(AggregateColumn.Function.COUNT_STAR, Optional.empty(),
                    new ArrowFlightColumnHandle(outputName, BIGINT)));
        }
        if (arguments.size() != 1 || !(arguments.get(0) instanceof Variable variable)
                || !(assignments.get(variable.getName()) instanceof ArrowFlightColumnHandle input)) {
            return Optional.empty();
        }
        Type inputType = input.getColumnType();
        AggregateColumn.Function kind;
        Type expected;
        switch (function) {
            case "count" -> {
                kind = AggregateColumn.Function.COUNT;
                expected = BIGINT;
            }
            case "sum" -> {
                kind = AggregateColumn.Function.SUM;
                expected = SqlQueryBuilder.isIntegral(inputType) ? BIGINT
                        : inputType.equals(DOUBLE) ? DOUBLE
                        : inputType instanceof DecimalType decimal ? createDecimalType(38, decimal.getScale()) : null;
            }
            case "avg" -> {
                kind = AggregateColumn.Function.AVG;
                expected = SqlQueryBuilder.isIntegral(inputType) || inputType.equals(DOUBLE) ? DOUBLE
                        : inputType instanceof DecimalType ? inputType : null;
            }
            case "min", "max" -> {
                kind = function.equals("min") ? AggregateColumn.Function.MIN : AggregateColumn.Function.MAX;
                expected = isGroupable(inputType) && !inputType.equals(BOOLEAN) ? inputType : null;
            }
            default -> {
                return Optional.empty();
            }
        }
        if (expected == null || !expected.equals(outputType)) {
            return Optional.empty();
        }
        // Expression inputs (sum(a * b)) are rendered inline inside the aggregate.
        return Optional.of(new AggregateColumn(kind, Optional.of(input), new ArrowFlightColumnHandle(outputName, outputType)));
    }

    private static boolean credentialsRejected(Throwable e)
    {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof pro.surpin.arrowflight.common.FlightCredentialsRejectedException) {
                return true;
            }
        }
        return false;
    }

    private static boolean isGroupable(Type type)
    {
        return SqlQueryBuilder.isPushdownSupported(type);
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        if (!ArrowFlightSessionProperties.isLimitPushdownEnabled(session) || handle.getTopN().isPresent()
                || (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit)) {
            return Optional.empty();
        }
        // Not guaranteed: every split stops after `limit` rows, Trino still applies the final LIMIT.
        return Optional.of(new LimitApplicationResult<>(handle.withLimit(limit), false, false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        if (!ArrowFlightSessionProperties.isTopNPushdownEnabled(session)) {
            return Optional.empty();
        }
        return applyTopN((ArrowFlightTableHandle) table, topNCount, sortItems, assignments)
                .map(handle -> new TopNApplicationResult<ConnectorTableHandle>(handle, false, false));
    }

    /**
     * The handle with {@code ORDER BY ... LIMIT topNCount} for the server, if every sort key is a plain column
     * whose order the server sorts exactly like Trino. Never guaranteed: in partial mode every endpoint (and
     * every range split) returns its own top rows, so Trino keeps its TopN over their union.
     */
    static Optional<ArrowFlightTableHandle> applyTopN(
            ArrowFlightTableHandle handle,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        // Already sorted and cut (a second TopN above the first may sort differently); a LIMIT below or an
        // aggregation would change which rows the top is chosen from.
        if (handle.getTopN().isPresent() || handle.getLimit().isPresent() || handle.getAggregation().isPresent()
                || sortItems.isEmpty() || topNCount <= 0) {
            return Optional.empty();
        }
        String q = handle.getIdentifierQuote();
        List<String> keys = new ArrayList<>();
        for (SortItem item : sortItems) {
            if (!(assignments.get(item.getName()) instanceof ArrowFlightColumnHandle column)
                    || column.getExpression().isPresent()
                    || !handle.getColumns().contains(column)
                    || !SqlQueryBuilder.isTopNSortable(column.getColumnType())) {
                return Optional.empty();
            }
            SortOrder order = item.getSortOrder();
            keys.add(SqlQueryBuilder.quoteIdentifier(column.getColumnName(), q)
                    + (order.isAscending() ? " ASC" : " DESC")
                    + (order.isNullsFirst() ? " NULLS FIRST" : " NULLS LAST"));
        }
        return Optional.of(handle.withTopN(new TopN(String.join(", ", keys), topNCount)));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;
        if (!ArrowFlightSessionProperties.isStatisticsEnabled(session) || handle.getAggregation().isPresent()) {
            return TableStatistics.empty();
        }
        OptionalLong rows = statistics.rowCount(handle);
        if (rows.isEmpty()) {
            return TableStatistics.empty();
        }
        // Rough selectivity for filters already pushed into the scan (the engine no longer sees them).
        // Kept deliberately high: underestimating a big table makes Trino broadcast it as the
        // build side of a join (TPC-H q13 went 3 s -> 17 s on a NOT LIKE estimated at 25%),
        // while overestimating only costs a partitioned join.
        double selectivity = 1.0;
        for (Domain domain : handle.getConstraint().getDomains().map(Map::values).orElse(List.of())) {
            selectivity *= domain.isNullableSingleValue() || isSmallDiscreteSet(domain) ? 0.1 : 0.5;
        }
        for (String predicate : handle.getExtraPredicates()) {
            selectivity *= predicate.startsWith("(NOT ") ? 0.9 : 0.5;
        }
        double estimate = rows.getAsLong() * Math.max(selectivity, 0.05);
        if (handle.getLimit().isPresent()) {
            estimate = Math.min(estimate, handle.getLimit().getAsLong());
        }
        return TableStatistics.builder().setRowCount(Estimate.of(Math.max(estimate, 1))).build();
    }

    private static boolean isSmallDiscreteSet(Domain domain)
    {
        return domain.getValues().isDiscreteSet() && domain.getValues().getDiscreteSet().size() <= 10;
    }

    private String selectAll(SchemaTableName tableName)
    {
        String s = quote(tableName.getSchemaName());
        String t = quote(tableName.getTableName());
        return "SELECT * FROM " + s + "." + t;
    }

    private String quote(String identifier)
    {
        if (identifierQuote.isEmpty()) {
            return identifier;
        }
        return identifierQuote + identifier.replace(identifierQuote, identifierQuote + identifierQuote) + identifierQuote;
    }
}
