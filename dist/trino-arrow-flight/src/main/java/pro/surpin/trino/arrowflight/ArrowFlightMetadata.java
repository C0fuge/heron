package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArrowFlightMetadata
        implements ConnectorMetadata
{
    private final ArrowFlightClient client;
    private final ArrowTypeConverter typeConverter;
    private final String identifierQuote;

    @Inject
    public ArrowFlightMetadata(ArrowFlightClient client, ArrowTypeConverter typeConverter, ArrowFlightConfig config)
    {
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
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new io.trino.spi.TrinoException(
                    io.trino.spi.StandardErrorCode.NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        try {
            Schema arrowSchema = client.resultSchema(selectAll(tableName));
            ImmutableList.Builder<ArrowFlightColumnHandle> columns = ImmutableList.builder();
            for (Field field : arrowSchema.getFields()) {
                columns.add(new ArrowFlightColumnHandle(field.getName(), typeConverter.toTrinoType(field)));
            }
            return new ArrowFlightTableHandle(tableName.getSchemaName(), tableName.getTableName(),
                    columns.build(), identifierQuote);
        }
        catch (RuntimeException e) {
            // Unknown table -> Trino expects null so it can raise "Table does not exist".
            return null;
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

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        Map<ColumnHandle, Domain> pushable = new LinkedHashMap<>();
        Map<ColumnHandle, Domain> remaining = new LinkedHashMap<>();
        for (Map.Entry<ColumnHandle, Domain> entry : summary.getDomains().get().entrySet()) {
            ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) entry.getKey();
            if (SqlQueryBuilder.isPushdownSupported(column.getColumnType())) {
                pushable.put(column, entry.getValue());
            }
            else {
                remaining.put(column, entry.getValue());
            }
        }

        TupleDomain<ColumnHandle> newConstraint = handle.getConstraint()
                .intersect(TupleDomain.withColumnDomains(pushable));
        if (newConstraint.equals(handle.getConstraint())) {
            // Nothing new was pushed; returning a result here would loop the optimizer.
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                handle.withConstraint(newConstraint),
                TupleDomain.withColumnDomains(remaining),
                false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        ArrowFlightTableHandle handle = (ArrowFlightTableHandle) table;

        // Only direct column references are pushed; anything else keeps the full row.
        if (!projections.stream().allMatch(Variable.class::isInstance)) {
            return Optional.empty();
        }

        LinkedHashMap<String, ArrowFlightColumnHandle> used = new LinkedHashMap<>();
        for (ColumnHandle columnHandle : assignments.values()) {
            ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) columnHandle;
            used.putIfAbsent(column.getColumnName(), column);
        }

        List<ArrowFlightColumnHandle> newProjection = ImmutableList.copyOf(used.values());
        if (newProjection.equals(handle.effectiveColumns())) {
            return Optional.empty();
        }

        ImmutableList.Builder<Assignment> newAssignments = ImmutableList.builder();
        for (Map.Entry<String, ColumnHandle> entry : assignments.entrySet()) {
            ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) entry.getValue();
            newAssignments.add(new Assignment(entry.getKey(), column, column.getColumnType()));
        }

        return Optional.of(new ProjectionApplicationResult<>(
                handle.withProjectedColumns(newProjection),
                projections,
                newAssignments.build(),
                false));
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
