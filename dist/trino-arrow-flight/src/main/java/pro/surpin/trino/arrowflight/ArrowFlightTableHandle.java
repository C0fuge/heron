package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class ArrowFlightTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;

    /** Every column of the remote table, in table order. Used to render the schema. */
    private final List<ArrowFlightColumnHandle> columns;

    /** Columns actually required by the query; empty => all columns. Set by applyProjection. */
    private final Optional<List<ArrowFlightColumnHandle>> projectedColumns;

    /** Predicate the Flight server will enforce (translated to a WHERE clause). Set by applyFilter. */
    private final TupleDomain<ColumnHandle> constraint;

    /** Quote character for identifiers in generated SQL; empty = none (this server's default). */
    private final String identifierQuote;

    @JsonCreator
    public ArrowFlightTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") List<ArrowFlightColumnHandle> columns,
            @JsonProperty("projectedColumns") Optional<List<ArrowFlightColumnHandle>> projectedColumns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("identifierQuote") String identifierQuote)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null")
                .map(ImmutableList::copyOf);
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.identifierQuote = identifierQuote == null ? "" : identifierQuote;
    }

    public ArrowFlightTableHandle(String schemaName, String tableName,
            List<ArrowFlightColumnHandle> columns, String identifierQuote)
    {
        this(schemaName, tableName, columns, Optional.empty(), TupleDomain.all(), identifierQuote);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<ArrowFlightColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<List<ArrowFlightColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public String getIdentifierQuote()
    {
        return identifierQuote;
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    /** Columns to read from the server: the projection if one was pushed, otherwise every column. */
    public List<ArrowFlightColumnHandle> effectiveColumns()
    {
        return projectedColumns.orElse(columns);
    }

    public ArrowFlightTableHandle withProjectedColumns(List<ArrowFlightColumnHandle> newProjection)
    {
        return new ArrowFlightTableHandle(schemaName, tableName, columns, Optional.of(newProjection),
                constraint, identifierQuote);
    }

    public ArrowFlightTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new ArrowFlightTableHandle(schemaName, tableName, columns, projectedColumns,
                newConstraint, identifierQuote);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ArrowFlightTableHandle other)) {
            return false;
        }
        return schemaName.equals(other.schemaName)
                && tableName.equals(other.tableName)
                && columns.equals(other.columns)
                && projectedColumns.equals(other.projectedColumns)
                && constraint.equals(other.constraint)
                && identifierQuote.equals(other.identifierQuote);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, columns, projectedColumns, constraint, identifierQuote);
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName + projectedColumns.map(c -> " " + c).orElse("");
    }
}
