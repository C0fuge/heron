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
import java.util.OptionalLong;

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

    /** Extra WHERE conjuncts already rendered as SQL (LIKE / NOT LIKE from applyFilter). */
    private final List<String> extraPredicates;

    /** GROUP BY + aggregates computed on the server. Set by applyAggregation. */
    private final Optional<Aggregation> aggregation;

    /** Rows to read at most (connector-side; the server has no LIMIT). Set by applyLimit. */
    private final OptionalLong limit;

    /** ORDER BY ... LIMIT n run by the server per query (not guaranteed: Trino keeps its TopN). Set by applyTopN. */
    private final Optional<TopN> topN;

    /** Iceberg time travel (FOR VERSION / TIMESTAMP AS OF) written after the table name. Set by getTableHandle. */
    private final Optional<TableVersion> version;

    /** Quote character for identifiers in generated SQL; empty = none (this server's default). */
    private final String identifierQuote;

    @JsonCreator
    public ArrowFlightTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") List<ArrowFlightColumnHandle> columns,
            @JsonProperty("projectedColumns") Optional<List<ArrowFlightColumnHandle>> projectedColumns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("extraPredicates") List<String> extraPredicates,
            @JsonProperty("aggregation") Optional<Aggregation> aggregation,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("topN") Optional<TopN> topN,
            @JsonProperty("version") Optional<TableVersion> version,
            @JsonProperty("identifierQuote") String identifierQuote)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null")
                .map(ImmutableList::copyOf);
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.extraPredicates = ImmutableList.copyOf(requireNonNull(extraPredicates, "extraPredicates is null"));
        this.aggregation = requireNonNull(aggregation, "aggregation is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.topN = topN == null ? Optional.empty() : topN;
        this.version = version == null ? Optional.empty() : version;
        this.identifierQuote = identifierQuote == null ? "" : identifierQuote;
    }

    public ArrowFlightTableHandle(String schemaName, String tableName,
            List<ArrowFlightColumnHandle> columns, String identifierQuote)
    {
        this(schemaName, tableName, columns, Optional.empty(), identifierQuote);
    }

    public ArrowFlightTableHandle(String schemaName, String tableName,
            List<ArrowFlightColumnHandle> columns, Optional<TableVersion> version, String identifierQuote)
    {
        this(schemaName, tableName, columns, Optional.empty(), TupleDomain.all(), List.of(),
                Optional.empty(), OptionalLong.empty(), Optional.empty(), version, identifierQuote);
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
    public List<String> getExtraPredicates()
    {
        return extraPredicates;
    }

    @JsonProperty
    public Optional<Aggregation> getAggregation()
    {
        return aggregation;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<TopN> getTopN()
    {
        return topN;
    }

    @JsonProperty
    public Optional<TableVersion> getVersion()
    {
        return version;
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
                constraint, extraPredicates, aggregation, limit, topN, version, identifierQuote);
    }

    public ArrowFlightTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new ArrowFlightTableHandle(schemaName, tableName, columns, projectedColumns,
                newConstraint, extraPredicates, aggregation, limit, topN, version, identifierQuote);
    }

    public ArrowFlightTableHandle withExtraPredicates(List<String> newPredicates)
    {
        return new ArrowFlightTableHandle(schemaName, tableName, columns, projectedColumns,
                constraint, newPredicates, aggregation, limit, topN, version, identifierQuote);
    }

    public ArrowFlightTableHandle withAggregation(Aggregation newAggregation)
    {
        return new ArrowFlightTableHandle(schemaName, tableName, columns, Optional.empty(),
                constraint, extraPredicates, Optional.of(newAggregation), limit, topN, version, identifierQuote);
    }

    public ArrowFlightTableHandle withLimit(long newLimit)
    {
        return new ArrowFlightTableHandle(schemaName, tableName, columns, projectedColumns,
                constraint, extraPredicates, aggregation, OptionalLong.of(newLimit), topN, version, identifierQuote);
    }

    public ArrowFlightTableHandle withTopN(TopN newTopN)
    {
        return new ArrowFlightTableHandle(schemaName, tableName, columns, projectedColumns,
                constraint, extraPredicates, aggregation, limit, Optional.of(newTopN), version, identifierQuote);
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
                && extraPredicates.equals(other.extraPredicates)
                && aggregation.equals(other.aggregation)
                && limit.equals(other.limit)
                && topN.equals(other.topN)
                && version.equals(other.version)
                && identifierQuote.equals(other.identifierQuote);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, columns, projectedColumns, constraint, extraPredicates,
                aggregation, limit, topN, version, identifierQuote);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(schemaName).append('.').append(tableName);
        version.ifPresent(v -> sb.append(' ').append(v));
        projectedColumns.ifPresent(c -> sb.append(' ').append(c));
        if (!extraPredicates.isEmpty()) {
            sb.append(" where ").append(extraPredicates);
        }
        aggregation.ifPresent(a -> sb.append(' ').append(a));
        limit.ifPresent(l -> sb.append(" limit=").append(l));
        topN.ifPresent(t -> sb.append(' ').append(t));
        return sb.toString();
    }
}
