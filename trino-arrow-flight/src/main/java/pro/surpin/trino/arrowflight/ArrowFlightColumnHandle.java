package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A table column, or a synthetic column: an arithmetic {@link Expr} over table columns
 * (from applyProjection) or an aggregate output (from applyAggregation, see {@link Aggregation}).
 */
public final class ArrowFlightColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final Optional<Expr> expression;

    @JsonCreator
    public ArrowFlightColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("expression") Optional<Expr> expression)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.expression = expression == null ? Optional.empty() : expression;
    }

    public ArrowFlightColumnHandle(String columnName, Type columnType)
    {
        this(columnName, columnType, Optional.empty());
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public Optional<Expr> getExpression()
    {
        return expression;
    }

    /** SQL for the value: the quoted column name, or the rendered expression. */
    public String toSql(String quote)
    {
        return expression.map(e -> e.toSql(quote)).orElseGet(() -> SqlQueryBuilder.quoteIdentifier(columnName, quote));
    }

    public ColumnMetadata toColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ArrowFlightColumnHandle other)) {
            return false;
        }
        return columnName.equals(other.columnName) && columnType.equals(other.columnType) && expression.equals(other.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, expression);
    }

    @Override
    public String toString()
    {
        return columnName + ":" + columnType + expression.map(e -> "=" + e).orElse("");
    }
}
