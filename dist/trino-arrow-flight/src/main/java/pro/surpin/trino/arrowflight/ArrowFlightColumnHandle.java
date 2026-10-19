package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ArrowFlightColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;

    @JsonCreator
    public ArrowFlightColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
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
        return columnName.equals(other.columnName) && columnType.equals(other.columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType);
    }

    @Override
    public String toString()
    {
        return columnName + ":" + columnType;
    }
}
