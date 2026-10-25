package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * Iceberg time travel ({@code FOR VERSION AS OF <snapshot>} / {@code FOR TIMESTAMP AS OF ...}), forwarded to the
 * server after the table name. The server (builds from c3b276054cc3) accepts a snapshot id or a
 * {@code TIMESTAMP '... UTC'} literal; tables that are not Iceberg tables answer with a server error.
 */
public final class TableVersion
{
    public enum Kind { SNAPSHOT, TIMESTAMP }

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final Kind kind;
    /** Snapshot id, or UTC timestamp {@code yyyy-MM-dd HH:mm:ss.SSSSSS}. */
    private final String value;

    @JsonCreator
    public TableVersion(@JsonProperty("kind") Kind kind, @JsonProperty("value") String value)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.value = requireNonNull(value, "value is null");
    }

    public static TableVersion snapshot(long snapshotId)
    {
        return new TableVersion(Kind.SNAPSHOT, Long.toString(snapshotId));
    }

    public static TableVersion timestamp(Instant instant)
    {
        return new TableVersion(Kind.TIMESTAMP, TIMESTAMP_FORMAT.format(instant.atOffset(ZoneOffset.UTC)));
    }

    /** Maps Trino's {@code FOR VERSION AS OF} / {@code FOR TIMESTAMP AS OF} value. */
    public static TableVersion from(ConnectorSession session, ConnectorTableVersion version)
    {
        Type type = version.getVersionType();
        Object pointer = version.getVersion();
        if (version.getPointerType() == PointerType.TARGET_ID) {
            if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
                return snapshot((Long) pointer);
            }
            String shown = pointer instanceof Slice slice ? slice.toStringUtf8() : String.valueOf(pointer);
            throw new TrinoException(NOT_SUPPORTED,
                    "FOR VERSION AS OF needs a snapshot id (a number); branches and tags are not supported: " + shown);
        }
        if (type instanceof TimestampWithTimeZoneType tz) {
            if (tz.isShort()) {
                return timestamp(Instant.ofEpochMilli(unpackMillisUtc((Long) pointer)));
            }
            LongTimestampWithTimeZone ts = (LongTimestampWithTimeZone) pointer;
            return timestamp(Instant.ofEpochMilli(ts.getEpochMillis()).plusNanos(ts.getPicosOfMilli() / 1000));
        }
        if (type instanceof TimestampType t) {
            // a timestamp without zone is read in the session zone, as Trino's Iceberg connector does
            long micros = t.isShort() ? (Long) pointer : ((LongTimestamp) pointer).getEpochMicros();
            LocalDateTime local = LocalDateTime.ofEpochSecond(Math.floorDiv(micros, 1_000_000L),
                    (int) Math.floorMod(micros, 1_000_000L) * 1000, ZoneOffset.UTC);
            return timestamp(local.atZone(session.getTimeZoneKey().getZoneId()).toInstant());
        }
        throw new TrinoException(INVALID_ARGUMENTS, "Unsupported type for FOR TIMESTAMP AS OF: " + type);
    }

    @JsonProperty
    public Kind getKind()
    {
        return kind;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    /** The clause written after the table name, with a leading space. */
    public String toSql()
    {
        return kind == Kind.SNAPSHOT
                ? " FOR VERSION AS OF " + value
                : " FOR TIMESTAMP AS OF TIMESTAMP '" + value + " UTC'";
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof TableVersion other && kind == other.kind && value.equals(other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, value);
    }

    @Override
    public String toString()
    {
        return toSql().trim();
    }
}
