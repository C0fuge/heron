package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Iceberg time travel: Trino's FOR VERSION / TIMESTAMP AS OF is forwarded after the table name. */
class TableVersionTest
{
    private static final ArrowFlightColumnHandle ID = new ArrowFlightColumnHandle("id", BIGINT);

    private static ConnectorSession session(String zone)
    {
        return (ConnectorSession) Proxy.newProxyInstance(ConnectorSession.class.getClassLoader(), new Class<?>[] {ConnectorSession.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getTimeZoneKey")) {
                        return TimeZoneKey.getTimeZoneKey(zone);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    @Test
    void snapshotIdBecomesForVersionAsOf()
    {
        TableVersion version = TableVersion.from(session("UTC"),
                new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, 8744736658442914487L));
        assertEquals(" FOR VERSION AS OF 8744736658442914487", version.toSql());
    }

    @Test
    void branchOrTagNameIsRejected()
    {
        TrinoException e = assertThrows(TrinoException.class, () -> TableVersion.from(session("UTC"),
                new ConnectorTableVersion(PointerType.TARGET_ID, VARCHAR, Slices.utf8Slice("main"))));
        assertTrue(e.getMessage().contains("main"), e.getMessage());
    }

    @Test
    void timestampWithZoneIsSentInUtc()
    {
        long packed = packDateTimeWithZone(Instant.parse("2026-10-25T10:15:30.123Z").toEpochMilli(), TimeZoneKey.getTimeZoneKey("Europe/Moscow"));
        assertEquals(" FOR TIMESTAMP AS OF TIMESTAMP '2026-10-25 10:15:30.123000 UTC'",
                TableVersion.from(session("UTC"), new ConnectorTableVersion(PointerType.TEMPORAL, TIMESTAMP_TZ_MILLIS, packed)).toSql());

        LongTimestampWithTimeZone precise = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                Instant.parse("2026-10-25T10:15:30.123Z").toEpochMilli(), 456_789_000, TimeZoneKey.UTC_KEY);
        assertEquals(" FOR TIMESTAMP AS OF TIMESTAMP '2026-10-25 10:15:30.123456 UTC'",
                TableVersion.from(session("UTC"), new ConnectorTableVersion(PointerType.TEMPORAL, TIMESTAMP_TZ_NANOS, precise)).toSql());
    }

    @Test
    void timestampWithoutZoneIsReadInTheSessionZone()
    {
        long micros = Instant.parse("2026-10-25T13:00:00Z").toEpochMilli() * 1000;   // local 13:00, no zone
        assertEquals(" FOR TIMESTAMP AS OF TIMESTAMP '2026-10-25 10:00:00.000000 UTC'",
                TableVersion.from(session("Europe/Moscow"), new ConnectorTableVersion(PointerType.TEMPORAL, TIMESTAMP_MICROS, micros)).toSql());
    }

    @Test
    void everyServerQueryOfAVersionedTableNamesTheVersion()
    {
        ArrowFlightTableHandle handle = new ArrowFlightTableHandle("ice", "orders", ImmutableList.of(ID),
                Optional.of(TableVersion.snapshot(42)), "");
        assertEquals("SELECT id FROM ice.orders FOR VERSION AS OF 42", SqlQueryBuilder.buildScanSql(handle));
        assertEquals("SELECT id FROM ice.orders FOR VERSION AS OF 42 WHERE (id >= 5)",
                SqlQueryBuilder.buildScanSql(handle, List.of(ID), List.of("(id >= 5)")));
        // the version survives every pushdown step
        assertEquals(handle.getVersion(), handle.withLimit(10).withExtraPredicates(List.of("x")).withProjectedColumns(List.of(ID)).getVersion());
        assertEquals("ice.orders FOR VERSION AS OF 42", SqlQueryBuilder.tableName(handle));
        // and an unversioned table is unchanged
        assertEquals("SELECT id FROM ice.orders", SqlQueryBuilder.buildScanSql(new ArrowFlightTableHandle("ice", "orders", ImmutableList.of(ID), "")));
    }

    @Test
    void getTableHandleResolvesTheColumnsOfThatVersion()
    {
        List<String> asked = new ArrayList<>();
        ArrowFlightConfig config = new ArrowFlightConfig().setHost("unused");
        ArrowFlightClient client = new ArrowFlightClient(config)
        {
            @Override
            public Schema resultSchema(String sql)
            {
                asked.add(sql);
                return new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
            }
        };
        try (client) {
            ArrowFlightMetadata metadata = new ArrowFlightMetadata(client, new ArrowTypeConverter(), config, new ArrowFlightStatistics(client, config));
            ConnectorTableHandle handle = metadata.getTableHandle(session("UTC"), new SchemaTableName("ice", "orders"),
                    Optional.empty(), Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, 7L)));
            assertEquals(List.of("SELECT * FROM ice.orders FOR VERSION AS OF 7"), asked);
            assertEquals(Optional.of(TableVersion.snapshot(7)), ((ArrowFlightTableHandle) handle).getVersion());

            metadata.getTableHandle(session("UTC"), new SchemaTableName("ice", "orders"), Optional.empty(), Optional.empty());
            assertEquals("SELECT * FROM ice.orders", asked.get(1));

            assertThrows(TrinoException.class, () -> metadata.getTableHandle(session("UTC"), new SchemaTableName("ice", "orders"),
                    Optional.of(new ConnectorTableVersion(PointerType.TARGET_ID, BIGINT, 7L)), Optional.empty()));
        }
    }
}
