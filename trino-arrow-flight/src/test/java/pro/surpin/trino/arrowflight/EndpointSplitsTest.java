package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSession;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EndpointSplitsTest
{
    private static final ArrowFlightColumnHandle ID = new ArrowFlightColumnHandle("id", BIGINT);
    private static final ArrowFlightColumnHandle NAME = new ArrowFlightColumnHandle("name", VARCHAR);
    private static final ArrowFlightColumnHandle NOTE = new ArrowFlightColumnHandle("note", VARCHAR);

    private static ConnectorSession session(boolean endpointSplits, boolean dynamicFiltering)
    {
        Map<String, Object> properties = Map.of(
                ArrowFlightSessionProperties.ENDPOINT_SPLITS_ENABLED, endpointSplits,
                ArrowFlightSessionProperties.DYNAMIC_FILTERING_ENABLED, dynamicFiltering);
        return (ConnectorSession) Proxy.newProxyInstance(ConnectorSession.class.getClassLoader(), new Class<?>[] {ConnectorSession.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getProperty") && properties.containsKey((String) args[0])) {
                        return properties.get((String) args[0]);
                    }
                    throw new UnsupportedOperationException(method.getName() + (args == null ? "" : " " + List.of(args)));
                });
    }

    private static ArrowFlightTableHandle table()
    {
        return new ArrowFlightTableHandle("s", "t", ImmutableList.of(ID, NAME, NOTE), "\"");
    }

    @Test
    void onlyPlainScansWithoutDynamicFiltersUseEndpointSplits()
    {
        assertTrue(AbstractArrowFlightSplitManager.useEndpointSplits(session(true, false), table()));
        assertTrue(AbstractArrowFlightSplitManager.useEndpointSplits(session(true, false), table().withProjectedColumns(List.of(ID))));
        assertFalse(AbstractArrowFlightSplitManager.useEndpointSplits(session(false, false), table()));
        // dynamic filters are applied when the worker plans the query: they win
        assertFalse(AbstractArrowFlightSplitManager.useEndpointSplits(session(true, true), table()));
        assertFalse(AbstractArrowFlightSplitManager.useEndpointSplits(session(true, false), table().withLimit(10)));
    }

    @Test
    void everyEndpointOfOneStatementBecomesASplit()
    {
        byte[] first = {0, 1, (byte) 0xff, (byte) 0x80, 42};
        byte[] second = "ticket-2".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        List<String> queries = new ArrayList<>();
        ArrowFlightConfig config = new ArrowFlightConfig().setHost("unused");
        try (ArrowFlightClient client = new ArrowFlightClient(config)
        {
            @Override
            public FlightInfo getFlightInfo(String sql)
            {
                queries.add(sql);
                return new FlightInfo(new Schema(List.of()), FlightDescriptor.command(new byte[0]), List.of(
                        new FlightEndpoint(new Ticket(first), Location.forGrpcInsecure("n1", 32010), Location.forGrpcInsecure("n2", 32010)),
                        new FlightEndpoint(new Ticket(second), Location.forGrpcInsecure("n2", 32010))), -1, -1);
            }
        }) {
            ArrowFlightSplitManager manager = new ArrowFlightSplitManager(client, new ArrowFlightStatistics(client, config), config);
            List<ArrowFlightSplit> splits = manager.endpointSplits(table().withProjectedColumns(List.of(NOTE, ID)));

            // one statement for the whole scan, with no key range
            assertEquals(List.of("SELECT \"id\", \"note\" FROM \"s\".\"t\""), queries);
            assertEquals(2, splits.size());
            assertArrayEquals(first, splits.get(0).ticketBytes().orElseThrow());
            assertEquals(List.of("grpc+tcp://n1:32010", "grpc+tcp://n2:32010"), splits.get(0).getLocations());
            assertArrayEquals(second, splits.get(1).ticketBytes().orElseThrow());
            assertEquals(List.of("grpc+tcp://n2:32010"), splits.get(1).getLocations());
            for (ArrowFlightSplit split : splits) {
                assertEquals(List.of("id", "note"), split.getPlannedColumns());
                assertEquals(Optional.empty(), split.getRangePredicate());
            }
        }
    }

    @Test
    void jsonCreatorKeepsTheEndpoint()
    {
        ArrowFlightSplit split = ArrowFlightSplit.forEndpoint(new byte[] {7, 8, 9}, List.of("grpc+tcp://n1:32010"), List.of("id"));
        ArrowFlightSplit copy = new ArrowFlightSplit(split.getRangePredicate(), split.getTicket(), split.getLocations(), split.getPlannedColumns());
        assertArrayEquals(new byte[] {7, 8, 9}, copy.ticketBytes().orElseThrow());
        assertEquals("ArrowFlightSplit[endpoint [grpc+tcp://n1:32010]]", copy.toString());
        assertEquals(Optional.empty(), new ArrowFlightSplit(Optional.of("x < 1")).getTicket());
    }
}
