package pro.surpin.trino.arrowflight;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static pro.surpin.trino.arrowflight.ArrowFlightClient.SEVERAL_ENDPOINTS_MEMORY_NANOS;

class ArrowFlightClientTest
{
    @Test
    void severalEndpointsExpectedUntilOnlySingleEndpointsWereSeen()
    {
        try (ArrowFlightClient client = new ArrowFlightClient(new ArrowFlightConfig().setHost("unused"))) {
            assertTrue(client.expectsSeveralEndpoints(0));
            // single-node install: every FlightInfo has one endpoint
            client.recordEndpointCount(1, 0);
            assertFalse(client.expectsSeveralEndpoints(1));
        }
    }

    @Test
    void singleEndpointAnswerOnAClusterKeepsTheHint()
    {
        try (ArrowFlightClient client = new ArrowFlightClient(new ArrowFlightConfig().setHost("unused"))) {
            client.recordEndpointCount(3, 100);
            // a query answered completely (one endpoint) must not switch the next aggregation to the in-memory merge
            client.recordEndpointCount(1, 200);
            assertTrue(client.expectsSeveralEndpoints(300));
            assertTrue(client.expectsSeveralEndpoints(100 + SEVERAL_ENDPOINTS_MEMORY_NANOS - 1));
            // only single endpoints for that long: the install shrank to one node
            assertFalse(client.expectsSeveralEndpoints(100 + SEVERAL_ENDPOINTS_MEMORY_NANOS));
            client.recordEndpointCount(2, 100 + SEVERAL_ENDPOINTS_MEMORY_NANOS + 5);
            assertTrue(client.expectsSeveralEndpoints(100 + SEVERAL_ENDPOINTS_MEMORY_NANOS + 6));
        }
    }
}
