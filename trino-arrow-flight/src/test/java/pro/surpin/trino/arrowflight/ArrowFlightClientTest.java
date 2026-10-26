package pro.surpin.trino.arrowflight;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static pro.surpin.trino.arrowflight.ArrowFlightClient.SEVERAL_ENDPOINTS_MEMORY_NANOS;
import static pro.surpin.trino.arrowflight.ArrowFlightClient.SINGLE_ENDPOINT_ANSWERS;

class ArrowFlightClientTest
{
    @Test
    void singleNodeInstallIsRecognisedAfterManySingleEndpointAnswers()
    {
        try (ArrowFlightClient client = new ArrowFlightClient(new ArrowFlightConfig().setHost("unused"))) {
            assertTrue(client.expectsSeveralEndpoints(0));
            // the first answers of a cluster may well have one endpoint (q18 on 3 nodes: a small table first)
            client.recordEndpointCount(1, 0);
            assertTrue(client.expectsSeveralEndpoints(1));
            for (int i = 1; i < SINGLE_ENDPOINT_ANSWERS; i++) {
                client.recordEndpointCount(1, i);
            }
            assertFalse(client.expectsSeveralEndpoints(SINGLE_ENDPOINT_ANSWERS));
            // empty answers (no endpoint) say nothing
            client.recordEndpointCount(0, SINGLE_ENDPOINT_ANSWERS + 1);
            assertFalse(client.expectsSeveralEndpoints(SINGLE_ENDPOINT_ANSWERS + 2));
        }
    }

    @Test
    void singleEndpointAnswersOnAClusterKeepTheHint()
    {
        try (ArrowFlightClient client = new ArrowFlightClient(new ArrowFlightConfig().setHost("unused"))) {
            client.recordEndpointCount(3, 100);
            for (int i = 0; i < 2 * SINGLE_ENDPOINT_ANSWERS; i++) {
                client.recordEndpointCount(1, 200 + i);
            }
            assertTrue(client.expectsSeveralEndpoints(1000));
            assertTrue(client.expectsSeveralEndpoints(100 + SEVERAL_ENDPOINTS_MEMORY_NANOS - 1));
            // only single endpoints for an hour (and many of them): the install shrank to one node
            assertFalse(client.expectsSeveralEndpoints(100 + SEVERAL_ENDPOINTS_MEMORY_NANOS));
            client.recordEndpointCount(2, 100 + SEVERAL_ENDPOINTS_MEMORY_NANOS + 5);
            assertTrue(client.expectsSeveralEndpoints(100 + SEVERAL_ENDPOINTS_MEMORY_NANOS + 6));
        }
    }

    @Test
    void fewSingleEndpointAnswersAfterAnHourKeepTheHint()
    {
        try (ArrowFlightClient client = new ArrowFlightClient(new ArrowFlightConfig().setHost("unused"))) {
            client.recordEndpointCount(3, 0);
            client.recordEndpointCount(1, 1);
            // an idle hour, then one single-endpoint answer: still a cluster
            assertTrue(client.expectsSeveralEndpoints(2 * SEVERAL_ENDPOINTS_MEMORY_NANOS));
        }
    }
}
