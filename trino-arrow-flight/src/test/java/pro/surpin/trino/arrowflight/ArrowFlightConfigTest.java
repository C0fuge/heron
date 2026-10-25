package pro.surpin.trino.arrowflight;

import org.junit.jupiter.api.Test;
import pro.surpin.arrowflight.common.FlightConfig;
import pro.surpin.arrowflight.common.FlightConfig.Coordinator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowFlightConfigTest
{
    @Test
    void singleHostKeepsOneCoordinator()
    {
        ArrowFlightConfig config = new ArrowFlightConfig().setHost("flight1").setPort(32011).setBearerToken("t");
        assertTrue(config.isCoordinatorConfigured());
        FlightConfig flight = config.toFlightConfig();
        assertEquals("flight1", flight.getHost());
        assertEquals(32011, flight.getPort());
        assertEquals(List.of(new Coordinator("flight1", 32011)), flight.getCoordinators());
    }

    @Test
    void hostListIsTriedInOrderWithTheCatalogPortAsDefault()
    {
        ArrowFlightConfig config = new ArrowFlightConfig()
                .setHosts(" flight1, flight2:32020 ,,[::1], flight3")
                .setPort(32011)
                .setBearerToken("t");
        assertTrue(config.isCoordinatorConfigured());
        FlightConfig flight = config.toFlightConfig();
        assertEquals("flight1", flight.getHost());
        assertEquals(32011, flight.getPort());
        assertEquals(List.of(new Coordinator("flight1", 32011), new Coordinator("flight2", 32020),
                new Coordinator("::1", 32011), new Coordinator("flight3", 32011)), flight.getCoordinators());

        FlightConfig ported = new ArrowFlightConfig().setHosts("flight1:32030,flight2").setBearerToken("t").toFlightConfig();
        assertEquals(List.of(new Coordinator("flight1", 32030), new Coordinator("flight2", 32010)), ported.getCoordinators());
    }

    @Test
    void exactlyOneOfHostAndHostsMustBeSet()
    {
        assertFalse(new ArrowFlightConfig().isCoordinatorConfigured());
        assertFalse(new ArrowFlightConfig().setHosts(" , ").isCoordinatorConfigured());
        assertFalse(new ArrowFlightConfig().setHost("a").setHosts("b").isCoordinatorConfigured());
    }
}
