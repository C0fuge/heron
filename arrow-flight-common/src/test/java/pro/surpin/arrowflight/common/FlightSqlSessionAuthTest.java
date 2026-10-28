package pro.surpin.arrowflight.common;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Credentials the server refuses: a refused login is final (no retry, so one failed bind against a
 * directory with an account-lockout policy), an expired session token gets one fresh login.
 */
class FlightSqlSessionAuthTest
{
    /** Logs in "user"/{@link #password} (BasicAuth) or accepts bearer "token"; issues session tokens it can expire. */
    private static final class Server
            extends NoOpFlightProducer
            implements CallHeaderAuthenticator
    {
        private final AtomicInteger logins = new AtomicInteger();
        private final AtomicInteger bearerChecks = new AtomicInteger();
        private final AtomicInteger infoCalls = new AtomicInteger();
        private volatile String password = "secret";
        private volatile String session;
        private FlightServer server;

        @Override
        public AuthResult authenticate(CallHeaders headers)
        {
            String value = headers.get("Authorization");
            if (value != null && value.startsWith("Basic ")) {
                logins.incrementAndGet();
                String decoded = new String(Base64.getDecoder().decode(value.substring(6)), StandardCharsets.UTF_8);
                if (!decoded.equals("user:" + password)) {
                    throw CallStatus.UNAUTHENTICATED.withDescription("Invalid LDAP user name or password").toRuntimeException();
                }
                String issued = "session-" + logins.get();
                session = issued;
                return new AuthResult()
                {
                    @Override
                    public String getPeerIdentity()
                    {
                        return "user";
                    }

                    @Override
                    public void appendToOutgoingHeaders(CallHeaders outgoing)
                    {
                        outgoing.insert("Authorization", "Bearer " + issued);
                    }
                };
            }
            if (value != null && value.startsWith("Bearer ")) {
                bearerChecks.incrementAndGet();
                String token = value.substring(7);
                if (token.equals("token") || token.equals(session)) {
                    return () -> "user";
                }
                throw CallStatus.UNAUTHENTICATED.withDescription("token expired or invalid").toRuntimeException();
            }
            throw CallStatus.UNAUTHENTICATED.withDescription("no credentials").toRuntimeException();
        }

        @Override
        public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor)
        {
            infoCalls.incrementAndGet();
            Schema schema = new Schema(List.of(Field.nullable("v", new ArrowType.Int(32, true))));
            return new FlightInfo(schema, descriptor, List.of(new FlightEndpoint(new Ticket(new byte[] {1}))), -1, -1);
        }
    }

    private BufferAllocator allocator;
    private Server server;

    @BeforeEach
    void setUp()
            throws Exception
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        server = new Server();
        server.server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), server)
                .headerAuthenticator(server)
                .build()
                .start();
    }

    @AfterEach
    void tearDown()
            throws Exception
    {
        server.server.shutdown();
        server.server.awaitTermination(5, TimeUnit.SECONDS);
        server.server.close();
        allocator.close();
    }

    private FlightSqlSession session(FlightConfig.Builder builder)
    {
        return new FlightSqlSession(builder.port(server.server.getPort()).maxRetries(3).retryBackoffMillis(10).build());
    }

    private FlightSqlSession passwordSession(String password)
    {
        return session(FlightConfig.builder("127.0.0.1").user("user").password(password));
    }

    @Test
    void wrongPasswordIsOneLoginAttempt()
    {
        try (FlightSqlSession session = passwordSession("wrong")) {
            FlightCredentialsRejectedException e = assertThrows(FlightCredentialsRejectedException.class,
                    () -> session.getFlightInfo("SELECT 1"));
            assertTrue(e.getMessage().contains("user name and password (user 'user')"), e.getMessage());
            assertTrue(e.getMessage().contains("Invalid LDAP user name or password"), e.getMessage());
            assertEquals(1, server.logins.get());
            assertEquals(0, server.infoCalls.get());
            // the next query asks once more, not four times
            assertThrows(FlightCredentialsRejectedException.class, () -> session.resultSchema("SELECT 1"));
            assertEquals(2, server.logins.get());
        }
    }

    @Test
    void rejectedBearerTokenIsNotRetried()
    {
        try (FlightSqlSession session = session(FlightConfig.builder("127.0.0.1").bearerToken("revoked"))) {
            FlightCredentialsRejectedException e = assertThrows(FlightCredentialsRejectedException.class,
                    () -> session.getFlightInfo("SELECT 1"));
            assertTrue(e.getMessage().contains("bearer token"), e.getMessage());
            assertEquals(1, server.bearerChecks.get());
            assertEquals(0, server.infoCalls.get());
        }
    }

    @Test
    void schemaListingDoesNotHideRejectedCredentials()
    {
        try (FlightSqlSession session = passwordSession("wrong")) {
            assertThrows(FlightCredentialsRejectedException.class, session::listSchemas);
            assertEquals(1, server.logins.get());
        }
    }

    @Test
    void expiredSessionTokenGetsOneFreshLogin()
    {
        try (FlightSqlSession session = passwordSession("secret")) {
            session.getFlightInfo("SELECT 1");
            assertEquals(1, server.logins.get());
            server.session = null;
            session.getFlightInfo("SELECT 1");
            assertEquals(2, server.logins.get());
            assertEquals(2, server.infoCalls.get());
        }
    }

    @Test
    void refusedFreshLoginAfterExpiryIsFinal()
    {
        try (FlightSqlSession session = passwordSession("secret")) {
            session.getFlightInfo("SELECT 1");
            server.session = null;
            server.password = "changed";
            assertThrows(FlightCredentialsRejectedException.class, () -> session.getFlightInfo("SELECT 1"));
            assertEquals(2, server.logins.get());
            assertEquals(1, server.infoCalls.get());
        }
    }
}
