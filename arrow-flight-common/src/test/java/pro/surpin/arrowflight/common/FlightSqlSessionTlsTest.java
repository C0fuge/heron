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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A server certificate the catalog does not trust is final: no retries with backoff (they looked like
 * "failed after 4 attempts: io exception" after ~15 s), and the message names the truststore setting.
 */
class FlightSqlSessionTlsTest
{
    // With a retry the call would take at least this long.
    private static final long BACKOFF_MILLIS = 5_000;

    @TempDir
    static Path dir;
    // Server certificate for 127.0.0.1 / localhost, a certificate for another host, an unrelated one.
    private static Pem server;
    private static Pem otherHost;
    private static Pem unrelated;

    private record Pem(Path cert, Path key) {}

    /** Accepts bearer "token"; counts the calls that got through TLS and authentication. */
    private static final class Producer
            extends NoOpFlightProducer
            implements CallHeaderAuthenticator
    {
        private final AtomicInteger infoCalls = new AtomicInteger();

        @Override
        public AuthResult authenticate(CallHeaders headers)
        {
            if ("Bearer token".equals(headers.get("Authorization"))) {
                return () -> "user";
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
    private Producer producer;
    private FlightServer flightServer;

    @BeforeAll
    static void certificates()
            throws Exception
    {
        server = selfSigned("server", "CN=localhost", "SAN=dns:localhost,ip:127.0.0.1");
        otherHost = selfSigned("other-host", "CN=flight9", "SAN=dns:flight9");
        unrelated = selfSigned("unrelated", "CN=localhost", "SAN=dns:localhost,ip:127.0.0.1");
    }

    /** A self-signed RSA certificate made with the JDK's keytool, written as PEM certificate + PKCS#8 key. */
    private static Pem selfSigned(String name, String dname, String san)
            throws Exception
    {
        Path store = dir.resolve(name + ".p12");
        Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
        Process process = new ProcessBuilder(keytool.toString(), "-genkeypair", "-alias", name, "-keyalg", "RSA",
                "-keysize", "2048", "-validity", "2", "-dname", dname, "-ext", san, "-storetype", "PKCS12",
                "-keystore", store.toString(), "-storepass", "changeit", "-keypass", "changeit")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(process.waitFor(60, TimeUnit.SECONDS) && process.exitValue() == 0, "keytool: " + output);

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(store)) {
            keyStore.load(in, "changeit".toCharArray());
        }
        Pem pem = new Pem(dir.resolve(name + ".pem"), dir.resolve(name + ".key"));
        Files.writeString(pem.cert(), pem("CERTIFICATE", keyStore.getCertificate(name).getEncoded()));
        Files.writeString(pem.key(), pem("PRIVATE KEY", keyStore.getKey(name, "changeit".toCharArray()).getEncoded()));
        return pem;
    }

    private static String pem(String type, byte[] der)
    {
        return "-----BEGIN " + type + "-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
                + "\n-----END " + type + "-----\n";
    }

    @BeforeEach
    void setUp()
            throws Exception
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        producer = new Producer();
        flightServer = startServer(server);
    }

    private FlightServer startServer(Pem pem)
            throws Exception
    {
        return FlightServer.builder(allocator, Location.forGrpcTls("127.0.0.1", 0), producer)
                .useTls(new ByteArrayInputStream(Files.readAllBytes(pem.cert())), new ByteArrayInputStream(Files.readAllBytes(pem.key())))
                .headerAuthenticator(producer)
                .build()
                .start();
    }

    @AfterEach
    void tearDown()
            throws Exception
    {
        flightServer.shutdown();
        flightServer.awaitTermination(5, TimeUnit.SECONDS);
        flightServer.close();
        allocator.close();
    }

    private static FlightConfig.Builder tls(String host)
    {
        return FlightConfig.builder(host).tlsEnabled(true).bearerToken("token")
                .maxRetries(3).retryBackoffMillis(BACKOFF_MILLIS);
    }

    private FlightTlsHandshakeException assertFinal(FlightConfig config)
    {
        try (FlightSqlSession session = new FlightSqlSession(config)) {
            long start = System.nanoTime();
            FlightTlsHandshakeException e = assertThrows(FlightTlsHandshakeException.class, () -> session.getFlightInfo("SELECT 1"));
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            assertTrue(millis < BACKOFF_MILLIS, "retried: " + millis + " ms");
            assertTrue(e.getMessage().contains("arrow-flight.tls.truststore-path"), e.getMessage());
            assertFalse(e.getMessage().contains("attempts"), e.getMessage());
            // a schema listing must not turn it into "only the default schema"
            assertThrows(FlightTlsHandshakeException.class, session::listSchemas);
            assertEquals(0, producer.infoCalls.get());
            return e;
        }
    }

    @Test
    void trustedCertificateConnects()
    {
        try (FlightSqlSession session = new FlightSqlSession(tls("127.0.0.1").port(flightServer.getPort())
                .tlsTruststorePath(server.cert().toString()).build())) {
            session.getFlightInfo("SELECT 1");
            assertEquals(1, producer.infoCalls.get());
        }
    }

    @Test
    void untrustedCertificateIsFinal()
    {
        FlightTlsHandshakeException e = assertFinal(tls("127.0.0.1").port(flightServer.getPort())
                .tlsTruststorePath(unrelated.cert().toString()).build());
        assertTrue(e.getMessage().contains("127.0.0.1:" + flightServer.getPort()), e.getMessage());
        assertTrue(e.getMessage().contains("the truststore " + unrelated.cert()), e.getMessage());
    }

    @Test
    void certificateUnknownToDefaultTruststoreIsFinal()
    {
        FlightTlsHandshakeException e = assertFinal(tls("127.0.0.1").port(flightServer.getPort()).build());
        assertTrue(e.getMessage().contains("the JVM's default truststore"), e.getMessage());
    }

    @Test
    void certificateForAnotherHostIsFinal()
            throws Exception
    {
        flightServer.close();
        flightServer = startServer(otherHost);
        assertFinal(tls("127.0.0.1").port(flightServer.getPort()).tlsTruststorePath(otherHost.cert().toString()).build());
    }

    @Test
    void everyCoordinatorRefusedIsFinal()
    {
        int port = flightServer.getPort();
        assertFinal(tls("127.0.0.1").port(port).alternateHosts(List.of("localhost:" + port))
                .tlsTruststorePath(unrelated.cert().toString()).build());
    }

    @Test
    void unreachableCoordinatorIsStillRetried()
            throws Exception
    {
        // A refused certificate on one coordinator while another cannot be reached: the old retry path.
        int port = flightServer.getPort();
        int closedPort;
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        FlightConfig config = tls("127.0.0.1").port(closedPort).alternateHosts(List.of("127.0.0.1:" + port))
                .tlsTruststorePath(unrelated.cert().toString()).maxRetries(1).retryBackoffMillis(10).build();
        try (FlightSqlSession session = new FlightSqlSession(config)) {
            RuntimeException e = assertThrows(RuntimeException.class, () -> session.getFlightInfo("SELECT 1"));
            assertFalse(e instanceof FlightTlsHandshakeException, e.toString());
            assertTrue(e.getMessage().contains("after 2 attempts"), e.getMessage());
        }
    }
}
