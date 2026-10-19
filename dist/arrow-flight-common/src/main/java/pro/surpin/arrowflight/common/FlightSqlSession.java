package pro.surpin.arrowflight.common;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Engine-neutral wrapper over {@link FlightSqlClient}. Reproduces the handshake and endpoint
 * routing of the reference client in the arrow-flight-server repo
 * ({@code pro.surpin.data.arrowflight.client.Client}) without its server-side dependencies
 * (Hazelcast, DuckDB, Hadoop).
 *
 * <p>The coordinator connection is opened lazily on first use, so a temporarily unreachable
 * server does not break connector/catalog creation.
 */
public class FlightSqlSession
        implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(FlightSqlSession.class);

    private final FlightConfig config;
    private final BufferAllocator allocator;

    // Shared between the client builder (intercept) and the handshake so the bearer token
    // the server returns for BasicAuth can be captured and reused on later calls.
    private final ClientIncomingAuthHeaderMiddleware.Factory bearerFactory =
            new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

    private final ConcurrentMap<String, FlightClient> routedClients = new ConcurrentHashMap<>();

    private volatile boolean connected;
    private FlightClient coordinatorClient;
    private FlightSqlClient sqlClient;
    private CredentialCallOption authOption;

    public FlightSqlSession(FlightConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    private synchronized void ensureConnected()
    {
        if (connected) {
            return;
        }
        Location location = config.isTlsEnabled()
                ? Location.forGrpcTls(config.getHost(), config.getPort())
                : Location.forGrpcInsecure(config.getHost(), config.getPort());

        this.coordinatorClient = buildClient(location, config.getPassword().isPresent());
        this.authOption = authenticate(coordinatorClient);
        this.sqlClient = new FlightSqlClient(coordinatorClient);
        this.connected = true;
    }

    // ------------------------------------------------------------------ metadata

    /** Schema names via Flight SQL {@code CommandGetDbSchemas}; falls back to the configured default. */
    public List<String> listSchemas()
    {
        ensureConnected();
        List<String> schemas = new ArrayList<>();
        try (FlightStream stream = openInfoStream(sqlClient.getSchemas(null, null, callOptions()))) {
            readStringColumn(stream, "db_schema_name", schemas);
        }
        catch (Exception e) {
            LOG.warn("getSchemas failed, using configured default schema only: {}", e.toString());
        }
        config.getDefaultSchema().ifPresent(s -> {
            if (!schemas.contains(s)) {
                schemas.add(s);
            }
        });
        return List.copyOf(schemas);
    }

    /** Table names in a schema via Flight SQL {@code CommandGetTables}. */
    public List<String> listTables(String schema)
    {
        ensureConnected();
        List<String> tables = new ArrayList<>();
        try (FlightStream stream = openInfoStream(
                sqlClient.getTables(null, schema, null, null, false, callOptions()))) {
            readStringColumn(stream, "table_name", tables);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to list tables in schema '" + schema + "': " + e.getMessage(), e);
        }
        return List.copyOf(tables);
    }

    /** Result schema of a statement, without reserving stream tickets. */
    public Schema resultSchema(String sql)
    {
        ensureConnected();
        return retry(() -> sqlClient.getExecuteSchema(sql, callOptions()).getSchema(), "getExecuteSchema");
    }

    /** Submits the statement and returns the endpoints the data can be pulled from. */
    public FlightInfo getFlightInfo(String sql)
    {
        ensureConnected();
        return retry(() -> sqlClient.execute(sql, callOptions()), "execute");
    }

    // ------------------------------------------------------------------ data

    /**
     * Opens the Arrow stream for a single endpoint ticket. When the endpoint advertises a
     * non-coordinator location, a dedicated client is used and the coordinator-issued
     * credential is forwarded on the getStream call.
     */
    public FlightStream openStream(byte[] ticket, List<String> locationUris)
    {
        ensureConnected();
        Ticket flightTicket = new Ticket(requireNonNull(ticket, "ticket is null"));

        if (locationUris.isEmpty()) {
            return coordinatorClient.getStream(flightTicket, callOptions());
        }
        for (String uri : locationUris) {
            if (isCoordinator(uri)) {
                return coordinatorClient.getStream(flightTicket, callOptions());
            }
        }
        String uri = locationUris.get(0);
        FlightClient routed = routedClients.computeIfAbsent(uri, key -> buildClient(location(key), false));
        return routed.getStream(flightTicket, callOptions());
    }

    public BufferAllocator allocator()
    {
        return allocator;
    }

    // ------------------------------------------------------------------ wiring

    private FlightClient buildClient(Location location, boolean captureBearer)
    {
        FlightClient.Builder builder = FlightClient.builder().allocator(allocator).location(location);

        String scheme = Optional.ofNullable(location.getUri().getScheme()).orElse("").toLowerCase(Locale.ROOT);
        if (scheme.equals("grpc+tls")) {
            builder.useTls();
            Optional<byte[]> certs = trustedCertificates();
            if (certs.isPresent()) {
                builder.trustedCertificates(new ByteArrayInputStream(certs.get()));
            }
            else {
                builder.verifyServer(config.isTlsVerifyServer());
            }
        }
        if (captureBearer) {
            builder.intercept(bearerFactory);
        }
        return builder.build();
    }

    private CredentialCallOption authenticate(FlightClient client)
    {
        List<CallOption> handshakeOptions = new ArrayList<>();
        headerCallOption().ifPresent(handshakeOptions::add);

        boolean usePassword = config.getPassword().isPresent();
        CredentialCallOption credential = new CredentialCallOption(usePassword
                ? new BasicAuthCredentialWriter(config.getUser().orElse(""), config.getPassword().get())
                : new BearerCredentialWriter(config.getBearerToken().orElseThrow(() -> new IllegalStateException(
                        "Either a password (BasicAuth) or a bearer token must be configured"))));
        handshakeOptions.add(credential);

        client.handshake(handshakeOptions.toArray(new CallOption[0]));

        return usePassword ? bearerFactory.getCredentialCallOption() : credential;
    }

    private Optional<HeaderCallOption> headerCallOption()
    {
        FlightCallHeaders headers = new FlightCallHeaders();
        config.getDefaultSchema().ifPresent(v -> headers.insert("SCHEMA", v));
        config.getRoutingTag().ifPresent(v -> headers.insert("ROUTING_TAG", v));
        config.getRoutingQueue().ifPresent(v -> headers.insert("ROUTING_QUEUE", v));
        return headers.keys().isEmpty() ? Optional.empty() : Optional.of(new HeaderCallOption(headers));
    }

    private CallOption[] callOptions()
    {
        List<CallOption> options = new ArrayList<>();
        options.add(authOption);
        if (config.getRequestTimeoutMillis() > 0) {
            options.add(CallOptions.timeout(config.getRequestTimeoutMillis(), TimeUnit.MILLISECONDS));
        }
        return options.toArray(new CallOption[0]);
    }

    private static Location location(String uri)
    {
        try {
            return new Location(uri);
        }
        catch (java.net.URISyntaxException e) {
            throw new IllegalArgumentException("Malformed Flight endpoint location: " + uri, e);
        }
    }

    private boolean isCoordinator(String uri)
    {
        try {
            URI parsed = URI.create(uri);
            return config.getHost().equalsIgnoreCase(parsed.getHost()) && config.getPort() == parsed.getPort();
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private Optional<byte[]> trustedCertificates()
    {
        Optional<String> pathString = config.getTlsTruststorePath();
        if (pathString.isEmpty()) {
            return Optional.empty();
        }
        Path path = Path.of(pathString.get());
        try {
            if (pathString.get().toLowerCase(Locale.ROOT).endsWith(".jks")) {
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                try (InputStream in = Files.newInputStream(path)) {
                    keyStore.load(in, config.getTlsTruststorePassword().map(String::toCharArray).orElse(null));
                }
                Enumeration<String> aliases = keyStore.aliases();
                while (aliases.hasMoreElements()) {
                    String alias = aliases.nextElement();
                    if (keyStore.isCertificateEntry(alias)) {
                        Certificate cert = keyStore.getCertificate(alias);
                        String pem = "-----BEGIN CERTIFICATE-----\n"
                                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.UTF_8))
                                        .encodeToString(cert.getEncoded())
                                + "\n-----END CERTIFICATE-----\n";
                        return Optional.of(pem.getBytes(StandardCharsets.UTF_8));
                    }
                }
                return Optional.empty();
            }
            return Optional.of(Files.readAllBytes(path));
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot read TLS truststore " + path + ": " + e.getMessage(), e);
        }
    }

    private FlightStream openInfoStream(FlightInfo info)
    {
        if (info.getEndpoints().isEmpty()) {
            throw new IllegalStateException("FlightInfo has no endpoints");
        }
        FlightEndpoint endpoint = info.getEndpoints().get(0);
        return coordinatorClient.getStream(endpoint.getTicket(), callOptions());
    }

    private static void readStringColumn(FlightStream stream, String columnName, List<String> out)
    {
        VectorSchemaRoot root = stream.getRoot();
        while (stream.next()) {
            if (!(root.getVector(columnName) instanceof VarCharVector vector)) {
                continue;
            }
            for (int i = 0; i < root.getRowCount(); i++) {
                if (!vector.isNull(i)) {
                    out.add(new String(vector.get(i), StandardCharsets.UTF_8));
                }
            }
        }
    }

    private <T> T retry(FlightCall<T> call, String name)
    {
        int maxRetries = config.getMaxRetries();
        RuntimeException last = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                sleep(config.getRetryBackoffMillis() * (1L << (attempt - 1)));
            }
            try {
                return call.call();
            }
            catch (RuntimeException e) {
                last = e;
                LOG.warn("Flight call '{}' failed (attempt {}/{}): {}", name, attempt + 1, maxRetries + 1, e.getMessage());
            }
        }
        throw new RuntimeException("Flight call '" + name + "' failed after " + (maxRetries + 1) + " attempts", last);
    }

    private static void sleep(long millis)
    {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while retrying Flight call", e);
        }
    }

    @Override
    public void close()
    {
        routedClients.values().forEach(FlightSqlSession::closeQuietly);
        routedClients.clear();
        if (coordinatorClient != null) {
            closeQuietly(coordinatorClient);
        }
        try {
            allocator.close();
        }
        catch (Exception e) {
            LOG.warn("Error closing Arrow allocator: {}", e.toString());
        }
    }

    private static void closeQuietly(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception e) {
            LOG.warn("Error closing Flight client: {}", e.toString());
        }
    }

    @FunctionalInterface
    private interface FlightCall<T>
    {
        T call();
    }
}
