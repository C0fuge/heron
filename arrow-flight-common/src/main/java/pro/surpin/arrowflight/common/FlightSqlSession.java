package pro.surpin.arrowflight.common;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
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
import java.util.Deque;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
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
 *
 * <p>Every data stream gets a connection to itself. gRPC keeps the bytes of all streams of one
 * HTTP/2 connection in one chain of buffers; with many parallel streams, some of them read
 * slowly (a join waiting for its other side), that chain grows deep enough to end the stream
 * with "Failed to read message" (a StackOverflowError when the chain is released).
 */
public class FlightSqlSession
        implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger(FlightSqlSession.class);
    private static final int MAX_IDLE_STREAM_CLIENTS = 32;
    // A connection idle for longer may have been dropped by a firewall without notice.
    private static final long MAX_STREAM_CLIENT_IDLE_NANOS = TimeUnit.SECONDS.toNanos(60);

    private record IdleClient(FlightClient client, long sinceNanos) {}

    private final FlightConfig config;
    private final BufferAllocator allocator;

    // Shared between the client builder (intercept) and the handshake so the bearer token
    // the server returns for BasicAuth can be captured and reused on later calls.
    private final ClientIncomingAuthHeaderMiddleware.Factory bearerFactory =
            new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

    // Connections waiting for their next data stream, by location; most recently used first.
    private final ConcurrentMap<String, Deque<IdleClient>> idleStreamClients = new ConcurrentHashMap<>();
    // Clients replaced by reconnect(); kept open until close() so calls still running on them are not cut.
    private final List<FlightClient> retiredClients = new ArrayList<>();

    private volatile boolean connected;
    private volatile boolean closed;
    // Bumped by reconnect(): stream connections opened before it are not reused.
    private volatile int generation;
    private volatile FlightClient coordinatorClient;
    private volatile FlightSqlClient sqlClient;
    private volatile CredentialCallOption authOption;

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

        FlightClient client = buildClient(location, config.getPassword().isPresent());
        try {
            this.authOption = authenticate(client);
        }
        catch (RuntimeException e) {
            closeQuietly(client);
            throw e;
        }
        this.coordinatorClient = client;
        this.sqlClient = new FlightSqlClient(client);
        this.connected = true;
    }

    /**
     * Drops the current connection so the next call builds a fresh one. After the server
     * restarts, gRPC keeps failing an existing channel fast (UNAVAILABLE) for up to its
     * reconnect backoff (~2 min); a new channel connects immediately. The old coordinator client
     * stays open until {@link #close()} because other calls may still be running on it.
     */
    private synchronized void reconnect()
    {
        if (!connected) {
            return;
        }
        connected = false;
        if (coordinatorClient != null) {
            retiredClients.add(coordinatorClient);
        }
        generation++;
        closeIdleStreamClients();
        LOG.info("Reconnecting to Flight server {}:{}", config.getHost(), config.getPort());
    }

    private static boolean isConnectionLost(Throwable e)
    {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof FlightRuntimeException f) {
                FlightStatusCode code = f.status().code();
                return code == FlightStatusCode.UNAVAILABLE || code == FlightStatusCode.UNAUTHENTICATED;
            }
        }
        return false;
    }

    private static boolean isFinal(Throwable e)
    {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof FlightRuntimeException f) {
                FlightStatusCode code = f.status().code();
                return code == FlightStatusCode.INVALID_ARGUMENT || code == FlightStatusCode.NOT_FOUND
                        || code == FlightStatusCode.UNIMPLEMENTED || code == FlightStatusCode.UNAUTHORIZED
                        || code == FlightStatusCode.ALREADY_EXISTS;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------ metadata

    /** Schema names via Flight SQL {@code CommandGetDbSchemas}; falls back to the configured default. */
    public List<String> listSchemas()
    {
        List<String> schemas = new ArrayList<>();
        try {
            schemas.addAll(retry(() -> {
                List<String> names = new ArrayList<>();
                try (FlightStream stream = openInfoStream(sqlClient.getSchemas(null, null, callOptions()))) {
                    readStringColumn(stream, "db_schema_name", names);
                }
                catch (FlightRuntimeException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return names;
            }, "getSchemas"));
        }
        catch (RuntimeException e) {
            // A server without CommandGetDbSchemas still works through the default schema; an
            // unreachable one must fail loudly instead of looking like "schema does not exist".
            if (isConnectionLost(e)) {
                throw e;
            }
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
        try {
            return retry(() -> {
                List<String> tables = new ArrayList<>();
                try (FlightStream stream = openInfoStream(
                        sqlClient.getTables(null, schema, null, null, false, callOptions()))) {
                    readStringColumn(stream, "table_name", tables);
                }
                catch (FlightRuntimeException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return List.copyOf(tables);
            }, "getTables");
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Failed to list tables in schema '" + schema + "': " + e.getMessage(), e);
        }
    }

    /** Result schema of a statement, without reserving stream tickets. */
    public Schema resultSchema(String sql)
    {
        return retry(() -> sqlClient.getExecuteSchema(sql, callOptions()).getSchema(), "getExecuteSchema");
    }

    /** Submits the statement and returns the endpoints the data can be pulled from. */
    public FlightInfo getFlightInfo(String sql)
    {
        return retry(() -> sqlClient.execute(sql, callOptions()), "execute");
    }

    // ------------------------------------------------------------------ data

    /**
     * Opens the Arrow stream for a single endpoint ticket on a connection of its own (reused
     * once the stream is closed). The coordinator-issued credential is forwarded on the
     * getStream call.
     */
    public FlightDataStream openStream(byte[] ticket, List<String> locationUris)
    {
        ensureConnected();
        Ticket flightTicket = new Ticket(requireNonNull(ticket, "ticket is null"));

        String uri = streamLocation(locationUris);
        int leasedGeneration = generation;
        FlightClient client = leaseStreamClient(uri);
        FlightStream stream;
        try {
            stream = client.getStream(flightTicket, callOptions());
        }
        catch (RuntimeException e) {
            closeQuietly(client);
            throw e;
        }
        return new FlightDataStream(
                stream,
                e -> {
                    if (isConnectionLost(e)) {
                        reconnect();
                    }
                },
                reusable -> releaseStreamClient(uri, client, reusable && leasedGeneration == generation));
    }

    /** The coordinator, unless every advertised location is another node. */
    private String streamLocation(List<String> locationUris)
    {
        if (!locationUris.isEmpty() && locationUris.stream().noneMatch(this::isCoordinator)) {
            return locationUris.get(0);
        }
        return (config.isTlsEnabled() ? "grpc+tls://" : "grpc+tcp://") + config.getHost() + ":" + config.getPort();
    }

    private FlightClient leaseStreamClient(String uri)
    {
        Deque<IdleClient> idle = idleStreamClients.get(uri);
        if (idle != null) {
            IdleClient candidate;
            while ((candidate = idle.pollFirst()) != null) {
                if (System.nanoTime() - candidate.sinceNanos() < MAX_STREAM_CLIENT_IDLE_NANOS) {
                    return candidate.client();
                }
                closeQuietly(candidate.client());
            }
        }
        return buildClient(location(uri), false);
    }

    private void releaseStreamClient(String uri, FlightClient client, boolean reusable)
    {
        Deque<IdleClient> idle = idleStreamClients.computeIfAbsent(uri, key -> new ConcurrentLinkedDeque<>());
        if (!reusable || closed || idle.size() >= MAX_IDLE_STREAM_CLIENTS) {
            closeQuietly(client);
            return;
        }
        idle.addFirst(new IdleClient(client, System.nanoTime()));
        // The least recently used connections sit at the tail: drop the ones nobody came back for.
        IdleClient oldest;
        while ((oldest = idle.peekLast()) != null
                && System.nanoTime() - oldest.sinceNanos() >= MAX_STREAM_CLIENT_IDLE_NANOS
                && idle.removeLastOccurrence(oldest)) {
            closeQuietly(oldest.client());
        }
        if (closed) {
            closeIdleStreamClients();
        }
    }

    private void closeIdleStreamClients()
    {
        for (Deque<IdleClient> idle : idleStreamClients.values()) {
            IdleClient candidate;
            while ((candidate = idle.pollFirst()) != null) {
                closeQuietly(candidate.client());
            }
        }
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

    // This connector merges the parts each Flight node computes from its own files (parallel,
    // close to the data); the server's default for other clients is one complete answer.
    private static final HeaderCallOption PARTIAL_RESULTS;

    static {
        FlightCallHeaders headers = new FlightCallHeaders();
        headers.insert("x-flight-result-mode", "partial");
        PARTIAL_RESULTS = new HeaderCallOption(headers);
    }

    private CallOption[] callOptions()
    {
        List<CallOption> options = new ArrayList<>();
        options.add(authOption);
        options.add(PARTIAL_RESULTS);
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
                ensureConnected();
                return call.call();
            }
            catch (RuntimeException e) {
                last = e;
                LOG.warn("Flight call '{}' failed (attempt {}/{}): {}", name, attempt + 1, maxRetries + 1, e.getMessage());
                if (isConnectionLost(e)) {
                    reconnect();
                }
                else if (isFinal(e)) {
                    // The server rejected the request itself; asking again gives the same answer.
                    break;
                }
            }
        }
        throw new RuntimeException("Flight call '" + name + "' failed after " + (maxRetries + 1) + " attempts: "
                + (last == null ? "unknown error" : last.getMessage()), last);
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
        closed = true;
        closeIdleStreamClients();
        synchronized (this) {
            retiredClients.forEach(FlightSqlSession::closeQuietly);
            retiredClients.clear();
        }
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
