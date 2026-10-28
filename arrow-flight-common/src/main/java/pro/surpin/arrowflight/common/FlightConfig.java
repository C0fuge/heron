package pro.surpin.arrowflight.common;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Plain connection settings for {@link FlightSqlSession}. Engine modules (Trino, StarRocks)
 * translate their own catalog/property objects into this POJO so the session code has no
 * dependency on any query engine.
 */
public final class FlightConfig
{
    /** A coordinator to connect to; {@code host} without brackets. */
    public record Coordinator(String host, int port) {}

    private final String host;
    private final int port;
    private final List<Coordinator> coordinators;
    private final boolean tlsEnabled;
    private final boolean tlsVerifyServer;
    private final Optional<String> tlsTruststorePath;
    private final Optional<String> tlsTruststorePassword;
    private final Optional<String> user;
    private final Optional<String> password;
    private final Optional<String> bearerToken;
    private final Optional<String> defaultSchema;
    private final Optional<String> routingTag;
    private final Optional<String> routingQueue;
    private final int maxRetries;
    private final long retryBackoffMillis;
    private final long requestTimeoutMillis;
    private final long connectTimeoutMillis;
    private final long keepAliveMillis;
    private final ResultCompression resultCompression;

    private FlightConfig(Builder b)
    {
        this.host = requireNonNull(b.host, "host is null");
        this.port = b.port;
        this.coordinators = coordinators(b.host, b.port, b.alternateHosts);
        this.tlsEnabled = b.tlsEnabled;
        this.tlsVerifyServer = b.tlsVerifyServer;
        this.tlsTruststorePath = Optional.ofNullable(b.tlsTruststorePath);
        this.tlsTruststorePassword = Optional.ofNullable(b.tlsTruststorePassword);
        this.user = Optional.ofNullable(b.user);
        this.password = Optional.ofNullable(b.password);
        this.bearerToken = Optional.ofNullable(b.bearerToken);
        this.defaultSchema = Optional.ofNullable(b.defaultSchema);
        this.routingTag = Optional.ofNullable(b.routingTag);
        this.routingQueue = Optional.ofNullable(b.routingQueue);
        this.maxRetries = b.maxRetries;
        this.retryBackoffMillis = b.retryBackoffMillis;
        this.requestTimeoutMillis = b.requestTimeoutMillis;
        if (b.connectTimeoutMillis < 0 || b.connectTimeoutMillis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("connectTimeoutMillis out of range: " + b.connectTimeoutMillis);
        }
        if (b.keepAliveMillis < 0) {
            throw new IllegalArgumentException("keepAliveMillis must not be negative: " + b.keepAliveMillis);
        }
        this.connectTimeoutMillis = b.connectTimeoutMillis;
        this.keepAliveMillis = b.keepAliveMillis;
        this.resultCompression = requireNonNull(b.resultCompression, "resultCompression is null");
    }

    public static Builder builder(String host)
    {
        return new Builder(host);
    }

    /**
     * Parses {@code host}, {@code host:port} or {@code [ipv6]:port}; the port defaults to
     * {@code defaultPort}. Entries naming the same host and port twice are kept once.
     */
    private static List<Coordinator> coordinators(String host, int defaultPort, List<String> alternateHosts)
    {
        LinkedHashSet<Coordinator> result = new LinkedHashSet<>();
        result.add(new Coordinator(host, defaultPort));
        for (String entry : alternateHosts) {
            result.add(parseCoordinator(entry, defaultPort));
        }
        return List.copyOf(result);
    }

    public static Coordinator parseCoordinator(String entry, int defaultPort)
    {
        String value = entry.trim();
        String host = value;
        int port = defaultPort;
        if (value.startsWith("[")) {
            int end = value.indexOf(']');
            if (end < 0) {
                throw new IllegalArgumentException("Malformed Flight host: " + entry);
            }
            host = value.substring(1, end);
            String rest = value.substring(end + 1);
            if (!rest.isEmpty()) {
                if (!rest.startsWith(":")) {
                    throw new IllegalArgumentException("Malformed Flight host: " + entry);
                }
                port = parsePort(rest.substring(1), entry);
            }
        }
        else if (value.indexOf(':') >= 0 && value.indexOf(':') == value.lastIndexOf(':')) {
            host = value.substring(0, value.indexOf(':'));
            port = parsePort(value.substring(value.indexOf(':') + 1), entry);
        }
        if (host.isEmpty()) {
            throw new IllegalArgumentException("Malformed Flight host: " + entry);
        }
        return new Coordinator(host, port);
    }

    private static int parsePort(String value, String entry)
    {
        try {
            int port = Integer.parseInt(value);
            if (port < 1 || port > 65535) {
                throw new NumberFormatException();
            }
            return port;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Malformed port in Flight host: " + entry);
        }
    }

    /** The first coordinator's host. */
    public String getHost() { return host; }
    public int getPort() { return port; }
    /**
     * Coordinators in the order they are tried: the configured host first, then the alternates.
     * The session moves to the next one only when the current one cannot be reached.
     */
    public List<Coordinator> getCoordinators() { return coordinators; }
    public boolean isTlsEnabled() { return tlsEnabled; }
    public boolean isTlsVerifyServer() { return tlsVerifyServer; }
    public Optional<String> getTlsTruststorePath() { return tlsTruststorePath; }
    public Optional<String> getTlsTruststorePassword() { return tlsTruststorePassword; }
    public Optional<String> getUser() { return user; }
    public Optional<String> getPassword() { return password; }
    public Optional<String> getBearerToken() { return bearerToken; }
    public Optional<String> getDefaultSchema() { return defaultSchema; }
    public Optional<String> getRoutingTag() { return routingTag; }
    public Optional<String> getRoutingQueue() { return routingQueue; }
    public int getMaxRetries() { return maxRetries; }
    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public long getRequestTimeoutMillis() { return requestTimeoutMillis; }
    /** TCP connect timeout of a data-stream connection, per location tried; 0 = Netty's default (30 s). */
    public long getConnectTimeoutMillis() { return connectTimeoutMillis; }
    /**
     * Keepalive ping interval of data-stream connections to a server that lists backup locations;
     * a node that does not answer a ping within the connect timeout is taken as gone. 0 = no pings.
     */
    public long getKeepAliveMillis() { return keepAliveMillis; }
    /** Codec the data streams are asked to be compressed with; NONE = not asked (the server's default). */
    public ResultCompression getResultCompression() { return resultCompression; }

    public static final class Builder
    {
        private final String host;
        private int port = 32010;
        private List<String> alternateHosts = List.of();
        private boolean tlsEnabled;
        private boolean tlsVerifyServer = true;
        private String tlsTruststorePath;
        private String tlsTruststorePassword;
        private String user;
        private String password;
        private String bearerToken;
        private String defaultSchema;
        private String routingTag;
        private String routingQueue;
        private int maxRetries = 3;
        private long retryBackoffMillis = 1000;
        private long requestTimeoutMillis;
        private long connectTimeoutMillis = 5_000;
        private long keepAliveMillis = 10_000;
        private ResultCompression resultCompression = ResultCompression.NONE;

        private Builder(String host)
        {
            this.host = host;
        }

        public Builder port(int port) { this.port = port; return this; }
        /** Further coordinators ({@code host} or {@code host:port}) tried when the ones before cannot be reached. */
        public Builder alternateHosts(List<String> v)
        {
            List<String> hosts = new ArrayList<>();
            for (String entry : v) {
                if (entry != null && !entry.isBlank()) {
                    hosts.add(entry);
                }
            }
            this.alternateHosts = List.copyOf(hosts);
            return this;
        }
        public Builder tlsEnabled(boolean v) { this.tlsEnabled = v; return this; }
        public Builder tlsVerifyServer(boolean v) { this.tlsVerifyServer = v; return this; }
        public Builder tlsTruststorePath(String v) { this.tlsTruststorePath = emptyToNull(v); return this; }
        public Builder tlsTruststorePassword(String v) { this.tlsTruststorePassword = emptyToNull(v); return this; }
        public Builder user(String v) { this.user = emptyToNull(v); return this; }
        public Builder password(String v) { this.password = emptyToNull(v); return this; }
        public Builder bearerToken(String v) { this.bearerToken = emptyToNull(v); return this; }
        public Builder defaultSchema(String v) { this.defaultSchema = emptyToNull(v); return this; }
        public Builder routingTag(String v) { this.routingTag = emptyToNull(v); return this; }
        public Builder routingQueue(String v) { this.routingQueue = emptyToNull(v); return this; }
        public Builder maxRetries(int v) { this.maxRetries = v; return this; }
        public Builder retryBackoffMillis(long v) { this.retryBackoffMillis = v; return this; }
        public Builder requestTimeoutMillis(long v) { this.requestTimeoutMillis = v; return this; }
        public Builder connectTimeoutMillis(long v) { this.connectTimeoutMillis = v; return this; }
        public Builder keepAliveMillis(long v) { this.keepAliveMillis = v; return this; }
        public Builder resultCompression(ResultCompression v) { this.resultCompression = v; return this; }

        public FlightConfig build()
        {
            return new FlightConfig(this);
        }

        private static String emptyToNull(String v)
        {
            return (v == null || v.isEmpty()) ? null : v;
        }
    }
}
