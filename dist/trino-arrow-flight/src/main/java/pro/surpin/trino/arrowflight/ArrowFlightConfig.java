package pro.surpin.trino.arrowflight;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import pro.surpin.arrowflight.common.FlightConfig;

import java.util.Optional;

/**
 * Catalog properties (file {@code etc/catalog/<name>.properties}). Mirrors the option
 * set exposed by the reference Spark connector in the arrow-flight-server repo.
 */
public class ArrowFlightConfig
{
    private String host;
    private int port = 32010;

    private boolean tlsEnabled;
    private boolean tlsVerifyServer = true;
    private Optional<String> tlsTruststorePath = Optional.empty();
    private Optional<String> tlsTruststorePassword = Optional.empty();

    private Optional<String> user = Optional.empty();
    private Optional<String> password = Optional.empty();
    private Optional<String> bearerToken = Optional.empty();

    // Sent as Flight call headers on the handshake, exactly like the Spark client.
    private Optional<String> defaultSchema = Optional.empty();
    private Optional<String> routingTag = Optional.empty();
    private Optional<String> routingQueue = Optional.empty();

    private int maxRetries = 3;
    private long retryBackoffMillis = 1000;
    private long requestTimeoutMillis = 0;

    // Empty by default: the reference server parses SQL with regexes and expects bare
    // schema.table / column names. Set to "\"" only if your server accepts quoted identifiers.
    private String identifierQuote = "";

    @NotNull
    public String getHost()
    {
        return host;
    }

    @Config("arrow-flight.host")
    @ConfigDescription("Host of the Arrow Flight SQL coordinator")
    public ArrowFlightConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Min(1)
    public int getPort()
    {
        return port;
    }

    @Config("arrow-flight.port")
    public ArrowFlightConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    public boolean isTlsEnabled()
    {
        return tlsEnabled;
    }

    @Config("arrow-flight.tls.enabled")
    public ArrowFlightConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public boolean isTlsVerifyServer()
    {
        return tlsVerifyServer;
    }

    @Config("arrow-flight.tls.verify-server")
    public ArrowFlightConfig setTlsVerifyServer(boolean tlsVerifyServer)
    {
        this.tlsVerifyServer = tlsVerifyServer;
        return this;
    }

    public Optional<String> getTlsTruststorePath()
    {
        return tlsTruststorePath;
    }

    @Config("arrow-flight.tls.truststore-path")
    @ConfigDescription("PEM or JKS file with trusted server certificates")
    public ArrowFlightConfig setTlsTruststorePath(String tlsTruststorePath)
    {
        this.tlsTruststorePath = Optional.ofNullable(tlsTruststorePath);
        return this;
    }

    public Optional<String> getTlsTruststorePassword()
    {
        return tlsTruststorePassword;
    }

    @Config("arrow-flight.tls.truststore-password")
    @ConfigSecuritySensitive
    public ArrowFlightConfig setTlsTruststorePassword(String tlsTruststorePassword)
    {
        this.tlsTruststorePassword = Optional.ofNullable(tlsTruststorePassword);
        return this;
    }

    public Optional<String> getUser()
    {
        return user;
    }

    @Config("arrow-flight.user")
    public ArrowFlightConfig setUser(String user)
    {
        this.user = Optional.ofNullable(user);
        return this;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    @Config("arrow-flight.password")
    @ConfigSecuritySensitive
    public ArrowFlightConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }

    public Optional<String> getBearerToken()
    {
        return bearerToken;
    }

    @Config("arrow-flight.bearer-token")
    @ConfigSecuritySensitive
    public ArrowFlightConfig setBearerToken(String bearerToken)
    {
        this.bearerToken = Optional.ofNullable(bearerToken);
        return this;
    }

    public Optional<String> getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("arrow-flight.default-schema")
    @ConfigDescription("Schema used when a query does not qualify a table, and fallback for listSchemas")
    public ArrowFlightConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = Optional.ofNullable(defaultSchema);
        return this;
    }

    public Optional<String> getRoutingTag()
    {
        return routingTag;
    }

    @Config("arrow-flight.routing-tag")
    public ArrowFlightConfig setRoutingTag(String routingTag)
    {
        this.routingTag = Optional.ofNullable(routingTag);
        return this;
    }

    public Optional<String> getRoutingQueue()
    {
        return routingQueue;
    }

    @Config("arrow-flight.routing-queue")
    public ArrowFlightConfig setRoutingQueue(String routingQueue)
    {
        this.routingQueue = Optional.ofNullable(routingQueue);
        return this;
    }

    @Min(0)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("arrow-flight.max-retries")
    public ArrowFlightConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @Min(0)
    public long getRetryBackoffMillis()
    {
        return retryBackoffMillis;
    }

    @Config("arrow-flight.retry-backoff-millis")
    public ArrowFlightConfig setRetryBackoffMillis(long retryBackoffMillis)
    {
        this.retryBackoffMillis = retryBackoffMillis;
        return this;
    }

    @Min(0)
    public long getRequestTimeoutMillis()
    {
        return requestTimeoutMillis;
    }

    @Config("arrow-flight.request-timeout-millis")
    @ConfigDescription("Per-call deadline; 0 disables it")
    public ArrowFlightConfig setRequestTimeoutMillis(long requestTimeoutMillis)
    {
        this.requestTimeoutMillis = requestTimeoutMillis;
        return this;
    }

    public String getIdentifierQuote()
    {
        return identifierQuote;
    }

    @Config("arrow-flight.identifier-quote")
    @ConfigDescription("Quote character wrapped around schema/table/column names in generated SQL; empty = none")
    public ArrowFlightConfig setIdentifierQuote(String identifierQuote)
    {
        this.identifierQuote = identifierQuote == null ? "" : identifierQuote;
        return this;
    }

    /** Translate the Trino/airlift catalog properties into the engine-neutral common config. */
    public FlightConfig toFlightConfig()
    {
        return FlightConfig.builder(host)
                .port(port)
                .tlsEnabled(tlsEnabled)
                .tlsVerifyServer(tlsVerifyServer)
                .tlsTruststorePath(tlsTruststorePath.orElse(null))
                .tlsTruststorePassword(tlsTruststorePassword.orElse(null))
                .user(user.orElse(null))
                .password(password.orElse(null))
                .bearerToken(bearerToken.orElse(null))
                .defaultSchema(defaultSchema.orElse(null))
                .routingTag(routingTag.orElse(null))
                .routingQueue(routingQueue.orElse(null))
                .maxRetries(maxRetries)
                .retryBackoffMillis(retryBackoffMillis)
                .requestTimeoutMillis(requestTimeoutMillis)
                .build();
    }
}
