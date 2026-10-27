package pro.surpin.trino.arrowflight;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import pro.surpin.arrowflight.common.FlightConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Catalog properties (file {@code etc/catalog/<name>.properties}). Mirrors the option
 * set exposed by the reference Spark connector in the arrow-flight-server repo.
 */
public class ArrowFlightConfig
{
    private String host;
    // Alternative to host: coordinators tried in order while the ones before cannot be reached.
    private List<String> hosts = List.of();
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
    // A data connection to a node that is gone fails after this long; the next location is tried.
    private long connectTimeoutMillis = 5_000;
    private long keepAliveMillis = 10_000;

    // Empty by default: the reference server parses SQL with regexes and expects bare
    // schema.table / column names. Set to "\"" only if your server accepts quoted identifiers.
    private String identifierQuote = "";

    // Pushdown / planning features. Each can also be switched per query with the session
    // property of the same name (see ArrowFlightSessionProperties).
    private boolean aggregationPushdownEnabled = true;
    private boolean limitPushdownEnabled = true;
    private boolean topNPushdownEnabled;
    private boolean expressionPushdownEnabled = true;
    private boolean statisticsEnabled = true;
    private long statisticsCacheTtlSeconds = 600;
    private int splitCount = 4;
    private long splitMinRows = 1_000_000;
    private String splitColumns = "";
    private int dynamicFilteringWaitMillis = 10_000;
    private int dynamicFilteringMaxValues = 1000;
    private int endpointReadParallelism = 1;
    private boolean aggregationSortedMergeEnabled = true;
    private boolean readAheadEnabled = true;
    private boolean endpointSplitsEnabled;
    // Off by default: every query's filter values differ, and the server keeps a prepared
    // statement per distinct SQL text forever (DuckDbAdapter.stmtCache), i.e. leaks memory.
    private boolean dynamicFilteringEnabled;

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

    public List<String> getHosts()
    {
        return hosts;
    }

    @Config("arrow-flight.hosts")
    @ConfigDescription("Comma-separated Arrow Flight SQL coordinators (host or host:port), tried in order "
            + "while the ones before cannot be reached; instead of arrow-flight.host")
    public ArrowFlightConfig setHosts(String hosts)
    {
        List<String> list = new ArrayList<>();
        if (hosts != null) {
            for (String entry : hosts.split(",")) {
                if (!entry.isBlank()) {
                    list.add(entry.trim());
                }
            }
        }
        this.hosts = List.copyOf(list);
        return this;
    }

    @AssertTrue(message = "Set exactly one of arrow-flight.host and arrow-flight.hosts")
    public boolean isCoordinatorConfigured()
    {
        boolean single = host != null && !host.isBlank();
        return single != !hosts.isEmpty();
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

    @Min(0)
    public long getConnectTimeoutMillis()
    {
        return connectTimeoutMillis;
    }

    @Config("arrow-flight.connect-timeout-millis")
    @ConfigDescription("Connect timeout of a data stream per endpoint location; on timeout or refusal before any data the next location is tried. 0 = Netty default (30 s)")
    public ArrowFlightConfig setConnectTimeoutMillis(long connectTimeoutMillis)
    {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    @Min(0)
    public long getKeepAliveMillis()
    {
        return keepAliveMillis;
    }

    @Config("arrow-flight.keepalive-millis")
    @ConfigDescription("Keepalive ping interval of data streams from servers that list backup locations; a node that does not answer within the connect timeout is taken as gone. 0 = no pings")
    public ArrowFlightConfig setKeepAliveMillis(long keepAliveMillis)
    {
        this.keepAliveMillis = keepAliveMillis;
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

    public boolean isAggregationPushdownEnabled()
    {
        return aggregationPushdownEnabled;
    }

    @Config("arrow-flight.aggregation-pushdown.enabled")
    @ConfigDescription("Run count/sum/min/max/avg + GROUP BY on the Flight server instead of shipping rows to Trino")
    public ArrowFlightConfig setAggregationPushdownEnabled(boolean enabled)
    {
        this.aggregationPushdownEnabled = enabled;
        return this;
    }

    public boolean isLimitPushdownEnabled()
    {
        return limitPushdownEnabled;
    }

    @Config("arrow-flight.limit-pushdown.enabled")
    @ConfigDescription("Stop reading (and cancel the server stream) once a LIMIT is satisfied")
    public ArrowFlightConfig setLimitPushdownEnabled(boolean enabled)
    {
        this.limitPushdownEnabled = enabled;
        return this;
    }

    public boolean isTopNPushdownEnabled()
    {
        return topNPushdownEnabled;
    }

    @Config("arrow-flight.topn-pushdown.enabled")
    @ConfigDescription("Send ORDER BY ... LIMIT n to the server so each query returns only its own top n rows")
    public ArrowFlightConfig setTopNPushdownEnabled(boolean enabled)
    {
        this.topNPushdownEnabled = enabled;
        return this;
    }

    public boolean isExpressionPushdownEnabled()
    {
        return expressionPushdownEnabled;
    }

    @Config("arrow-flight.expression-pushdown.enabled")
    @ConfigDescription("Push LIKE / NOT LIKE predicates to the Flight server")
    public ArrowFlightConfig setExpressionPushdownEnabled(boolean enabled)
    {
        this.expressionPushdownEnabled = enabled;
        return this;
    }

    public boolean isStatisticsEnabled()
    {
        return statisticsEnabled;
    }

    @Config("arrow-flight.statistics.enabled")
    @ConfigDescription("Report table row counts (count(*) from Parquet footers) to the Trino cost-based optimizer")
    public ArrowFlightConfig setStatisticsEnabled(boolean enabled)
    {
        this.statisticsEnabled = enabled;
        return this;
    }

    @Min(0)
    public long getStatisticsCacheTtlSeconds()
    {
        return statisticsCacheTtlSeconds;
    }

    @Config("arrow-flight.statistics.cache-ttl-seconds")
    @ConfigDescription("How long a table row count is reused before asking the server again")
    public ArrowFlightConfig setStatisticsCacheTtlSeconds(long seconds)
    {
        this.statisticsCacheTtlSeconds = seconds;
        return this;
    }

    @Min(1)
    public int getSplitCount()
    {
        return splitCount;
    }

    @Config("arrow-flight.split-count")
    @ConfigDescription("Parallel range splits per large table scan (1 = one stream per table)")
    public ArrowFlightConfig setSplitCount(int splitCount)
    {
        this.splitCount = splitCount;
        return this;
    }

    @Min(1)
    public long getSplitMinRows()
    {
        return splitMinRows;
    }

    @Config("arrow-flight.split-min-rows")
    @ConfigDescription("Do not make range splits smaller than this many table rows")
    public ArrowFlightConfig setSplitMinRows(long splitMinRows)
    {
        this.splitMinRows = splitMinRows;
        return this;
    }

    public String getSplitColumns()
    {
        return splitColumns;
    }

    @Config("arrow-flight.split-columns")
    @ConfigDescription("Range-split column per table, e.g. 'tpch.lineitem=l_orderkey,shop.events=event_id'; default: first integer column")
    public ArrowFlightConfig setSplitColumns(String splitColumns)
    {
        this.splitColumns = splitColumns == null ? "" : splitColumns;
        return this;
    }

    public boolean isDynamicFilteringEnabled()
    {
        return dynamicFilteringEnabled;
    }

    @Config("arrow-flight.dynamic-filtering.enabled")
    @ConfigDescription("Add join dynamic filters to the server SQL (makes every query text unique; see README on server memory)")
    public ArrowFlightConfig setDynamicFilteringEnabled(boolean enabled)
    {
        this.dynamicFilteringEnabled = enabled;
        return this;
    }

    @Min(0)
    public int getDynamicFilteringWaitMillis()
    {
        return dynamicFilteringWaitMillis;
    }

    @Config("arrow-flight.dynamic-filtering.wait-timeout-ms")
    @ConfigDescription("How long a scan waits for join dynamic filters before starting (0 = do not wait)")
    public ArrowFlightConfig setDynamicFilteringWaitMillis(int millis)
    {
        this.dynamicFilteringWaitMillis = millis;
        return this;
    }

    @Min(1)
    public int getDynamicFilteringMaxValues()
    {
        return dynamicFilteringMaxValues;
    }

    @Config("arrow-flight.dynamic-filtering.max-values")
    @ConfigDescription("Largest IN list a join dynamic filter becomes in the server SQL; more values become their [min, max] range")
    public ArrowFlightConfig setDynamicFilteringMaxValues(int maxValues)
    {
        this.dynamicFilteringMaxValues = maxValues;
        return this;
    }

    @Min(1)
    public int getEndpointReadParallelism()
    {
        return endpointReadParallelism;
    }

    @Config("arrow-flight.endpoint-read-parallelism")
    @ConfigDescription("Endpoints (one per Flight node) of one split read at the same time (1 = one after another)")
    public ArrowFlightConfig setEndpointReadParallelism(int endpointReadParallelism)
    {
        this.endpointReadParallelism = endpointReadParallelism;
        return this;
    }

    public boolean isAggregationSortedMergeEnabled()
    {
        return aggregationSortedMergeEnabled;
    }

    @Config("arrow-flight.aggregation-sorted-merge.enabled")
    @ConfigDescription("Ask the server for grouped aggregation results sorted by the GROUP BY columns, so the groups of "
            + "several Flight nodes are merged as sorted streams instead of in one in-memory hash table")
    public ArrowFlightConfig setAggregationSortedMergeEnabled(boolean aggregationSortedMergeEnabled)
    {
        this.aggregationSortedMergeEnabled = aggregationSortedMergeEnabled;
        return this;
    }

    public boolean isReadAheadEnabled()
    {
        return readAheadEnabled;
    }

    @Config("arrow-flight.read-ahead.enabled")
    @ConfigDescription("Read and decode the next Arrow batch of a scan on a background thread while Trino processes "
            + "the current page")
    public ArrowFlightConfig setReadAheadEnabled(boolean readAheadEnabled)
    {
        this.readAheadEnabled = readAheadEnabled;
        return this;
    }

    public boolean isEndpointSplitsEnabled()
    {
        return endpointSplitsEnabled;
    }

    @Config("arrow-flight.endpoint-splits.enabled")
    @ConfigDescription("Plan a plain scan on the coordinator and make each Flight endpoint (one per node, or "
            + "endpointsPerServer per server) its own split, instead of key-range splits that each query the server")
    public ArrowFlightConfig setEndpointSplitsEnabled(boolean endpointSplitsEnabled)
    {
        this.endpointSplitsEnabled = endpointSplitsEnabled;
        return this;
    }

    /** Translate the Trino/airlift catalog properties into the engine-neutral common config. */
    public FlightConfig toFlightConfig()
    {
        String firstHost = host;
        int firstPort = port;
        List<String> alternates = new ArrayList<>();
        if (!hosts.isEmpty()) {
            // arrow-flight.port is the default for every entry without its own port
            FlightConfig.Coordinator first = FlightConfig.parseCoordinator(hosts.get(0), port);
            firstHost = first.host();
            firstPort = first.port();
            for (String entry : hosts.subList(1, hosts.size())) {
                FlightConfig.Coordinator c = FlightConfig.parseCoordinator(entry, port);
                alternates.add((c.host().indexOf(':') >= 0 ? "[" + c.host() + "]" : c.host()) + ":" + c.port());
            }
        }
        return FlightConfig.builder(firstHost)
                .port(firstPort)
                .alternateHosts(alternates)
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
                .connectTimeoutMillis(connectTimeoutMillis)
                .keepAliveMillis(keepAliveMillis)
                .build();
    }
}
