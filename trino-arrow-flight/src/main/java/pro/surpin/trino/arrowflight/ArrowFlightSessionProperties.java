package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

/**
 * Per-query switches for the pushdowns (defaults come from the catalog properties), e.g.
 * {@code SET SESSION flight.aggregation_pushdown_enabled = false} to compare plans or results.
 */
public final class ArrowFlightSessionProperties
{
    public static final String AGGREGATION_PUSHDOWN_ENABLED = "aggregation_pushdown_enabled";
    public static final String LIMIT_PUSHDOWN_ENABLED = "limit_pushdown_enabled";
    public static final String TOPN_PUSHDOWN_ENABLED = "topn_pushdown_enabled";
    public static final String EXPRESSION_PUSHDOWN_ENABLED = "expression_pushdown_enabled";
    public static final String STATISTICS_ENABLED = "statistics_enabled";
    public static final String SPLIT_COUNT = "split_count";
    public static final String DYNAMIC_FILTERING_ENABLED = "dynamic_filtering_enabled";
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT_MS = "dynamic_filtering_wait_timeout_ms";
    public static final String DYNAMIC_FILTERING_MAX_VALUES = "dynamic_filtering_max_values";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public ArrowFlightSessionProperties(ArrowFlightConfig config)
    {
        properties = ImmutableList.of(
                booleanProperty(AGGREGATION_PUSHDOWN_ENABLED,
                        "Run aggregations on the Flight server", config.isAggregationPushdownEnabled(), false),
                booleanProperty(LIMIT_PUSHDOWN_ENABLED,
                        "Stop reading once a LIMIT is satisfied", config.isLimitPushdownEnabled(), false),
                booleanProperty(TOPN_PUSHDOWN_ENABLED,
                        "Send ORDER BY ... LIMIT n to the Flight server", config.isTopNPushdownEnabled(), false),
                booleanProperty(EXPRESSION_PUSHDOWN_ENABLED,
                        "Push LIKE / NOT LIKE to the Flight server", config.isExpressionPushdownEnabled(), false),
                booleanProperty(STATISTICS_ENABLED,
                        "Report table row counts to the optimizer", config.isStatisticsEnabled(), false),
                integerProperty(SPLIT_COUNT,
                        "Parallel range splits per large table scan", config.getSplitCount(), false),
                booleanProperty(DYNAMIC_FILTERING_ENABLED,
                        "Add join dynamic filters to the server SQL", config.isDynamicFilteringEnabled(), false),
                integerProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT_MS,
                        "Milliseconds a scan waits for join dynamic filters", config.getDynamicFilteringWaitMillis(), false),
                integerProperty(DYNAMIC_FILTERING_MAX_VALUES,
                        "Largest IN list a join dynamic filter becomes in the server SQL", config.getDynamicFilteringMaxValues(), false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static boolean isAggregationPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isLimitPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(LIMIT_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isTopNPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(TOPN_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isExpressionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(EXPRESSION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static int getSplitCount(ConnectorSession session)
    {
        return Math.max(1, session.getProperty(SPLIT_COUNT, Integer.class));
    }

    /** Wait time for dynamic filters; 0 when dynamic filtering is disabled. */
    public static int getDynamicFilteringWaitMillis(ConnectorSession session)
    {
        if (!session.getProperty(DYNAMIC_FILTERING_ENABLED, Boolean.class)) {
            return 0;
        }
        return Math.max(0, session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT_MS, Integer.class));
    }

    public static int getDynamicFilteringMaxValues(ConnectorSession session)
    {
        return Math.max(1, session.getProperty(DYNAMIC_FILTERING_MAX_VALUES, Integer.class));
    }

    public static boolean isDynamicFilteringEnabled(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_ENABLED, Boolean.class);
    }

    public static boolean isStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(STATISTICS_ENABLED, Boolean.class);
    }
}
