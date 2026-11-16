package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A GROUP BY (possibly empty = global aggregation) plus the aggregates computed on the server.
 * The rendered SQL always lists the grouping columns first, in this order: the server returns
 * GROUP BY columns before aggregates regardless of the SELECT order.
 */
public final class Aggregation
{
    private final List<ArrowFlightColumnHandle> groupingColumns;
    private final List<AggregateColumn> aggregates;
    private final boolean partial;

    public Aggregation(List<ArrowFlightColumnHandle> groupingColumns, List<AggregateColumn> aggregates)
    {
        this(groupingColumns, aggregates, false);
    }

    /**
     * {@code partial}: set by applyPartialAggregation (patched Trino). Every split returns, per group, Trino's
     * intermediate state of each aggregate (the output column types are the state types; AVG is
     * ROW(DOUBLE sum, BIGINT count)); Trino's FINAL aggregation merges them, so splits need not see whole groups.
     */
    @JsonCreator
    public Aggregation(
            @JsonProperty("groupingColumns") List<ArrowFlightColumnHandle> groupingColumns,
            @JsonProperty("aggregates") List<AggregateColumn> aggregates,
            @JsonProperty("partial") boolean partial)
    {
        this.groupingColumns = ImmutableList.copyOf(requireNonNull(groupingColumns, "groupingColumns is null"));
        this.aggregates = ImmutableList.copyOf(requireNonNull(aggregates, "aggregates is null"));
        this.partial = partial;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @JsonProperty
    public List<ArrowFlightColumnHandle> getGroupingColumns()
    {
        return groupingColumns;
    }

    @JsonProperty
    public List<AggregateColumn> getAggregates()
    {
        return aggregates;
    }

    /** The aggregate whose output column is {@code column}, if it is one. */
    public Optional<AggregateColumn> aggregateFor(ArrowFlightColumnHandle column)
    {
        return aggregates.stream().filter(a -> a.getOutput().equals(column)).findFirst();
    }

    /** Columns this aggregation exposes to Trino: grouping columns, then aggregate outputs. */
    public List<ArrowFlightColumnHandle> outputColumns()
    {
        ImmutableList.Builder<ArrowFlightColumnHandle> out = ImmutableList.builder();
        out.addAll(groupingColumns);
        aggregates.forEach(a -> out.add(a.getOutput()));
        return out.build();
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof Aggregation other
                && groupingColumns.equals(other.groupingColumns) && aggregates.equals(other.aggregates) && partial == other.partial;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(groupingColumns, aggregates, partial);
    }

    @Override
    public String toString()
    {
        return (partial ? "partial " : "") + "groupBy=" + groupingColumns + " aggs=" + aggregates;
    }
}
