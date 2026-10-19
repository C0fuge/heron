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

    @JsonCreator
    public Aggregation(
            @JsonProperty("groupingColumns") List<ArrowFlightColumnHandle> groupingColumns,
            @JsonProperty("aggregates") List<AggregateColumn> aggregates)
    {
        this.groupingColumns = ImmutableList.copyOf(requireNonNull(groupingColumns, "groupingColumns is null"));
        this.aggregates = ImmutableList.copyOf(requireNonNull(aggregates, "aggregates is null"));
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
                && groupingColumns.equals(other.groupingColumns) && aggregates.equals(other.aggregates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(groupingColumns, aggregates);
    }

    @Override
    public String toString()
    {
        return "groupBy=" + groupingColumns + " aggs=" + aggregates;
    }
}
