package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * {@code ORDER BY <orderBy> LIMIT <count>} run by the server on every query of the scan (each split, each
 * endpoint), so each returns its own top rows and Trino keeps its final TopN. Set by applyTopN.
 */
public final class TopN
{
    private final String orderBy;
    private final long count;

    @JsonCreator
    public TopN(@JsonProperty("orderBy") String orderBy, @JsonProperty("count") long count)
    {
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
        this.count = count;
    }

    /** Rendered sort keys, e.g. {@code l_extendedprice DESC NULLS LAST, l_orderkey ASC NULLS LAST}. */
    @JsonProperty
    public String getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty
    public long getCount()
    {
        return count;
    }

    public String toSql()
    {
        return " ORDER BY " + orderBy + " LIMIT " + count;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof TopN other && orderBy.equals(other.orderBy) && count == other.count;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(orderBy, count);
    }

    @Override
    public String toString()
    {
        return "topN=" + count + " order by " + orderBy;
    }
}
