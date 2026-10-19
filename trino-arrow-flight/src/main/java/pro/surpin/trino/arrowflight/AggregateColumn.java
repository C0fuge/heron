package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * One aggregate pushed to the Flight server. {@link #output} is the synthetic column Trino reads;
 * its type is exactly the Trino result type of the aggregate (count -> bigint, sum(int) -> bigint,
 * sum(decimal(p,s)) -> decimal(38,s), avg(decimal(p,s)) -> decimal(p,s), ...).
 * <p>
 * The server has no AVG, so AVG is sent as {@code sum(x)} + {@code count(x)} under the names
 * {@link #sumName()} / {@link #countName()} and divided in the page source.
 */
public final class AggregateColumn
{
    public enum Function
    {
        COUNT_STAR, COUNT, SUM, MIN, MAX, AVG
    }

    private final Function function;
    private final Optional<ArrowFlightColumnHandle> input;
    private final ArrowFlightColumnHandle output;

    @JsonCreator
    public AggregateColumn(
            @JsonProperty("function") Function function,
            @JsonProperty("input") Optional<ArrowFlightColumnHandle> input,
            @JsonProperty("output") ArrowFlightColumnHandle output)
    {
        this.function = requireNonNull(function, "function is null");
        this.input = requireNonNull(input, "input is null");
        this.output = requireNonNull(output, "output is null");
    }

    @JsonProperty
    public Function getFunction()
    {
        return function;
    }

    @JsonProperty
    public Optional<ArrowFlightColumnHandle> getInput()
    {
        return input;
    }

    @JsonProperty
    public ArrowFlightColumnHandle getOutput()
    {
        return output;
    }

    public String sumName()
    {
        return output.getColumnName() + "_s";
    }

    public String countName()
    {
        return output.getColumnName() + "_c";
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof AggregateColumn other
                && function == other.function && input.equals(other.input) && output.equals(other.output);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(function, input, output);
    }

    @Override
    public String toString()
    {
        return function + input.map(c -> "(" + c.getColumnName() + ")").orElse("(*)") + "->" + output.getColumnName();
    }
}
