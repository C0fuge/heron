package pro.surpin.trino.arrowflight;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Estimates how many rows a scan returns after the filters pushed into it. Used to decide
 * whether a scan is worth range splitting: each split is a separate server query with its own
 * planning cost, which only pays off when a lot of rows come back (TPC-H q14/q19 on a 60M-row
 * table with a selective filter: 0.9 s as one stream, 2.3-3.6 s as four).
 * <p>
 * Range filters on integer and date columns are measured against the column's real min/max
 * (cheap: answered from Parquet statistics, cached); other filters use fixed guesses.
 */
final class ScanEstimator
{
    private ScanEstimator() {}

    static OptionalDouble estimateRows(ArrowFlightTableHandle handle, ArrowFlightStatistics statistics, long tableRows)
    {
        double selectivity = 1.0;
        Map<ColumnHandle, Domain> domains = handle.getConstraint().getDomains().orElse(null);
        if (domains == null) {
            return OptionalDouble.of(0);   // constraint is NONE: nothing qualifies
        }
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) entry.getKey();
            selectivity *= domainSelectivity(handle, statistics, column, entry.getValue());
        }
        for (String predicate : handle.getExtraPredicates()) {
            selectivity *= predicate.startsWith("(NOT ") ? 0.9 : 0.5;
        }
        return OptionalDouble.of(tableRows * selectivity);
    }

    private static double domainSelectivity(ArrowFlightTableHandle handle, ArrowFlightStatistics statistics,
            ArrowFlightColumnHandle column, Domain domain)
    {
        if (domain.isNone()) {
            return 0;
        }
        if (domain.getValues().isAll()) {
            return 1;
        }
        if (domain.getValues().isDiscreteSet()) {
            return domain.getValues().getDiscreteSet().size() <= 10 ? 0.1 : 0.5;
        }
        Type type = column.getColumnType();
        if (!isOrderedInteger(type)) {
            return 0.5;
        }
        Optional<ArrowFlightStatistics.Range> stats = statistics.range(handle, column.getColumnName());
        if (stats.isEmpty()) {
            return 0.5;
        }
        double min = stats.get().min();
        double max = stats.get().max();
        double width = max - min + 1;
        double covered = 0;
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            double low = range.isLowUnbounded() ? min : Math.max(min, ((Long) range.getLowBoundedValue()).doubleValue());
            double high = range.isHighUnbounded() ? max : Math.min(max, ((Long) range.getHighBoundedValue()).doubleValue());
            if (high >= low) {
                covered += high - low + 1;
            }
        }
        return Math.min(1, Math.max(covered / width, 0.0001));
    }

    private static boolean isOrderedInteger(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT) || type.equals(DATE);
    }
}
