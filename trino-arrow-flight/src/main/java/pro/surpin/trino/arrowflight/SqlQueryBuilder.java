package pro.surpin.trino.arrowflight;

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Renders the SQL text sent to the Flight SQL server. The server parses it with jOOQ and runs it
 * in DuckDB; it understands a single-table SELECT with WHERE, GROUP BY and the aggregates
 * count(*), count(x), sum, min, max. It ignores ORDER BY / LIMIT (Trino applies those).
 */
public final class SqlQueryBuilder
{
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);

    private SqlQueryBuilder() {}

    /** SQL the split planner / statistics use: the table's own scan with its pushed filters. */
    public static String buildScanSql(ArrowFlightTableHandle table)
    {
        if (table.getAggregation().isPresent()) {
            return buildAggregationSql(table, List.of());
        }
        return buildScanSql(table, baseColumns(table, table.effectiveColumns()), List.of());
    }

    /**
     * Plain scan of {@code columns} (table columns only). {@code extraConjuncts} are ANDed to the
     * pushed filters (split range, dynamic filter).
     */
    public static String buildScanSql(ArrowFlightTableHandle table, List<ArrowFlightColumnHandle> columns, List<String> extraConjuncts)
    {
        String q = table.getIdentifierQuote();
        String projection;
        if (!columns.isEmpty()) {
            projection = columns.stream().map(c -> quoteIdentifier(c.getColumnName(), q)).collect(Collectors.joining(", "));
        }
        else {
            // No column needed (e.g. count(*) evaluated by Trino): read the narrowest one, not "*".
            projection = cheapestColumn(table.getColumns())
                    .map(c -> quoteIdentifier(c.getColumnName(), q))
                    .orElse("*");
        }

        StringBuilder sql = new StringBuilder("SELECT ").append(projection).append(" FROM ").append(tableName(table));
        appendWhere(sql, table, extraConjuncts);
        return sql.toString();
    }

    /**
     * Table columns the server must return so that {@code requested} can be produced: plain
     * columns as they are, expression columns replaced by the columns they read.
     */
    public static List<ArrowFlightColumnHandle> baseColumns(ArrowFlightTableHandle table, List<ArrowFlightColumnHandle> requested)
    {
        java.util.Set<String> names = new java.util.LinkedHashSet<>();
        for (ArrowFlightColumnHandle column : requested) {
            if (column.getExpression().isPresent()) {
                column.getExpression().get().collectColumns(names);
            }
            else {
                names.add(column.getColumnName());
            }
        }
        List<ArrowFlightColumnHandle> out = new ArrayList<>();
        for (ArrowFlightColumnHandle column : table.getColumns()) {
            if (names.contains(column.getColumnName())) {
                out.add(column);
            }
        }
        return out;
    }

    public static String buildAggregationSql(ArrowFlightTableHandle table, List<String> extraConjuncts)
    {
        Aggregation aggregation = table.getAggregation().orElseThrow();
        String q = table.getIdentifierQuote();
        List<String> select = new ArrayList<>();
        for (ArrowFlightColumnHandle group : aggregation.getGroupingColumns()) {
            select.add(quoteIdentifier(group.getColumnName(), q));
        }
        for (AggregateColumn aggregate : aggregation.getAggregates()) {
            String out = aggregate.getOutput().getColumnName();
            String input = aggregate.getInput().map(c -> c.toSql(q)).orElse(null);
            switch (aggregate.getFunction()) {
                case COUNT_STAR -> select.add("count(*) AS " + out);
                case COUNT -> select.add("count(" + input + ") AS " + out);
                case SUM -> select.add(sumExpression(aggregate, q) + " AS " + out);
                case MIN -> select.add("min(" + input + ") AS " + out);
                case MAX -> select.add("max(" + input + ") AS " + out);
                case AVG -> {
                    select.add(sumExpression(aggregate, q) + " AS " + aggregate.sumName());
                    select.add("count(" + input + ") AS " + aggregate.countName());
                }
            }
        }
        StringBuilder sql = new StringBuilder("SELECT ").append(String.join(", ", select))
                .append(" FROM ").append(tableName(table));
        boolean filtered = appendWhere(sql, table, extraConjuncts);
        if (!filtered && aggregation.getGroupingColumns().isEmpty() && aggregation.getAggregates().stream()
                .anyMatch(a -> a.getFunction() == AggregateColumn.Function.MIN || a.getFunction() == AggregateColumn.Function.MAX)) {
            // Server bug: an unfiltered query made only of min/max/count is answered from Parquet
            // footer statistics, which returns NULL for DATE columns and fails on DECIMAL ones.
            // Any WHERE clause makes it run the query for real.
            sql.append(" WHERE TRUE");
        }
        if (!aggregation.getGroupingColumns().isEmpty()) {
            sql.append(" GROUP BY ").append(aggregation.getGroupingColumns().stream()
                    .map(c -> quoteIdentifier(c.getColumnName(), q))
                    .collect(Collectors.joining(", ")));
        }
        return sql.toString();
    }

    /**
     * Integer sums go through DECIMAL(38,0): the server otherwise declares sum(int) as DOUBLE,
     * which loses precision above 2^53. Decimal and floating sums are sent as is.
     */
    private static String sumExpression(AggregateColumn aggregate, String quote)
    {
        ArrowFlightColumnHandle input = aggregate.getInput().orElseThrow();
        String name = input.toSql(quote);
        Type type = input.getColumnType();
        if (isIntegral(type)) {
            return "sum(CAST(" + name + " AS DECIMAL(38,0)))";
        }
        return "sum(" + name + ")";
    }

    static boolean isIntegral(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT);
    }

    private static String tableName(ArrowFlightTableHandle table)
    {
        String q = table.getIdentifierQuote();
        return quoteIdentifier(table.getSchemaName(), q) + '.' + quoteIdentifier(table.getTableName(), q);
    }

    /** Appends the WHERE clause, if any; returns whether one was written. */
    private static boolean appendWhere(StringBuilder sql, ArrowFlightTableHandle table, List<String> extraConjuncts)
    {
        List<String> conjuncts = new ArrayList<>();
        String domains = toWhereClause(table.getConstraint(), table.getIdentifierQuote());
        if (!domains.isEmpty()) {
            conjuncts.add(domains);
        }
        conjuncts.addAll(table.getExtraPredicates());
        conjuncts.addAll(extraConjuncts);
        if (conjuncts.isEmpty()) {
            return false;
        }
        sql.append(" WHERE ").append(String.join(" AND ", conjuncts));
        return true;
    }

    /** Narrowest column by fixed width; variable-width columns last. */
    private static java.util.Optional<ArrowFlightColumnHandle> cheapestColumn(List<ArrowFlightColumnHandle> columns)
    {
        return columns.stream().min(Comparator.comparingInt(c -> width(c.getColumnType())));
    }

    private static int width(Type type)
    {
        if (type.equals(BOOLEAN) || type.equals(TINYINT)) {
            return 1;
        }
        if (type.equals(SMALLINT)) {
            return 2;
        }
        if (type.equals(INTEGER) || type.equals(DATE)) {
            return 4;
        }
        if (type.equals(BIGINT) || type.equals(DOUBLE) || type.equals(TIMESTAMP_MICROS)) {
            return 8;
        }
        if (type instanceof DecimalType) {
            return 16;
        }
        return 1000;
    }

    /** True when this connector can translate a {@link Domain} on {@code type} to SQL exactly. */
    public static boolean isPushdownSupported(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)
                || type.equals(DOUBLE) || type.equals(BOOLEAN) || type.equals(DATE)
                || type.equals(TIMESTAMP_MICROS) || type instanceof DecimalType
                || type instanceof VarcharType;
    }

    public static String toWhereClause(TupleDomain<ColumnHandle> constraint, String quote)
    {
        if (constraint.isAll()) {
            return "";
        }
        if (constraint.isNone()) {
            return "FALSE";
        }

        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElseThrow();
        List<String> predicates = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ArrowFlightColumnHandle column = (ArrowFlightColumnHandle) entry.getKey();
            if (column.getExpression().isEmpty() && isPushdownSupported(column.getColumnType())) {
                predicates.add(toPredicate(column, entry.getValue(), quote));
            }
        }
        return String.join(" AND ", predicates);
    }

    private static String toPredicate(ArrowFlightColumnHandle column, Domain domain, String quote)
    {
        String name = quoteIdentifier(column.getColumnName(), quote);
        Type type = column.getColumnType();

        if (domain.isNone()) {
            return "FALSE";
        }
        if (domain.isOnlyNull()) {
            return name + " IS NULL";
        }
        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? "TRUE" : name + " IS NOT NULL";
        }

        List<String> disjuncts = new ArrayList<>();

        if (domain.getValues().isDiscreteSet()) {
            List<Object> values = domain.getValues().getDiscreteSet();
            if (values.size() == 1) {
                disjuncts.add(name + " = " + formatValue(type, values.get(0)));
            }
            else {
                String inList = values.stream()
                        .map(value -> formatValue(type, value))
                        .collect(Collectors.joining(", "));
                disjuncts.add(name + " IN (" + inList + ")");
            }
        }
        else {
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                disjuncts.add(rangeToSql(name, type, range));
            }
        }

        if (domain.isNullAllowed()) {
            disjuncts.add(name + " IS NULL");
        }

        String joined = String.join(" OR ", disjuncts);
        return disjuncts.size() > 1 ? "(" + joined + ")" : joined;
    }

    private static String rangeToSql(String name, Type type, Range range)
    {
        if (range.isSingleValue()) {
            return name + " = " + formatValue(type, range.getSingleValue());
        }
        StringJoiner conjunction = new StringJoiner(" AND ");
        if (!range.isLowUnbounded()) {
            String op = range.isLowInclusive() ? " >= " : " > ";
            conjunction.add(name + op + formatValue(type, range.getLowBoundedValue()));
        }
        if (!range.isHighUnbounded()) {
            String op = range.isHighInclusive() ? " <= " : " < ";
            conjunction.add(name + op + formatValue(type, range.getHighBoundedValue()));
        }
        return "(" + conjunction + ")";
    }

    static String formatValue(Type type, Object value)
    {
        if (type.equals(BOOLEAN)) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        if (isIntegral(type)) {
            return Long.toString((long) value);
        }
        if (type.equals(DOUBLE)) {
            double d = (double) value;
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw new IllegalArgumentException("Non-finite double cannot be pushed down");
            }
            // Exact decimal expansion: parses back to the very same double.
            return new BigDecimal(d).toPlainString();
        }
        if (type instanceof DecimalType decimal) {
            BigInteger unscaled = decimal.isShort()
                    ? BigInteger.valueOf((long) value)
                    : ((Int128) value).toBigInteger();
            return new BigDecimal(unscaled, decimal.getScale()).toPlainString();
        }
        if (type.equals(DATE)) {
            return "DATE '" + LocalDate.ofEpochDay((long) value) + "'";
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            long micros = (long) value;
            Instant instant = Instant.ofEpochSecond(Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
            return "TIMESTAMP '" + TIMESTAMP_FORMAT.format(instant) + "'";
        }
        if (type instanceof VarcharType) {
            return stringLiteral(((Slice) value).toStringUtf8());
        }
        throw new IllegalArgumentException("Unsupported pushdown type: " + type);
    }

    static String stringLiteral(String text)
    {
        return "'" + text.replace("'", "''") + "'";
    }

    static String quoteIdentifier(String identifier, String quote)
    {
        if (quote == null || quote.isEmpty()) {
            return identifier;
        }
        return quote + identifier.replace(quote, quote + quote) + quote;
    }
}
