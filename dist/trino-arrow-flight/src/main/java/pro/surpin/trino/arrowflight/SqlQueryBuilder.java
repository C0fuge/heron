package pro.surpin.trino.arrowflight;

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.time.LocalDate;
import java.util.ArrayList;
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
import static io.trino.spi.type.TinyintType.TINYINT;

/**
 * Renders the {@code SELECT ... FROM ... WHERE ...} text sent to the Flight SQL server.
 * The server executes with DuckDB and supports projection, WHERE and (client-merged)
 * GROUP BY, but no ORDER BY / LIMIT / OFFSET — Trino applies those itself.
 */
public final class SqlQueryBuilder
{
    private SqlQueryBuilder() {}

    public static String buildScanSql(ArrowFlightTableHandle table)
    {
        String q = table.getIdentifierQuote();
        List<ArrowFlightColumnHandle> columns = table.effectiveColumns();
        String projection = columns.isEmpty()
                ? "*"
                : columns.stream().map(c -> quoteIdentifier(c.getColumnName(), q)).collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder("SELECT ").append(projection)
                .append(" FROM ").append(quoteIdentifier(table.getSchemaName(), q))
                .append('.').append(quoteIdentifier(table.getTableName(), q));

        String where = toWhereClause(table.getConstraint(), q);
        if (!where.isEmpty()) {
            sql.append(" WHERE ").append(where);
        }
        return sql.toString();
    }

    /** True when this connector can translate a {@link Domain} on {@code type} to SQL exactly. */
    public static boolean isPushdownSupported(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)
                || type.equals(DOUBLE) || type.equals(BOOLEAN) || type.equals(DATE)
                || type instanceof io.trino.spi.type.VarcharType;
    }

    private static String toWhereClause(TupleDomain<ColumnHandle> constraint, String quote)
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
            if (isPushdownSupported(column.getColumnType())) {
                predicates.add(toPredicate(column, entry.getValue(), quote));
            }
        }
        return String.join(" AND ", predicates);
    }

    private static String toPredicate(ArrowFlightColumnHandle column, Domain domain, String quote)
    {
        String name = quoteIdentifier(column.getColumnName(), quote);
        Type type = column.getColumnType();

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

    private static String formatValue(Type type, Object value)
    {
        if (type.equals(BOOLEAN)) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return Long.toString((long) value);
        }
        if (type.equals(DOUBLE)) {
            return Double.toString((double) value);
        }
        if (type.equals(DATE)) {
            return "DATE '" + LocalDate.ofEpochDay((long) value) + "'";
        }
        if (type instanceof io.trino.spi.type.VarcharType) {
            String text = ((Slice) value).toStringUtf8();
            return "'" + text.replace("'", "''") + "'";
        }
        throw new IllegalArgumentException("Unsupported pushdown type: " + type);
    }

    private static String quoteIdentifier(String identifier, String quote)
    {
        if (quote == null || quote.isEmpty()) {
            return identifier;
        }
        return quote + identifier.replace(quote, quote + quote) + quote;
    }
}
