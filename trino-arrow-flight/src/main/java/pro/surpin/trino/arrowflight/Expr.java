package pro.surpin.trino.arrowflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * Arithmetic over columns pushed with a projection: + - * unary minus and widening casts on
 * integer, double and decimal values. The server evaluates it only inside an aggregate
 * ({@code sum(a * (1 - b))}); a scan that still needs the value computes it here with
 * {@link #evaluate}, following Trino semantics (exact decimals, overflow errors).
 */
public final class Expr
{
    public enum Kind
    {
        COLUMN, CONSTANT, ADD, SUBTRACT, MULTIPLY, NEGATE, CAST
    }

    private final Kind kind;
    private final Type type;
    private final Optional<String> column;
    private final Optional<String> literal;
    private final List<Expr> args;

    @JsonCreator
    public Expr(
            @JsonProperty("kind") Kind kind,
            @JsonProperty("type") Type type,
            @JsonProperty("column") Optional<String> column,
            @JsonProperty("literal") Optional<String> literal,
            @JsonProperty("args") List<Expr> args)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.type = requireNonNull(type, "type is null");
        this.column = requireNonNull(column, "column is null");
        this.literal = requireNonNull(literal, "literal is null");
        this.args = ImmutableList.copyOf(requireNonNull(args, "args is null"));
    }

    public static Expr column(String name, Type type)
    {
        return new Expr(Kind.COLUMN, type, Optional.of(name), Optional.empty(), List.of());
    }

    public static Expr constant(String literal, Type type)
    {
        return new Expr(Kind.CONSTANT, type, Optional.empty(), Optional.of(literal), List.of());
    }

    public static Expr call(Kind kind, Type type, List<Expr> args)
    {
        return new Expr(kind, type, Optional.empty(), Optional.empty(), args);
    }

    @JsonProperty
    public Kind getKind()
    {
        return kind;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getColumn()
    {
        return column;
    }

    @JsonProperty
    public Optional<String> getLiteral()
    {
        return literal;
    }

    @JsonProperty
    public List<Expr> getArgs()
    {
        return args;
    }

    public static boolean isNumeric(Type type)
    {
        return SqlQueryBuilder.isIntegral(type) || type.equals(DOUBLE) || type instanceof DecimalType;
    }

    /** Names of the table columns the expression reads. */
    public void collectColumns(Set<String> out)
    {
        column.ifPresent(out::add);
        args.forEach(a -> a.collectColumns(out));
    }

    public String toSql(String quote)
    {
        return switch (kind) {
            case COLUMN -> SqlQueryBuilder.quoteIdentifier(column.orElseThrow(), quote);
            case CONSTANT -> literal.orElseThrow().startsWith("-") ? "(" + literal.orElseThrow() + ")" : literal.orElseThrow();
            case ADD -> "(" + args.get(0).toSql(quote) + " + " + args.get(1).toSql(quote) + ")";
            case SUBTRACT -> "(" + args.get(0).toSql(quote) + " - " + args.get(1).toSql(quote) + ")";
            case MULTIPLY -> "(" + args.get(0).toSql(quote) + " * " + args.get(1).toSql(quote) + ")";
            case NEGATE -> "(-" + args.get(0).toSql(quote) + ")";
            case CAST -> "CAST(" + args.get(0).toSql(quote) + " AS " + sqlType(type) + ")";
        };
    }

    static String sqlType(Type type)
    {
        if (type instanceof DecimalType decimal) {
            return "DECIMAL(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
        }
        if (type.equals(BIGINT)) {
            return "BIGINT";
        }
        if (type.equals(INTEGER)) {
            return "INTEGER";
        }
        if (type.equals(SMALLINT)) {
            return "SMALLINT";
        }
        if (type.equals(TINYINT)) {
            return "TINYINT";
        }
        if (type.equals(DOUBLE)) {
            return "DOUBLE";
        }
        throw new IllegalArgumentException("No SQL type for " + type);
    }

    /**
     * Evaluates for one row. Column values come as Long (integer types), Double or BigDecimal;
     * the result uses the same representation, or null if any input is null.
     */
    public Object evaluate(Map<String, Object> row)
    {
        switch (kind) {
            case COLUMN:
                return row.get(column.orElseThrow());
            case CONSTANT:
                return parseLiteral(literal.orElseThrow(), type);
            default:
                break;
        }
        Object a = args.get(0).evaluate(row);
        if (a == null) {
            return null;
        }
        if (kind == Kind.NEGATE) {
            if (a instanceof Long l) {
                return checkRange(Math.negateExact(l));
            }
            if (a instanceof Double d) {
                return -d;
            }
            return checkDecimal(((BigDecimal) a).negate());
        }
        if (kind == Kind.CAST) {
            return cast(a, type);
        }
        Object b = args.get(1).evaluate(row);
        if (b == null) {
            return null;
        }
        if (type.equals(DOUBLE)) {
            double x = toDouble(a);
            double y = toDouble(b);
            return switch (kind) {
                case ADD -> x + y;
                case SUBTRACT -> x - y;
                default -> x * y;
            };
        }
        if (type instanceof DecimalType) {
            BigDecimal x = toDecimal(a);
            BigDecimal y = toDecimal(b);
            return checkDecimal(switch (kind) {
                case ADD -> x.add(y);
                case SUBTRACT -> x.subtract(y);
                default -> x.multiply(y);
            });
        }
        long x = (Long) a;
        long y = (Long) b;
        try {
            return checkRange(switch (kind) {
                case ADD -> Math.addExact(x, y);
                case SUBTRACT -> Math.subtractExact(x, y);
                default -> Math.multiplyExact(x, y);
            });
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, type.getDisplayName() + " overflow in " + kind.name().toLowerCase());
        }
    }

    private long checkRange(long value)
    {
        long min;
        long max;
        if (type.equals(INTEGER)) {
            min = Integer.MIN_VALUE;
            max = Integer.MAX_VALUE;
        }
        else if (type.equals(SMALLINT)) {
            min = Short.MIN_VALUE;
            max = Short.MAX_VALUE;
        }
        else if (type.equals(TINYINT)) {
            min = Byte.MIN_VALUE;
            max = Byte.MAX_VALUE;
        }
        else {
            return value;
        }
        if (value < min || value > max) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, type.getDisplayName() + " overflow: " + value);
        }
        return value;
    }

    private BigDecimal checkDecimal(BigDecimal value)
    {
        DecimalType decimal = (DecimalType) type;
        BigDecimal scaled = value.setScale(decimal.getScale(), RoundingMode.HALF_UP);
        if (scaled.precision() > decimal.getPrecision()) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow: " + value + " does not fit " + type);
        }
        return scaled;
    }

    private Object cast(Object value, Type target)
    {
        if (target.equals(DOUBLE)) {
            return toDouble(value);
        }
        if (target instanceof DecimalType) {
            return checkDecimal(toDecimal(value));
        }
        return checkRange((Long) value);
    }

    private static double toDouble(Object value)
    {
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof BigDecimal d) {
            return d.doubleValue();
        }
        return (Double) value;
    }

    private static BigDecimal toDecimal(Object value)
    {
        if (value instanceof Long l) {
            return BigDecimal.valueOf(l);
        }
        return (BigDecimal) value;
    }

    static Object parseLiteral(String literal, Type type)
    {
        if (type.equals(DOUBLE)) {
            return Double.parseDouble(literal);
        }
        if (type instanceof DecimalType) {
            return new BigDecimal(literal);
        }
        return Long.parseLong(literal);
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof Expr other && kind == other.kind && type.equals(other.type)
                && column.equals(other.column) && literal.equals(other.literal) && args.equals(other.args);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, type, column, literal, args);
    }

    @Override
    public String toString()
    {
        return toSql("");
    }
}
