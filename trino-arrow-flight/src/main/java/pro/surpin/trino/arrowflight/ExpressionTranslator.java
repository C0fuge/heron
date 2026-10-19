package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.MULTIPLY_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NEGATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.SUBTRACT_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;

/**
 * Translates Trino {@link ConnectorExpression}s into what the Flight server understands:
 * boolean predicates (comparisons, LIKE, IN, IS NULL, AND/OR/NOT over plain columns and
 * literals) into WHERE text, and arithmetic into {@link Expr} trees for aggregate inputs.
 * Anything else yields empty and stays with the engine.
 */
public final class ExpressionTranslator
{
    private ExpressionTranslator() {}

    /** Splits a top-level AND into its conjuncts. */
    public static List<ConnectorExpression> conjuncts(ConnectorExpression expression)
    {
        if (expression instanceof Call call && is(call, AND_FUNCTION_NAME)) {
            List<ConnectorExpression> out = new ArrayList<>();
            call.getArguments().forEach(arg -> out.addAll(conjuncts(arg)));
            return out;
        }
        if (expression.equals(Constant.TRUE)) {
            return List.of();
        }
        return List.of(expression);
    }

    public static ConnectorExpression and(List<ConnectorExpression> conjuncts)
    {
        if (conjuncts.isEmpty()) {
            return Constant.TRUE;
        }
        if (conjuncts.size() == 1) {
            return conjuncts.get(0);
        }
        return new Call(BOOLEAN, AND_FUNCTION_NAME, ImmutableList.copyOf(conjuncts));
    }

    // ------------------------------------------------------------------ predicates

    public static Optional<String> toPredicateSql(ConnectorExpression expression, Map<String, ColumnHandle> assignments, String quote)
    {
        if (!(expression instanceof Call call) || call.getFunctionName().getCatalogSchema().isPresent()) {
            return Optional.empty();
        }
        FunctionName name = call.getFunctionName();
        List<ConnectorExpression> args = call.getArguments();

        if (name.equals(AND_FUNCTION_NAME) || name.equals(OR_FUNCTION_NAME)) {
            List<String> parts = new ArrayList<>();
            for (ConnectorExpression arg : args) {
                Optional<String> part = toPredicateSql(arg, assignments, quote);
                if (part.isEmpty()) {
                    return Optional.empty();
                }
                parts.add(part.get());
            }
            return Optional.of("(" + String.join(name.equals(AND_FUNCTION_NAME) ? " AND " : " OR ", parts) + ")");
        }
        if (name.equals(NOT_FUNCTION_NAME) && args.size() == 1) {
            return toPredicateSql(args.get(0), assignments, quote).map(inner -> "(NOT " + inner + ")");
        }
        if (name.equals(IS_NULL_FUNCTION_NAME) && args.size() == 1) {
            return operand(args.get(0), assignments, quote).map(v -> "(" + v + " IS NULL)");
        }
        if (name.equals(LIKE_FUNCTION_NAME) && args.size() == 2) {
            if (!(args.get(1) instanceof Constant pattern) || !(pattern.getValue() instanceof Slice slice)
                    || !(args.get(0).getType() instanceof VarcharType)) {
                return Optional.empty();
            }
            return column(args.get(0), assignments, quote)
                    .map(v -> "(" + v + " LIKE " + SqlQueryBuilder.stringLiteral(slice.toStringUtf8()) + ")");
        }
        if (name.equals(IN_PREDICATE_FUNCTION_NAME) && args.size() == 2
                && args.get(1) instanceof Call array && array.getFunctionName().equals(ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            Optional<String> value = column(args.get(0), assignments, quote);
            if (value.isEmpty() || array.getArguments().isEmpty()) {
                return Optional.empty();
            }
            List<String> items = new ArrayList<>();
            for (ConnectorExpression item : array.getArguments()) {
                Optional<String> literal = literal(item);
                if (literal.isEmpty()) {
                    return Optional.empty();
                }
                items.add(literal.get());
            }
            return Optional.of("(" + value.get() + " IN (" + String.join(", ", items) + "))");
        }
        String operator = comparisonOperator(name);
        if (operator != null && args.size() == 2) {
            if (!args.get(0).getType().equals(args.get(1).getType())) {
                return Optional.empty();
            }
            Optional<String> left = operand(args.get(0), assignments, quote);
            Optional<String> right = operand(args.get(1), assignments, quote);
            if (left.isEmpty() || right.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of("(" + left.get() + " " + operator + " " + right.get() + ")");
        }
        return Optional.empty();
    }

    private static String comparisonOperator(FunctionName name)
    {
        if (name.equals(EQUAL_OPERATOR_FUNCTION_NAME)) {
            return "=";
        }
        if (name.equals(NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return "<>";
        }
        if (name.equals(LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            return "<";
        }
        if (name.equals(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return "<=";
        }
        if (name.equals(GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return ">";
        }
        if (name.equals(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return ">=";
        }
        return null;
    }

    private static Optional<String> operand(ConnectorExpression expression, Map<String, ColumnHandle> assignments, String quote)
    {
        if (expression instanceof Constant) {
            return literal(expression);
        }
        return column(expression, assignments, quote);
    }

    /** A plain table column (not a synthetic one) whose type the server compares like Trino does. */
    private static Optional<String> column(ConnectorExpression expression, Map<String, ColumnHandle> assignments, String quote)
    {
        if (!(expression instanceof Variable variable)) {
            return Optional.empty();
        }
        if (!(assignments.get(variable.getName()) instanceof ArrowFlightColumnHandle column)
                || column.getExpression().isPresent()
                || !SqlQueryBuilder.isPushdownSupported(column.getColumnType())) {
            return Optional.empty();
        }
        return Optional.of(SqlQueryBuilder.quoteIdentifier(column.getColumnName(), quote));
    }

    private static Optional<String> literal(ConnectorExpression expression)
    {
        if (!(expression instanceof Constant constant) || constant.getValue() == null
                || !SqlQueryBuilder.isPushdownSupported(constant.getType())) {
            return Optional.empty();
        }
        try {
            return Optional.of(SqlQueryBuilder.formatValue(constant.getType(), constant.getValue()));
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    // ------------------------------------------------------------------ arithmetic

    public static Optional<Expr> toExpr(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        Type type = expression.getType();
        if (!Expr.isNumeric(type)) {
            return Optional.empty();
        }
        if (expression instanceof Variable variable) {
            if (assignments.get(variable.getName()) instanceof ArrowFlightColumnHandle column && column.getColumnType().equals(type)) {
                return column.getExpression().isPresent()
                        ? column.getExpression()
                        : Optional.of(Expr.column(column.getColumnName(), type));
            }
            return Optional.empty();
        }
        if (expression instanceof Constant constant) {
            if (constant.getValue() == null) {
                return Optional.empty();
            }
            try {
                return Optional.of(Expr.constant(SqlQueryBuilder.formatValue(type, constant.getValue()), type));
            }
            catch (RuntimeException e) {
                return Optional.empty();
            }
        }
        if (!(expression instanceof Call call) || call.getFunctionName().getCatalogSchema().isPresent()) {
            return Optional.empty();
        }
        FunctionName name = call.getFunctionName();
        List<Expr> args = new ArrayList<>();
        for (ConnectorExpression arg : call.getArguments()) {
            Optional<Expr> translated = toExpr(arg, assignments);
            if (translated.isEmpty()) {
                return Optional.empty();
            }
            args.add(translated.get());
        }
        if (args.size() == 2 && (name.equals(ADD_FUNCTION_NAME) || name.equals(SUBTRACT_FUNCTION_NAME) || name.equals(MULTIPLY_FUNCTION_NAME))) {
            if (!operandsMatch(type, args.get(0).getType(), args.get(1).getType())) {
                return Optional.empty();
            }
            Expr.Kind kind = name.equals(ADD_FUNCTION_NAME) ? Expr.Kind.ADD
                    : name.equals(SUBTRACT_FUNCTION_NAME) ? Expr.Kind.SUBTRACT : Expr.Kind.MULTIPLY;
            return Optional.of(Expr.call(kind, type, args));
        }
        if (args.size() == 1 && name.equals(NEGATE_FUNCTION_NAME) && args.get(0).getType().equals(type)) {
            return Optional.of(Expr.call(Expr.Kind.NEGATE, type, args));
        }
        if (args.size() == 1 && name.equals(CAST_FUNCTION_NAME) && isExactWidening(args.get(0).getType(), type)) {
            Expr source = args.get(0);
            if (source.getKind() == Expr.Kind.CONSTANT) {
                // Fold: render the literal in the target type.
                Object value = Expr.parseLiteral(source.getLiteral().orElseThrow(), source.getType());
                return Optional.of(Expr.constant(foldedLiteral(value, type), type));
            }
            return Optional.of(Expr.call(Expr.Kind.CAST, type, args));
        }
        return Optional.empty();
    }

    /** Trino has already coerced operands; same family on both sides is all we accept. */
    private static boolean operandsMatch(Type result, Type left, Type right)
    {
        if (result.equals(DOUBLE)) {
            return left.equals(DOUBLE) && right.equals(DOUBLE);
        }
        if (result instanceof DecimalType) {
            return left instanceof DecimalType && right instanceof DecimalType;
        }
        return result.equals(left) && result.equals(right);
    }

    /** Casts that never round: integer to wider integer/decimal/double, decimal to double or to a decimal with no fewer fraction digits. */
    private static boolean isExactWidening(Type from, Type to)
    {
        if (to.equals(DOUBLE)) {
            return SqlQueryBuilder.isIntegral(from) || from instanceof DecimalType;
        }
        if (SqlQueryBuilder.isIntegral(to)) {
            return SqlQueryBuilder.isIntegral(from) && integerDigits(to) >= integerDigits(from);
        }
        if (to instanceof DecimalType target) {
            if (SqlQueryBuilder.isIntegral(from)) {
                return target.getPrecision() - target.getScale() >= integerDigits(from);
            }
            if (from instanceof DecimalType source) {
                return target.getScale() >= source.getScale()
                        && target.getPrecision() - target.getScale() >= source.getPrecision() - source.getScale();
            }
            return false;
        }
        return false;
    }

    private static int integerDigits(Type integral)
    {
        if (integral.equals(io.trino.spi.type.TinyintType.TINYINT)) {
            return 3;
        }
        if (integral.equals(io.trino.spi.type.SmallintType.SMALLINT)) {
            return 5;
        }
        if (integral.equals(io.trino.spi.type.IntegerType.INTEGER)) {
            return 10;
        }
        return 19;
    }

    private static String foldedLiteral(Object value, Type type)
    {
        if (type.equals(DOUBLE)) {
            double d = value instanceof Long l ? l : ((java.math.BigDecimal) value).doubleValue();
            return new java.math.BigDecimal(d).toPlainString();
        }
        if (SqlQueryBuilder.isIntegral(type)) {
            return Long.toString((Long) value);
        }
        java.math.BigDecimal decimal = value instanceof Long l ? java.math.BigDecimal.valueOf(l) : (java.math.BigDecimal) value;
        return decimal.setScale(((DecimalType) type).getScale()).toPlainString();
    }

    private static boolean is(Call call, FunctionName name)
    {
        return call.getFunctionName().equals(name);
    }
}
