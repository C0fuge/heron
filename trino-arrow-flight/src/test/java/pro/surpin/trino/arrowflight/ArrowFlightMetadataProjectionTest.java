package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.expression.StandardFunctions.ADD_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * applyProjection must return empty once nothing more can be pushed, whatever order Trino passes the projections in;
 * otherwise PushProjectionIntoTableScan applies forever (seen with {@code checksum(ROW(...))} over lineitem).
 */
class ArrowFlightMetadataProjectionTest
{
    private static final ArrowFlightColumnHandle A = new ArrowFlightColumnHandle("a", BIGINT);
    private static final ArrowFlightColumnHandle B = new ArrowFlightColumnHandle("b", BIGINT);
    private static final ArrowFlightColumnHandle C = new ArrowFlightColumnHandle("c", VARCHAR);
    private static final Map<String, ColumnHandle> SCAN = ImmutableMap.of("a", A, "b", B, "c", C);

    private static ArrowFlightTableHandle table()
    {
        return new ArrowFlightTableHandle("s", "t", ImmutableList.of(A, B, C), "");
    }

    private static Variable var(ArrowFlightColumnHandle column)
    {
        return new Variable(column.getColumnName(), column.getColumnType());
    }

    /** Stand-in for an expression the connector cannot push (e.g. a ROW constructor). */
    private static Call row(ArrowFlightColumnHandle... columns)
    {
        List<ConnectorExpression> args = ImmutableList.copyOf(Arrays.stream(columns).map(ArrowFlightMetadataProjectionTest::var).toList());
        RowType type = RowType.anonymous(Arrays.stream(columns).map(ArrowFlightColumnHandle::getColumnType).toList());
        return new Call(type, new FunctionName("$test_row"), args);
    }

    private static ArrowFlightTableHandle handleOf(Optional<ProjectionApplicationResult<ConnectorTableHandle>> result)
    {
        assertTrue(result.isPresent());
        return (ArrowFlightTableHandle) result.get().getHandle();
    }

    @Test
    void allColumnsInAnyOrderIsNoChange()
    {
        for (List<ConnectorExpression> projections : List.<List<ConnectorExpression>>of(
                List.of(var(A), var(B), var(C)),
                List.of(var(C), var(A), var(B)),
                List.of(row(C, B, A)))) {
            for (boolean pushdown : new boolean[] {true, false}) {
                assertEquals(Optional.empty(), ArrowFlightMetadata.applyProjection(table(), projections, SCAN, pushdown),
                        projections + " pushdown=" + pushdown);
            }
        }
    }

    @Test
    void secondCallWithReorderedProjectionsIsEmpty()
    {
        ArrowFlightTableHandle narrowed = handleOf(ArrowFlightMetadata.applyProjection(table(), List.of(var(A), var(B)), SCAN, true));
        assertEquals(ImmutableSet.of(A, B), ImmutableSet.copyOf(narrowed.effectiveColumns()));

        Map<String, ColumnHandle> narrowedScan = ImmutableMap.of("a", A, "b", B);
        assertEquals(Optional.empty(), ArrowFlightMetadata.applyProjection(narrowed, List.of(var(B), var(A)), narrowedScan, true));
        assertEquals(Optional.empty(), ArrowFlightMetadata.applyProjection(narrowed, List.of(row(B, A)), narrowedScan, true));
        assertEquals(Optional.empty(), ArrowFlightMetadata.applyProjection(narrowed, List.of(row(A, B), var(B)), narrowedScan, false));
    }

    @Test
    void rowProjectionNarrowsOnceThenStops()
    {
        ArrowFlightTableHandle handle = table();
        Map<String, ColumnHandle> scan = ImmutableMap.of("b", B, "c", C);
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> first = ArrowFlightMetadata.applyProjection(handle, List.of(row(C, B)), scan, true);
        ArrowFlightTableHandle narrowed = handleOf(first);
        assertEquals(ImmutableSet.of(B, C), ImmutableSet.copyOf(narrowed.effectiveColumns()));
        assertEquals(List.of(row(C, B)), first.get().getProjections());

        assertEquals(Optional.empty(), ArrowFlightMetadata.applyProjection(narrowed, List.of(row(C, B)), scan, true));
        assertEquals(Optional.empty(), ArrowFlightMetadata.applyProjection(narrowed, List.of(row(B, C)), scan, true));
    }

    @Test
    void pushedExpressionIsNotPushedAgain()
    {
        Call sum = new Call(BIGINT, ADD_FUNCTION_NAME, List.of(var(A), var(B)));
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> first = ArrowFlightMetadata.applyProjection(table(), List.of(sum), SCAN, true);
        ArrowFlightTableHandle pushed = handleOf(first);
        Variable output = (Variable) first.get().getProjections().get(0);

        ImmutableMap.Builder<String, ColumnHandle> scan = ImmutableMap.builder();
        for (Assignment assignment : first.get().getAssignments()) {
            scan.put(assignment.getVariable(), assignment.getColumn());
        }
        assertEquals(Optional.empty(), ArrowFlightMetadata.applyProjection(pushed, List.of(output), scan.buildOrThrow(), true));
    }
}
