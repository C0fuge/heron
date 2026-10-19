package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlQueryBuilderTest
{
    private static final ArrowFlightColumnHandle ID = new ArrowFlightColumnHandle("id", BIGINT);
    private static final ArrowFlightColumnHandle NAME = new ArrowFlightColumnHandle("name", VARCHAR);

    private static ArrowFlightTableHandle table(String quote, ArrowFlightColumnHandle... cols)
    {
        return new ArrowFlightTableHandle("test_schema", "test_table", ImmutableList.copyOf(cols), quote);
    }

    @Test
    void unquotedByDefault()
    {
        assertEquals(
                "SELECT id, name FROM test_schema.test_table",
                SqlQueryBuilder.buildScanSql(table("", ID, NAME)));
    }

    @Test
    void quotedWhenConfigured()
    {
        assertEquals(
                "SELECT \"id\", \"name\" FROM \"test_schema\".\"test_table\"",
                SqlQueryBuilder.buildScanSql(table("\"", ID, NAME)));
    }

    @Test
    void rangeAndEqualityBecomeWhere()
    {
        Map<ColumnHandle, Domain> domains = Map.of(
                ID, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 10L)), false),
                NAME, Domain.singleValue(VARCHAR, Slices.utf8Slice("o'brien")));

        String sql = SqlQueryBuilder.buildScanSql(
                table("", ID, NAME).withConstraint(TupleDomain.withColumnDomains(domains)));

        assertTrue(sql.contains("id >= 10"), sql);
        assertTrue(sql.contains("name = 'o''brien'"), sql);
    }

    @Test
    void unconstrainedTableHasNoWhere()
    {
        ArrowFlightTableHandle handle = new ArrowFlightTableHandle(
                "s", "t", ImmutableList.of(ID), Optional.empty(), TupleDomain.all(), "");
        assertEquals("SELECT id FROM s.t", SqlQueryBuilder.buildScanSql(handle));
    }
}
