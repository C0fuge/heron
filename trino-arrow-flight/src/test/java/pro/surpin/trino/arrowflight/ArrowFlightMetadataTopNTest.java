package pro.surpin.trino.arrowflight;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowFlightMetadataTopNTest
{
    private static final ArrowFlightColumnHandle KEY = new ArrowFlightColumnHandle("k", BIGINT);
    private static final ArrowFlightColumnHandle PRICE = new ArrowFlightColumnHandle("price", createDecimalType(15, 2));
    private static final ArrowFlightColumnHandle DAY = new ArrowFlightColumnHandle("day", DATE);
    private static final ArrowFlightColumnHandle SCORE = new ArrowFlightColumnHandle("score", DOUBLE);
    private static final ArrowFlightColumnHandle NAME = new ArrowFlightColumnHandle("name", VARCHAR);
    private static final Map<String, ColumnHandle> SCAN = ImmutableMap.of(
            "k", KEY, "price_0", PRICE, "day", DAY, "score", SCORE, "name", NAME);

    private static ArrowFlightTableHandle table()
    {
        return new ArrowFlightTableHandle("s", "t", ImmutableList.of(KEY, PRICE, DAY, SCORE, NAME), "");
    }

    private static Optional<ArrowFlightTableHandle> topN(ArrowFlightTableHandle handle, long count, SortItem... items)
    {
        return ArrowFlightMetadata.applyTopN(handle, count, List.of(items), SCAN);
    }

    @Test
    void rendersOrderByAndLimitAfterTheFilters()
    {
        ArrowFlightTableHandle handle = topN(table(), 10,
                new SortItem("price_0", SortOrder.DESC_NULLS_LAST),
                new SortItem("k", SortOrder.ASC_NULLS_FIRST)).orElseThrow();
        assertEquals(Optional.of(new TopN("price DESC NULLS LAST, k ASC NULLS FIRST", 10)), handle.getTopN());
        assertEquals("SELECT k, price FROM s.t WHERE (k >= 5) ORDER BY price DESC NULLS LAST, k ASC NULLS FIRST LIMIT 10",
                SqlQueryBuilder.buildScanSql(handle, List.of(KEY, PRICE), List.of("(k >= 5)")));
        // no TopN, no ORDER BY
        assertEquals("SELECT k FROM s.t", SqlQueryBuilder.buildScanSql(table(), List.of(KEY), List.of()));
    }

    @Test
    void quotesSortKeysLikeOtherIdentifiers()
    {
        ArrowFlightTableHandle quoted = new ArrowFlightTableHandle("s", "t", ImmutableList.of(KEY, DAY), "\"");
        assertEquals("\"day\" ASC NULLS LAST",
                topN(quoted, 3, new SortItem("day", SortOrder.ASC_NULLS_LAST)).orElseThrow().getTopN().orElseThrow().getOrderBy());
    }

    @Test
    void onlyExactlySortableColumnsArePushed()
    {
        assertTrue(topN(table(), 5, new SortItem("score", SortOrder.DESC_NULLS_LAST)).isEmpty());
        assertTrue(topN(table(), 5, new SortItem("k", SortOrder.ASC_NULLS_LAST), new SortItem("score", SortOrder.ASC_NULLS_LAST)).isEmpty());
        assertTrue(topN(table(), 5, new SortItem("unknown", SortOrder.ASC_NULLS_LAST)).isEmpty());
        ArrowFlightColumnHandle padded = new ArrowFlightColumnHandle("code", createCharType(3));
        assertTrue(ArrowFlightMetadata.applyTopN(new ArrowFlightTableHandle("s", "t", ImmutableList.of(padded), ""), 5,
                List.of(new SortItem("code", SortOrder.ASC_NULLS_LAST)), ImmutableMap.of("code", padded)).isEmpty());
        ArrowFlightColumnHandle expression = new ArrowFlightColumnHandle("afexpr_1", BIGINT, Optional.of(Expr.column("k", BIGINT)));
        assertTrue(ArrowFlightMetadata.applyTopN(table(), 5, List.of(new SortItem("e", SortOrder.ASC_NULLS_LAST)),
                ImmutableMap.of("e", expression)).isEmpty());
    }

    @Test
    void varcharSortKeysArePushedBecauseTheServerComparesUtf8Bytes()
    {
        ArrowFlightTableHandle handle = topN(table(), 7,
                new SortItem("name", SortOrder.DESC_NULLS_FIRST),
                new SortItem("k", SortOrder.ASC_NULLS_LAST)).orElseThrow();
        assertEquals(Optional.of(new TopN("name DESC NULLS FIRST, k ASC NULLS LAST", 7)), handle.getTopN());
        ArrowFlightColumnHandle bounded = new ArrowFlightColumnHandle("tag", createVarcharType(10));
        assertTrue(ArrowFlightMetadata.applyTopN(new ArrowFlightTableHandle("s", "t", ImmutableList.of(bounded), ""), 5,
                List.of(new SortItem("tag", SortOrder.ASC_NULLS_LAST)), ImmutableMap.of("tag", bounded)).isPresent());
    }

    @Test
    void notAppliedTwiceNorOverALimit()
    {
        ArrowFlightTableHandle sorted = topN(table(), 10, new SortItem("k", SortOrder.ASC_NULLS_LAST)).orElseThrow();
        assertTrue(topN(sorted, 10, new SortItem("k", SortOrder.ASC_NULLS_LAST)).isEmpty());
        assertTrue(topN(sorted, 5, new SortItem("day", SortOrder.ASC_NULLS_LAST)).isEmpty());
        assertTrue(topN(table().withLimit(100), 10, new SortItem("k", SortOrder.ASC_NULLS_LAST)).isEmpty());
        assertTrue(topN(table(), 0, new SortItem("k", SortOrder.ASC_NULLS_LAST)).isEmpty());
    }
}
