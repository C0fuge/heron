// Arrow Flight SQL connector for StarRocks — metadata bridge. Compiled only with -Pstarrocks.
// See ArrowFlightConnector.java for scope and wiring notes.

package com.starrocks.connector.arrowflight;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.ConnectContext;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import pro.surpin.arrowflight.common.FlightSqlSession;

import java.util.ArrayList;
import java.util.List;

public class ArrowFlightMetadata
        implements ConnectorMetadata
{
    private final String catalogName;
    private final FlightSqlSession session;

    public ArrowFlightMetadata(String catalogName, FlightSqlSession session)
    {
        this.catalogName = catalogName;
        this.session = session;
    }

    @Override
    public List<String> listDbNames(ConnectContext context)
    {
        return session.listSchemas();
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName)
    {
        return session.listTables(dbName);
    }

    @Override
    public Database getDb(ConnectContext context, String name)
    {
        // A stable synthetic id is fine for an external, read-only catalog.
        return new Database(Math.abs((catalogName + "." + name).hashCode()), name);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName)
    {
        List<Column> columns = resolveColumns(dbName, tblName);

        // Column mapping is done; what is missing is a scannable Table + a BE-side reader.
        // Building an ArrowFlightTable/ArrowFlightScanNode and the C++ BE scanner is the
        // remaining native work (see DESIGN.md). Until then, point users at the JDBC route.
        throw new UnsupportedOperationException(
                "arrow_flight catalog resolves metadata (" + columns.size() + " columns for "
                        + dbName + "." + tblName + ") but has no scan path yet. "
                        + "Use a JDBC external catalog with the Arrow Flight SQL JDBC driver "
                        + "(see starrocks-arrow-flight/jdbc-catalog/).");
    }

    @Override
    public void shutdown()
    {
        // FlightSqlSession is owned and closed by ArrowFlightConnector.
    }

    // ------------------------------------------------------------------------

    List<Column> resolveColumns(String dbName, String tblName)
    {
        Schema arrow = session.resultSchema(
                "SELECT * FROM \"" + dbName.replace("\"", "\"\"") + "\".\"" + tblName.replace("\"", "\"\"") + "\"");
        List<Column> columns = new ArrayList<>();
        for (Field field : arrow.getFields()) {
            // NOTE: the com.starrocks.catalog.Column constructor arity has changed across
            // StarRocks releases; adjust for your target (this is the 3.2/3.3 form).
            columns.add(new Column(field.getName(), toStarRocksType(field), false, null, field.isNullable(), null, ""));
        }
        return columns;
    }

    static Type toStarRocksType(Field field)
    {
        ArrowType arrow = field.getType();
        return switch (arrow.getTypeID()) {
            case Bool -> Type.BOOLEAN;
            case Int -> switch (((ArrowType.Int) arrow).getBitWidth()) {
                case 8 -> Type.TINYINT;
                case 16 -> Type.SMALLINT;
                case 32 -> Type.INT;
                default -> Type.BIGINT;
            };
            case FloatingPoint -> switch (((ArrowType.FloatingPoint) arrow).getPrecision()) {
                case SINGLE, HALF -> Type.FLOAT;
                case DOUBLE -> Type.DOUBLE;
            };
            case Utf8, LargeUtf8 -> Type.STRING;
            case Binary, LargeBinary -> Type.VARBINARY;
            case Date -> Type.DATE;
            case Timestamp -> Type.DATETIME;
            case Decimal -> {
                ArrowType.Decimal d = (ArrowType.Decimal) arrow;
                yield ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, d.getPrecision(), d.getScale());
            }
            default -> throw new UnsupportedOperationException("Unsupported Arrow type: " + arrow);
        };
    }
}
