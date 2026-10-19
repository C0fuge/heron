package pro.surpin.trino.arrowflight;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

/**
 * Maps the Arrow schema/vectors returned by the Flight server onto Trino types and blocks.
 * Covers exactly the set the server advertises (see FlightTable.TYPE_MAPPING_DEFAULT in the
 * arrow-flight-server repo): BIT, integer family, FLOAT4/8, DECIMAL(256), (LARGE)VARCHAR,
 * DATE, TIME, TIMESTAMP, plus VARBINARY.
 */
public class ArrowTypeConverter
{
    private static final long PICOS_PER_MICRO = 1_000_000L;
    private static final long MILLIS_PER_DAY = 86_400_000L;

    public Type toTrinoType(Field field)
    {
        ArrowType arrowType = field.getType();
        return switch (arrowType.getTypeID()) {
            case Bool -> BOOLEAN;
            case Int -> {
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                yield switch (intType.getBitWidth()) {
                    case 8 -> intType.getIsSigned() ? TINYINT : SMALLINT;
                    case 16 -> intType.getIsSigned() ? SMALLINT : INTEGER;
                    case 32 -> intType.getIsSigned() ? INTEGER : BIGINT;
                    case 64 -> BIGINT;
                    default -> throw unsupported(field);
                };
            }
            case FloatingPoint -> switch (((ArrowType.FloatingPoint) arrowType).getPrecision()) {
                case SINGLE, HALF -> REAL;
                case DOUBLE -> DOUBLE;
            };
            case Decimal -> {
                ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
                yield DecimalType.createDecimalType(decimal.getPrecision(), decimal.getScale());
            }
            case Utf8, LargeUtf8 -> VarcharType.VARCHAR;
            case Binary, LargeBinary -> VARBINARY;
            case Date -> DATE;
            case Time -> TIME_MICROS;
            case Timestamp -> TIMESTAMP_MICROS;
            default -> throw unsupported(field);
        };
    }

    public Page toPage(org.apache.arrow.vector.VectorSchemaRoot root, List<ArrowFlightColumnHandle> columns)
    {
        int positionCount = root.getRowCount();
        Block[] blocks = new Block[columns.size()];

        for (int channel = 0; channel < columns.size(); channel++) {
            ArrowFlightColumnHandle column = columns.get(channel);
            Type type = column.getColumnType();
            BlockBuilder builder = type.createBlockBuilder(null, positionCount);

            FieldVector vector = root.getVector(column.getColumnName());
            if (vector == null) {
                for (int position = 0; position < positionCount; position++) {
                    builder.appendNull();
                }
            }
            else {
                for (int position = 0; position < positionCount; position++) {
                    if (vector.isNull(position)) {
                        builder.appendNull();
                    }
                    else {
                        writeValue(type, vector, builder, position);
                    }
                }
            }
            blocks[channel] = builder.build();
        }
        return new Page(positionCount, blocks);
    }

    private void writeValue(Type type, FieldVector vector, BlockBuilder builder, int position)
    {
        if (type.equals(BOOLEAN)) {
            BOOLEAN.writeBoolean(builder, ((BitVector) vector).get(position) != 0);
        }
        else if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
            type.writeLong(builder, ((BaseIntVector) vector).getValueAsLong(position));
        }
        else if (type.equals(REAL)) {
            REAL.writeLong(builder, Float.floatToRawIntBits(((Float4Vector) vector).get(position)));
        }
        else if (type.equals(DOUBLE)) {
            DOUBLE.writeDouble(builder, ((Float8Vector) vector).get(position));
        }
        else if (type instanceof DecimalType decimalType) {
            BigDecimal value = readDecimal(vector, position).setScale(decimalType.getScale());
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, value.unscaledValue().longValueExact());
            }
            else {
                decimalType.writeObject(builder, Int128.valueOf(value.unscaledValue()));
            }
        }
        else if (type instanceof VarcharType) {
            byte[] bytes = vector instanceof LargeVarCharVector large
                    ? large.get(position)
                    : ((VarCharVector) vector).get(position);
            type.writeSlice(builder, Slices.wrappedBuffer(bytes));
        }
        else if (type.equals(VARBINARY)) {
            type.writeSlice(builder, Slices.wrappedBuffer((byte[]) vector.getObject(position)));
        }
        else if (type.equals(DATE)) {
            if (vector instanceof DateDayVector dateDay) {
                DATE.writeLong(builder, dateDay.get(position));
            }
            else {
                DATE.writeLong(builder, Math.floorDiv(((DateMilliVector) vector).get(position), MILLIS_PER_DAY));
            }
        }
        else if (type.equals(TIMESTAMP_MICROS)) {
            TIMESTAMP_MICROS.writeLong(builder, toEpochMicros(vector, position));
        }
        else if (type.equals(TIME_MICROS)) {
            TIME_MICROS.writeLong(builder, toMicrosOfDay(vector, position) * PICOS_PER_MICRO);
        }
        else {
            throw new TrinoException(TYPE_MISMATCH, "Cannot convert Arrow vector "
                    + vector.getClass().getSimpleName() + " to Trino type " + type);
        }
    }

    private static BigDecimal readDecimal(FieldVector vector, int position)
    {
        if (vector instanceof DecimalVector decimal) {
            return decimal.getObject(position);
        }
        if (vector instanceof Decimal256Vector decimal256) {
            return decimal256.getObject(position);
        }
        throw new TrinoException(TYPE_MISMATCH, "Expected a decimal vector, got " + vector.getClass().getSimpleName());
    }

    private static long toEpochMicros(FieldVector vector, int position)
    {
        // Each concrete TimeStamp*Vector exposes get(int) returning the raw long in its own unit.
        if (vector instanceof TimeStampMicroVector v) {
            return v.get(position);
        }
        if (vector instanceof TimeStampMicroTZVector v) {
            return v.get(position);
        }
        if (vector instanceof TimeStampMilliVector v) {
            return v.get(position) * 1_000L;
        }
        if (vector instanceof TimeStampMilliTZVector v) {
            return v.get(position) * 1_000L;
        }
        if (vector instanceof TimeStampSecVector v) {
            return v.get(position) * 1_000_000L;
        }
        if (vector instanceof TimeStampSecTZVector v) {
            return v.get(position) * 1_000_000L;
        }
        if (vector instanceof TimeStampNanoVector v) {
            return Math.floorDiv(v.get(position), 1_000L);
        }
        if (vector instanceof TimeStampNanoTZVector v) {
            return Math.floorDiv(v.get(position), 1_000L);
        }
        throw new TrinoException(TYPE_MISMATCH, "Unsupported Arrow timestamp vector " + vector.getClass().getSimpleName());
    }

    private static long toMicrosOfDay(FieldVector vector, int position)
    {
        if (vector instanceof TimeMicroVector micro) {
            return micro.get(position);
        }
        if (vector instanceof TimeMilliVector milli) {
            return milli.get(position) * 1_000L;
        }
        if (vector instanceof TimeSecVector sec) {
            return sec.get(position) * 1_000_000L;
        }
        if (vector instanceof TimeNanoVector nano) {
            return Math.floorDiv(nano.get(position), 1_000L);
        }
        throw new TrinoException(TYPE_MISMATCH, "Unsupported Arrow time vector " + vector.getClass().getSimpleName());
    }

    private static TrinoException unsupported(Field field)
    {
        return new TrinoException(NOT_SUPPORTED,
                "Unsupported Arrow type for column '" + field.getName() + "': " + field.getType());
    }
}
