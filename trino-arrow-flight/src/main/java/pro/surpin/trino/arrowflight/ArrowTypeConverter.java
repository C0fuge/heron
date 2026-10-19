package pro.surpin.trino.arrowflight;

import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
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
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

/**
 * Maps the Arrow schema/vectors returned by the Flight server onto Trino types and blocks.
 * <p>
 * Common vector/type pairs are converted a whole vector at a time straight from the Arrow
 * buffers into flat Trino arrays. Everything else (and mismatches between the type the
 * server declares and the vector it actually sends, e.g. sum(decimal) declared DOUBLE but
 * streamed as DECIMAL(38,s)) goes through {@link #readNative} value by value.
 */
public class ArrowTypeConverter
{
    private static final long PICOS_PER_MICRO = 1_000_000L;
    private static final DecimalType UNSIGNED_BIGINT = DecimalType.createDecimalType(20, 0);
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
                    // Unsigned 64-bit values above Long.MAX_VALUE need 20 decimal digits.
                    case 64 -> intType.getIsSigned() ? BIGINT : UNSIGNED_BIGINT;
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
            case Timestamp -> {
                ArrowType.Timestamp timestamp = (ArrowType.Timestamp) arrowType;
                int precision = timestamp.getUnit() == TimeUnit.NANOSECOND ? 9 : 6;
                yield timestamp.getTimezone() == null
                        ? TimestampType.createTimestampType(precision)
                        : TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision);
            }
            default -> throw unsupported(field);
        };
    }

    /** Converts the first {@code positions} values of {@code vector} into a block of {@code type}. */
    public Block toBlock(FieldVector vector, Type type, int positions)
    {
        boolean[] isNull = nulls(vector, positions);

        if (type.equals(BIGINT) && vector instanceof BigIntVector v) {
            ArrowBuf data = v.getDataBuffer();
            long[] values = new long[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = data.getLong((long) i * 8);
            }
            return Blocks.longs(positions, isNull, values);
        }
        if ((type.equals(INTEGER) && vector instanceof IntVector) || (type.equals(DATE) && vector instanceof DateDayVector)
                || (type.equals(REAL) && vector instanceof Float4Vector)) {
            ArrowBuf data = vector.getDataBuffer();
            int[] values = new int[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = data.getInt((long) i * 4);
            }
            return Blocks.ints(positions, isNull, values);
        }
        if (type.equals(SMALLINT) && vector instanceof SmallIntVector v) {
            ArrowBuf data = v.getDataBuffer();
            short[] values = new short[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = data.getShort((long) i * 2);
            }
            return Blocks.shorts(positions, isNull, values);
        }
        if (type.equals(TINYINT) && vector instanceof TinyIntVector v) {
            ArrowBuf data = v.getDataBuffer();
            byte[] values = new byte[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = data.getByte(i);
            }
            return Blocks.bytes(positions, isNull, values);
        }
        if (type.equals(DOUBLE) && vector instanceof Float8Vector v) {
            ArrowBuf data = v.getDataBuffer();
            long[] values = new long[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = Double.doubleToLongBits(data.getDouble((long) i * 8));
            }
            return Blocks.longs(positions, isNull, values);
        }
        if (type.equals(TIMESTAMP_MICROS) && (vector instanceof TimeStampMicroVector || vector instanceof TimeStampMicroTZVector)) {
            ArrowBuf data = vector.getDataBuffer();
            long[] values = new long[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = data.getLong((long) i * 8);
            }
            return Blocks.longs(positions, isNull, values);
        }
        if (type.equals(BOOLEAN) && vector instanceof BitVector v) {
            byte[] values = new byte[positions];
            for (int i = 0; i < positions; i++) {
                values[i] = (byte) (v.isNull(i) ? 0 : v.get(i));
            }
            return Blocks.bytes(positions, isNull, values);
        }
        if (type instanceof DecimalType decimal && vector instanceof DecimalVector v && v.getScale() == decimal.getScale()) {
            ArrowBuf data = v.getDataBuffer();
            if (decimal.isShort()) {
                long[] values = new long[positions];
                for (int i = 0; i < positions; i++) {
                    long low = data.getLong((long) i * 16);
                    long high = data.getLong((long) i * 16 + 8);
                    if (high != (low >> 63) && (isNull == null || !isNull[i])) {
                        throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal value does not fit " + type);
                    }
                    values[i] = low;
                }
                return Blocks.longs(positions, isNull, values);
            }
            long[] values = new long[positions * 2];
            for (int i = 0; i < positions; i++) {
                values[2 * i] = data.getLong((long) i * 16 + 8);
                values[2 * i + 1] = data.getLong((long) i * 16);
            }
            return Blocks.int128s(positions, isNull, values);
        }
        if ((type instanceof VarcharType && vector instanceof VarCharVector) || (type.equals(VARBINARY) && vector instanceof VarBinaryVector)) {
            ArrowBuf offsetsBuffer = vector.getOffsetBuffer();
            int[] offsets = new int[positions + 1];
            int base = offsetsBuffer.getInt(0);
            for (int i = 0; i <= positions; i++) {
                offsets[i] = offsetsBuffer.getInt((long) i * 4) - base;
            }
            byte[] bytes = new byte[offsets[positions]];
            vector.getDataBuffer().getBytes(base, bytes);
            return Blocks.variableWidth(positions, isNull, Slices.wrappedBuffer(bytes), offsets);
        }

        BlockBuilder builder = type.createBlockBuilder(null, positions);
        for (int i = 0; i < positions; i++) {
            writeNativeValue(type, builder, vector.isNull(i) ? null : readNative(vector, type, i));
        }
        return builder.build();
    }

    /** A block of {@code positions} nulls (column the server did not return). */
    public Block nullBlock(Type type, int positions)
    {
        BlockBuilder builder = type.createBlockBuilder(null, positions);
        for (int i = 0; i < positions; i++) {
            builder.appendNull();
        }
        return builder.build();
    }

    private static boolean[] nulls(FieldVector vector, int positions)
    {
        if (vector.getNullCount() == 0) {
            return null;
        }
        boolean[] isNull = new boolean[positions];
        for (int i = 0; i < positions; i++) {
            isNull[i] = vector.isNull(i);
        }
        return isNull;
    }

    /**
     * One non-null value in Trino's native stack representation for {@code type}
     * (long / double / boolean / Slice / Int128), converting between numeric families.
     */
    public Object readNative(FieldVector vector, Type type, int position)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            if (vector instanceof BaseIntVector ints) {
                return ints.getValueAsLong(position);
            }
            BigDecimal value = readDecimal(vector, position);
            try {
                return value.setScale(0, RoundingMode.UNNECESSARY).longValueExact();
            }
            catch (ArithmeticException e) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value " + value + " out of range for " + type);
            }
        }
        if (type.equals(DOUBLE)) {
            if (vector instanceof Float8Vector doubles) {
                return doubles.get(position);
            }
            if (vector instanceof BaseIntVector ints) {
                return (double) ints.getValueAsLong(position);
            }
            return readDecimal(vector, position).doubleValue();
        }
        if (type instanceof DecimalType decimal) {
            BigDecimal value;
            if (vector instanceof UInt8Vector unsigned) {
                value = new BigDecimal(unsigned.getObjectNoOverflow(position));
            }
            else if (vector instanceof Float8Vector doubles) {
                value = BigDecimal.valueOf(doubles.get(position));
            }
            else if (vector instanceof BaseIntVector ints) {
                value = BigDecimal.valueOf(ints.getValueAsLong(position));
            }
            else {
                value = readDecimal(vector, position);
            }
            return encodeDecimal(value, decimal);
        }
        if (type.equals(BOOLEAN)) {
            return ((BitVector) vector).get(position) != 0;
        }
        if (type.equals(REAL)) {
            return (long) Float.floatToRawIntBits(((Float4Vector) vector).get(position));
        }
        if (type instanceof VarcharType) {
            byte[] bytes = vector instanceof LargeVarCharVector large
                    ? large.get(position)
                    : ((VarCharVector) vector).get(position);
            return Slices.wrappedBuffer(bytes);
        }
        if (type.equals(VARBINARY)) {
            return Slices.wrappedBuffer((byte[]) vector.getObject(position));
        }
        if (type.equals(DATE)) {
            if (vector instanceof DateDayVector dateDay) {
                return (long) dateDay.get(position);
            }
            return Math.floorDiv(((DateMilliVector) vector).get(position), MILLIS_PER_DAY);
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return toEpochMicros(vector, position);
        }
        if (type instanceof TimestampType) {
            // Nanosecond precision: microseconds plus the remaining picoseconds.
            return new LongTimestamp(toEpochMicros(vector, position), extraPicosOfMicro(vector, position));
        }
        if (type instanceof TimestampWithTimeZoneType zoned) {
            long micros = toEpochMicros(vector, position);
            long millis = Math.floorDiv(micros, 1_000L);
            int picosOfMilli = (int) Math.floorMod(micros, 1_000L) * 1_000_000 + extraPicosOfMicro(vector, position);
            TimeZoneKey zone = zoneKey(vector);
            return zoned.isShort()
                    ? DateTimeEncoding.packDateTimeWithZone(millis, zone)
                    : LongTimestampWithTimeZone.fromEpochMillisAndFraction(millis, picosOfMilli, zone);
        }
        if (type.equals(TIME_MICROS)) {
            return toMicrosOfDay(vector, position) * PICOS_PER_MICRO;
        }
        throw new TrinoException(TYPE_MISMATCH, "Cannot convert Arrow vector "
                + vector.getClass().getSimpleName() + " to Trino type " + type);
    }

    /** BigDecimal -> native decimal (long for short decimals, Int128 otherwise), rounding half up. */
    public static Object encodeDecimal(BigDecimal value, DecimalType type)
    {
        BigDecimal scaled = value.setScale(type.getScale(), RoundingMode.HALF_UP);
        if (scaled.precision() > type.getPrecision()) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Value " + value + " does not fit " + type);
        }
        if (type.isShort()) {
            return scaled.unscaledValue().longValueExact();
        }
        return Int128.valueOf(scaled.unscaledValue());
    }

    /** Native decimal -> BigDecimal. */
    public static BigDecimal decodeDecimal(Object value, DecimalType type)
    {
        if (type.isShort()) {
            return BigDecimal.valueOf((Long) value, type.getScale());
        }
        return new BigDecimal(((Int128) value).toBigInteger(), type.getScale());
    }

    /** Native value -> the Long / Double / BigDecimal form {@link Expr#evaluate} works with. */
    public static Object toNumber(Object value, Type type)
    {
        if (value == null) {
            return null;
        }
        if (type instanceof DecimalType decimal) {
            return decodeDecimal(value, decimal);
        }
        return value;
    }

    /** Inverse of {@link #toNumber}. */
    public static Object fromNumber(Object value, Type type)
    {
        if (value == null) {
            return null;
        }
        if (type instanceof DecimalType decimal) {
            return encodeDecimal((BigDecimal) value, decimal);
        }
        return value;
    }

    private static BigDecimal readDecimal(FieldVector vector, int position)
    {
        if (vector instanceof DecimalVector decimal) {
            return decimal.getObject(position);
        }
        if (vector instanceof Decimal256Vector decimal256) {
            return decimal256.getObject(position);
        }
        throw new TrinoException(TYPE_MISMATCH, "Expected a numeric vector, got " + vector.getClass().getSimpleName());
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

    private static int extraPicosOfMicro(FieldVector vector, int position)
    {
        if (vector instanceof TimeStampNanoVector v) {
            return (int) Math.floorMod(v.get(position), 1_000L) * 1_000;
        }
        if (vector instanceof TimeStampNanoTZVector v) {
            return (int) Math.floorMod(v.get(position), 1_000L) * 1_000;
        }
        return 0;
    }

    /** The zone named in the Arrow field (values themselves are UTC instants); UTC if unknown. */
    private static TimeZoneKey zoneKey(FieldVector vector)
    {
        String zone = vector.getField().getType() instanceof ArrowType.Timestamp timestamp ? timestamp.getTimezone() : null;
        if (zone == null) {
            return TimeZoneKey.UTC_KEY;
        }
        try {
            return TimeZoneKey.getTimeZoneKey(zone);
        }
        catch (RuntimeException e) {
            return TimeZoneKey.UTC_KEY;
        }
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
