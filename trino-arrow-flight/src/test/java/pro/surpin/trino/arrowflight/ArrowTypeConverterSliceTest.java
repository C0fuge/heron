package pro.surpin.trino.arrowflight;

import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link ArrowTypeConverter#toBlock(FieldVector, Type, int, int)} converts a slice of a batch (the sorted aggregation
 * merge converts server batches 8192 rows at a time); every slice must equal the same rows of the whole batch, and the
 * whole batch (bulk copies) must equal the value-by-value {@link ArrowTypeConverter#readNative}.
 */
class ArrowTypeConverterSliceTest
{
    private static final int ROWS = 23;
    private static final DecimalType SHORT_DECIMAL = createDecimalType(15, 2);
    private static final DecimalType LONG_DECIMAL = createDecimalType(38, 2);

    private final ArrowTypeConverter converter = new ArrowTypeConverter();
    private BufferAllocator allocator;

    @BeforeEach
    void setUp()
    {
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown()
    {
        allocator.close();
    }

    @Test
    void sliceEqualsTheSameRowsOfTheWholeBatch()
    {
        List<FieldVector> vectors = new ArrayList<>();
        List<Type> types = new ArrayList<>();
        try {
            BigIntVector bigints = new BigIntVector("b", allocator);
            IntVector ints = new IntVector("i", allocator);
            DateDayVector days = new DateDayVector("d", allocator);
            Float8Vector doubles = new Float8Vector("f", allocator);
            BitVector bits = new BitVector("x", allocator);
            DecimalVector shortDecimals = new DecimalVector("s", allocator, 15, 2);
            DecimalVector longDecimals = new DecimalVector("l", allocator, 38, 2);
            VarCharVector strings = new VarCharVector("v", allocator);
            UInt8Vector unsigned = new UInt8Vector("u", allocator);   // no fast path: converted value by value
            SmallIntVector shorts = new SmallIntVector("h", allocator);
            TinyIntVector tinies = new TinyIntVector("t", allocator);
            Float4Vector reals = new Float4Vector("r", allocator);
            TimeStampMicroVector micros = new TimeStampMicroVector("m", allocator);
            for (int row = 0; row < ROWS; row++) {
                if (row % 5 == 3) {
                    bigints.setNull(row);
                    ints.setNull(row);
                    days.setNull(row);
                    doubles.setNull(row);
                    bits.setNull(row);
                    shortDecimals.setNull(row);
                    longDecimals.setNull(row);
                    strings.setNull(row);
                    unsigned.setNull(row);
                    shorts.setNull(row);
                    tinies.setNull(row);
                    reals.setNull(row);
                    micros.setNull(row);
                    continue;
                }
                bigints.setSafe(row, row * 1_000_000_007L);
                ints.setSafe(row, -row);
                days.setSafe(row, 19000 + row);
                doubles.setSafe(row, row / 3.0);
                bits.setSafe(row, row % 2);
                shortDecimals.setSafe(row, new BigDecimal(row + ".25"));
                longDecimals.setSafe(row, new BigDecimal("12345678901234567890123." + (10 + row)));
                strings.setSafe(row, ("value-" + "x".repeat(row)).getBytes(StandardCharsets.UTF_8));
                unsigned.setSafe(row, row * 7L);
                shorts.setSafe(row, (short) (row * -1000));
                tinies.setSafe(row, (byte) (row * 5 - 50));
                reals.setSafe(row, row * -1.5f);
                micros.setSafe(row, 1_700_000_000_000_000L + row * 123_456_789L);
            }
            add(vectors, types, bigints, BIGINT);
            add(vectors, types, ints, INTEGER);
            add(vectors, types, days, DATE);
            add(vectors, types, doubles, DOUBLE);
            add(vectors, types, bits, BOOLEAN);
            add(vectors, types, shortDecimals, SHORT_DECIMAL);
            add(vectors, types, longDecimals, LONG_DECIMAL);
            add(vectors, types, strings, VARCHAR);
            add(vectors, types, unsigned, createDecimalType(20, 0));
            add(vectors, types, shorts, SMALLINT);
            add(vectors, types, tinies, TINYINT);
            add(vectors, types, reals, REAL);
            add(vectors, types, micros, TIMESTAMP_MICROS);
            for (FieldVector vector : vectors) {
                vector.setValueCount(ROWS);
            }

            for (int column = 0; column < vectors.size(); column++) {
                FieldVector vector = vectors.get(column);
                Type type = types.get(column);
                Block whole = converter.toBlock(vector, type, ROWS);
                // the bulk fast paths must give what the value-by-value reader gives
                for (int row = 0; row < ROWS; row++) {
                    Object expected = vector.isNull(row) ? null : converter.readNative(vector, type, row);
                    assertEquals(expected, readNativeValue(type, whole, row), type + " row " + row + " of the whole batch");
                }
                for (int offset = 0; offset < ROWS; offset += 4) {
                    for (int length : new int[] {0, 1, 4, ROWS - offset}) {
                        if (offset + length > ROWS) {
                            continue;
                        }
                        Block slice = converter.toBlock(vector, type, offset, length);
                        assertEquals(length, slice.getPositionCount());
                        for (int position = 0; position < length; position++) {
                            assertEquals(readNativeValue(type, whole, offset + position), readNativeValue(type, slice, position),
                                    type + " row " + (offset + position) + " in slice " + offset + "+" + length);
                        }
                    }
                }
            }
        }
        finally {
            vectors.forEach(FieldVector::close);
        }
    }

    private static void add(List<FieldVector> vectors, List<Type> types, FieldVector vector, Type type)
    {
        vectors.add(vector);
        types.add(type);
    }
}
