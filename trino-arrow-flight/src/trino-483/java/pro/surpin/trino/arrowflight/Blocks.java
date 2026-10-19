package pro.surpin.trino.arrowflight;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;

import java.util.Optional;

/**
 * Builds flat blocks from arrays. {@code isNull} is null when the column has no nulls.
 * From SPI 483 the null mask is a validity bitmap (bit set = value present, LSB first).
 */
final class Blocks
{
    private Blocks() {}

    private static Optional<long[]> nulls(boolean[] isNull)
    {
        if (isNull == null) {
            return Optional.empty();
        }
        long[] valid = new long[(isNull.length + 63) >>> 6];
        for (int i = 0; i < isNull.length; i++) {
            if (!isNull[i]) {
                valid[i >>> 6] |= 1L << (i & 63);
            }
        }
        return Optional.of(valid);
    }

    static Block longs(int positions, boolean[] isNull, long[] values)
    {
        return new LongArrayBlock(positions, nulls(isNull), values);
    }

    static Block ints(int positions, boolean[] isNull, int[] values)
    {
        return new IntArrayBlock(positions, nulls(isNull), values);
    }

    static Block shorts(int positions, boolean[] isNull, short[] values)
    {
        return new ShortArrayBlock(positions, nulls(isNull), values);
    }

    static Block bytes(int positions, boolean[] isNull, byte[] values)
    {
        return new ByteArrayBlock(positions, nulls(isNull), values);
    }

    /** {@code values} holds (high, low) pairs. */
    static Block int128s(int positions, boolean[] isNull, long[] values)
    {
        return new Int128ArrayBlock(positions, nulls(isNull), values);
    }

    static Block variableWidth(int positions, boolean[] isNull, Slice data, int[] offsets)
    {
        return new VariableWidthBlock(positions, data, offsets, nulls(isNull));
    }
}
