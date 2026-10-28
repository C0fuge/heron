package pro.surpin.arrowflight.common;

import java.util.Optional;

/**
 * Compression of the Arrow IPC bodies of a data stream, asked for with the {@code arrow-compression} call header.
 * A server that does not know the header ignores it and sends plain bodies, which are read as before.
 */
public enum ResultCompression
{
    NONE(null),
    ZSTD("zstd"),
    LZ4("lz4");

    private final String headerValue;

    ResultCompression(String headerValue)
    {
        this.headerValue = headerValue;
    }

    /** Value of the {@code arrow-compression} header; empty = the header is not sent. */
    public Optional<String> headerValue()
    {
        return Optional.ofNullable(headerValue);
    }
}
