package pro.surpin.arrowflight.common;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in result compression ({@link FlightConfig#getResultCompression()}): the {@code arrow-compression} header on
 * data streams and decoding of compressed IPC bodies, against in-process Flight servers that compress as the server
 * does (a codec in the stream's {@code VectorUnloader}).
 */
class FlightSqlSessionCompressionTest
{
    private static final int ROWS = 3000;
    private static final int BATCHES = 3;
    private static final FlightServerMiddleware.Key<Headers> HEADERS = FlightServerMiddleware.Key.of("headers");

    private enum Mode { SERVE, FAIL_UNAVAILABLE, FAIL_INVALID, END_WITHOUT_SCHEMA }

    private record Headers(String compression)
            implements FlightServerMiddleware
    {
        @Override
        public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

        @Override
        public void onCallCompleted(CallStatus status) {}

        @Override
        public void onCallErrored(Throwable err) {}
    }

    /** Serves ROWS (bigint, varchar) rows in BATCHES batches, compressed as the arrow-compression header asks. */
    private static final class Node
            extends NoOpFlightProducer
    {
        private final BufferAllocator allocator;
        // arrow-compression header of every DoGet, "-" when absent
        private final List<String> requested = new CopyOnWriteArrayList<>();
        private final List<String> otherCalls = new CopyOnWriteArrayList<>();
        private final AtomicInteger compressedBatches = new AtomicInteger();
        private volatile Mode mode = Mode.SERVE;
        // compress with this codec whatever the client asked
        private volatile CompressionUtil.CodecType forced;
        private FlightServer server;

        Node(BufferAllocator allocator)
        {
            this.allocator = allocator;
        }

        Node start()
                throws IOException
        {
            FlightServerMiddleware.Factory<Headers> headers = (CallInfo info, CallHeaders incoming, RequestContext context) -> {
                String value = incoming.get(StreamDecompression.HEADER);
                if (info.method() != FlightMethod.DO_GET && value != null) {
                    otherCalls.add(info.method() + "=" + value);
                }
                return new Headers(value);
            };
            server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), this)
                    .middleware(HEADERS, headers)
                    .build()
                    .start();
            return this;
        }

        int port()
        {
            return server.getPort();
        }

        String uri()
        {
            return "grpc+tcp://127.0.0.1:" + server.getPort();
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
        {
            String value = context.getMiddleware(HEADERS).compression();
            requested.add(value == null ? "-" : value);
            if (mode == Mode.FAIL_UNAVAILABLE) {
                listener.error(CallStatus.UNAVAILABLE.withDescription("going away").toRuntimeException());
                return;
            }
            if (mode == Mode.FAIL_INVALID) {
                listener.error(CallStatus.INVALID_ARGUMENT.withDescription("codec not available").toRuntimeException());
                return;
            }
            if (mode == Mode.END_WITHOUT_SCHEMA) {
                listener.completed();
                return;
            }
            CompressionUtil.CodecType codec = forced != null ? forced : codecOf(value);
            Schema schema = new Schema(List.of(
                    Field.nullable("id", new ArrowType.Int(64, true)),
                    Field.nullable("name", ArrowType.Utf8.INSTANCE)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                listener.start(root);
                if (codec != CompressionUtil.CodecType.NO_COMPRESSION) {
                    compressWith(listener, root, codec);
                }
                int rows = ROWS / BATCHES;
                for (int batch = 0; batch < BATCHES; batch++) {
                    BigIntVector ids = (BigIntVector) root.getVector(0);
                    VarCharVector names = (VarCharVector) root.getVector(1);
                    ids.allocateNew(rows);
                    names.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        int row = batch * rows + i;
                        ids.set(i, row);
                        if (row % 7 == 0) {
                            names.setNull(i);
                        }
                        else {
                            names.setSafe(i, ("name-" + row).getBytes(StandardCharsets.UTF_8));
                        }
                    }
                    root.setRowCount(rows);
                    listener.putNext();
                    if (codec != CompressionUtil.CodecType.NO_COMPRESSION) {
                        compressedBatches.incrementAndGet();
                    }
                }
                listener.completed();
            }
        }

        private static CompressionUtil.CodecType codecOf(String value)
        {
            if (value == null) {
                return CompressionUtil.CodecType.NO_COMPRESSION;
            }
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "zstd" -> CompressionUtil.CodecType.ZSTD;
                case "lz4" -> CompressionUtil.CodecType.LZ4_FRAME;
                default -> throw CallStatus.INVALID_ARGUMENT.withDescription("unknown codec " + value).toRuntimeException();
            };
        }

        /** As the server does: the listener's unloader gets a codec, so every batch body is compressed. */
        private static void compressWith(ServerStreamListener listener, VectorSchemaRoot root, CompressionUtil.CodecType codec)
        {
            try {
                java.lang.reflect.Field unloader = Class.forName("org.apache.arrow.flight.OutboundStreamListenerImpl")
                        .getDeclaredField("unloader");
                unloader.setAccessible(true);
                unloader.set(listener, new VectorUnloader(root, true, CommonsCompressionFactory.INSTANCE.createCodec(codec), true));
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }

        void close()
                throws InterruptedException
        {
            server.close();
        }
    }

    private BufferAllocator allocator;
    private Node coordinator;
    private Node backup;

    @BeforeEach
    void setUp()
            throws IOException
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        coordinator = new Node(allocator).start();
        backup = new Node(allocator).start();
    }

    @AfterEach
    void tearDown()
            throws Exception
    {
        coordinator.close();
        backup.close();
        allocator.close();
    }

    private FlightSqlSession session(ResultCompression compression)
    {
        return new FlightSqlSession(FlightConfig.builder("127.0.0.1")
                .port(coordinator.port())
                .bearerToken("token")
                .maxRetries(0)
                .connectTimeoutMillis(500)
                .resultCompression(compression)
                .build());
    }

    private static List<String> readAll(FlightDataStream stream)
    {
        List<String> rows = new ArrayList<>();
        while (stream.next()) {
            VectorSchemaRoot root = stream.getRoot();
            BigIntVector ids = (BigIntVector) root.getVector(0);
            VarCharVector names = (VarCharVector) root.getVector(1);
            for (int i = 0; i < root.getRowCount(); i++) {
                rows.add(ids.get(i) + "|" + (names.isNull(i) ? "NULL" : new String(names.get(i), StandardCharsets.UTF_8)));
            }
        }
        return rows;
    }

    private static List<String> expectedRows()
    {
        List<String> rows = new ArrayList<>();
        for (int row = 0; row < ROWS; row++) {
            rows.add(row + "|" + (row % 7 == 0 ? "NULL" : "name-" + row));
        }
        return rows;
    }

    private List<String> read(ResultCompression compression, List<String> locations)
    {
        try (FlightSqlSession session = session(compression);
                FlightDataStream stream = session.openStream(new byte[] {1}, locations)) {
            return readAll(stream);
        }
    }

    @Test
    void offByDefaultSendsNoHeader()
    {
        assertEquals(ResultCompression.NONE, FlightConfig.builder("h").build().getResultCompression());
        assertEquals(expectedRows(), read(ResultCompression.NONE, List.of(coordinator.uri())));
        assertEquals(List.of("-"), coordinator.requested);
        assertEquals(0, coordinator.compressedBatches.get());
    }

    @Test
    void zstdStreamIsAskedForAndDecoded()
    {
        assertEquals(expectedRows(), read(ResultCompression.ZSTD, List.of(coordinator.uri())));
        assertEquals(List.of("zstd"), coordinator.requested);
        assertEquals(BATCHES, coordinator.compressedBatches.get());
    }

    @Test
    void lz4StreamIsAskedForAndDecoded()
    {
        assertEquals(expectedRows(), read(ResultCompression.LZ4, List.of(coordinator.uri())));
        assertEquals(List.of("lz4"), coordinator.requested);
        assertEquals(BATCHES, coordinator.compressedBatches.get());
    }

    @Test
    void onlyDataStreamsAskForCompression()
    {
        try (FlightSqlSession session = session(ResultCompression.ZSTD)) {
            session.getFlightInfo("SELECT 1");
        }
        catch (RuntimeException e) {
            // NoOpFlightProducer has no flight info; only the headers of the call matter
        }
        assertEquals(List.of(), coordinator.otherCalls);
    }

    @Test
    void serverThatIgnoresTheHeaderIsReadUnchanged()
    {
        // an old server: plain bodies whatever was asked
        coordinator.forced = CompressionUtil.CodecType.NO_COMPRESSION;
        assertEquals(expectedRows(), read(ResultCompression.ZSTD, List.of(coordinator.uri())));
        assertEquals(List.of("zstd"), coordinator.requested);
        assertEquals(0, coordinator.compressedBatches.get());
    }

    @Test
    void compressedBodiesNeedTheDecodingLoader()
    {
        // what the opt-in is for: without it, Arrow Java's FlightStream cannot load a compressed batch
        coordinator.forced = CompressionUtil.CodecType.ZSTD;
        assertThrows(RuntimeException.class, () -> read(ResultCompression.NONE, List.of(coordinator.uri())));
    }

    @Test
    void unavailableBeforeAnyDataStillFailsOver()
    {
        coordinator.mode = Mode.FAIL_UNAVAILABLE;
        assertEquals(expectedRows(), read(ResultCompression.ZSTD, List.of(coordinator.uri(), backup.uri())));
        assertEquals(List.of("zstd"), coordinator.requested);
        assertEquals(List.of("zstd"), backup.requested);
        assertEquals(BATCHES, backup.compressedBatches.get());
    }

    @Test
    void refusedLocationStillFailsOver()
            throws Exception
    {
        String closed;
        try (ServerSocket socket = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            closed = "grpc+tcp://127.0.0.1:" + socket.getLocalPort();
        }
        assertEquals(expectedRows(), read(ResultCompression.LZ4, List.of(closed, backup.uri())));
        assertEquals(List.of("lz4"), backup.requested);
    }

    @Test
    void serverRefusingTheCodecIsNotFailedOver()
    {
        // a server without the codec answers INVALID_ARGUMENT: a server error, never read elsewhere
        coordinator.mode = Mode.FAIL_INVALID;
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                () -> read(ResultCompression.LZ4, List.of(coordinator.uri(), backup.uri())));
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code());
        assertEquals(List.of(), backup.requested);
    }

    @Test
    void streamThatEndsWithoutASchemaDoesNotHang()
    {
        coordinator.mode = Mode.END_WITHOUT_SCHEMA;
        List<String> rows = assertTimeoutPreemptively(Duration.ofSeconds(20),
                () -> read(ResultCompression.ZSTD, List.of(coordinator.uri())));
        assertTrue(rows.isEmpty());
        assertEquals(List.of("zstd"), coordinator.requested);
    }

    @Test
    void decompressionIsAvailable()
    {
        StreamDecompression.checkAvailable();
        assertNotNull(ResultCompression.ZSTD.headerValue().orElse(null));
        assertFalse(ResultCompression.NONE.headerValue().isPresent());
    }
}
