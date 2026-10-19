package pro.surpin.arrowflight.common;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Ordered failover of data streams over an endpoint's locations, against in-process Flight servers. */
class FlightSqlSessionFailoverTest
{
    private static final int ROWS = 100;

    private enum Mode { SERVE, FAIL_UNAVAILABLE, FAIL_INVALID, FAIL_AFTER_FIRST_BATCH, SLOW }

    /** Serves ROWS ints in two batches, or fails as told. */
    private static final class Node
            extends NoOpFlightProducer
    {
        private final BufferAllocator allocator;
        private final AtomicInteger calls = new AtomicInteger();
        private volatile Mode mode = Mode.SERVE;
        private volatile boolean stopping;
        private FlightServer server;

        Node(BufferAllocator allocator)
        {
            this.allocator = allocator;
        }

        Node start()
                throws IOException
        {
            server = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), this).build().start();
            return this;
        }

        String uri()
        {
            return "grpc+tcp://127.0.0.1:" + server.getPort();
        }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
        {
            calls.incrementAndGet();
            // a long computation before the first batch
            while (mode == Mode.SLOW && !stopping) {
                try {
                    Thread.sleep(50);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if (mode == Mode.FAIL_UNAVAILABLE) {
                listener.error(CallStatus.UNAVAILABLE.withDescription("going away").toRuntimeException());
                return;
            }
            if (mode == Mode.FAIL_INVALID) {
                listener.error(CallStatus.INVALID_ARGUMENT.withDescription("bad query").toRuntimeException());
                return;
            }
            Schema schema = new Schema(List.of(Field.nullable("v", new ArrowType.Int(32, true))));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                listener.start(root);
                for (int batch = 0; batch < 2; batch++) {
                    IntVector vector = (IntVector) root.getVector(0);
                    int rows = ROWS / 2;
                    vector.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        vector.set(i, batch * rows + i);
                    }
                    root.setRowCount(rows);
                    listener.putNext();
                    if (mode == Mode.FAIL_AFTER_FIRST_BATCH) {
                        listener.error(CallStatus.UNAVAILABLE.withDescription("node gone mid-stream").toRuntimeException());
                        return;
                    }
                }
                listener.completed();
            }
        }

        void close()
                throws InterruptedException
        {
            stopping = true;
            server.shutdown();
            server.awaitTermination(5, TimeUnit.SECONDS);
            server.close();
        }
    }

    /** Relays TCP to a node; once silent it drops everything, like a host that vanished. */
    private static final class SilencingProxy
            implements AutoCloseable
    {
        private final ServerSocket listener = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
        private final List<Socket> sockets = java.util.Collections.synchronizedList(new ArrayList<>());
        private volatile boolean silent;

        SilencingProxy(int targetPort)
                throws IOException
        {
            Thread acceptor = new Thread(() -> {
                while (!listener.isClosed()) {
                    try {
                        Socket client = listener.accept();
                        Socket target = new Socket(java.net.InetAddress.getLoopbackAddress(), targetPort);
                        sockets.add(client);
                        sockets.add(target);
                        pump(client, target);
                        pump(target, client);
                    }
                    catch (IOException e) {
                        return;
                    }
                }
            }, "proxy-accept");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        private void pump(Socket from, Socket to)
        {
            Thread pump = new Thread(() -> {
                byte[] buffer = new byte[8192];
                try {
                    int n;
                    while ((n = from.getInputStream().read(buffer)) >= 0) {
                        if (!silent) {
                            to.getOutputStream().write(buffer, 0, n);
                        }
                    }
                }
                catch (IOException ignored) {
                    // closed
                }
            }, "proxy-pump");
            pump.setDaemon(true);
            pump.start();
        }

        String uri()
        {
            return "grpc+tcp://127.0.0.1:" + listener.getLocalPort();
        }

        @Override
        public void close()
                throws IOException
        {
            listener.close();
            synchronized (sockets) {
                for (Socket socket : sockets) {
                    socket.close();
                }
            }
        }
    }

    private BufferAllocator allocator;
    private Node coordinator;
    private Node backup;
    private FlightSqlSession session;

    @BeforeEach
    void setUp()
            throws IOException
    {
        allocator = new RootAllocator(Long.MAX_VALUE);
        coordinator = new Node(allocator).start();
        backup = new Node(allocator).start();
        session = newSession(500);
    }

    private FlightSqlSession newSession(long connectTimeoutMillis)
    {
        String coordinatorUri = coordinator.uri();
        int port = Integer.parseInt(coordinatorUri.substring(coordinatorUri.lastIndexOf(':') + 1));
        return new FlightSqlSession(FlightConfig.builder("127.0.0.1")
                .port(port)
                .bearerToken("token")
                .maxRetries(0)
                .connectTimeoutMillis(connectTimeoutMillis)
                .keepAliveMillis(10_000)
                .build());
    }

    @AfterEach
    void tearDown()
            throws Exception
    {
        session.close();
        coordinator.close();
        backup.close();
        allocator.close();
    }

    private static List<Integer> readAll(FlightDataStream stream)
    {
        List<Integer> values = new ArrayList<>();
        while (stream.next()) {
            IntVector vector = (IntVector) stream.getRoot().getVector(0);
            for (int i = 0; i < stream.getRoot().getRowCount(); i++) {
                values.add(vector.get(i));
            }
        }
        return values;
    }

    private static String closedPortUri()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            return "grpc+tcp://127.0.0.1:" + socket.getLocalPort();
        }
    }

    @Test
    void refusedLocationFailsOverToTheNextOne()
            throws Exception
    {
        try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of(closedPortUri(), backup.uri()))) {
            assertEquals(ROWS, readAll(stream).size());
            assertEquals(backup.uri(), stream.location());
        }
        assertEquals(1, backup.calls.get());
    }

    @Test
    void connectTimeoutFailsOverWithinTheConfiguredTimeout()
            throws Exception
    {
        // A listener whose accept queue is full drops new SYNs: the connection attempt times out.
        try (ServerSocket blackhole = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            List<Socket> fillers = new ArrayList<>();
            try {
                for (int i = 0; i < 8; i++) {
                    Socket filler = new Socket();
                    try {
                        filler.connect(new InetSocketAddress(blackhole.getInetAddress(), blackhole.getLocalPort()), 200);
                    }
                    catch (IOException e) {
                        filler.close();
                        break;
                    }
                    fillers.add(filler);
                }
                String silent = "grpc+tcp://127.0.0.1:" + blackhole.getLocalPort();
                long start = System.nanoTime();
                try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of(silent, backup.uri()))) {
                    assertEquals(ROWS, readAll(stream).size());
                }
                long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                // >= the 500 ms connect timeout: the first location timed out rather than refused
                assertTrue(millis >= 450 && millis < 10_000, "failed over after " + millis + " ms");
                assertEquals(1, backup.calls.get());
            }
            finally {
                for (Socket filler : fillers) {
                    filler.close();
                }
            }
        }
    }

    @Test
    void unavailableBeforeAnyDataFailsOver()
    {
        coordinator.mode = Mode.FAIL_UNAVAILABLE;
        try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of(coordinator.uri(), backup.uri()))) {
            assertEquals(ROWS, readAll(stream).size());
        }
        assertEquals(1, coordinator.calls.get());
        assertEquals(1, backup.calls.get());
    }

    @Test
    void nodeThatGoesSilentBeforeAnyDataIsDetectedByKeepaliveAndFailedOver()
            throws Exception
    {
        Node slow = new Node(allocator).start();
        slow.mode = Mode.SLOW;
        int slowPort = Integer.parseInt(slow.uri().substring(slow.uri().lastIndexOf(':') + 1));
        try (SilencingProxy proxy = new SilencingProxy(slowPort)) {
            long start = System.nanoTime();
            try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of(proxy.uri(), backup.uri()))) {
                Thread silencer = new Thread(() -> {
                    while (slow.calls.get() == 0) {
                        Thread.onSpinWait();
                    }
                    proxy.silent = true;
                });
                silencer.setDaemon(true);
                silencer.start();
                assertEquals(ROWS, readAll(stream).size());
                assertEquals(backup.uri(), stream.location());
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            // gRPC pings at most every 10 s; the ping is not answered within the 500 ms connect timeout
            assertTrue(millis >= 9_000 && millis < 25_000, "failed over after " + millis + " ms");
            assertEquals(1, slow.calls.get());
            assertEquals(1, backup.calls.get());
        }
        finally {
            slow.close();
        }
    }

    @Test
    void failureAfterDataIsFinalSoNoRowIsReadTwice()
    {
        coordinator.mode = Mode.FAIL_AFTER_FIRST_BATCH;
        try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of(coordinator.uri(), backup.uri()))) {
            assertTrue(stream.next());
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class, () -> readAll(stream));
            assertEquals(FlightStatusCode.UNAVAILABLE, e.status().code());
        }
        assertEquals(0, backup.calls.get());
    }

    @Test
    void serverRefusingTheQueryIsNotFailedOver()
    {
        coordinator.mode = Mode.FAIL_INVALID;
        try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of(coordinator.uri(), backup.uri()))) {
            FlightRuntimeException e = assertThrows(FlightRuntimeException.class, stream::next);
            assertEquals(FlightStatusCode.INVALID_ARGUMENT, e.status().code());
        }
        assertEquals(0, backup.calls.get());
    }

    @Test
    void lastLocationFailureIsReported()
            throws Exception
    {
        try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of(closedPortUri(), closedPortUri()))) {
            RuntimeException e = assertThrows(RuntimeException.class, stream::next);
            assertTrue(FlightDataStream.isUnreachable(e), e.toString());
        }
    }

    @Test
    void singleLocationAndPooledConnectionsStillWork()
    {
        for (int i = 0; i < 3; i++) {
            try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of(backup.uri()))) {
                assertEquals(ROWS, readAll(stream).size());
            }
        }
        // no location: the coordinator
        try (FlightDataStream stream = session.openStream(new byte[] {1}, List.of())) {
            assertEquals(ROWS, readAll(stream).size());
        }
        assertEquals(3, backup.calls.get());
        assertEquals(1, coordinator.calls.get());
    }

    @Test
    void locationsKeepTheirOrderAndNameTheCoordinatorAsConfigured()
    {
        String coordinatorUri = coordinator.uri();
        assertEquals(List.of("grpc+tcp://n2:32010", coordinatorUri, "grpc+tcp://n3:32010"),
                session.streamLocations(List.of("grpc+tcp://n2:32010", coordinatorUri.replace("grpc+tcp", "grpc"),
                        "grpc+tcp://n3:32010", "grpc+tcp://n2:32010")));
        assertEquals(List.of(coordinatorUri), session.streamLocations(List.of()));
        assertEquals(List.of(coordinatorUri), session.streamLocations(List.of(coordinatorUri)));
    }

    @Test
    void unreachableMeansUnavailableOrAConnectError()
    {
        assertTrue(FlightDataStream.isUnreachable(CallStatus.UNAVAILABLE.toRuntimeException()));
        assertTrue(FlightDataStream.isUnreachable(new RuntimeException(new java.net.ConnectException("refused"))));
        assertTrue(FlightDataStream.isUnreachable(new RuntimeException(new java.net.UnknownHostException("n3"))));
        assertFalse(FlightDataStream.isUnreachable(CallStatus.INTERNAL.toRuntimeException()));
        assertFalse(FlightDataStream.isUnreachable(CallStatus.UNAUTHENTICATED.toRuntimeException()));
        assertFalse(FlightDataStream.isUnreachable(CallStatus.CANCELLED.toRuntimeException()));
        assertFalse(FlightDataStream.isUnreachable(new IllegalStateException("closed")));
    }
}
