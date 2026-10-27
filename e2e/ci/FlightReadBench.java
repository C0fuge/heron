// Plugin-only Flight read benchmark (STATUS Stage 2: plugin throughput). Diagnostics only (profile.sh), not shipped.
//
// Reads one scan through the plugin's own Flight client (arrow-flight-common FlightSqlSession / FlightDataStream, the
// plugin's jars on the classpath, same headers, pooling and failover code) and only drains the Arrow batches: no Trino,
// no Arrow->Block conversion. Compared with the direct ADBC times (C++/Go client) and the Trino scan this separates the
// Java Flight client from the server and from Trino.
//
// Modes per pass (name = s<splits><p|q>): splits 1 = the scan as one statement, 4 = the plugin's range splits on the first
// column (same predicates as AbstractArrowFlightSplitManager), each split on its own thread like Trino drivers;
// q = a statement's endpoints read one after the other (endpoint_read_parallelism 1, the default), p = all in parallel.
//
// usage: java -cp '<plugin dir>/*' FlightReadBench.java host port token schema scan.sql repeat [modes...]
// prints: mode <TAB> best seconds <TAB> GetFlightInfo seconds <TAB> rows <TAB> MB (Arrow buffers) <TAB> endpoints <TAB> MB/s
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;

import pro.surpin.arrowflight.common.FlightConfig;
import pro.surpin.arrowflight.common.FlightDataStream;
import pro.surpin.arrowflight.common.FlightSqlSession;

public class FlightReadBench
{
    record Result(double seconds, double planSeconds, long rows, long bytes, int endpoints) {}

    public static void main(String[] args)
            throws Exception
    {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String token = args[2];
        String schema = args[3];
        String scan = Files.readString(Path.of(args[4])).strip().replaceAll(";$", "")
                .replaceAll("\\bFROM\\s+(\\w+)", "FROM " + schema + ".$1");
        int repeat = Integer.parseInt(args[5]);
        List<String> modes = args.length > 6 ? List.of(args).subList(6, args.length) : List.of("s1q", "s1p", "s4q", "s4p");

        FlightConfig config = FlightConfig.builder(host).port(port).bearerToken(token).build();
        try (FlightSqlSession session = new FlightSqlSession(config)) {
            String column = scan.replaceFirst("(?is)^SELECT\\s+([^,\\s]+).*", "$1");
            for (String mode : modes) {
                int splits = Integer.parseInt(mode.substring(1, mode.length() - 1));
                boolean parallel = mode.endsWith("p");
                List<String> statements = splits <= 1 ? List.of(scan) : rangeStatements(session, scan, column, splits);
                Result best = null;
                for (int i = 0; i <= repeat; i++) {  // pass 0 warms up the JVM and the pooled connections
                    Result r = run(session, statements, parallel);
                    if (i > 0 && (best == null || r.seconds() < best.seconds())) {
                        best = r;
                    }
                }
                double mb = best.bytes() / 1e6;
                System.out.printf("%s\t%.2f\t%.2f\t%d\t%.0f\t%d\t%.0f%n", mode, best.seconds(), best.planSeconds(), best.rows(),
                        mb, best.endpoints(), mb / best.seconds());
            }
        }
    }

    // the plugin's split predicates: (col < b1 OR col IS NULL), (col >= b1 AND col < b2), ..., (col >= bn-1)
    static List<String> rangeStatements(FlightSqlSession session, String scan, String column, int count)
            throws Exception
    {
        String table = scan.replaceFirst("(?is).*\\bFROM\\s+(\\S+).*", "$1");
        long[] minMax = {Long.MAX_VALUE, Long.MIN_VALUE};  // partial mode: one min/max per endpoint
        FlightInfo info = session.getFlightInfo("SELECT min(" + column + "), max(" + column + ") FROM " + table);
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightDataStream stream = open(session, endpoint)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    if (root.getRowCount() > 0) {
                        minMax[0] = Math.min(minMax[0], value(root.getVector(0)));
                        minMax[1] = Math.max(minMax[1], value(root.getVector(1)));
                    }
                }
            }
        }
        long min = minMax[0];
        long max = minMax[1];
        double width = ((double) max - (double) min + 1) / count;
        List<String> statements = new ArrayList<>();
        long low = min;
        for (int i = 0; i < count; i++) {
            boolean last = i == count - 1;
            long high = last ? max : (long) Math.floor(min + width * (i + 1));
            String predicate = i == 0 ? "(" + column + " < " + high + " OR " + column + " IS NULL)"
                    : last ? "(" + column + " >= " + low + ")"
                    : "(" + column + " >= " + low + " AND " + column + " < " + high + ")";
            statements.add(scan + " WHERE " + predicate);
            low = high;
        }
        return statements;
    }

    static long value(FieldVector vector)
    {
        if (vector instanceof BigIntVector v) {
            return v.get(0);
        }
        if (vector instanceof IntVector v) {
            return v.get(0);
        }
        throw new IllegalArgumentException("split column must be INTEGER/BIGINT, got " + vector.getField());
    }

    static Result run(FlightSqlSession session, List<String> statements, boolean parallel)
            throws Exception
    {
        AtomicLong rows = new AtomicLong();
        AtomicLong bytes = new AtomicLong();
        AtomicLong planNanos = new AtomicLong();
        AtomicLong endpoints = new AtomicLong();
        ExecutorService pool = Executors.newCachedThreadPool();
        long start = System.nanoTime();
        try {
            List<Future<?>> splits = new ArrayList<>();
            for (String sql : statements) {
                splits.add(pool.submit(() -> {
                    long t0 = System.nanoTime();
                    FlightInfo info = session.getFlightInfo(sql);
                    planNanos.accumulateAndGet(System.nanoTime() - t0, Math::max);
                    endpoints.addAndGet(info.getEndpoints().size());
                    List<Future<?>> reads = new ArrayList<>();
                    for (FlightEndpoint endpoint : info.getEndpoints()) {
                        if (parallel) {
                            reads.add(pool.submit(() -> drain(session, endpoint, rows, bytes)));
                        }
                        else {
                            drain(session, endpoint, rows, bytes);
                        }
                    }
                    for (Future<?> read : reads) {
                        read.get();
                    }
                    return null;
                }));
            }
            for (Future<?> split : splits) {
                split.get();
            }
        }
        finally {
            pool.shutdownNow();
        }
        return new Result((System.nanoTime() - start) / 1e9, planNanos.get() / 1e9, rows.get(), bytes.get(), (int) endpoints.get());
    }

    static Void drain(FlightSqlSession session, FlightEndpoint endpoint, AtomicLong rows, AtomicLong bytes)
    {
        try (FlightDataStream stream = open(session, endpoint)) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                rows.addAndGet(root.getRowCount());
                long size = 0;
                for (FieldVector vector : root.getFieldVectors()) {
                    size += vector.getBufferSize();
                }
                bytes.addAndGet(size);
            }
        }
        return null;
    }

    static FlightDataStream open(FlightSqlSession session, FlightEndpoint endpoint)
    {
        List<String> uris = endpoint.getLocations().stream().map(l -> l.getUri().toString()).toList();
        return session.openStream(endpoint.getTicket().getBytes(), uris);
    }
}
