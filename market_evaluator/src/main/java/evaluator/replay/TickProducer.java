package evaluator.replay;

import evaluator.kafka.Topics;
import evaluator.model.PriceEvent;
import evaluator.processing.QueryOnePipeline;
import evaluator.wire.PriceEventCodec;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Replays DEBS CSV files into the ticks topic. Keying by symbol makes Kafka keep each symbol's
 * events in one partition, in send order; the record timestamp is the event's trading time.
 * Late events are dropped here, see {@link LateEventFilter}.
 */
public final class TickProducer {
    private static final String USAGE = "Usage: evaluator.replay.TickProducer [--bootstrap-servers HOST:PORT]"
            + " [--topic NAME] [--partitions N] [--rate EVENTS_PER_SECOND] [--boundary-log FILE] <csv-file>...";

    record Options(String bootstrapServers, String topic, int partitions, double rate,
            Path boundaryLog, List<Path> inputs) { }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) { System.exit(exitCode); }
    }

    public static int run(String[] args, PrintStream output, PrintStream error) {
        final Options options;
        try {
            options = parse(args);
        } catch (IllegalArgumentException exception) {
            error.println(exception.getMessage());
            error.println(USAGE);
            return 2;
        }
        try {
            replay(options, output, error);
            return 0;
        } catch (Exception exception) {
            Throwable cause = exception;
            while (cause.getCause() != null) { cause = cause.getCause(); }
            error.println("Could not replay " + options.inputs() + ": " + cause.getMessage());
            return 1;
        }
    }

    static Options parse(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "ticks";
        int partitions = 16;
        double rate = 0;
        Path boundaryLog = null;
        List<Path> inputs = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap-servers" -> bootstrapServers = value(args, ++i);
                case "--topic" -> topic = value(args, ++i);
                case "--partitions" -> partitions = Integer.parseInt(value(args, ++i));
                case "--rate" -> rate = Double.parseDouble(value(args, ++i));
                case "--boundary-log" -> boundaryLog = Path.of(value(args, ++i));
                default -> {
                    if (args[i].startsWith("--")) {
                        throw new IllegalArgumentException("Unknown option: " + args[i]);
                    }
                    inputs.add(Path.of(args[i]));
                }
            }
        }
        if (inputs.isEmpty() || partitions <= 0 || rate < 0 || !Double.isFinite(rate)) {
            throw new IllegalArgumentException("Require input files, positive partitions and a non-negative rate");
        }
        for (Path input : inputs) {
            if (!Files.isRegularFile(input) || !Files.isReadable(input)) {
                throw new IllegalArgumentException("Not a readable CSV file: " + input);
            }
        }
        return new Options(bootstrapServers, topic, partitions, rate, boundaryLog, CsvReplay.chronological(inputs));
    }

    private static void replay(Options options, PrintStream output, PrintStream error) throws Exception {
        try (Topics topics = new Topics(options.bootstrapServers())) {
            topics.ensure(options.topic(), options.partitions());
        }
        AtomicLongArray perPartition = new AtomicLongArray(options.partitions());
        AtomicReference<Exception> failure = new AtomicReference<>();
        LateEventFilter lateEvents = new LateEventFilter();
        long[] sent = {0};
        long started = System.nanoTime();
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties(options));
             BoundaryLog boundaries = BoundaryLog.open(options.boundaryLog())) {
            CsvReplay.forEach(options.inputs(), event -> {
                if (failure.get() != null) {
                    throw new IllegalStateException("Send failed", failure.get());
                }
                if (!lateEvents.accept(event)) {
                    return;
                }
                pace(options.rate(), started, sent[0]);
                boundaries.beforeSend(event);
                producer.send(record(options.topic(), event), (metadata, exception) -> {
                    if (exception != null) {
                        failure.compareAndSet(null, exception);
                    } else {
                        perPartition.incrementAndGet(metadata.partition());
                    }
                });
                sent[0]++;
            });
            producer.flush();
        }
        if (failure.get() != null) {
            throw failure.get();
        }
        double seconds = (System.nanoTime() - started) / 1e9;
        output.printf("Sent %d events to %s in %.1f s (%.0f events/s); dropped %d late events%n",
                sent[0], options.topic(), seconds, sent[0] / seconds, lateEvents.dropped());
        for (int partition = 0; partition < perPartition.length(); partition++) {
            if (perPartition.get(partition) == 0) {
                // An empty partition holds back the job's watermark in unbounded mode, so windows never fire.
                error.println("Warning: partition " + partition + " of " + options.topic() + " received no events");
            }
        }
    }

    private static ProducerRecord<byte[], byte[]> record(String topic, PriceEvent event) {
        return new ProducerRecord<>(topic, null, event.timestampMillis(),
                PriceEventCodec.key(event), PriceEventCodec.value(event));
    }

    /** Schedules against the start time rather than the previous send, so pauses don't accumulate drift. */
    private static void pace(double rate, long started, long sent) {
        if (rate == 0) {
            return;
        }
        long wait = started + (long) (sent * 1e9 / rate) - System.nanoTime();
        if (wait > 0) {
            LockSupport.parkNanos(wait);
        }
    }

    private static Properties producerProperties(Options options) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Per-symbol order relies on idempotence, which the client enables by default with acks=all.
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024);
        return properties;
    }

    private static String value(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing option value");
        }
        return args[index];
    }

    /**
     * Records, for each five-minute boundary, the wall-clock time the first event at or after it was
     * sent. A window ending at that boundary cannot fire before this event exists, so the subscriber
     * measures window latency from here. Events are replayed in chronological order, so each new
     * boundary seen is the earliest event at or past it.
     */
    static final class BoundaryLog implements AutoCloseable {
        private final BufferedWriter writer;
        private long lastBoundary = Long.MIN_VALUE;

        private BoundaryLog(BufferedWriter writer) { this.writer = writer; }

        static BoundaryLog open(Path path) throws IOException {
            if (path == null) {
                return new BoundaryLog(null);
            }
            BufferedWriter writer = Files.newBufferedWriter(path);
            writer.write("boundary_millis,sent_at_millis\n");
            return new BoundaryLog(writer);
        }

        void beforeSend(PriceEvent event) {
            if (writer == null) {
                return;
            }
            long boundary = QueryOnePipeline.windowStart(event.timestampMillis());
            if (boundary > lastBoundary) {
                lastBoundary = boundary;
                try {
                    writer.write(boundary + "," + System.currentTimeMillis() + "\n");
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
