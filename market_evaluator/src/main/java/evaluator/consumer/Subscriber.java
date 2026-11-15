package evaluator.consumer;

import evaluator.CsvTradeParser;
import evaluator.kafka.Topics;
import evaluator.output.RecordFormat;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * A "trader": reads the indicators and advisories topics, keeps the subscribed symbols and writes
 * them to CSV files. With a producer boundary log it also reports per-window output latency.
 */
public final class Subscriber {
    private static final String USAGE = "Usage: evaluator.consumer.Subscriber [--bootstrap-servers HOST:PORT]"
            + " [--indicators-topic NAME] [--advisories-topic NAME] [--subscribe FULL_SYMBOL ...]"
            + " [--boundary-log FILE] [--idle-timeout SECONDS] --output-dir DIRECTORY";

    record Options(String bootstrapServers, String indicatorsTopic, String advisoriesTopic,
            Set<String> subscriptions, Path boundaryLog, Duration idleTimeout, Path outputDirectory) {
        boolean isSubscribed(String symbol) {
            return subscriptions.isEmpty() || subscriptions.contains(symbol);
        }
    }

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
            consume(options, output);
            return 0;
        } catch (Exception exception) {
            Throwable cause = exception;
            while (cause.getCause() != null) { cause = cause.getCause(); }
            error.println("Could not consume results: " + cause.getMessage());
            return 1;
        }
    }

    static Options parse(String[] args) {
        String bootstrapServers = "localhost:9092";
        String indicatorsTopic = "indicators";
        String advisoriesTopic = "advisories";
        Set<String> subscriptions = new HashSet<>();
        Path boundaryLog = null;
        Duration idleTimeout = null;
        Path outputDirectory = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap-servers" -> bootstrapServers = value(args, ++i);
                case "--indicators-topic" -> indicatorsTopic = value(args, ++i);
                case "--advisories-topic" -> advisoriesTopic = value(args, ++i);
                case "--subscribe" -> {
                    String symbol = value(args, ++i);
                    if (symbol.isBlank() || !CsvTradeParser.isValidSymbol(symbol)) {
                        throw new IllegalArgumentException("Invalid full symbol for --subscribe: " + symbol);
                    }
                    subscriptions.add(symbol);
                }
                case "--boundary-log" -> boundaryLog = Path.of(value(args, ++i));
                case "--idle-timeout" -> idleTimeout = Duration.ofSeconds(Long.parseLong(value(args, ++i)));
                case "--output-dir" -> outputDirectory = Path.of(value(args, ++i));
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        if (outputDirectory == null) {
            throw new IllegalArgumentException("Require --output-dir");
        }
        return new Options(bootstrapServers, indicatorsTopic, advisoriesTopic, Set.copyOf(subscriptions),
                boundaryLog, idleTimeout, outputDirectory);
    }

    /**
     * Without {@code --idle-timeout}, stops at the end offsets present at startup (for a finished
     * bounded job). With it, follows the topics until no record arrives for that long (for a live job).
     */
    private static void consume(Options options, PrintStream output) throws Exception {
        NavigableMap<Long, Long> boundaries = options.boundaryLog() == null
                ? null : readBoundaries(options.boundaryLog());
        List<TopicPartition> partitions = new ArrayList<>();
        try (Topics topics = new Topics(options.bootstrapServers())) {
            partitions.addAll(topics.partitions(options.indicatorsTopic()));
            partitions.addAll(topics.partitions(options.advisoriesTopic()));
        }
        Files.createDirectories(options.outputDirectory());
        List<Long> latencies = new ArrayList<>();
        long indicatorCount = 0;
        long advisoryCount = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties(options));
             BufferedWriter indicators = csv(options.outputDirectory().resolve("indicators.csv"),
                     RecordFormat.INDICATOR_HEADER);
             BufferedWriter advisories = csv(options.outputDirectory().resolve("advisories.csv"),
                     RecordFormat.ADVISORY_HEADER);
             BufferedWriter latencyLog = boundaries == null ? null : csv(options.outputDirectory().resolve(
                     "latency.csv"), "window_end_millis,trigger_sent_millis,emitted_millis,latency_millis")) {
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = options.idleTimeout() == null ? consumer.endOffsets(partitions) : null;
            long lastRecord = System.nanoTime();
            while (true) {
                var records = consumer.poll(Duration.ofMillis(200));
                if (!records.isEmpty()) {
                    lastRecord = System.nanoTime();
                }
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    String line = new String(record.value(), StandardCharsets.UTF_8);
                    boolean isIndicator = record.topic().equals(options.indicatorsTopic());
                    if (isIndicator && boundaries != null) {
                        measure(line, record.timestamp(), boundaries, latencies, latencyLog);
                    }
                    if (!options.isSubscribed(new String(record.key(), StandardCharsets.UTF_8))) {
                        continue;
                    }
                    BufferedWriter target = isIndicator ? indicators : advisories;
                    target.write(line);
                    target.newLine();
                    if (isIndicator) { indicatorCount++; } else { advisoryCount++; }
                }
                if (endOffsets != null) {
                    if (endOffsets.entrySet().stream()
                            .allMatch(end -> consumer.position(end.getKey()) >= end.getValue())) {
                        break;
                    }
                } else if (System.nanoTime() - lastRecord > options.idleTimeout().toNanos()) {
                    break;
                }
            }
        }
        output.printf("Wrote %d indicators and %d advisories to %s%n", indicatorCount, advisoryCount,
                options.outputDirectory().toAbsolutePath().normalize());
        if (boundaries != null) {
            printLatency(latencies, output);
        }
    }

    /**
     * Latency of one window: from the producer sending the first event at or after the window's end
     * (the earliest moment the window could fire) to the job emitting the indicator.
     */
    private static void measure(String line, long emittedMillis, NavigableMap<Long, Long> boundaries,
            List<Long> latencies, BufferedWriter log) throws IOException {
        long windowEnd = RecordFormat.windowEndMillis(line);
        Map.Entry<Long, Long> trigger = boundaries.ceilingEntry(windowEnd);
        if (trigger == null) {
            return;
        }
        long latency = emittedMillis - trigger.getValue();
        latencies.add(latency);
        log.write(windowEnd + "," + trigger.getValue() + "," + emittedMillis + "," + latency);
        log.newLine();
    }

    private static void printLatency(List<Long> latencies, PrintStream output) {
        if (latencies.isEmpty()) {
            output.println("No indicator could be matched to the boundary log");
            return;
        }
        long[] sorted = latencies.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(sorted);
        output.printf("Latency over %d indicators: p50 %d ms, p99 %d ms, max %d ms%n",
                sorted.length, percentile(sorted, 0.50), percentile(sorted, 0.99), sorted[sorted.length - 1]);
    }

    private static long percentile(long[] sorted, double quantile) {
        return sorted[(int) Math.min(sorted.length - 1, Math.ceil(quantile * sorted.length) - 1)];
    }

    private static NavigableMap<Long, Long> readBoundaries(Path path) throws IOException {
        NavigableMap<Long, Long> boundaries = new TreeMap<>();
        List<String> lines = Files.readAllLines(path);
        for (String line : lines.subList(1, lines.size())) {
            String[] fields = line.split(",");
            boundaries.put(Long.parseLong(fields[0]), Long.parseLong(fields[1]));
        }
        return boundaries;
    }

    private static BufferedWriter csv(Path path, String header) throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(path);
        writer.write(header);
        writer.newLine();
        return writer;
    }

    private static Properties consumerProperties(Options options) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5_000);
        return properties;
    }

    private static String value(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing option value");
        }
        return args[index];
    }
}
