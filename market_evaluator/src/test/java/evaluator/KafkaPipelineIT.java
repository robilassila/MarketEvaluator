package evaluator;

import static org.junit.jupiter.api.Assertions.*;

import evaluator.consumer.Subscriber;
import evaluator.output.RecordFormat;
import evaluator.replay.TickProducer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** End to end through Kafka: producer CLI → job CLI → subscriber CLI. Requires Docker. */
@Testcontainers
class KafkaPipelineIT {
    private static final Path SAMPLE = Path.of("../data/sample-positive-prices-08.csv");

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.2");

    @Test
    void parallelKafkaRunMatchesSingleSourceRun(@TempDir Path temp) throws Exception {
        String[] topics = topics("sample");
        Path boundaries = temp.resolve("boundaries.csv");
        cli(TickProducer.run(new String[]{"--bootstrap-servers", bootstrap(), "--topic", topics[0],
                "--partitions", "4", "--boundary-log", boundaries.toString(), SAMPLE.toString()}, out(), err()));
        cli(job(topics, 4));

        PipelineRun.Result expected = PipelineRun.run(SAMPLE, 1);
        Path all = temp.resolve("all");
        String report = subscribe(topics, all, "--boundary-log", boundaries.toString());
        assertEquals(sorted(expected.indicators().stream().map(RecordFormat::indicator)),
                body(all.resolve("indicators.csv"), RecordFormat.INDICATOR_HEADER));
        assertEquals(sorted(expected.advisories().stream().map(RecordFormat::advisory)),
                body(all.resolve("advisories.csv"), RecordFormat.ADVISORY_HEADER));
        // Windows in the final five minutes have no later event to trigger them, so they have no latency.
        assertTrue(report.contains("Latency over "), report);

        Path subscribed = temp.resolve("subscribed");
        subscribe(topics, subscribed, "--subscribe", "IEBBB.FR");
        assertEquals(sorted(expected.indicators().stream().filter(r -> r.symbol.equals("IEBBB.FR"))
                        .map(RecordFormat::indicator)),
                body(subscribed.resolve("indicators.csv"), RecordFormat.INDICATOR_HEADER));
        assertEquals(sorted(expected.advisories().stream().filter(r -> r.symbol.equals("IEBBB.FR"))
                        .map(RecordFormat::advisory)),
                body(subscribed.resolve("advisories.csv"), RecordFormat.ADVISORY_HEADER));
    }

    @Test
    void emptyTopicCompletesWithHeaderOnlyOutput(@TempDir Path temp) throws Exception {
        String[] topics = topics("empty");
        Path input = Files.writeString(temp.resolve("zero-only.csv"), """
                ID,SecType,Date,Time,Last,Trading date,Trading time
                ZERO.FR,E,08-11-2021,09:00:00,0,08-11-2021,09:00:00
                """);
        cli(TickProducer.run(new String[]{"--bootstrap-servers", bootstrap(), "--topic", topics[0],
                "--partitions", "2", input.toString()}, out(), err()));
        cli(job(topics, 2));
        subscribe(topics, temp.resolve("out"));
        assertEquals(List.of(RecordFormat.INDICATOR_HEADER), Files.readAllLines(temp.resolve("out/indicators.csv")));
        assertEquals(List.of(RecordFormat.ADVISORY_HEADER), Files.readAllLines(temp.resolve("out/advisories.csv")));
    }

    @Test
    void parallelismAboveThePartitionCountIsRejected(@TempDir Path temp) throws Exception {
        String[] topics = topics("narrow");
        cli(TickProducer.run(new String[]{"--bootstrap-servers", bootstrap(), "--topic", topics[0],
                "--partitions", "2", SAMPLE.toString()}, out(), err()));
        assertEquals(2, job(topics, 3));
    }

    /** The job runs in a fresh JVM so shutdown problems of the local cluster show up in its log. */
    @Test
    void jobProcessShutsDownCleanly(@TempDir Path temp) throws Exception {
        String[] topics = topics("process");
        cli(TickProducer.run(new String[]{"--bootstrap-servers", bootstrap(), "--topic", topics[0],
                "--partitions", "2", SAMPLE.toString()}, out(), err()));
        Path log = temp.resolve("job.log");
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
                "-cp", System.getProperty("java.class.path"), "evaluator.Main"));
        command.addAll(List.of(jobArguments(topics, 2)));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(process.waitFor(90, TimeUnit.SECONDS), "Job did not terminate");
            String output = Files.readString(log);
            assertEquals(0, process.exitValue(), output);
            assertTrue(output.contains("Processed 10000 events"), output);
            assertFalse(output.contains("MiniCluster is not yet running or has already been shut down"), output);
            assertFalse(output.contains("Failed to get job status"), output);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly().waitFor();
            }
        }
    }

    private static String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    /** Separate topics per test keep the tests independent on the shared broker. */
    private static String[] topics(String name) {
        return new String[]{name + "-ticks", name + "-indicators", name + "-advisories"};
    }

    private static String[] jobArguments(String[] topics, int parallelism) {
        return new String[]{"--bootstrap-servers", bootstrap(), "--bounded",
                "--parallelism", Integer.toString(parallelism), "--ticks-topic", topics[0],
                "--indicators-topic", topics[1], "--advisories-topic", topics[2]};
    }

    private static int job(String[] topics, int parallelism) {
        return Main.run(jobArguments(topics, parallelism), out(), err());
    }

    private static String subscribe(String[] topics, Path outputDirectory, String... extra) {
        ByteArrayOutputStream report = new ByteArrayOutputStream();
        List<String> args = new ArrayList<>(List.of("--bootstrap-servers", bootstrap(),
                "--indicators-topic", topics[1], "--advisories-topic", topics[2],
                "--output-dir", outputDirectory.toString()));
        args.addAll(List.of(extra));
        cli(Subscriber.run(args.toArray(String[]::new), new PrintStream(report), err()));
        return report.toString();
    }

    private static void cli(int exitCode) {
        assertEquals(0, exitCode);
    }

    private static List<String> body(Path csv, String header) throws Exception {
        List<String> lines = Files.readAllLines(csv);
        assertEquals(header, lines.get(0));
        return sorted(lines.subList(1, lines.size()).stream());
    }

    private static List<String> sorted(Stream<String> lines) {
        return lines.sorted().toList();
    }

    private static PrintStream out() {
        return new PrintStream(new ByteArrayOutputStream());
    }

    private static PrintStream err() {
        return System.err;
    }
}
