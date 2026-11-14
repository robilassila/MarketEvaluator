package evaluator;

import static org.junit.jupiter.api.Assertions.*;

import evaluator.configuration.JobConfig;
import evaluator.model.AdvisoryRecord;
import evaluator.model.QueryOutput;
import evaluator.processing.EmaFunction;
import evaluator.processing.QueryOnePipeline;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryTwoPipelineTest {
    private static final Path FIXTURES = Path.of("src/test/resources/fixtures");

    private static List<QueryOutput> run(Path input, int parallelism) throws Exception {
        JobConfig config = new JobConfig(List.of(input.toAbsolutePath().toString()), parallelism);
        try (var runtime = new LocalFlinkRuntime()) {
            var environment = runtime.environment();
            var indicators = QueryOnePipeline.build(environment, config);
            DataStream.Collector<QueryOutput> collector = new DataStream.Collector<>();
            QueryOnePipeline.queryOutput(indicators).collectAsync(collector);
            var job = environment.executeAsync("Query 2 test");
            try (var results = collector.getOutput()) {
                List<QueryOutput> outputs = new ArrayList<>();
                results.forEachRemaining(outputs::add);
                job.getJobExecutionResult().get();
                return outputs;
            }
        }
    }

    @Test
    void crossoverUsesStrictCurrentAndInclusivePreviousComparisons() {
        assertEquals("BUY", EmaFunction.crossoverDirection(2, 1, 1, 1));
        assertEquals("SELL", EmaFunction.crossoverDirection(1, 2, 2, 2));
        assertNull(EmaFunction.crossoverDirection(3, 2, 2, 1));
        assertNull(EmaFunction.crossoverDirection(1, 2, 2, 3));
        assertNull(EmaFunction.crossoverDirection(2, 2, 1, 3));
        assertNull(EmaFunction.crossoverDirection(2, 2, 3, 1));
        assertNull(EmaFunction.crossoverDirection(3, 2, 3, 1));
        assertNull(EmaFunction.crossoverDirection(1, 2, 1, 3));
    }

    @Test
    void crossoversAreEmittedFromFinalizedWindows() throws Exception {
        List<AdvisoryRecord> signals = run(FIXTURES.resolve("crossovers.csv"), 1).stream()
                .filter(record -> record.advisory != null)
                .map(record -> record.advisory)
                .sorted(Comparator.comparingLong(record -> record.windowStart))
                .toList();

        assertEquals(List.of("BUY", "SELL", "BUY"),
                signals.stream().map(record -> record.direction).toList());
        assertEquals(3, signals.size());
        for (AdvisoryRecord signal : signals) {
            assertEquals("CROSS.FR", signal.symbol);
            assertEquals(signal.windowStart + 300_000, signal.windowEnd);
        }
        assertEquals(java.time.OffsetDateTime.parse("2021-11-08T00:10:00+02:00")
                .toInstant().toEpochMilli(), signals.get(0).windowStart);
    }

    @Test
    void subscriptionsFilterPublishedRecords(@TempDir Path temp) throws Exception {
        Path outputDirectory = temp.resolve("query-two-run");
        Path input = FIXTURES.resolve("window-ordering.csv");
        JobConfig config = JobConfig.parse(new String[]{"--subscribe", "SAME.FR",
                "--output-dir", outputDirectory.toString(), input.toString()});
        assertFalse(config.isSubscribed("SAME.NL"));
        assertTrue(config.isSubscribed("SAME.FR"));
        assertEquals(0, Main.run(new String[]{"--subscribe", "SAME.FR", "--output-dir",
                outputDirectory.toString(), input.toString()},
                new PrintStream(new ByteArrayOutputStream()), new PrintStream(new ByteArrayOutputStream())));

        List<String> indicatorLines = Files.readAllLines(outputDirectory.resolve("indicators.csv"));
        List<String> advisoryLines = Files.readAllLines(outputDirectory.resolve("advisories.csv"));
        assertEquals(3, indicatorLines.size());
        assertTrue(indicatorLines.get(1).startsWith("SAME.FR,"));
        assertTrue(indicatorLines.get(2).startsWith("SAME.FR,"));
        assertEquals(2, advisoryLines.size());
        assertTrue(advisoryLines.get(1).startsWith("SAME.FR,"));
        assertTrue(advisoryLines.get(1).endsWith(",BUY"));

        Path allSymbolsDirectory = temp.resolve("all-symbols-run");
        assertEquals(0, Main.run(new String[]{"--output-dir", allSymbolsDirectory.toString(), input.toString()},
                new PrintStream(new ByteArrayOutputStream()), new PrintStream(new ByteArrayOutputStream())));
        List<String> allIndicators = Files.readAllLines(allSymbolsDirectory.resolve("indicators.csv")).stream()
                .filter(line -> line.startsWith("SAME.FR,")).sorted().toList();
        List<String> publishedIndicators = indicatorLines.stream()
                .filter(line -> line.startsWith("SAME.FR,")).sorted().toList();
        assertEquals(allIndicators, publishedIndicators);
        List<String> allAdvisories = Files.readAllLines(allSymbolsDirectory.resolve("advisories.csv")).stream()
                .filter(line -> line.startsWith("SAME.FR,")).sorted().toList();
        List<String> publishedAdvisories = advisoryLines.stream()
                .filter(line -> line.startsWith("SAME.FR,")).sorted().toList();
        assertEquals(allAdvisories, publishedAdvisories);
    }

    @Test
    void subscriptionArgumentsAreValidatedAndDefaultToAll() throws Exception {
        JobConfig all = JobConfig.parse(new String[]{"--output-dir", "out",
                FIXTURES.resolve("window-ordering.csv").toString()});
        assertTrue(all.isSubscribed("SAME.FR"));
        assertTrue(all.isSubscribed("SAME.NL"));
        assertThrows(IllegalArgumentException.class, () -> JobConfig.parse(new String[]{
                "--subscribe", "UNKNOWN.XY", "--output-dir", "out",
                FIXTURES.resolve("window-ordering.csv").toString()}));
    }

    @Test
    void emptyResultsProduceHeaderOnlyFiles(@TempDir Path temp) throws Exception {
        Path input = temp.resolve("zero-only.csv");
        Files.writeString(input, """
                ID,SecType,Date,Time,Last,Trading date,Trading time
                ZERO.FR,E,08-11-2021,09:00:00,0,08-11-2021,09:00:00
                """);
        Path outputDirectory = temp.resolve("empty-run");
        assertEquals(0, Main.run(new String[]{"--output-dir", outputDirectory.toString(), input.toString()},
                new PrintStream(new ByteArrayOutputStream()), new PrintStream(new ByteArrayOutputStream())));
        assertEquals(1, Files.readAllLines(outputDirectory.resolve("indicators.csv")).size());
        assertEquals(1, Files.readAllLines(outputDirectory.resolve("advisories.csv")).size());
    }
}
