package evaluator;

import static org.junit.jupiter.api.Assertions.*;

import evaluator.configuration.JobConfig;
import evaluator.model.AdvisoryRecord;
import evaluator.model.IndicatorRecord;
import evaluator.model.QueryOutput;
import evaluator.output.RunOutput;
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
        JobConfig config = new JobConfig(List.of(input.toAbsolutePath().toString()), parallelism, 0);
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
    void crossoversAreEmittedFromFinalizedWindowsWithStableIdentity() throws Exception {
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
            assertEquals(signal.symbol + "|" + signal.windowStart + "|" + signal.windowEnd + "|"
                    + signal.direction, signal.advisoryId);
        }
        assertEquals(java.time.OffsetDateTime.parse("2021-11-08T00:10:00+02:00")
                .toInstant().toEpochMilli(), signals.get(0).windowStart);
    }

    @Test
    void subscriptionsFilterPublishedRecordsAndOutputIsCommittedAsOneRun(@TempDir Path temp) throws Exception {
        Path outputDirectory = temp.resolve("query-two-run");
        Path input = FIXTURES.resolve("window-ordering.csv");
        JobConfig config = JobConfig.parse(new String[]{"--subscribe", "SAME.FR",
                "--output-dir", outputDirectory.toString(), input.toString()});
        assertFalse(config.isSubscribed("SAME.NL"));
        assertTrue(config.isSubscribed("SAME.FR"));
        ByteArrayOutputStream console = new ByteArrayOutputStream();
        assertEquals(0, Main.run(new String[]{"--subscribe", "SAME.FR", "--output-dir",
                outputDirectory.toString(), input.toString()},
                new PrintStream(console), new PrintStream(new ByteArrayOutputStream())));

        List<String> indicatorLines = Files.readAllLines(outputDirectory.resolve("indicators.csv"));
        List<String> advisoryLines = Files.readAllLines(outputDirectory.resolve("advisories.csv"));
        assertEquals(3, indicatorLines.size());
        assertTrue(indicatorLines.get(1).startsWith("SAME.FR,"));
        assertTrue(indicatorLines.get(2).startsWith("SAME.FR,"));
        assertEquals(2, advisoryLines.size());
        assertTrue(advisoryLines.get(1).contains(",SAME.FR,"));
        assertTrue(advisoryLines.get(1).endsWith(",BUY"));

        String metadata = Files.readString(outputDirectory.resolve("run-metadata.csv"));
        assertTrue(metadata.contains("subscription_mode,allowlist"));
        assertTrue(metadata.contains("published_indicators,2"));
        assertTrue(metadata.contains("published_advisories,1"));
        assertTrue(metadata.contains("counter.indicators,4"));
        assertTrue(metadata.contains("counter.advisories,3"));
        assertTrue(console.toString().contains("published_indicators=2"));
        assertTrue(console.toString().contains("advisories=3"));
        assertThrows(java.io.IOException.class, () -> new RunOutput(config, new PrintStream(console)));

        Path allSymbolsDirectory = temp.resolve("all-symbols-run");
        assertEquals(0, Main.run(new String[]{"--output-dir", allSymbolsDirectory.toString(), input.toString()},
                new PrintStream(new ByteArrayOutputStream()), new PrintStream(new ByteArrayOutputStream())));
        List<String> allIndicators = Files.readAllLines(allSymbolsDirectory.resolve("indicators.csv")).stream()
                .filter(line -> line.startsWith("SAME.FR,")).sorted().toList();
        List<String> publishedIndicators = indicatorLines.stream()
                .filter(line -> line.startsWith("SAME.FR,")).sorted().toList();
        assertEquals(allIndicators, publishedIndicators);
        List<String> allAdvisories = Files.readAllLines(allSymbolsDirectory.resolve("advisories.csv")).stream()
                .filter(line -> line.contains(",SAME.FR,")).sorted().toList();
        List<String> publishedAdvisories = advisoryLines.stream()
                .filter(line -> line.contains(",SAME.FR,")).sorted().toList();
        assertEquals(allAdvisories, publishedAdvisories);
    }

    @Test
    void consoleOutputRetainsIndicatorsAndPrintsAdvisories() throws Exception {
        ByteArrayOutputStream console = new ByteArrayOutputStream();
        assertEquals(0, Main.run(new String[]{FIXTURES.resolve("crossovers.csv").toString()},
                new PrintStream(console), new PrintStream(new ByteArrayOutputStream())));
        String text = console.toString();
        assertTrue(text.startsWith("symbol,window_start,window_end"));
        assertEquals(3, text.lines().filter(line -> line.startsWith("ADVISORY,")).count());
        assertTrue(text.contains("published_advisories=3"));
    }

    @Test
    void subscriptionArgumentsAreValidatedAndDefaultToAll() throws Exception {
        JobConfig all = JobConfig.parse(new String[]{FIXTURES.resolve("window-ordering.csv").toString()});
        assertTrue(all.isSubscribed("SAME.FR"));
        assertTrue(all.isSubscribed("SAME.NL"));
        for (String[] args : List.of(
                new String[]{"--subscribe", "UNKNOWN.XY", FIXTURES.resolve("window-ordering.csv").toString()},
                new String[]{"--subscribe", "SAME.FR", "--subscribe", "SAME.FR",
                        FIXTURES.resolve("window-ordering.csv").toString()},
                new String[]{"--output-dir", "one", "--output-dir", "two",
                        FIXTURES.resolve("window-ordering.csv").toString()})) {
            assertThrows(IllegalArgumentException.class, () -> JobConfig.parse(args));
        }
    }

    @Test
    void emptyResultsProduceHeaderOnlyFilesAndCsvFieldsAreEscaped(@TempDir Path temp) throws Exception {
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

        Path escapedDirectory = temp.resolve("escaped-run");
        JobConfig config = new JobConfig(List.of(input.toString()), 1, 0, java.util.Set.of(), escapedDirectory);
        try (RunOutput result = new RunOutput(config, new PrintStream(new ByteArrayOutputStream()))) {
            IndicatorRecord indicator = new IndicatorRecord();
            indicator.symbol = "ODD,\"NAME.FR";
            indicator.windowStart = 1_636_364_400_000L;
            indicator.windowEnd = indicator.windowStart + 300_000;
            indicator.close = 10;
            indicator.ema38 = 1;
            indicator.ema100 = 0.5;
            indicator.closingTimestampMicros = indicator.windowStart * 1000;
            result.accept(QueryOutput.indicator(indicator));
            result.finish(java.util.Map.of());
        }
        assertTrue(Files.readString(escapedDirectory.resolve("indicators.csv"))
                .contains("\"ODD,\"\"NAME.FR\""));
    }

}
