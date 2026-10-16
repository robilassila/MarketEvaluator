package evaluator;

import static org.junit.jupiter.api.Assertions.*;

import evaluator.configuration.JobConfig;
import evaluator.model.IndicatorRecord;
import evaluator.model.PriceEvent;
import evaluator.processing.ClosingPriceAggregate;
import evaluator.processing.EmaFunction;
import evaluator.processing.QueryOnePipeline;
import evaluator.processing.WindowTimes;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryOnePipelineTest {
    private static final Path FIXTURES = Path.of("src/test/resources/fixtures");
    private static final String HEADER = "ID,SecType,Date,Time,Last,Trading date,Trading time\n";

    record Run(List<IndicatorRecord> records, Map<String, Object> counts) { }

    static Run run(JobConfig config) throws Exception {
        try (var runtime = new LocalFlinkRuntime()) {
            var environment = runtime.environment();
            environment.enableCheckpointing(10);
            var stream = QueryOnePipeline.build(environment, config);
            assertFalse(environment.getCheckpointConfig().isCheckpointingEnabled());
            var collector = new DataStream.Collector<IndicatorRecord>();
            stream.collectAsync(collector);
            var job = environment.executeAsync("Query 1 test");
            try (var iterator = collector.getOutput()) {
                List<IndicatorRecord> records = new ArrayList<>();
                iterator.forEachRemaining(records::add);
                var result = job.getJobExecutionResult().get();
                records.sort(Comparator.comparingLong((IndicatorRecord r) -> r.windowStart).thenComparing(r -> r.symbol));
                return new Run(records, result.getAllAccumulatorResults());
            }
        }
    }

    static Run run(Path input, int parallelism, long delay) throws Exception {
        return run(new JobConfig(List.of(input.toAbsolutePath().toString()), parallelism, delay));
    }

    static void near(double expected, double actual) {
        assertEquals(expected, actual, Math.max(1e-9, Math.abs(expected) * 1e-9));
    }

    static long millis(String text) { return OffsetDateTime.parse(text).toInstant().toEpochMilli(); }

    static void record(IndicatorRecord actual, String symbol, String start, double price,
            long line, int file, String timestamp, double ema38, double ema100) {
        assertEquals(symbol, actual.symbol);
        assertEquals(millis(start), actual.windowStart);
        assertEquals(millis(start) + 300_000, actual.windowEnd);
        assertEquals(price, actual.close);
        assertEquals(line, actual.sourceLine);
        assertEquals(file, actual.fileOrdinal);
        OffsetDateTime date = OffsetDateTime.parse(timestamp);
        assertEquals(date.toEpochSecond() * 1_000_000L + date.getNano() / 1000, actual.closingTimestampMicros);
        near(ema38, actual.ema38);
        near(ema100, actual.ema100);
    }

    @Test void orderingFixtureMatchesIndependentExpectations() throws Exception {
        Run result = run(FIXTURES.resolve("window-ordering.csv"), 1, 65_000);
        assertEquals(4, result.records.size());
        String start = "2021-11-08T09:00:00+02:00";
        record(result.records.get(0), "INDEX.ETR", start, 20, 10, 0,
                "2021-11-08T09:00:40+02:00", 40.0 / 39, 40.0 / 101);
        record(result.records.get(1), "SAME.FR", start, 13, 12, 0,
                "2021-11-08T09:04:59.900+02:00", 26.0 / 39, 26.0 / 101);
        record(result.records.get(2), "SAME.NL", start, 30, 9, 0,
                "2021-11-08T09:00:30+02:00", 60.0 / 39, 60.0 / 101);
        record(result.records.get(3), "SAME.FR", "2021-11-08T09:05:00+02:00", 15, 13, 0,
                "2021-11-08T09:05:00+02:00",
                30.0 / 39 + 26.0 / 39 * 37.0 / 39,
                30.0 / 101 + 26.0 / 101 * 99.0 / 101);
        assertEquals(10L, result.counts.get("parser_accepted"));
        assertEquals(10L, result.counts.get("accepted"));
        assertEquals(0L, result.counts.get("late"));
        assertEquals(4L, result.counts.get("indicators"));
    }

    @Test void zeroDelayFinalizesAtExactBoundary() throws Exception {
        JobConfig config = JobConfig.parse(new String[]{FIXTURES.resolve("window-ordering.csv").toString()});
        assertEquals(0L, config.watermarkDelayMs());
        Run result = run(config);
        assertEquals(4, result.records.size());
        record(result.records.get(1), "SAME.FR", "2021-11-08T09:00:00+02:00", 12, 8, 0,
                "2021-11-08T09:04:59.900+02:00", 24.0 / 39, 24.0 / 101);
        assertEquals(9L, result.counts.get("accepted"));
        assertEquals(1L, result.counts.get("late"));
    }

    @Test void zeroAndNonPriceRowsCannotAdvanceTimeOrCreateState(@TempDir Path temp) throws Exception {
        Path input = temp.resolve("ignored.csv");
        Files.writeString(input, HEADER + """
                PRICE.FR,E,08-11-2021,,10,08-11-2021,09:00:00
                ZERO.FR,E,08-11-2021,,0,invalid,invalid
                PRICE.FR,E,08-11-2021,,0,08-11-2021,23:59:59
                NONPRICE.FR,E,08-11-2021,,,08-11-2021,23:59:59
                PRICE.FR,E,08-11-2021,,20,08-11-2021,09:04:59.999999
                PRICE.FR,E,08-11-2021,,-0.0,08-11-2021,23:59:59
                """);
        Run result = run(input, 1, 0);
        assertEquals(1, result.records.size());
        record(result.records.get(0), "PRICE.FR", "2021-11-08T09:00:00+02:00", 20, 6, 0,
                "2021-11-08T09:04:59.999999+02:00", 40.0 / 39, 40.0 / 101);
        assertEquals(3L, result.counts.get("ignored_zero"));
        assertEquals(1L, result.counts.get("non_price"));
        assertEquals(0L, result.counts.get("late"));
        assertEquals(0L, result.counts.get("rejected_event_timestamp"));
    }

    @Test void globalCutoffMicrosecondsGapsAndParallelismMatchIndependentRecords(@TempDir Path temp) throws Exception {
        Path input = temp.resolve("boundaries.csv");
        Files.writeString(input, HEADER + """
                SAME.FR,E,08-11-2021,,10,08-11-2021,09:00:00
                SAME.FR,E,08-11-2021,,12,08-11-2021,09:00:00.000002
                SAME.FR,E,08-11-2021,,999,08-11-2021,09:00:00.000001
                SAME.FR,E,08-11-2021,,13,08-11-2021,09:00:00.000002
                SAME.NL,E,08-11-2021,,40,08-11-2021,09:00:00
                SAME.NL,E,08-11-2021,,50,08-11-2021,09:06:05.001
                SAME.FR,E,08-11-2021,,999,08-11-2021,09:04:59
                OPEN.FR,E,08-11-2021,,25,08-11-2021,09:05:00.000001
                SAME.FR,E,09-11-2021,,26,09-11-2021,00:00:00
                """);
        for (int parallelism : new int[]{1, 2, 4}) {
            Run result = run(input, parallelism, 65_000);
            assertEquals(5, result.records.size());
            record(result.records.get(0), "SAME.FR", "2021-11-08T09:00:00+02:00", 13, 5, 0,
                    "2021-11-08T09:00:00.000002+02:00", 26.0 / 39, 26.0 / 101);
            record(result.records.get(1), "SAME.NL", "2021-11-08T09:00:00+02:00", 40, 6, 0,
                    "2021-11-08T09:00:00+02:00", 80.0 / 39, 80.0 / 101);
            record(result.records.get(2), "OPEN.FR", "2021-11-08T09:05:00+02:00", 25, 9, 0,
                    "2021-11-08T09:05:00.000001+02:00", 50.0 / 39, 50.0 / 101);
            record(result.records.get(3), "SAME.NL", "2021-11-08T09:05:00+02:00", 50, 7, 0,
                    "2021-11-08T09:06:05.001+02:00",
                    100.0 / 39 + 80.0 / 39 * 37.0 / 39, 100.0 / 101 + 80.0 / 101 * 99.0 / 101);
            record(result.records.get(4), "SAME.FR", "2021-11-09T00:00:00+02:00", 26, 10, 0,
                    "2021-11-09T00:00:00+02:00",
                    52.0 / 39 + 26.0 / 39 * 37.0 / 39, 52.0 / 101 + 26.0 / 101 * 99.0 / 101);
            assertEquals(9L, result.counts.get("parser_accepted"));
            assertEquals(8L, result.counts.get("accepted"));
            assertEquals(1L, result.counts.get("late"));
            assertEquals(5L, result.counts.get("indicators"));
        }
    }

    @Test void sortsDailyFilesAndUsesFileOrdinalForTies(@TempDir Path temp) throws Exception {
        Path later = temp.resolve("a-next.csv");
        Path first = temp.resolve("b-first.csv");
        Path tied = temp.resolve("c-tied.csv");
        Files.writeString(later, HEADER + "SAME.FR,E,09-11-2021,,20,09-11-2021,09:00:00\n");
        Files.writeString(first, HEADER + "SAME.FR,E,08-11-2021,,10,08-11-2021,09:00:00\n");
        Files.writeString(tied, HEADER + "SAME.FR,E,08-11-2021,,15,08-11-2021,09:00:00\n");
        JobConfig config = JobConfig.parse(new String[]{later.toString(), tied.toString(), first.toString()});
        assertEquals(List.of(first.toString(), tied.toString(), later.toString()), config.inputs());
        Run result = run(config);
        assertEquals(2, result.records.size());
        record(result.records.get(0), "SAME.FR", "2021-11-08T09:00:00+02:00", 15, 2, 1,
                "2021-11-08T09:00:00+02:00", 30.0 / 39, 30.0 / 101);
        record(result.records.get(1), "SAME.FR", "2021-11-09T09:00:00+02:00", 20, 2, 2,
                "2021-11-09T09:00:00+02:00",
                40.0 / 39 + 30.0 / 39 * 37.0 / 39, 40.0 / 101 + 30.0 / 101 * 99.0 / 101);
    }

    @Test void headerOnlyFileCompletesWithZeroCounts(@TempDir Path temp) throws Exception {
        Path input = temp.resolve("empty.csv");
        Files.writeString(input, HEADER);
        Run result = run(input, 1, 65_000);
        assertTrue(result.records.isEmpty());
        assertEquals(0L, result.counts.get("accepted"));
        assertEquals(0L, result.counts.get("indicators"));
        assertThrows(java.io.IOException.class,
                () -> JobConfig.parse(new String[]{input.toString(), FIXTURES.resolve("window-ordering.csv").toString()}));
    }

    @Test void validationSummariesPassThroughWithoutBecomingEvents() throws Exception {
        Run result = run(FIXTURES.resolve("special-price-literals.csv"), 1, 65_000);
        assertEquals(1, result.records.size());
        assertEquals(1L, result.counts.get("parser_accepted"));
        assertEquals(1L, result.counts.get("non_price"));
        assertEquals(3L, result.counts.get("rejected_price"));
    }

    @Test void everyRowValidationReasonAppearsInFinalCounters(@TempDir Path temp) throws Exception {
        Path input = temp.resolve("validation.csv");
        Files.writeString(input, HEADER + """
                SHORT.FR,E
                BAD.X,E,08-11-2021,,10,08-11-2021,09:00:00
                BADPRICE.FR,E,08-11-2021,,-1,08-11-2021,09:00:00
                BADTIME.FR,E,08-11-2021,,10,08-11-2021,25:00:00
                NONPRICE.FR,E,08-11-2021,,,08-11-2021,23:59:59
                ZERO.FR,E,,,0,invalid,invalid
                VALID.FR,E,invalid,,10,08-11-2021,09:00:00
                """);
        Run result = run(input, 1, 65_000);
        assertEquals(1, result.records.size());
        for (String name : List.of("parser_accepted", "accepted", "non_price", "ignored_zero",
                "rejected_shape", "rejected_identity", "rejected_price", "rejected_event_timestamp", "indicators")) {
            assertEquals(1L, result.counts.get(name), name);
        }
        assertEquals(0L, result.counts.get("late"));
    }

    @Test void noHeaderAndMalformedCsvFailTheActualJob(@TempDir Path temp) throws Exception {
        Path input = temp.resolve("broken.csv");
        Files.writeString(input, "# no records\n");
        assertThrows(Exception.class, () -> run(input, 1, 65_000));
        Files.writeString(input, "ID,SecType\nSAME.FR,E\n");
        assertThrows(Exception.class, () -> run(input, 1, 65_000));
        Files.writeString(input, HEADER + "\"unterminated\n");
        assertThrows(Exception.class, () -> run(input, 1, 65_000));
    }

    @Test void cliPrintsIndicatorsAndFinalSummary() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        assertEquals(0, Main.run(new String[]{FIXTURES.resolve("window-ordering.csv").toString()},
                new PrintStream(output), new PrintStream(errors)), errors.toString());
        assertTrue(output.toString().startsWith("symbol,window_start,window_end,close,ema38,ema100"));
        assertTrue(output.toString().contains("SAME.FR,2021-11-08T09:00+02:00,2021-11-08T09:05+02:00,12.0"));
        assertTrue(output.toString().contains("indicators=4"));
        assertTrue(output.toString().contains("parser_accepted=10"));
        assertFalse(output.toString().contains("dataStreamCollect_"));
        assertEquals("", errors.toString());
    }

    /** Independent oracle for this unquoted, positive-price sample only; no production parser/math calls. */
    @Test void realSampleMatchesIndependentScanAtAllParallelisms() throws Exception {
        Path sample = Path.of("../data/sample-positive-prices-08.csv");
        record Candidate(String symbol, long start, long micros, long line, double price) { }
        record Expected(Candidate close, double ema38, double ema100) { }
        Map<String, Integer> columns = new HashMap<>();
        TreeMap<String, Candidate> closes = new TreeMap<>();
        long highWater = Long.MIN_VALUE;
        long late = 0;
        long parsed = 0;
        long line = 0;
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("dd-MM-uuuu");
        try (var reader = Files.newBufferedReader(sample)) {
            String text;
            while ((text = reader.readLine()) != null) {
                line++;
                if (text.isBlank() || text.startsWith("#")) { continue; }
                String[] fields = text.split(",", -1);
                if (columns.isEmpty()) {
                    for (int i = 0; i < fields.length; i++) { columns.put(fields[i].strip(), i); }
                    continue;
                }
                assertFalse(text.contains("\""), "This test-only scanner assumes the sample has no quoted fields");
                double price = Double.parseDouble(fields[columns.get("Last")]);
                assertTrue(price > 0 && Double.isFinite(price), "Sample oracle expects positive prices");
                parsed++;
                String date = fields[columns.get("Trading date")];
                if (date.isEmpty()) { date = fields[columns.get("Date")]; }
                OffsetDateTime time = OffsetDateTime.of(LocalDate.parse(date, dateFormat),
                        LocalTime.parse(fields[columns.get("Trading time")]), ZoneOffset.ofHours(2));
                long micros = time.toEpochSecond() * 1_000_000L + time.getNano() / 1000;
                long eventMs = Math.floorDiv(micros, 1000);
                long start = Math.floorDiv(eventMs, 300_000) * 300_000;
                if (highWater != Long.MIN_VALUE && start + 300_000 <= highWater - 65_000) {
                    late++;
                    continue;
                }
                highWater = Math.max(highWater, eventMs);
                String symbol = fields[columns.get("ID")];
                String key = start + "/" + symbol;
                Candidate existing = closes.get(key);
                if (existing == null || micros > existing.micros
                        || micros == existing.micros && line > existing.line) {
                    closes.put(key, new Candidate(symbol, start, micros, line, price));
                }
            }
        }
        Map<String, double[]> previous = new HashMap<>();
        List<Expected> expected = new ArrayList<>();
        List<Candidate> ordered = closes.values().stream()
                .sorted(Comparator.comparingLong(Candidate::start).thenComparing(Candidate::symbol)).toList();
        for (Candidate close : ordered) {
            double[] old = previous.computeIfAbsent(close.symbol, ignored -> new double[2]);
            double current38 = close.price * (2.0 / 39.0) + old[0] * (1.0 - 2.0 / 39.0);
            double current100 = close.price * (2.0 / 101.0) + old[1] * (1.0 - 2.0 / 101.0);
            expected.add(new Expected(close, current38, current100));
            old[0] = current38;
            old[1] = current100;
        }
        assertEquals(10_000, parsed);
        assertEquals(0, late);
        assertEquals(1_263, expected.size());
        assertEquals(1_227, previous.size());
        for (int parallelism : new int[]{1, 2, 4}) {
            Run result = run(sample, parallelism, 65_000);
            assertEquals(expected.size(), result.records.size());
            for (int i = 0; i < expected.size(); i++) {
                Expected oracle = expected.get(i);
                IndicatorRecord actual = result.records.get(i);
                assertEquals(oracle.close.symbol, actual.symbol);
                assertEquals(oracle.close.start, actual.windowStart);
                assertEquals(oracle.close.start + 300_000, actual.windowEnd);
                assertEquals(oracle.close.price, actual.close);
                assertEquals(oracle.close.micros, actual.closingTimestampMicros);
                assertEquals(oracle.close.line, actual.sourceLine);
                assertEquals(0, actual.fileOrdinal);
                near(oracle.ema38, actual.ema38);
                near(oracle.ema100, actual.ema100);
            }
            assertEquals(parsed, result.counts.get("parser_accepted"));
            assertEquals(parsed - late, result.counts.get("accepted"));
            assertEquals(late, result.counts.get("late"));
            assertEquals(1_263L, result.counts.get("indicators"));
        }
        // Independently recorded four-window sequence provides stable, explicit sample checks.
        List<Expected> selected = expected.stream().filter(r -> r.close.symbol.equals("IEBBB.FR")).toList();
        assertEquals(List.of(242L, 491L, 722L, 9566L), selected.stream().map(r -> r.close.line).toList());
        double[] ema38 = {2.702051282051282, 5.265535831689677, 7.69755963519277, 10.004864269285449};
        double[] ema100 = {1.0433663366336634, 2.0660719537300265, 3.0685259744482436, 4.051129420498773};
        assertEquals(4, selected.size());
        for (int i = 0; i < 4; i++) {
            assertEquals(52.69, selected.get(i).close.price);
            near(ema38[i], selected.get(i).ema38);
            near(ema100[i], selected.get(i).ema100);
        }
    }

    @Test void configurationRejectsInvalidOptionsAndDuplicates() {
        for (String[] args : List.of(new String[0], new String[]{"--parallelism"},
                new String[]{"--parallelism", "0", "file.csv"},
                new String[]{"--watermark-delay-ms", "-1", "file.csv"},
                new String[]{"--unknown", "file.csv"}, new String[]{"file.csv", "./file.csv"})) {
            assertThrows(IllegalArgumentException.class, () -> JobConfig.parse(args));
        }
    }

    @Test void configurationAllowsAnExplicitWatermarkDelay() throws Exception {
        JobConfig config = JobConfig.parse(new String[]{"--watermark-delay-ms", "65000",
                FIXTURES.resolve("window-ordering.csv").toString()});
        assertEquals(65_000L, config.watermarkDelayMs());
    }

    @Test void incrementalAggregateAndTimeModelsUseExplicitTypes() {
        assertInstanceOf(PojoTypeInfo.class, TypeInformation.of(PriceEvent.class));
        assertInstanceOf(PojoTypeInfo.class, TypeInformation.of(IndicatorRecord.class));
        ClosingPriceAggregate aggregate = new ClosingPriceAggregate();
        PriceEvent first = new PriceEvent();
        first.symbol = "SAME.FR"; first.timestampMicros = 2; first.fileOrdinal = 0; first.sourceLine = 9;
        PriceEvent next = new PriceEvent();
        next.symbol = "SAME.FR"; next.timestampMicros = 2; next.fileOrdinal = 1; next.sourceLine = 2;
        assertSame(first, aggregate.add(first, aggregate.createAccumulator()));
        assertSame(next, aggregate.add(next, first));
        assertSame(next, aggregate.merge(first, next));
        assertEquals(-300_000, WindowTimes.start(-1));
        assertEquals(0, WindowTimes.start(299_999));
        assertEquals(300_000, WindowTimes.start(300_000));
        assertEquals(Long.MIN_VALUE, WindowTimes.cutoff(-1, Long.MAX_VALUE));
        near(20.0 / 39 + 2.0 * 37.0 / 39, EmaFunction.update(10, 2, 38));
    }
}
