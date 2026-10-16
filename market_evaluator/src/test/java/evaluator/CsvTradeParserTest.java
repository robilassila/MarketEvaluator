package evaluator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvTradeParserTest {
    private static final Path FIXTURES = Path.of("src/test/resources/fixtures");
    private static final Path RAW_SAMPLE = Path.of("../data/sample-positive-prices-08.csv");
    private final CsvTradeParser parser = new CsvTradeParser();

    @Test
    void cliPrintsUsageWhenInputArgumentIsMissingOrOptionsAreInvalid() {
        ByteArrayOutputStream errorBytes = new ByteArrayOutputStream();
        PrintStream error = new PrintStream(errorBytes);

        assertEquals(2, Main.run(new String[0], new PrintStream(new ByteArrayOutputStream()), error));
        assertTrue(errorBytes.toString().contains("Usage: evaluator.Main [--parallelism N]"));

        errorBytes.reset();
        assertEquals(2, Main.run(new String[]{"--parallelism", "0", "first.csv"},
                new PrintStream(new ByteArrayOutputStream()), error));
        assertTrue(errorBytes.toString().contains("Usage: evaluator.Main [--parallelism N]"));
    }

    @Test
    void cliPrintsReadableErrorForMissingInputFile(@TempDir Path tempDir) {
        ByteArrayOutputStream errorBytes = new ByteArrayOutputStream();
        PrintStream error = new PrintStream(errorBytes);

        int exitCode = Main.run(new String[]{tempDir.resolve("missing.csv").toString()},
                new PrintStream(new ByteArrayOutputStream()), error);

        assertEquals(1, exitCode);
        assertTrue(errorBytes.toString().contains("Could not read CSV"));
        assertTrue(errorBytes.toString().contains("missing.csv"));
    }

    @Test
    void parsesWindowOrderingFixtureAndRetainsSourceLines() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        CsvTradeParser.ParseSummary summary = parser.parse(
                FIXTURES.resolve("window-ordering.csv"), events::add);

        assertEquals(10, summary.acceptedPriceEvents());
        assertEquals(10, events.size());
        assertEquals(4, events.get(0).sourceLine());
        assertEquals("SAME.FR", events.get(0).symbol());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:04:59.900+02:00"),
                events.get(1).timestamp());
        assertEquals(12.0, events.get(3).price());
        assertEquals(7L, events.get(3).sourceLine());
        assertEquals(12.0, events.get(4).price());
        assertEquals(8L, events.get(4).sourceLine());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:40+02:00"),
                events.get(6).timestamp());
    }

    @Test
    void parsesCrossoverFixture() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        CsvTradeParser.ParseSummary summary = parser.parse(
                FIXTURES.resolve("crossovers.csv"), events::add);

        assertEquals(37, summary.acceptedPriceEvents());
        assertEquals(2, summary.ignoredZero());
        assertEquals(37, events.size());
        assertEquals("CROSS.FR", events.get(0).symbol());
        assertEquals(1000.0, events.get(events.size() - 1).price());
        assertEquals(42, events.get(events.size() - 1).sourceLine());
    }

    @Test
    void parsesOriginalSampleDataWithoutShapeOrValueRejections() throws IOException {
        AtomicLong eventCount = new AtomicLong();
        AtomicReference<CsvTradeParser.TradeEvent> firstEvent = new AtomicReference<>();
        AtomicReference<CsvTradeParser.TradeEvent> lastEvent = new AtomicReference<>();
        Set<String> symbols = new HashSet<>();

        CsvTradeParser.ParseSummary summary = parser.parse(RAW_SAMPLE, event -> {
            eventCount.incrementAndGet();
            firstEvent.compareAndSet(null, event);
            lastEvent.set(event);
            symbols.add(event.symbol());
        });

        assertEquals(10_000, summary.acceptedPriceEvents());
        assertEquals(10_000, eventCount.get());
        assertEquals(0, summary.nonPrice());
        assertEquals(0, summary.ignoredZero());
        for (CsvTradeParser.RejectionReason reason : CsvTradeParser.RejectionReason.values()) {
            assertEquals(0, summary.rejected(reason), "unexpected " + reason + " rejection");
        }
        assertEquals(1_227, symbols.size());
        assertEquals("IEBBB.FR", firstEvent.get().symbol());
        assertEquals(14, firstEvent.get().sourceLine());
        assertEquals(OffsetDateTime.parse("2021-11-08T07:00:00.021+02:00"),
                firstEvent.get().timestamp());
        assertEquals("IPR1S.FR", lastEvent.get().symbol());
        assertEquals(10_013, lastEvent.get().sourceLine());
        assertEquals(18.87, lastEvent.get().price());
        assertEquals(OffsetDateTime.parse("2021-11-08T07:16:45.485+02:00"),
                lastEvent.get().timestamp());
    }

    @Test
    void ignoresZeroPricesBeforeTimestampValidation() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        CsvTradeParser.ParseSummary summary = parser.parse(
                FIXTURES.resolve("zero-price-handling.csv"), events::add);

        assertEquals(4, summary.acceptedPriceEvents());
        assertEquals(4, events.size());
        assertEquals(7, summary.ignoredZero());
        assertEquals(0, summary.rejected(CsvTradeParser.RejectionReason.EVENT_TIMESTAMP));
        assertEquals(4, events.stream().filter(event -> event.symbol().equals("PRICE.FR")).count());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:00+02:00"),
                events.get(0).timestamp());
    }

    @Test
    void usesSystemDateFallbackWhenTradingDateIsMissing() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        CsvTradeParser.ParseSummary summary = parser.parse(
                FIXTURES.resolve("trading-date-fallback.csv"), events::add);

        assertEquals(1, summary.acceptedPriceEvents());
        assertEquals(1, events.size());
        assertEquals("FALLBACK.FR", events.get(0).symbol());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:00.250+02:00"),
                events.get(0).timestamp());
        assertEquals(0, summary.nonPrice());
        assertEquals(0, summary.ignoredZero());
        for (CsvTradeParser.RejectionReason reason : CsvTradeParser.RejectionReason.values()) {
            assertEquals(0, summary.rejected(reason));
        }
    }

    @Test
    void rejectsPopulatedInvalidTradingDateEvenWhenSystemDateIsValid() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        CsvTradeParser.ParseSummary summary = parser.parse(
                FIXTURES.resolve("invalid-trading-date.csv"), events::add);

        assertEquals(0, summary.acceptedPriceEvents());
        assertEquals(0, events.size());
        assertEquals(1, summary.rejected(CsvTradeParser.RejectionReason.EVENT_TIMESTAMP));
    }

    @Test
    void treatsNullNaNAndInfinityAsMeaningfulNonEmptyPriceValues() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        CsvTradeParser.ParseSummary summary = parser.parse(
                FIXTURES.resolve("special-price-literals.csv"), events::add);

        assertEquals(1, summary.acceptedPriceEvents());
        assertEquals(1, events.size());
        assertEquals(1, summary.nonPrice());
        assertEquals(3, summary.rejected(CsvTradeParser.RejectionReason.PRICE));
        assertEquals(0, summary.rejected(CsvTradeParser.RejectionReason.EVENT_TIMESTAMP));
        assertEquals("VALID.FR", events.get(0).symbol());
    }

    @Test
    void supportsQuotedMultilineFieldsAndCountsValidationOutcomes(@TempDir Path tempDir) throws IOException {
        Path csv = tempDir.resolve("quoted.csv");
        Files.writeString(csv, """
                # comment

                ID,SecType,Date,Time,Last,Trading time,Trading date,Description
                MULTI.FR,E,08-11-2021,, ,09:00:00.123456,,"quoted,
                value"
                BAD.X,E,08-11-2021,,2,09:00:00,08-11-2021,
                VALID.NL,E,08-11-2021,,bad,09:00:00,08-11-2021,
                SHORT.FR,E,08-11-2021
                EXTRA.FR,E,08-11-2021,,3,09:00:00,08-11-2021,description,extra
                BADTIME.FR,E,08-11-2021,,3,25:00:00,08-11-2021,
                VALID.FR,E,08-11-2021,,3,09:00:00.123456,08-11-2021,
                """);

        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        CsvTradeParser.ParseSummary summary = parser.parse(csv, events::add);

        assertEquals(1, summary.acceptedPriceEvents());
        assertEquals(1, summary.nonPrice());
        assertEquals(2, summary.rejected(CsvTradeParser.RejectionReason.SHAPE));
        assertEquals(1, summary.rejected(CsvTradeParser.RejectionReason.IDENTITY));
        assertEquals(1, summary.rejected(CsvTradeParser.RejectionReason.PRICE));
        assertEquals(1, summary.rejected(CsvTradeParser.RejectionReason.EVENT_TIMESTAMP));
        assertEquals(11, events.get(0).sourceLine());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:00.123456+02:00"),
                events.get(0).timestamp());
    }

    @Test
    void rejectsMissingOrRepeatedRequiredHeaders(@TempDir Path tempDir) throws IOException {
        Path missing = tempDir.resolve("missing.csv");
        Files.writeString(missing, "ID,SecType\n");
        assertThrows(IOException.class, () -> parser.parse(missing, ignored -> { }));

        Path repeated = tempDir.resolve("repeated.csv");
        Files.writeString(repeated,
                "ID,ID,SecType,Date,Time,Last,Trading date,Trading time\n");
        assertThrows(IOException.class, () -> parser.parse(repeated, ignored -> { }));
    }

    @Test
    void rejectsMalformedCsvWithItsPhysicalLine(@TempDir Path tempDir) throws IOException {
        Path malformed = tempDir.resolve("malformed.csv");
        Files.writeString(malformed, """
                ID,SecType,Date,Time,Last,Trading date,Trading time
                "BROKEN.FR,E,08-11-2021,,1,08-11-2021,09:00:00
                """);

        IOException exception = assertThrows(IOException.class,
                () -> parser.parse(malformed, ignored -> { }));
        assertTrue(exception.getMessage().contains(":2:"));
    }
}
