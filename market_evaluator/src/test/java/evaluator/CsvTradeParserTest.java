package evaluator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import evaluator.replay.TickProducer;
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
    void cliPrintsUsageForInvalidOptions() {
        ByteArrayOutputStream errorBytes = new ByteArrayOutputStream();
        PrintStream error = new PrintStream(errorBytes);

        assertEquals(2, Main.run(new String[]{"--parallelism", "0"},
                new PrintStream(new ByteArrayOutputStream()), error));
        assertTrue(errorBytes.toString().contains("Usage: evaluator.Main"));

        errorBytes.reset();
        assertEquals(2, TickProducer.run(new String[0], new PrintStream(new ByteArrayOutputStream()), error));
        assertTrue(errorBytes.toString().contains("Usage: evaluator.replay.TickProducer"));
    }

    @Test
    void producerPrintsReadableErrorForMissingInputFile(@TempDir Path tempDir) {
        ByteArrayOutputStream errorBytes = new ByteArrayOutputStream();
        PrintStream error = new PrintStream(errorBytes);

        int exitCode = TickProducer.run(new String[]{tempDir.resolve("missing.csv").toString()},
                new PrintStream(new ByteArrayOutputStream()), error);

        assertEquals(2, exitCode);
        assertTrue(errorBytes.toString().contains("Not a readable CSV file"));
        assertTrue(errorBytes.toString().contains("missing.csv"));
    }

    @Test
    void parsesWindowOrderingFixtureAndRetainsSourceLines() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        parser.parse(
                FIXTURES.resolve("window-ordering.csv"), events::add);

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
        parser.parse(
                FIXTURES.resolve("crossovers.csv"), events::add);

        assertEquals(37, events.size());
        assertEquals("CROSS.FR", events.get(0).symbol());
        assertEquals(1000.0, events.get(events.size() - 1).price());
        assertEquals(42, events.get(events.size() - 1).sourceLine());
    }

    @Test
    void parsesEveryRowOfOriginalSampleData() throws IOException {
        AtomicLong eventCount = new AtomicLong();
        AtomicReference<CsvTradeParser.TradeEvent> firstEvent = new AtomicReference<>();
        AtomicReference<CsvTradeParser.TradeEvent> lastEvent = new AtomicReference<>();
        Set<String> symbols = new HashSet<>();

        parser.parse(RAW_SAMPLE, event -> {
            eventCount.incrementAndGet();
            firstEvent.compareAndSet(null, event);
            lastEvent.set(event);
            symbols.add(event.symbol());
        });

        assertEquals(10_000, eventCount.get());
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
    void ignoresZeroPrices() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        parser.parse(
                FIXTURES.resolve("zero-price-handling.csv"), events::add);

        assertEquals(4, events.size());
        assertEquals(4, events.stream().filter(event -> event.symbol().equals("PRICE.FR")).count());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:00+02:00"),
                events.get(0).timestamp());
    }

    @Test
    void usesSystemDateFallbackWhenTradingDateIsMissing() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        parser.parse(
                FIXTURES.resolve("trading-date-fallback.csv"), events::add);

        assertEquals(1, events.size());
        assertEquals("FALLBACK.FR", events.get(0).symbol());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:00.250+02:00"),
                events.get(0).timestamp());
    }

    @Test
    void rejectsPopulatedInvalidTradingDateEvenWhenSystemDateIsValid() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        parser.parse(
                FIXTURES.resolve("invalid-trading-date.csv"), events::add);

        assertEquals(0, events.size());
    }

    @Test
    void treatsNullNaNAndInfinityAsMeaningfulNonEmptyPriceValues() throws IOException {
        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        parser.parse(
                FIXTURES.resolve("special-price-literals.csv"), events::add);

        assertEquals(1, events.size());
        assertEquals("VALID.FR", events.get(0).symbol());
    }

    @Test
    void skipsCommentsAndInvalidRows(@TempDir Path tempDir) throws IOException {
        Path csv = tempDir.resolve("outcomes.csv");
        Files.writeString(csv, """
                # comment

                ID,SecType,Date,Time,Last,Trading time,Trading date,Description
                BLANK.FR,E,08-11-2021,, ,09:00:00.123456,,description
                # comment between records
                BAD.X,E,08-11-2021,,2,09:00:00,08-11-2021,
                VALID.NL,E,08-11-2021,,bad,09:00:00,08-11-2021,
                SHORT.FR,E,08-11-2021
                EXTRA.FR,E,08-11-2021,,3,09:00:00,08-11-2021,description,extra
                BADTIME.FR,E,08-11-2021,,3,25:00:00,08-11-2021,
                VALID.FR,E,08-11-2021,,3,09:00:00.123456,08-11-2021,
                """);

        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        parser.parse(csv, events::add);

        assertEquals(List.of("VALID.FR"), events.stream().map(CsvTradeParser.TradeEvent::symbol).toList());
        assertEquals(11, events.get(0).sourceLine());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:00.123456+02:00"),
                events.get(0).timestamp());
    }

    @Test
    void parsesEventDateAndTimeStrictlyAndStripsUnquotedFields(@TempDir Path tempDir) throws IOException {
        Path csv = tempDir.resolve("timestamps.csv");
        Files.writeString(csv, """
                ID,SecType,Date,Time,Last,Trading time,Trading date
                 PADDED.FR , E ,08-11-2021,, 3.5 , 09:00:00 , 08-11-2021\s
                ONE.FR,E,08-11-2021,,1,09:00:00.1,08-11-2021
                SIX.FR,E,08-11-2021,,1,23:59:59.000123,08-11-2021
                LEAP.FR,E,29-02-2024,,1,09:00:00,
                SHORTHOUR.FR,E,08-11-2021,,1,9:00:00,08-11-2021
                SEVEN.FR,E,08-11-2021,,1,09:00:00.1234567,08-11-2021
                MIDNIGHT.FR,E,08-11-2021,,1,24:00:00,08-11-2021
                DOT.FR,E,08-11-2021,,1,09:00:00.,08-11-2021
                FEB.FR,E,08-11-2021,,1,09:00:00,31-02-2021
                NOTLEAP.FR,E,08-11-2021,,1,09:00:00,29-02-2021
                SHORTDAY.FR,E,08-11-2021,,1,09:00:00,8-11-2021
                SHORTYEAR.FR,E,08-11-2021,,1,09:00:00,08-11-21
                """);

        List<CsvTradeParser.TradeEvent> events = new ArrayList<>();
        parser.parse(csv, events::add);

        assertEquals(List.of("PADDED.FR", "ONE.FR", "SIX.FR", "LEAP.FR"),
                events.stream().map(CsvTradeParser.TradeEvent::symbol).toList());
        assertEquals("PADDED.FR", events.get(0).symbol());
        assertEquals("E", events.get(0).securityType());
        assertEquals(3.5, events.get(0).price());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:00+02:00"), events.get(0).timestamp());
        assertEquals(OffsetDateTime.parse("2021-11-08T09:00:00.1+02:00"), events.get(1).timestamp());
        assertEquals(OffsetDateTime.parse("2021-11-08T23:59:59.000123+02:00"), events.get(2).timestamp());
        assertEquals(OffsetDateTime.parse("2024-02-29T09:00:00+02:00"), events.get(3).timestamp());
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
    void rejectsQuotedFieldsWithTheirPhysicalLine(@TempDir Path tempDir) throws IOException {
        Path quoted = tempDir.resolve("quoted.csv");
        Files.writeString(quoted, """
                ID,SecType,Date,Time,Last,Trading date,Trading time
                VALID.FR,E,08-11-2021,,1,08-11-2021,09:00:00
                "QUOTED.FR",E,08-11-2021,,1,08-11-2021,09:00:00
                """);

        IOException exception = assertThrows(IOException.class,
                () -> parser.parse(quoted, ignored -> { }));
        assertTrue(exception.getMessage().contains(":3:"));
        assertTrue(exception.getMessage().contains("quoted CSV fields are not supported"));
    }
}
