package evaluator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Reads DEBS trading CSV files and streams eligible price events to a consumer.
 */
public final class CsvTradeParser {
    private static final Set<String> REQUIRED_HEADERS = Set.of(
            "ID", "SecType", "Date", "Time", "Last", "Trading date", "Trading time");
    private static final Set<String> EXCHANGES = Set.of(".FR", ".NL", ".ETR");
    private static final ZoneOffset EVENT_OFFSET = ZoneOffset.ofHours(2);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter
            .ofPattern("dd-MM-uuuu", Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter TIME_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

    public static void printEventColumnMeanings(PrintStream output) {
        Objects.requireNonNull(output, "output");
        output.println("Event columns:");
        output.println("symbol       - Full instrument ID, including exchange suffix");
        output.println("securityType - E for equity or I for index");
        output.println("price        - Positive Last price");
        output.println("timestamp    - Trading date/time at UTC+02:00");
        output.println("line         - Physical starting line number in the CSV file");
        output.println();
    }

    public ParseSummary parse(Path path, Consumer<TradeEvent> eventConsumer) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(eventConsumer, "eventConsumer");

        try (RecordReader reader = open(path)) {
            ParsedRow row;
            while ((row = reader.nextRow()) != null) {
                if (row.event() != null) {
                    eventConsumer.accept(row.event());
                }
            }
            return reader.summary();
        }
    }

    public RecordReader open(Path path) throws IOException {
        return new RecordReader(path);
    }

    /** The system date identifies a daily file; it does not change event-time validation. */
    public LocalDate discoverDate(Path path) throws IOException {
        try (RecordReader reader = open(path)) {
            CsvRecord record;
            while ((record = reader.nextRecord()) != null) {
                try {
                    return LocalDate.parse(value(record.fields(), reader.header, "Date"), DATE_FORMAT);
                } catch (DateTimeParseException ignored) {
                    // Keep scanning until a usable system date is encountered.
                }
            }
        }
        throw new IOException(path + ": no valid system Date for daily-file ordering");
    }

    public record ParsedRow(TradeEvent event) { }

    /** Reads at most one logical data record per call, preserving physical provenance. */
    public static final class RecordReader implements AutoCloseable {
        private final BufferedReader reader;
        private final CsvState state;
        private final CsvRecord[] ready = new CsvRecord[1];
        private final MutableSummary summary = new MutableSummary();
        private final Header header;
        private long lineNumber;
        private boolean finished;

        private RecordReader(Path path) throws IOException {
            reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
            state = new CsvState(path, record -> ready[0] = record);
            try {
                CsvRecord first = nextRecord();
                if (first == null) {
                    throw new IOException(path + ": CSV has no header record");
                }
                header = validateHeader(path, first);
            } catch (IOException | RuntimeException exception) {
                reader.close();
                throw exception;
            }
        }

        private CsvRecord nextRecord() throws IOException {
            if (finished) {
                return null;
            }
            String line;
            while ((line = reader.readLine()) != null) {
                state.acceptLine(line, ++lineNumber);
                if (ready[0] != null) {
                    CsvRecord record = ready[0];
                    ready[0] = null;
                    return record;
                }
            }
            state.finish();
            finished = true;
            return null;
        }

        public ParsedRow nextRow() throws IOException {
            CsvRecord record = nextRecord();
            if (record == null) {
                return null;
            }
            TradeEvent[] event = new TradeEvent[1];
            processRow(record, header, value -> event[0] = value, summary);
            return new ParsedRow(event[0]);
        }

        public ParseSummary summary() {
            return summary.toImmutable();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static Header validateHeader(Path path, CsvRecord record) throws IOException {
        List<String> names = new ArrayList<>(record.fields());
        if (!names.isEmpty() && names.get(0).startsWith("\uFEFF")) {
            names.set(0, names.get(0).substring(1));
        }
        Map<String, Integer> indexes = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i).strip();
            names.set(i, name);
            if (REQUIRED_HEADERS.contains(name) && indexes.putIfAbsent(name, i) != null) {
                throw new IOException(path + ":" + record.startLine()
                        + ": repeated required header '" + name + "'");
            }
            indexes.putIfAbsent(name, i);
        }
        for (String required : REQUIRED_HEADERS) {
            if (!indexes.containsKey(required)) {
                throw new IOException(path + ":" + record.startLine()
                        + ": missing required header '" + required + "'");
            }
        }
        return new Header(names.size(), indexes);
    }

    private static void processRow(
            CsvRecord record,
            Header header,
            Consumer<TradeEvent> eventConsumer,
            MutableSummary summary) {
        List<String> fields = record.fields();
        if (fields.size() < header.width()
                || fields.size() > header.width()
                && fields.subList(header.width(), fields.size()).stream().anyMatch(value -> !value.isEmpty())) {
            summary.reject(RejectionReason.SHAPE);
            return;
        }

        String symbol = value(fields, header, "ID");
        String securityType = value(fields, header, "SecType");
        if (symbol.isEmpty() || EXCHANGES.stream().noneMatch(symbol::endsWith)
                || !(securityType.equals("E") || securityType.equals("I"))) {
            summary.reject(RejectionReason.IDENTITY);
            return;
        }

        String priceText = value(fields, header, "Last");
        if (priceText.isEmpty()) {
            summary.nonPrice++;
            return;
        }

        final double price;
        try {
            price = Double.parseDouble(priceText);
        } catch (NumberFormatException exception) {
            summary.reject(RejectionReason.PRICE);
            return;
        }
        if (!Double.isFinite(price) || price < 0.0) {
            summary.reject(RejectionReason.PRICE);
            return;
        }
        if (price == 0.0) {
            summary.ignoredZero++;
            return;
        }

        String eventDateText = value(fields, header, "Trading date");
        if (eventDateText.isEmpty()) {
            eventDateText = value(fields, header, "Date");
        }
        String eventTimeText = value(fields, header, "Trading time");

        final OffsetDateTime timestamp;
        try {
            if (eventDateText.isEmpty() || eventTimeText.isEmpty()) {
                throw new DateTimeParseException("Missing event date or time", "", 0);
            }
            LocalDate date = LocalDate.parse(eventDateText, DATE_FORMAT);
            LocalTime time = LocalTime.parse(eventTimeText, TIME_FORMAT);
            timestamp = OffsetDateTime.of(date, time, EVENT_OFFSET);
        } catch (DateTimeParseException exception) {
            summary.reject(RejectionReason.EVENT_TIMESTAMP);
            return;
        }

        eventConsumer.accept(new TradeEvent(symbol, securityType, price, timestamp, record.startLine()));
        summary.acceptedPriceEvents++;
    }

    private static String value(List<String> fields, Header header, String name) {
        int index = header.indexes().get(name);
        return index < fields.size() ? fields.get(index) : "";
    }

    public record TradeEvent(
            String symbol,
            String securityType,
            double price,
            OffsetDateTime timestamp,
            long sourceLine) {
    }

    public record ParseSummary(
            long acceptedPriceEvents,
            long nonPrice,
            long ignoredZero,
            Map<RejectionReason, Long> rejectedRows) {
        public ParseSummary {
            EnumMap<RejectionReason, Long> copy = new EnumMap<>(RejectionReason.class);
            copy.putAll(rejectedRows);
            rejectedRows = Collections.unmodifiableMap(copy);
        }

        public long rejected(RejectionReason reason) {
            return rejectedRows.getOrDefault(reason, 0L);
        }
    }

    public enum RejectionReason {
        SHAPE,
        IDENTITY,
        PRICE,
        EVENT_TIMESTAMP
    }

    @FunctionalInterface
    private interface CsvRecordConsumer {
        void accept(CsvRecord record) throws IOException;
    }

    private record CsvRecord(List<String> fields, long startLine) {
    }

    private record Header(int width, Map<String, Integer> indexes) {
    }

    private static final class MutableSummary {
        private long acceptedPriceEvents;
        private long nonPrice;
        private long ignoredZero;
        private final EnumMap<RejectionReason, Long> rejected = new EnumMap<>(RejectionReason.class);

        private void reject(RejectionReason reason) {
            rejected.merge(reason, 1L, Long::sum);
        }

        private ParseSummary toImmutable() {
            return new ParseSummary(acceptedPriceEvents, nonPrice, ignoredZero, rejected);
        }
    }

    private static final class CsvState {
        private final Path path;
        private final CsvRecordConsumer consumer;
        private final List<String> fields = new ArrayList<>();
        private final StringBuilder field = new StringBuilder();
        private CsvFieldState fieldState = CsvFieldState.START;
        private boolean active;
        private long startLine;

        private CsvState(Path path, CsvRecordConsumer consumer) {
            this.path = path;
            this.consumer = consumer;
        }

        private void acceptLine(String line, long lineNumber) throws IOException {
            if (!active && (line.isBlank() || line.startsWith("#"))) {
                return;
            }
            if (!active) {
                active = true;
                startLine = lineNumber;
            }

            for (int i = 0; i < line.length(); i++) {
                char character = line.charAt(i);
                switch (fieldState) {
                    case START -> {
                        if (character == ',') {
                            finishField();
                        } else if (character == '"') {
                            fieldState = CsvFieldState.QUOTED;
                        } else {
                            field.append(character);
                            fieldState = CsvFieldState.UNQUOTED;
                        }
                    }
                    case UNQUOTED -> {
                        if (character == ',') {
                            finishField();
                        } else if (character == '"') {
                            throw malformed(lineNumber, "quote inside an unquoted field");
                        } else {
                            field.append(character);
                        }
                    }
                    case QUOTED -> {
                        if (character == '"') {
                            fieldState = CsvFieldState.AFTER_QUOTE;
                        } else {
                            field.append(character);
                        }
                    }
                    case AFTER_QUOTE -> {
                        if (character == '"') {
                            field.append('"');
                            fieldState = CsvFieldState.QUOTED;
                        } else if (character == ',') {
                            finishField();
                        } else if (Character.isWhitespace(character)) {
                            // Surrounding whitespace is stripped from each parsed field.
                        } else {
                            throw malformed(lineNumber, "unexpected character after a quoted field");
                        }
                    }
                }
            }

            if (fieldState == CsvFieldState.QUOTED) {
                field.append('\n');
            } else {
                finishRecord();
            }
        }

        private void finishField() {
            fields.add(field.toString().strip());
            field.setLength(0);
            fieldState = CsvFieldState.START;
        }

        private void finishRecord() throws IOException {
            finishField();
            consumer.accept(new CsvRecord(List.copyOf(fields), startLine));
            fields.clear();
            active = false;
        }

        private void finish() throws IOException {
            if (active) {
                throw malformed(startLine, "unterminated quoted field");
            }
        }

        private IOException malformed(long lineNumber, String message) {
            return new IOException(path + ":" + lineNumber + ": malformed CSV: " + message);
        }
    }

    private enum CsvFieldState {
        START,
        UNQUOTED,
        QUOTED,
        AFTER_QUOTE
    }
}
