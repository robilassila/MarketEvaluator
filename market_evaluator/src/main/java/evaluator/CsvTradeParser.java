package evaluator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.DateTimeException;
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
import java.util.Arrays;
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
    private static final ParsedRow NO_EVENT = new ParsedRow(null);

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

    public void parse(Path path, Consumer<TradeEvent> eventConsumer) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(eventConsumer, "eventConsumer");

        try (RecordReader reader = open(path)) {
            ParsedRow row;
            while ((row = reader.nextRow()) != null) {
                if (row.event() != null) {
                    eventConsumer.accept(row.event());
                }
            }
        }
    }

    public RecordReader open(Path path) throws IOException {
        return new RecordReader(path);
    }

    public record ParsedRow(TradeEvent event) { }

    /** Reads at most one data record (one physical line) per call, preserving its line number. */
    public static final class RecordReader implements AutoCloseable {
        private final Path path;
        private final BufferedReader reader;
        private final LineFields lineFields = new LineFields();
        private final Header header;
        private long lineNumber;

        private RecordReader(Path path) throws IOException {
            this.path = path;
            reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
            try {
                LineFields first = nextRecord();
                if (first == null) {
                    throw new IOException(path + ": CSV has no header record");
                }
                header = validateHeader(path, first);
            } catch (IOException | RuntimeException exception) {
                reader.close();
                throw exception;
            }
        }

        /** The returned fields are only valid until the next call. */
        private LineFields nextRecord() throws IOException {
            String line;
            while ((line = reader.readLine()) != null) {
                ++lineNumber;
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                if (line.indexOf('"') >= 0) {
                    throw new IOException(path + ":" + lineNumber
                            + ": quoted CSV fields are not supported");
                }
                lineFields.load(line, lineNumber);
                return lineFields;
            }
            return null;
        }

        public ParsedRow nextRow() throws IOException {
            LineFields record = nextRecord();
            if (record == null) {
                return null;
            }
            TradeEvent event = processRow(record, header);
            return event == null ? NO_EVENT : new ParsedRow(event);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    private static Header validateHeader(Path path, LineFields record) throws IOException {
        List<String> names = new ArrayList<>(record.size());
        for (int i = 0; i < record.size(); i++) {
            names.add(record.get(i));
        }
        if (!names.isEmpty() && names.get(0).startsWith("﻿")) {
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
        return new Header(
                names.size(),
                indexes.get("ID"),
                indexes.get("SecType"),
                indexes.get("Date"),
                indexes.get("Last"),
                indexes.get("Trading date"),
                indexes.get("Trading time"));
    }

    /** Returns the row's positive-price event, or null if the row is invalid or has no positive price. */
    private static TradeEvent processRow(LineFields fields, Header header) {
        if (fields.size() < header.width() || hasNonBlankExcess(fields, header.width())) {
            return null;
        }

        String symbol = fields.get(header.id());
        String securityType = fields.get(header.securityType());
        if (!isValidSymbol(symbol)
                || !(securityType.equals("E") || securityType.equals("I"))) {
            return null;
        }

        // Most rows have no price; skip them before parseDouble would throw for each one.
        if (fields.isBlank(header.last())) {
            return null;
        }

        final double price;
        try {
            price = Double.parseDouble(fields.get(header.last()));
        } catch (NumberFormatException exception) {
            return null;
        }
        if (!Double.isFinite(price) || price <= 0.0) {
            return null;
        }

        String eventDateText = fields.get(header.tradingDate());
        if (eventDateText.isEmpty()) {
            eventDateText = fields.get(header.date());
        }
        String eventTimeText = fields.get(header.tradingTime());

        if (eventDateText.isEmpty() || eventTimeText.isEmpty()) {
            return null;
        }
        final OffsetDateTime timestamp;
        try {
            timestamp = OffsetDateTime.of(parseDate(eventDateText), parseTime(eventTimeText), EVENT_OFFSET);
        } catch (DateTimeParseException exception) {
            return null;
        }

        return new TradeEvent(symbol, securityType, price, timestamp, fields.startLine());
    }

    private static boolean hasNonBlankExcess(LineFields fields, int width) {
        for (int i = width; i < fields.size(); i++) {
            if (!fields.isBlank(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parses the common {@code dd-MM-uuuu} shape directly. Anything else, including values the
     * direct path cannot vouch for, goes through the strict formatter so semantics stay identical.
     */
    private static LocalDate parseDate(String text) {
        if (text.length() == 10 && text.charAt(2) == '-' && text.charAt(5) == '-') {
            int day = digits(text, 0, 2);
            int month = digits(text, 3, 5);
            int year = digits(text, 6, 10);
            if (day >= 0 && month >= 0 && year >= 0) {
                try {
                    return LocalDate.of(year, month, day);
                } catch (DateTimeException ignored) {
                    // The formatter produces the authoritative rejection.
                }
            }
        }
        return LocalDate.parse(text, DATE_FORMAT);
    }

    /** Same fallback rule as {@link #parseDate} for {@code HH:mm:ss[.f{1,6}]}. */
    private static LocalTime parseTime(String text) {
        int length = text.length();
        boolean shaped = (length == 8 || length >= 10 && length <= 15 && text.charAt(8) == '.')
                && text.charAt(2) == ':' && text.charAt(5) == ':';
        if (shaped) {
            int hour = digits(text, 0, 2);
            int minute = digits(text, 3, 5);
            int second = digits(text, 6, 8);
            int fraction = length == 8 ? 0 : digits(text, 9, length);
            if (hour >= 0 && minute >= 0 && second >= 0 && fraction >= 0) {
                int nanos = fraction;
                for (int scale = length == 8 ? 9 : length - 9; scale < 9; scale++) {
                    nanos *= 10;
                }
                try {
                    return LocalTime.of(hour, minute, second, nanos);
                } catch (DateTimeException ignored) {
                    // The formatter produces the authoritative rejection.
                }
            }
        }
        return LocalTime.parse(text, TIME_FORMAT);
    }

    /** Returns the ASCII decimal value of {@code text[from, to)}, or -1 if any character is not a digit. */
    private static int digits(String text, int from, int to) {
        int value = 0;
        for (int i = from; i < to; i++) {
            char character = text.charAt(i);
            if (character < '0' || character > '9') {
                return -1;
            }
            value = value * 10 + (character - '0');
        }
        return value;
    }

    public record TradeEvent(
            String symbol,
            String securityType,
            double price,
            OffsetDateTime timestamp,
            long sourceLine) {
    }

    public static boolean isValidSymbol(String symbol) {
        return symbol != null && !symbol.isEmpty()
                && (symbol.endsWith(".FR") || symbol.endsWith(".NL") || symbol.endsWith(".ETR"));
    }

    /**
     * Reusable view over one unquoted CSV line. Stores raw field boundaries and only strips and
     * creates strings for fields that are read.
     */
    private static final class LineFields {
        private String line;
        private int[] starts = new int[64];
        private int[] ends = new int[64];
        private int size;
        private long startLine;

        private void load(String line, long lineNumber) {
            this.line = line;
            this.startLine = lineNumber;
            size = 0;
            int start = 0;
            while (true) {
                int comma = line.indexOf(',', start);
                int end = comma < 0 ? line.length() : comma;
                add(start, end);
                if (comma < 0) {
                    return;
                }
                start = comma + 1;
            }
        }

        private void add(int start, int end) {
            if (size == starts.length) {
                starts = Arrays.copyOf(starts, size * 2);
                ends = Arrays.copyOf(ends, size * 2);
            }
            starts[size] = start;
            ends[size] = end;
            size++;
        }

        private int size() { return size; }
        private String get(int index) { return line.substring(starts[index], ends[index]).strip(); }

        private boolean isBlank(int index) {
            for (int i = starts[index]; i < ends[index]; i++) {
                if (!Character.isWhitespace(line.charAt(i))) {
                    return false;
                }
            }
            return true;
        }

        private long startLine() { return startLine; }
    }

    private record Header(
            int width,
            int id,
            int securityType,
            int date,
            int last,
            int tradingDate,
            int tradingTime) {
    }
}
