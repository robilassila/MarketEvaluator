package evaluator.visualizer;

import evaluator.CsvTradeParser;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

final class MarketDataStore {
    static final int INDICATOR_LIMIT = 240;
    static final int ADVISORY_LIMIT = 100;
    private static final int CLIENT_QUEUE_LIMIT = 512;

    private final Map<String, SymbolHistory> history = new HashMap<>();
    private final TreeSet<String> symbols = new TreeSet<>();
    private final CopyOnWriteArrayList<Client> clients = new CopyOnWriteArrayList<>();
    private String streamError;
    private boolean streamReady;

    synchronized List<String> symbols() {
        return List.copyOf(symbols);
    }

    synchronized Client subscribe(String symbol) {
        Client client = new Client(symbol);
        SymbolHistory data = history.getOrDefault(symbol, new SymbolHistory());
        client.offer(new Event("snapshot", snapshotJson(symbol, data)));
        client.offer(new Event("status", statusJson()));
        clients.add(client);
        return client;
    }

    void remove(Client client) {
        clients.remove(client);
        client.close();
    }

    synchronized void addIndicator(String line) {
        String[] fields = fields(line, 9, "indicator");
        String symbol = validSymbol(fields[0]);
        Indicator indicator = new Indicator(
                symbol,
                parseTime(fields[2]),
                parseNumber(fields[3], "close"),
                parseNumber(fields[4], "ema38"),
                parseNumber(fields[5], "ema100"));
        SymbolHistory data = history.computeIfAbsent(symbol, ignored -> new SymbolHistory());
        symbols.add(symbol);
        data.indicators.put(indicator.windowEndMillis(), indicator);
        trim(data.indicators, INDICATOR_LIMIT);
        broadcast(symbol, new Event("indicator", indicator.toJson()));
    }

    synchronized void addAdvisory(String line) {
        String[] fields = fields(line, 4, "advisory");
        String symbol = validSymbol(fields[0]);
        long windowEndMillis = parseTime(fields[2]);
        String direction = fields[3];
        if (!direction.equals("BUY") && !direction.equals("SELL")) {
            throw new IllegalArgumentException("Invalid advisory direction: " + direction);
        }
        Advisory advisory = new Advisory(symbol, windowEndMillis, direction);
        SymbolHistory data = history.computeIfAbsent(symbol, ignored -> new SymbolHistory());
        symbols.add(symbol);
        String key = windowEndMillis + ":" + direction;
        data.advisories.put(key, advisory);
        trim(data.advisories, ADVISORY_LIMIT);
        broadcast(symbol, new Event("advisory", advisory.toJson()));
    }

    synchronized void setStreamError(String message) {
        streamError = message;
        Event event = new Event("status", errorJson(message));
        for (Client client : clients) {
            if (!client.offer(event)) {
                client.close();
            }
        }
    }

    synchronized void setStreamReady() {
        streamReady = true;
        streamError = null;
        Event event = new Event("status", statusJson());
        for (Client client : clients) {
            if (!client.offer(event)) {
                client.close();
            }
        }
    }

    private void broadcast(String symbol, Event event) {
        for (Client client : clients) {
            if (client.symbol.equals(symbol) && !client.offer(event)) {
                client.close();
            }
        }
    }

    private static String snapshotJson(String symbol, SymbolHistory data) {
        String indicators = data.indicators.values().stream()
                .map(Indicator::toJson).reduce((left, right) -> left + "," + right).orElse("");
        String advisories = data.advisories.values().stream()
                .sorted(Comparator.comparingLong(Advisory::windowEndMillis).reversed())
                .map(Advisory::toJson).reduce((left, right) -> left + "," + right).orElse("");
        return "{\"symbol\":" + quote(symbol) + ",\"indicators\":[" + indicators
                + "],\"advisories\":[" + advisories + "]}";
    }

    private static String[] fields(String line, int expected, String recordType) {
        String[] fields = line.split(",", -1);
        if (fields.length != expected) {
            throw new IllegalArgumentException("Invalid " + recordType + " record: expected "
                    + expected + " fields, got " + fields.length);
        }
        return fields;
    }

    private static String validSymbol(String symbol) {
        if (!CsvTradeParser.isValidSymbol(symbol)) {
            throw new IllegalArgumentException("Invalid symbol in Kafka record: " + symbol);
        }
        return symbol;
    }

    private static long parseTime(String value) {
        try {
            return OffsetDateTime.parse(value).toInstant().toEpochMilli();
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Invalid timestamp in Kafka record: " + value, exception);
        }
    }

    private static double parseNumber(String value, String field) {
        try {
            double number = Double.parseDouble(value);
            if (!Double.isFinite(number)) {
                throw new NumberFormatException("not finite");
            }
            return number;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid " + field + " in Kafka record: " + value, exception);
        }
    }

    private static <K, V> void trim(TreeMap<K, V> records, int limit) {
        while (records.size() > limit) {
            records.pollFirstEntry();
        }
    }

    private static <K, V> void trim(LinkedHashMapWithOrder<K, V> records, int limit) {
        while (records.size() > limit) {
            records.removeOldest();
        }
    }

    private static String errorJson(String message) {
        return "{\"state\":\"error\",\"message\":" + quote(message) + "}";
    }

    private String statusJson() {
        if (streamError != null) {
            return errorJson(streamError);
        }
        return "{\"state\":\"" + (streamReady ? "connected" : "connecting") + "\"}";
    }

    static String quote(String value) {
        StringBuilder json = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            switch (character) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (character < 0x20) {
                        json.append(String.format("\\u%04x", (int) character));
                    } else {
                        json.append(character);
                    }
                }
            }
        }
        return json.append('"').toString();
    }

    record Event(String name, String data) { }

    static final class Client {
        private final String symbol;
        private final ArrayBlockingQueue<Event> events = new ArrayBlockingQueue<>(CLIENT_QUEUE_LIMIT);
        private volatile boolean open = true;

        private Client(String symbol) {
            this.symbol = symbol;
        }

        boolean offer(Event event) {
            return open && events.offer(event);
        }

        Event poll(long timeout, TimeUnit unit) throws InterruptedException {
            return events.poll(timeout, unit);
        }

        boolean isOpen() {
            return open;
        }

        void close() {
            open = false;
            events.clear();
        }
    }

    private static final class SymbolHistory {
        private final TreeMap<Long, Indicator> indicators = new TreeMap<>();
        private final LinkedHashMapWithOrder<String, Advisory> advisories = new LinkedHashMapWithOrder<>();
    }

    private record Indicator(String symbol, long windowEndMillis, double close, double ema38, double ema100) {
        private String toJson() {
            return "{\"windowEndMillis\":" + windowEndMillis + ",\"close\":" + close
                    + ",\"ema38\":" + ema38 + ",\"ema100\":" + ema100 + "}";
        }
    }

    private record Advisory(String symbol, long windowEndMillis, String direction) {
        private String toJson() {
            return "{\"windowEndMillis\":" + windowEndMillis + ",\"direction\":"
                    + quote(direction) + "}";
        }
    }

    private static final class LinkedHashMapWithOrder<K, V> extends java.util.LinkedHashMap<K, V> {
        private LinkedHashMapWithOrder() {
            super(16, 0.75f, true);
        }

        private void removeOldest() {
            remove(keySet().iterator().next());
        }
    }
}
