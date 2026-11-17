package evaluator.visualizer;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import evaluator.kafka.Topics;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public final class VisualizerServer implements AutoCloseable {
    private static final String DEFAULT_INDICATORS_TOPIC = "indicators";
    private static final String DEFAULT_ADVISORIES_TOPIC = "advisories";
    private final String bootstrapServers;
    private final String indicatorsTopic;
    private final String advisoriesTopic;
    private final int port;
    private final MarketDataStore data = new MarketDataStore();
    private final ExecutorService httpThreads = Executors.newCachedThreadPool();
    private final HttpServer server;
    private volatile KafkaConsumer<byte[], byte[]> consumer;
    private volatile boolean running = true;
    private Thread kafkaThread;

    private VisualizerServer(Options options) throws IOException {
        bootstrapServers = options.bootstrapServers();
        indicatorsTopic = options.indicatorsTopic();
        advisoriesTopic = options.advisoriesTopic();
        port = options.port();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(httpThreads);
        server.createContext("/", this::handle);
    }

    public static void main(String[] args) throws Exception {
        Options options;
        try {
            options = Options.parse(args);
        } catch (IllegalArgumentException exception) {
            System.err.println(exception.getMessage());
            System.err.println(Options.USAGE);
            System.exit(2);
            return;
        }
        VisualizerServer visualizer = new VisualizerServer(options);
        Runtime.getRuntime().addShutdownHook(new Thread(visualizer::close));
        visualizer.start();
        System.out.println("Visualizer listening on http://0.0.0.0:" + options.port());
    }

    private void start() {
        kafkaThread = new Thread(this::consume, "market-visualizer-kafka");
        kafkaThread.setDaemon(true);
        kafkaThread.start();
        server.start();
    }

    private void consume() {
        try (Topics topics = new Topics(bootstrapServers)) {
            List<TopicPartition> partitions = new ArrayList<>();
            partitions.addAll(topics.partitions(indicatorsTopic));
            partitions.addAll(topics.partitions(advisoriesTopic));
            if (partitions.isEmpty()) {
                throw new IllegalStateException("No partitions found for the visualizer topics");
            }
            Properties properties = consumerProperties();
            try (KafkaConsumer<byte[], byte[]> kafka = new KafkaConsumer<>(properties)) {
                consumer = kafka;
                kafka.assign(partitions);
                kafka.seekToBeginning(partitions);
                data.setStreamReady();
                while (running) {
                    for (ConsumerRecord<byte[], byte[]> record : kafka.poll(Duration.ofMillis(500))) {
                        String line = new String(record.value(), StandardCharsets.UTF_8);
                        if (record.topic().equals(indicatorsTopic)) {
                            data.addIndicator(line);
                        } else if (record.topic().equals(advisoriesTopic)) {
                            data.addAdvisory(line);
                        }
                    }
                }
            }
        } catch (WakeupException exception) {
            if (running) {
                fail("Kafka consumer stopped unexpectedly", exception);
            }
        } catch (Exception exception) {
            if (running) {
                fail("Could not read Kafka output topics: " + exception.getMessage(), exception);
            }
        } finally {
            consumer = null;
        }
    }

    private void fail(String message, Exception exception) {
        System.err.println(message);
        exception.printStackTrace(System.err);
        data.setStreamError(message);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        switch (path) {
            case "/" -> sendResource(exchange, "/visualizer/index.html", "text/html; charset=utf-8");
            case "/app.js" -> sendResource(exchange, "/visualizer/app.js", "text/javascript; charset=utf-8");
            case "/styles.css" -> sendResource(exchange, "/visualizer/styles.css", "text/css; charset=utf-8");
            case "/api/symbols" -> sendJson(exchange, data.symbols().stream()
                    .map(MarketDataStore::quote).reduce((left, right) -> left + "," + right)
                    .map(items -> "[" + items + "]").orElse("[]"));
            case "/api/stream" -> stream(exchange);
            default -> send(exchange, 404, "text/plain; charset=utf-8", "Not found");
        }
    }

    private void stream(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equals("GET")) {
            send(exchange, 405, "text/plain; charset=utf-8", "GET required");
            return;
        }
        String symbol;
        try {
            symbol = queryParameter(exchange.getRequestURI().getRawQuery(), "symbol");
            if (!evaluator.CsvTradeParser.isValidSymbol(symbol)) {
                throw new IllegalArgumentException("Enter a full symbol ending in .FR, .NL or .ETR");
            }
        } catch (IllegalArgumentException exception) {
            send(exchange, 400, "text/plain; charset=utf-8", exception.getMessage());
            return;
        }

        MarketDataStore.Client client = data.subscribe(symbol);
        exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-cache, no-transform");
        exchange.getResponseHeaders().set("X-Accel-Buffering", "no");
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            while (client.isOpen()) {
                MarketDataStore.Event event = client.poll(15, java.util.concurrent.TimeUnit.SECONDS);
                if (event == null) {
                    output.write(": keep-alive\n\n".getBytes(StandardCharsets.UTF_8));
                } else {
                    output.write(("event: " + event.name() + "\ndata: " + event.data() + "\n\n")
                            .getBytes(StandardCharsets.UTF_8));
                }
                output.flush();
            }
        } catch (IOException ignored) {
            // Browsers close the stream when changing symbols or navigating away.
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } finally {
            data.remove(client);
            exchange.close();
        }
    }

    private static String queryParameter(String query, String name) {
        if (query == null) {
            throw new IllegalArgumentException("Missing symbol");
        }
        for (String part : query.split("&")) {
            String[] pair = part.split("=", 2);
            if (URLDecoder.decode(pair[0], StandardCharsets.UTF_8).equals(name)) {
                return pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "";
            }
        }
        throw new IllegalArgumentException("Missing symbol");
    }

    private static void sendResource(HttpExchange exchange, String path, String contentType) throws IOException {
        try (var resource = VisualizerServer.class.getResourceAsStream(path)) {
            if (resource == null) {
                send(exchange, 404, "text/plain; charset=utf-8", "Visualizer resource not found");
                return;
            }
            byte[] content = resource.readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.sendResponseHeaders(200, content.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(content);
            }
        }
    }

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        send(exchange, 200, "application/json; charset=utf-8", json);
    }

    private static void send(HttpExchange exchange, int status, String contentType, String text) throws IOException {
        byte[] content = text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, content.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(content);
        } finally {
            exchange.close();
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "market-visualizer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2_000);
        return properties;
    }

    @Override
    public void close() {
        running = false;
        KafkaConsumer<byte[], byte[]> activeConsumer = consumer;
        if (activeConsumer != null) {
            activeConsumer.wakeup();
        }
        server.stop(1);
        httpThreads.shutdownNow();
    }

    private record Options(String bootstrapServers, String indicatorsTopic, String advisoriesTopic, int port) {
        private static final String USAGE = "Usage: evaluator visualizer [--bootstrap-servers HOST:PORT]"
                + " [--indicators-topic NAME] [--advisories-topic NAME] [--port N]";

        private static Options parse(String[] args) {
            String bootstrap = "localhost:9092";
            String indicators = DEFAULT_INDICATORS_TOPIC;
            String advisories = DEFAULT_ADVISORIES_TOPIC;
            int port = 8080;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--bootstrap-servers" -> bootstrap = value(args, ++i);
                    case "--indicators-topic" -> indicators = value(args, ++i);
                    case "--advisories-topic" -> advisories = value(args, ++i);
                    case "--port" -> {
                        try {
                            port = Integer.parseInt(value(args, ++i));
                        } catch (NumberFormatException exception) {
                            throw new IllegalArgumentException("Port must be a number");
                        }
                        if (port < 1 || port > 65_535) {
                            throw new IllegalArgumentException("Port must be between 1 and 65535");
                        }
                    }
                    default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
                }
            }
            return new Options(bootstrap, indicators, advisories, port);
        }

        private static String value(String[] args, int index) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing option value");
            }
            return args[index];
        }
    }
}
