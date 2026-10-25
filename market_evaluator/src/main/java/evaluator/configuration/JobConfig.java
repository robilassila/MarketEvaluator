package evaluator.configuration;

import evaluator.CsvTradeParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record JobConfig(List<String> inputs, int parallelism, long watermarkDelayMs,
        Set<String> subscriptions, Path outputDirectory) {
    public JobConfig {
        inputs = List.copyOf(inputs);
        subscriptions = Set.copyOf(subscriptions);
        if (inputs.isEmpty() || parallelism <= 0 || watermarkDelayMs < 0) {
            throw new IllegalArgumentException("Require input files, positive parallelism and nonnegative watermark delay");
        }
    }

    public JobConfig(List<String> inputs, int parallelism, long watermarkDelayMs) {
        this(inputs, parallelism, watermarkDelayMs, Set.of(), null);
    }

    public static JobConfig parse(String[] args) throws IOException {
        int parallelism = 1;
        long delay = 0;
        List<Path> paths = new ArrayList<>();
        Set<String> subscriptions = new LinkedHashSet<>();
        Path outputDirectory = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--parallelism" -> parallelism = Integer.parseInt(optionValue(args, ++i));
                case "--watermark-delay-ms" -> delay = Long.parseLong(optionValue(args, ++i));
                case "--subscribe" -> {
                    String symbol = optionValue(args, ++i);
                    if (!validSymbol(symbol)) {
                        throw new IllegalArgumentException("Invalid full symbol for --subscribe: " + symbol);
                    }
                    if (!subscriptions.add(symbol)) {
                        throw new IllegalArgumentException("Duplicate subscription: " + symbol);
                    }
                }
                case "--output-dir" -> {
                    if (outputDirectory != null) {
                        throw new IllegalArgumentException("Duplicate --output-dir option");
                    }
                    outputDirectory = Path.of(optionValue(args, ++i)).toAbsolutePath().normalize();
                }
                default -> {
                    if (args[i].startsWith("--")) {
                        throw new IllegalArgumentException("Unknown option: " + args[i]);
                    }
                    paths.add(Path.of(args[i]).toAbsolutePath().normalize());
                }
            }
        }
        if (paths.isEmpty() || parallelism <= 0 || delay < 0) {
            throw new IllegalArgumentException("Require input files, positive parallelism and nonnegative watermark delay");
        }
        if (new HashSet<>(paths).size() != paths.size()) {
            throw new IllegalArgumentException("Duplicate input path");
        }
        for (Path path : paths) {
            if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
                throw new IOException("Not a readable CSV file: " + path);
            }
        }
        if (paths.size() > 1) {
            record DailyFile(Path path, LocalDate date) { }
            List<DailyFile> files = new ArrayList<>();
            CsvTradeParser parser = new CsvTradeParser();
            for (Path path : paths) {
                files.add(new DailyFile(path, parser.discoverDate(path)));
            }
            Path cwd = Path.of("").toAbsolutePath().normalize();
            files.sort(Comparator.comparing(DailyFile::date)
                    .thenComparing(file -> cwd.relativize(file.path()).toString().replace('\\', '/')));
            paths = files.stream().map(DailyFile::path).toList();
        }
        return new JobConfig(paths.stream().map(Path::toString).toList(), parallelism, delay,
                subscriptions, outputDirectory);
    }

    public boolean isSubscribed(String symbol) {
        return subscriptions.isEmpty() || subscriptions.contains(symbol);
    }

    private static boolean validSymbol(String symbol) {
        return symbol != null && !symbol.isBlank() && CsvTradeParser.isValidSymbol(symbol);
    }

    private static String optionValue(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing option value");
        }
        return args[index];
    }
}
