package evaluator.configuration;

import evaluator.CsvTradeParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record JobConfig(List<String> inputs, int parallelism,
        Set<String> subscriptions, Path outputDirectory) {
    public JobConfig {
        inputs = List.copyOf(inputs);
        subscriptions = Set.copyOf(subscriptions);
        if (inputs.isEmpty() || parallelism <= 0) {
            throw new IllegalArgumentException("Require input files and positive parallelism");
        }
    }

    public JobConfig(List<String> inputs, int parallelism) {
        this(inputs, parallelism, Set.of(), null);
    }

    public static JobConfig parse(String[] args) throws IOException {
        int parallelism = 1;
        List<Path> paths = new ArrayList<>();
        Set<String> subscriptions = new HashSet<>();
        Path outputDirectory = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--parallelism" -> parallelism = Integer.parseInt(optionValue(args, ++i));
                case "--subscribe" -> {
                    String symbol = optionValue(args, ++i);
                    if (!validSymbol(symbol)) {
                        throw new IllegalArgumentException("Invalid full symbol for --subscribe: " + symbol);
                    }
                    subscriptions.add(symbol);
                }
                case "--output-dir" -> outputDirectory = Path.of(optionValue(args, ++i));
                default -> {
                    if (args[i].startsWith("--")) {
                        throw new IllegalArgumentException("Unknown option: " + args[i]);
                    }
                    paths.add(Path.of(args[i]));
                }
            }
        }
        if (paths.isEmpty() || parallelism <= 0 || outputDirectory == null) {
            throw new IllegalArgumentException("Require input files, --output-dir and positive parallelism");
        }
        for (Path path : paths) {
            if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
                throw new IOException("Not a readable CSV file: " + path);
            }
        }
        // Daily files must be read in date order. The DEBS file names (debs2022-gc-trading-day-DD-11-21.csv)
        // sort chronologically because the whole data set lies within one month.
        paths.sort(Comparator.comparing(path -> path.getFileName().toString()));
        return new JobConfig(paths.stream().map(Path::toString).toList(), parallelism,
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
