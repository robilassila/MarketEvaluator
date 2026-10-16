package evaluator.configuration;

import evaluator.CsvTradeParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

public record JobConfig(List<String> inputs, int parallelism, long watermarkDelayMs) {
    public JobConfig {
        inputs = List.copyOf(inputs);
        if (inputs.isEmpty() || parallelism <= 0 || watermarkDelayMs < 0) {
            throw new IllegalArgumentException("Require input files, positive parallelism and nonnegative watermark delay");
        }
    }

    public static JobConfig parse(String[] args) throws IOException {
        int parallelism = 1;
        long delay = 0;
        List<Path> paths = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--parallelism" -> parallelism = Integer.parseInt(optionValue(args, ++i));
                case "--watermark-delay-ms" -> delay = Long.parseLong(optionValue(args, ++i));
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
        return new JobConfig(paths.stream().map(Path::toString).toList(), parallelism, delay);
    }

    private static String optionValue(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing option value");
        }
        return args[index];
    }
}
