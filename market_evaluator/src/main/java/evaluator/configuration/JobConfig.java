package evaluator.configuration;

public record JobConfig(String bootstrapServers, int parallelism, boolean bounded,
        String ticksTopic, String indicatorsTopic, String advisoriesTopic) {
    public static final String USAGE = "Usage: evaluator.Main [--bootstrap-servers HOST:PORT] [--parallelism N]"
            + " [--bounded] [--ticks-topic NAME] [--indicators-topic NAME] [--advisories-topic NAME]";

    public JobConfig {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Require positive parallelism");
        }
    }

    public static JobConfig parse(String[] args) {
        String bootstrapServers = "localhost:9092";
        int parallelism = 1;
        boolean bounded = false;
        String ticksTopic = "ticks";
        String indicatorsTopic = "indicators";
        String advisoriesTopic = "advisories";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap-servers" -> bootstrapServers = optionValue(args, ++i);
                case "--parallelism" -> parallelism = Integer.parseInt(optionValue(args, ++i));
                case "--bounded" -> bounded = true;
                case "--ticks-topic" -> ticksTopic = optionValue(args, ++i);
                case "--indicators-topic" -> indicatorsTopic = optionValue(args, ++i);
                case "--advisories-topic" -> advisoriesTopic = optionValue(args, ++i);
                default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
            }
        }
        return new JobConfig(bootstrapServers, parallelism, bounded, ticksTopic, indicatorsTopic, advisoriesTopic);
    }

    private static String optionValue(String[] args, int index) {
        if (index >= args.length) {
            throw new IllegalArgumentException("Missing option value");
        }
        return args[index];
    }
}
