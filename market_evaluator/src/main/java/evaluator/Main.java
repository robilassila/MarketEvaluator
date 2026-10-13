package evaluator;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream output, PrintStream error) {
        if (args.length != 1) {
            error.println("Usage: evaluator.Main <csv-file>");
            error.println("Example: mvn compile exec:java "
                    + "-Dexec.mainClass=evaluator.Main "
                    + "-Dexec.args=\"../data/sample-positive-prices-08.csv\"");
            return 2;
        }

        Path input = Path.of(args[0]);
        CsvTradeParser.printEventColumnMeanings(output);
        try {
            var summary = new CsvTradeParser().parse(
                    input,
                    event -> output.printf("%s,%s,%s,%s,line=%d%n",
                            event.symbol(),
                            event.securityType(),
                            event.price(),
                            event.timestamp(),
                            event.sourceLine()));
            output.println(summary);
            return 0;
        } catch (IOException exception) {
            error.println("Could not parse CSV '" + input + "': " + exception.getMessage());
            return 1;
        }
    }
}
