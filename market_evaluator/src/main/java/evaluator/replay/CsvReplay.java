package evaluator.replay;

import evaluator.CsvTradeParser;
import evaluator.model.PriceEvent;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/** Turns DEBS CSV files into price events in replay order. */
public final class CsvReplay {
    private CsvReplay() { }

    /**
     * Daily files must be replayed in date order. The DEBS file names (debs2022-gc-trading-day-DD-11-21.csv)
     * sort chronologically because the whole data set lies within one month.
     */
    public static List<Path> chronological(Collection<Path> paths) {
        return paths.stream().sorted(Comparator.comparing(path -> path.getFileName().toString())).toList();
    }

    /** Emits events in file order, then line order. The file ordinal is the position in {@code orderedPaths}. */
    public static void forEach(List<Path> orderedPaths, Consumer<PriceEvent> consumer) throws IOException {
        CsvTradeParser parser = new CsvTradeParser();
        for (int fileOrdinal = 0; fileOrdinal < orderedPaths.size(); fileOrdinal++) {
            int ordinal = fileOrdinal;
            parser.parse(orderedPaths.get(fileOrdinal), trade -> consumer.accept(PriceEvent.from(trade, ordinal)));
        }
    }

    public static List<PriceEvent> readAll(List<Path> orderedPaths) throws IOException {
        List<PriceEvent> events = new ArrayList<>();
        forEach(orderedPaths, events::add);
        return events;
    }
}
