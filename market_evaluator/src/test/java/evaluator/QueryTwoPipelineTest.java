package evaluator;

import static org.junit.jupiter.api.Assertions.*;

import evaluator.model.AdvisoryRecord;
import evaluator.processing.EmaFunction;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueryTwoPipelineTest {
    private static final Path FIXTURES = Path.of("src/test/resources/fixtures");

    @Test
    void crossoverUsesStrictCurrentAndInclusivePreviousComparisons() {
        assertEquals("BUY", EmaFunction.crossoverDirection(2, 1, 1, 1));
        assertEquals("SELL", EmaFunction.crossoverDirection(1, 2, 2, 2));
        assertNull(EmaFunction.crossoverDirection(3, 2, 2, 1));
        assertNull(EmaFunction.crossoverDirection(1, 2, 2, 3));
        assertNull(EmaFunction.crossoverDirection(2, 2, 1, 3));
        assertNull(EmaFunction.crossoverDirection(2, 2, 3, 1));
        assertNull(EmaFunction.crossoverDirection(3, 2, 3, 1));
        assertNull(EmaFunction.crossoverDirection(1, 2, 1, 3));
    }

    @Test
    void crossoversAreEmittedFromFinalizedWindows() throws Exception {
        List<AdvisoryRecord> signals = PipelineRun.run(FIXTURES.resolve("crossovers.csv"), 1).advisories();

        assertEquals(List.of("BUY", "SELL", "BUY"),
                signals.stream().map(record -> record.direction).toList());
        assertEquals(3, signals.size());
        for (AdvisoryRecord signal : signals) {
            assertEquals("CROSS.FR", signal.symbol);
            assertEquals(signal.windowStart + 300_000, signal.windowEnd);
        }
        assertEquals(java.time.OffsetDateTime.parse("2021-11-08T00:10:00+02:00")
                .toInstant().toEpochMilli(), signals.get(0).windowStart);
    }
}
