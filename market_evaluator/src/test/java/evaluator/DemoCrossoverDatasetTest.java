package evaluator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DemoCrossoverDatasetTest {
    @Test
    void demoFeedProducesRepeatedBuyAndSellAdvisories() throws Exception {
        var result = PipelineRun.run(Path.of("../data/ema-crossover-demo.csv"), 1);
        var signals = result.advisories().stream()
                .filter(signal -> signal.symbol.equals("DEMO.FR"))
                .toList();

        assertEquals(20, signals.size());
        assertEquals(Set.of("BUY", "SELL"), signals.stream()
                .map(signal -> signal.direction)
                .collect(java.util.stream.Collectors.toSet()));
    }
}
