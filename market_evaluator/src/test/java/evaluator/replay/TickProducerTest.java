package evaluator.replay;

import static org.junit.jupiter.api.Assertions.*;

import evaluator.model.PriceEvent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TickProducerTest {
    @Test
    void boundaryLogRecordsFirstEventAtOrPastEachNewBoundary(@TempDir Path temp) throws Exception {
        Path log = temp.resolve("boundaries.csv");
        try (var boundaries = TickProducer.BoundaryLog.open(log)) {
            for (long millis : new long[]{299_999, 300_000, 300_001, 299_000, 1_234_567}) {
                boundaries.beforeSend(event(millis));
            }
        }
        List<String> lines = Files.readAllLines(log);
        assertEquals("boundary_millis,sent_at_millis", lines.get(0));
        assertEquals(List.of(0L, 300_000L, 1_200_000L),
                lines.subList(1, lines.size()).stream().map(line -> Long.parseLong(line.split(",")[0])).toList());
    }

    @Test
    void lateEventFilterDropsEventsWhoseWindowEndedAtTheHighWaterMark() {
        LateEventFilter filter = new LateEventFilter();
        assertTrue(filter.accept(event(299_999)));
        assertTrue(filter.accept(event(250_000)), "Disorder inside an open window is kept");
        assertTrue(filter.accept(event(300_000)));
        assertFalse(filter.accept(event(1)), "Window [0, 300000) ended when time reached 300000");
        assertTrue(filter.accept(event(900_000)));
        assertFalse(filter.accept(event(599_999)));
        assertFalse(filter.accept(event(600_000)));
        assertTrue(filter.accept(event(900_001)));
        assertEquals(3, filter.dropped());
    }

    @Test
    void parseSortsInputsAndRejectsInvalidOptions(@TempDir Path temp) throws Exception {
        Path later = Files.writeString(temp.resolve("day-09.csv"), "");
        Path first = Files.writeString(temp.resolve("day-08.csv"), "");
        var options = TickProducer.parse(new String[]{"--rate", "1000", later.toString(), first.toString()});
        assertEquals(List.of(first, later), options.inputs());
        assertEquals(16, options.partitions());
        assertEquals(1000, options.rate());
        for (String[] args : List.of(new String[0], new String[]{"--rate", "-1", first.toString()},
                new String[]{"--partitions", "0", first.toString()},
                new String[]{temp.resolve("missing.csv").toString()})) {
            assertThrows(IllegalArgumentException.class, () -> TickProducer.parse(args));
        }
    }

    private static PriceEvent event(long millis) {
        PriceEvent event = new PriceEvent();
        event.symbol = "SAME.FR";
        event.securityType = "E";
        event.price = 1;
        event.timestampMicros = millis * 1_000;
        return event;
    }
}
