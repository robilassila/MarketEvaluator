package evaluator.visualizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MarketDataStoreTest {
    @Test
    void retainsBoundedSymbolHistoryAndStreamsNewRecords() throws Exception {
        MarketDataStore store = new MarketDataStore();
        MarketDataStore.Client client = store.subscribe("TEST.FR");
        assertEquals("snapshot", client.poll(1, TimeUnit.SECONDS).name());
        assertEquals("status", client.poll(1, TimeUnit.SECONDS).name());

        for (int i = 0; i < MarketDataStore.INDICATOR_LIMIT + 1; i++) {
            long windowEnd = 1_636_323_600_000L + i * 300_000L;
            String timestamp = java.time.Instant.ofEpochMilli(windowEnd)
                    .atOffset(java.time.ZoneOffset.ofHours(2)).toString();
            store.addIndicator("TEST.FR," + timestamp + "," + timestamp + ",12.5,12.4,12.3,"
                    + timestamp + ",0,1");
        }

        assertEquals(List.of("TEST.FR"), store.symbols());
        assertEquals("indicator", client.poll(1, TimeUnit.SECONDS).name());
        MarketDataStore.Client snapshotClient = store.subscribe("TEST.FR");
        MarketDataStore.Event snapshot = snapshotClient.poll(1, TimeUnit.SECONDS);
        assertEquals("snapshot", snapshot.name());
        assertTrue(snapshot.data().contains("\"indicators\":["));
        assertEquals(MarketDataStore.INDICATOR_LIMIT, occurrences(snapshot.data(), "\"windowEndMillis\""));
        assertFalse(snapshot.data().contains("\"windowEndMillis\":1636323600000"));
        store.remove(client);
        store.remove(snapshotClient);
    }

    @Test
    void rejectsMalformedRecordsAndInvalidAdvisories() {
        MarketDataStore store = new MarketDataStore();
        assertThrows(IllegalArgumentException.class, () -> store.addIndicator("bad"));
        assertThrows(IllegalArgumentException.class, () -> store.addAdvisory(
                "TEST.FR,2021-11-08T00:00:00+02:00,2021-11-08T00:05:00+02:00,HOLD"));
        assertThrows(IllegalArgumentException.class, () -> store.addIndicator(
                "INVALID.XY,2021-11-08T00:05:00+02:00,2021-11-08T00:05:00+02:00,12.5,12.4,12.3,"
                        + "2021-11-08T00:05:00+02:00,0,1"));
    }

    @Test
    void reportsExistingStreamErrorToLateSubscribers() throws Exception {
        MarketDataStore store = new MarketDataStore();
        store.setStreamError("Kafka unavailable");

        MarketDataStore.Client client = store.subscribe("TEST.FR");
        assertEquals("snapshot", client.poll(1, TimeUnit.SECONDS).name());
        MarketDataStore.Event status = client.poll(1, TimeUnit.SECONDS);
        assertEquals("status", status.name());
        assertEquals("{\"state\":\"error\",\"message\":\"Kafka unavailable\"}", status.data());
        store.remove(client);
    }

    private static int occurrences(String text, String value) {
        return (text.length() - text.replace(value, "").length()) / value.length();
    }
}
