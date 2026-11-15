package evaluator.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class SubscriberTest {
    @Test
    void subscriptionsAreValidatedAndDefaultToAll() {
        var all = Subscriber.parse(new String[]{"--output-dir", "out"});
        assertTrue(all.isSubscribed("SAME.FR"));
        assertTrue(all.isSubscribed("SAME.NL"));

        var some = Subscriber.parse(new String[]{"--subscribe", "SAME.FR", "--output-dir", "out"});
        assertTrue(some.isSubscribed("SAME.FR"));
        assertFalse(some.isSubscribed("SAME.NL"));

        for (String[] args : List.of(new String[0],
                new String[]{"--subscribe", "UNKNOWN.XY", "--output-dir", "out"},
                new String[]{"--unknown", "--output-dir", "out"})) {
            assertThrows(IllegalArgumentException.class, () -> Subscriber.parse(args));
        }
    }
}
