package evaluator.wire;

import static org.junit.jupiter.api.Assertions.*;

import evaluator.model.PriceEvent;
import org.junit.jupiter.api.Test;

class PriceEventCodecTest {
    @Test
    void roundTripKeepsEveryFieldIncludingTieBreakers() {
        PriceEvent event = new PriceEvent();
        event.symbol = "IEBBB.FR";
        event.securityType = "I";
        event.price = 52.69;
        event.timestampMicros = 1_636_355_100_000_001L;
        event.fileOrdinal = 6;
        event.sourceLine = 123_456_789_012L;

        PriceEvent decoded = PriceEventCodec.decode(PriceEventCodec.key(event), PriceEventCodec.value(event));

        assertEquals(event.symbol, decoded.symbol);
        assertEquals(event.securityType, decoded.securityType);
        assertEquals(event.price, decoded.price);
        assertEquals(event.timestampMicros, decoded.timestampMicros);
        assertEquals(event.fileOrdinal, decoded.fileOrdinal);
        assertEquals(event.sourceLine, decoded.sourceLine);
    }

    @Test
    void rejectsMalformedRecords() {
        assertThrows(IllegalArgumentException.class, () -> PriceEventCodec.decode(null, new byte[29]));
        assertThrows(IllegalArgumentException.class, () -> PriceEventCodec.decode(new byte[]{'A'}, new byte[3]));
        assertThrows(IllegalArgumentException.class, () -> PriceEventCodec.decode(new byte[]{'A'}, new byte[29]));
    }
}
