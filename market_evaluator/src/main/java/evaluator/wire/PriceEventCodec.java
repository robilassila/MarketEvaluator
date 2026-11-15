package evaluator.wire;

import evaluator.model.PriceEvent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Kafka wire format for the "ticks" topic. The record key is the symbol, which Kafka hashes to
 * choose the partition; the value carries the remaining fields in a fixed-width layout.
 */
public final class PriceEventCodec {
    private static final int VALUE_SIZE = Byte.BYTES + Double.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES;

    private PriceEventCodec() { }

    public static byte[] key(PriceEvent event) {
        return event.symbol.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] value(PriceEvent event) {
        return ByteBuffer.allocate(VALUE_SIZE)
                .put((byte) event.securityType.charAt(0))
                .putDouble(event.price)
                .putLong(event.timestampMicros)
                .putInt(event.fileOrdinal)
                .putLong(event.sourceLine)
                .array();
    }

    public static PriceEvent decode(byte[] key, byte[] value) {
        if (key == null || value == null || value.length != VALUE_SIZE) {
            throw new IllegalArgumentException("Malformed tick record");
        }
        ByteBuffer buffer = ByteBuffer.wrap(value);
        PriceEvent event = new PriceEvent();
        event.symbol = new String(key, StandardCharsets.UTF_8);
        event.securityType = securityType(buffer.get());
        event.price = buffer.getDouble();
        event.timestampMicros = buffer.getLong();
        event.fileOrdinal = buffer.getInt();
        event.sourceLine = buffer.getLong();
        return event;
    }

    private static String securityType(byte code) {
        return switch (code) {
            case 'E' -> "E";
            case 'I' -> "I";
            default -> throw new IllegalArgumentException("Unknown security type: " + (char) code);
        };
    }
}
