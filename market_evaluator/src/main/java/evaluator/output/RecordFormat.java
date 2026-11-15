package evaluator.output;

import evaluator.model.AdvisoryRecord;
import evaluator.model.IndicatorRecord;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** CSV lines published to the output topics; the subscriber writes them unchanged under these headers. */
public final class RecordFormat {
    public static final String INDICATOR_HEADER =
            "symbol,window_start,window_end,close,ema38,ema100,closing_timestamp,file_ordinal,source_line";
    public static final String ADVISORY_HEADER = "symbol,window_start,window_end,direction";
    /** Zero-based position of window_end in both formats. */
    public static final int WINDOW_END_COLUMN = 2;
    private static final ZoneOffset OUTPUT_OFFSET = ZoneOffset.ofHours(2);

    private RecordFormat() { }

    public static String indicator(IndicatorRecord indicator) {
        return String.join(",", indicator.symbol,
                timestamp(indicator.windowStart), timestamp(indicator.windowEnd),
                Double.toString(indicator.close),
                Double.toString(indicator.ema38), Double.toString(indicator.ema100),
                timestampMicros(indicator.closingTimestampMicros),
                Integer.toString(indicator.fileOrdinal), Long.toString(indicator.sourceLine));
    }

    public static String advisory(AdvisoryRecord advisory) {
        return String.join(",", advisory.symbol,
                timestamp(advisory.windowStart), timestamp(advisory.windowEnd), advisory.direction);
    }

    public static long windowEndMillis(String line) {
        return OffsetDateTime.parse(line.split(",", -1)[WINDOW_END_COLUMN]).toInstant().toEpochMilli();
    }

    private static String timestamp(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atOffset(OUTPUT_OFFSET).toString();
    }

    private static String timestampMicros(long epochMicros) {
        long seconds = Math.floorDiv(epochMicros, 1_000_000L);
        long nanos = Math.floorMod(epochMicros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos).atOffset(OUTPUT_OFFSET).toString();
    }
}
