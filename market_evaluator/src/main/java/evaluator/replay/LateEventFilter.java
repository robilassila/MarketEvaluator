package evaluator.replay;

import evaluator.model.PriceEvent;
import evaluator.processing.QueryOnePipeline;

/**
 * Drops events whose five-minute window has already ended at the highest event time replayed so
 * far, i.e. events the window would already have been evaluated without.
 *
 * <p>This lives in the single-threaded producer rather than the job: with several Kafka partitions,
 * whether Flink drops a late event depends on how far the other partitions have progressed at that
 * moment, which makes results differ between runs. After this filter no event in the topic can be
 * late at any partition, so Flink never drops one.
 */
final class LateEventFilter {
    private long highWater = Long.MIN_VALUE;
    private long dropped;

    boolean accept(PriceEvent event) {
        long millis = event.timestampMillis();
        long windowEnd = QueryOnePipeline.windowStart(millis) + QueryOnePipeline.WINDOW.toMillis();
        if (windowEnd <= highWater) {
            dropped++;
            return false;
        }
        highWater = Math.max(highWater, millis);
        return true;
    }

    long dropped() {
        return dropped;
    }
}
