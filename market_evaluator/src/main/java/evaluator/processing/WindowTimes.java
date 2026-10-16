package evaluator.processing;

public final class WindowTimes {
    public static final long SIZE_MS = 300_000L;
    private WindowTimes() { }
    public static long start(long timestampMillis) {
        return Math.floorDiv(timestampMillis, SIZE_MS) * SIZE_MS;
    }
    public static long cutoff(long highWater, long delay) {
        return highWater < Long.MIN_VALUE + delay ? Long.MIN_VALUE : highWater - delay;
    }
}
