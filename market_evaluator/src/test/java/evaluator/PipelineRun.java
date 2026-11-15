package evaluator;

import evaluator.model.AdvisoryRecord;
import evaluator.model.IndicatorRecord;
import evaluator.model.PriceEvent;
import evaluator.processing.EmaFunction;
import evaluator.processing.QueryOnePipeline;
import evaluator.replay.CsvReplay;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Runs the query on in-memory events instead of Kafka. A single ordered source with the production
 * watermark strategy matches a one-partition ticks topic.
 */
final class PipelineRun {
    record Result(List<IndicatorRecord> indicators, List<AdvisoryRecord> advisories) { }

    private PipelineRun() { }

    static Result run(Path csv, int parallelism) throws Exception {
        return run(CsvReplay.readAll(List.of(csv.toAbsolutePath())), parallelism);
    }

    /** Indicators come back sorted by window start, then symbol; advisories by window start. */
    static Result run(List<PriceEvent> events, int parallelism) throws Exception {
        try (var runtime = new LocalFlinkRuntime()) {
            var environment = runtime.environment();
            QueryOnePipeline.configure(environment, parallelism);
            DataStream<PriceEvent> source = environment
                    .fromData(events, TypeInformation.of(PriceEvent.class))
                    .assignTimestampsAndWatermarks(QueryOnePipeline.watermarks());
            var indicators = QueryOnePipeline.build(source, parallelism);
            var indicatorCollector = new DataStream.Collector<IndicatorRecord>();
            var advisoryCollector = new DataStream.Collector<AdvisoryRecord>();
            indicators.collectAsync(indicatorCollector);
            indicators.getSideOutput(EmaFunction.ADVISORIES).collectAsync(advisoryCollector);
            var job = environment.executeAsync("Pipeline test");
            List<IndicatorRecord> indicatorRecords = new ArrayList<>();
            List<AdvisoryRecord> advisoryRecords;
            try (var indicatorResults = indicatorCollector.getOutput();
                 var advisoryResults = advisoryCollector.getOutput()) {
                // Drain both concurrently: a full, unread collect buffer would back-pressure the shared operator.
                var advisoriesDrained = CompletableFuture.supplyAsync(() -> {
                    List<AdvisoryRecord> drained = new ArrayList<>();
                    advisoryResults.forEachRemaining(drained::add);
                    return drained;
                });
                indicatorResults.forEachRemaining(indicatorRecords::add);
                advisoryRecords = advisoriesDrained.get();
                job.getJobExecutionResult().get();
            }
            indicatorRecords.sort(Comparator.comparingLong((IndicatorRecord r) -> r.windowStart)
                    .thenComparing(r -> r.symbol));
            advisoryRecords.sort(Comparator.comparingLong((AdvisoryRecord r) -> r.windowStart)
                    .thenComparing(r -> r.symbol));
            return new Result(indicatorRecords, advisoryRecords);
        }
    }
}
