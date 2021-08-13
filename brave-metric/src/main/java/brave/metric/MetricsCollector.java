package brave.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MetricsCollector {

    private final Map<DistributionSummaryMetric, DistributionSummary> summaries = new HashMap<>();
    private final Map<CounterMetric, Counter> counters = new HashMap<>();
    private final MeterRegistry registry;
    private final int maximumMetrics;

    public MetricsCollector(MeterRegistry registry, int maximumMetrics) {
        this.registry = registry;
        this.maximumMetrics = maximumMetrics;
    }

    public void collect(DistributionSummaryMetric serviceMetric, double value) {
        var summary = getDistributionSummary(serviceMetric);
        if (summary == null) {
            log.warn("Metric " + serviceMetric + " is not register yet");
        } else {
            summary.record(value);
        }
    }

    public void increase(CounterMetric counterMetric, double value) {
        var counter = getCounter(counterMetric);
        if (counter == null) {
            log.warn("Metric " + counterMetric + " is not registered yet");
        } else {
            counter.increment(value);
        }
    }

    public void increase(CounterMetric counterMetric) {
        increase(counterMetric, 1);
    }

    private Counter getCounter(CounterMetric serviceMetric) {
        Counter counter;
        if ((counter = counters.get(serviceMetric)) == null) {
            if (reachMaximumMetrics()) {
                log.warn("Maximum metrics reached: {}", maximumMetrics);
                return null;
            }

            counter = this.createDefaultCounter(serviceMetric);
            counters.put(serviceMetric, counter);
        }

        return counter;
    }

    private DistributionSummary getDistributionSummary(DistributionSummaryMetric serviceMetric) {
        DistributionSummary summary;
        if ((summary = summaries.get(serviceMetric)) == null) {
            if (reachMaximumMetrics()) {
                log.warn("Maximum metrics reached: {}", maximumMetrics);
                return null;
            }

            summary = this.createDefaultSummary(serviceMetric);
            summaries.put(serviceMetric, summary);
        }

        return summary;

    }

    private boolean reachMaximumMetrics() {
        return counters.size() + summaries.size() >= maximumMetrics;
    }

    private DistributionSummary createDefaultSummary(DistributionSummaryMetric summaryMetric) {
        return DistributionSummary
                .builder(summaryMetric.metricName())
                .tags(summaryMetric.tags())
                .publishPercentileHistogram(summaryMetric.enableHistogram())
                .publishPercentiles(summaryMetric.publishPercentiles())
                .distributionStatisticExpiry(summaryMetric.sampleWindow())
                .distributionStatisticBufferLength(summaryMetric.sampleBuckets())
                .baseUnit(summaryMetric.baseUnit())
                .register(registry);
    }

    private Counter createDefaultCounter(CounterMetric counterMetric) {
        return Counter.builder(counterMetric.metricName())
                .tags(counterMetric.tags())
                .baseUnit(counterMetric.baseUnit())
                .register(registry);
    }

}
