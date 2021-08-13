package brave.metric;

import java.time.Duration;

public interface DistributionSummaryMetric extends Metric {

    default boolean enableHistogram() {
        return false;
    }

    default double[] publishPercentiles() {
        return new double[] { 0.9, 0.95, 0.99 };
    }

    default Duration sampleWindow() {
        return Duration.ofMinutes(2);
    }

    default int sampleBuckets() {
        return 4;
    }
}
