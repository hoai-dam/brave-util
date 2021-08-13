package brave.metric;

import io.micrometer.core.instrument.Tags;

public interface Metric {

    String metricName();

    Tags tags();

    default String baseUnit() {
        return null;
    }

}
