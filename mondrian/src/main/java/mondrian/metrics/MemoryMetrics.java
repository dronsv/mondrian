package mondrian.metrics;

import io.prometheus.client.Gauge;
import mondrian.olap.Util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class MemoryMetrics {

    private static final Gauge memoryThreshold = Gauge.build()
            .name("memory_threshold_bytes")
            .help("Memory threshold in bytes")
            .register();

    private static final Gauge usedMemoryForThreshold = Gauge.build()
            .name("used_memory_for_threshold_bytes")
            .help("Used memory for threshold in bytes")
            .register();

    public static void updateThresholdMemoryMetrics() {
        memoryThreshold.set(Util.getMemoryThreshold());
        usedMemoryForThreshold.set(Util.getUsedMemoryForThreshold());
    }
}
