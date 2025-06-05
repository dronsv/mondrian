/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2021-2025 Sergei Semenkov
// All Rights Reserved.
*/

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
