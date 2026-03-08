/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/

package mondrian.metrics;

import io.prometheus.client.Counter;

/**
 * Prometheus counters for Filter fast-path runtime stages.
 *
 * <p>Metrics are counters (not histograms) to keep overhead low and to remain
 * compatible with existing deployment setups.
 */
public class FilterExecutionMetrics {

    public static final Counter filterFastPathStageExecutions = Counter.build()
        .name("filter_fastpath_stage_executions_total")
        .help("Total number of Filter fast-path stage executions by path and stage")
        .labelNames("path", "stage")
        .register();

    public static final Counter filterFastPathStageSeconds = Counter.build()
        .name("filter_fastpath_stage_seconds_total")
        .help("Total wall-clock seconds spent in Filter fast-path stages")
        .labelNames("path", "stage")
        .register();

    public static final Counter filterFastPathStageTuples = Counter.build()
        .name("filter_fastpath_stage_tuples_total")
        .help("Total tuple counts observed in Filter fast-path stages")
        .labelNames("path", "stage")
        .register();

    private FilterExecutionMetrics() {
    }

    public static void recordStageExecution(String path, String stage) {
        filterFastPathStageExecutions
            .labels(normalize(path), normalize(stage))
            .inc();
    }

    public static void recordStageDurationNanos(
        String path,
        String stage,
        long durationNanos)
    {
        if (durationNanos <= 0L) {
            return;
        }
        filterFastPathStageSeconds
            .labels(normalize(path), normalize(stage))
            .inc(durationNanos / 1_000_000_000d);
    }

    public static void recordStageTuples(String path, String stage, int tuples) {
        if (tuples <= 0) {
            return;
        }
        filterFastPathStageTuples
            .labels(normalize(path), normalize(stage))
            .inc(tuples);
    }

    private static String normalize(String value) {
        if (value == null) {
            return "unknown";
        }
        final String trimmed = value.trim();
        if (trimmed.length() == 0) {
            return "unknown";
        }
        return trimmed.toLowerCase();
    }
}
