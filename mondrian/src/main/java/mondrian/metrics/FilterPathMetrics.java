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
 * Prometheus counters for Filter path selection.
 *
 * <p>Labels are intentionally low-cardinality to avoid metric cardinality
 * growth in production.
 */
public class FilterPathMetrics {

    public static final Counter filterPathSelected = Counter.build()
        .name("filter_path_selected_total")
        .help("Total number of Filter path selections by path and reason")
        .labelNames("path", "reason")
        .register();

    private FilterPathMetrics() {
    }

    public static void recordPathSelection(String path, String reason) {
        filterPathSelected
            .labels(normalize(path), normalize(reason))
            .inc();
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
