/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2025 Sergei Semenkov
// All Rights Reserved.
*/

package mondrian.metrics;

import io.prometheus.client.Counter;

public class ExceptionMetrics {

    // Prometheus counter with a label for exception type
    private static final Counter exceptionCounter = Counter.build()
            .name("emondrian_exceptions_total")
            .help("Total number of exceptions by exception class")
            .labelNames("exception")
            .register();

    /**
     * Record an exception by name (optional).
     */
    public static void recordException(String exceptionName) {
        exceptionCounter.labels(exceptionName).inc();
    }
}