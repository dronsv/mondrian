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

public class SessionMetrics {

    private static final Gauge sessionCount = Gauge.build()
            .name("sessions_active_total")
            .help("Current number of active sessions")
            .register();

    public static void setSessionCount(int currentCount) {
        sessionCount.set(currentCount);
    }
}