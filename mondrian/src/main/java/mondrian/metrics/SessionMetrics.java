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