package mondrian.metrics;

import io.prometheus.client.hotspot.DefaultExports;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class MetricsInitializer implements ServletContextListener {
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        DefaultExports.initialize();
    }
    @Override
    public void contextDestroyed(ServletContextEvent sce) {
    }
}
