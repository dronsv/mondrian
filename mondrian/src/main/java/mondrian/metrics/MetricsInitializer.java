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

import io.prometheus.client.hotspot.DefaultExports;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class MetricsInitializer implements ServletContextListener {
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        DefaultExports.initialize();
        ExecutionMetrics.updateMetrics();
    }
    @Override
    public void contextDestroyed(ServletContextEvent sce) {
    }
}
