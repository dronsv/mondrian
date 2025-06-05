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

public class ConnectionMetrics {

    // Prometheus metric with labels: user and database
    private static final Gauge connectionCount = Gauge.build()
            .name("active_connections_total")
            .help("Number of active connections by user and catalog")
            .labelNames("user", "catalog")
            .register();

    /**
     * Set the number of active connections for a given user and catalog.
     */
    public static void setConnectionCount(String user, String catalog, int count) {
        connectionCount.labels(user, catalog).set(count);
    }

    /**
     * Increment the count when a connection is opened.
     */
    public static void incConnection(String user, String catalog) {
        if(catalog==null){
            catalog = "null";
        }
        if(user==null) {
            user = "null";
        }
        connectionCount.labels(user, catalog).inc();
    }

    /**
     * Decrement the count when a connection is closed.
     */
    public static void decConnection(String user, String catalog) {
        if(catalog==null){
            catalog = "null";
        }
        if(user==null) {
            user = "null";
        }
        connectionCount.labels(user, catalog).dec();
    }
}
