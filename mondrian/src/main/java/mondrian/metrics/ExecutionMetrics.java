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

import io.prometheus.client.Gauge;
import mondrian.olap.MondrianServer;
import mondrian.rolap.RolapConnection;
import mondrian.server.Execution;
import mondrian.server.MondrianServerImpl;
import mondrian.server.MondrianServerRegistry;
import mondrian.server.Statement;

import java.util.*;

public class ExecutionMetrics {

    // Prometheus metric: sum of durations of running queries
    public static final Gauge sumExecutionTime = Gauge.build()
            .name("sum_current_query_execution_seconds")
            .help("Sum of durations of currently running queries by catalog, user, and cube")
            .labelNames("catalog", "user", "cube")
            .register();

    // Prometheus metric: count of running queries
    public static final Gauge runningQueryCount = Gauge.build()
            .name("current_running_query_count")
            .help("Number of currently running queries by catalog, user, and cube")
            .labelNames("catalog", "user", "cube")
            .register();

    public static void updateMetrics() {

        for(MondrianServer mondrianServer: MondrianServerImpl.getServers()) {
            List<Statement> statements = mondrianServer.getStatements(null);

            // Remove old metrics for all keys currently tracked in timeMap
            for (List<String> key : timeMap.keySet()) {
                String catalog = key.get(0);
                String user = key.get(1);
                String cube = key.get(2);
                sumExecutionTime.remove(catalog, user, cube);
                runningQueryCount.remove(catalog, user, cube);
            }

            // Clear the maps so we can rebuild fresh data
            timeMap.clear();
            countMap.clear();

            for (Statement statement : statements) {
                Execution execution = statement.getCurrentExecution();
                if (execution != null) {

                    RolapConnection rolapConnection = statement.getMondrianConnection();

                    String catalog = rolapConnection.getCatalogName();
                    if (catalog == null) {
                        catalog = "null";
                    }

                    String user = rolapConnection.getUserId();
                    if (user == null) {
                        user = "null";
                    }

                    String cube = statement.getQuery().getCube().getName();
                    if (cube == null) {
                        cube = "null";
                    }

                    long elapsedMillis = execution.getElapsedMillis();
                    double elapsedSeconds = elapsedMillis / 1000.0;

                    List<String> key = Arrays.asList(catalog, user, cube);

                    timeMap.put(key, timeMap.getOrDefault(key, 0.0) + elapsedSeconds);
                    countMap.put(key, countMap.getOrDefault(key, 0) + 1);
                }
            }

            // Now update Prometheus metrics with new data
            for (List<String> key : timeMap.keySet()) {
                String catalog = key.get(0);
                String user = key.get(1);
                String cube = key.get(2);

                sumExecutionTime.labels(catalog, user, cube).set(timeMap.get(key));
                runningQueryCount.labels(catalog, user, cube).set(countMap.get(key));
            }
        }
    }

    // Static maps to hold intermediate metric data
    private static final Map<List<String>, Double> timeMap = new HashMap<>();
    private static final Map<List<String>, Integer> countMap = new HashMap<>();

    static {
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                updateMetrics();
            }
        }, 0, 10000); // every 10 seconds
    }
}