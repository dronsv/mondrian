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

public class MdxMetrics {

    public static final Counter mdxRequests = Counter.build()
            .name("mdx_requests_total")
            .help("Total MDX requests per catalog, cube and user")
            .labelNames("catalog", "cube", "user")
            .register();

    public static final Counter mdxCompleted = Counter.build()
            .name("mdx_completed_total")
            .help("Total completed MDX queries per catalog, cube and user")
            .labelNames("catalog", "cube", "user")
            .register();

    public static final Counter mdxExecutionTimeSum = Counter.build()
            .name("mdx_execution_time_seconds_total")
            .help("Total sum of MDX query execution time in seconds per catalog, cube and user")
            .labelNames("catalog", "cube", "user")
            .register();

    public static final Counter resultCellsCount = Counter.build()
            .name("mdx_result_cells_count_total")
            .help("Total sum of cells in query result per catalog, cube and user")
            .labelNames("catalog", "cube", "user")
            .register();
}