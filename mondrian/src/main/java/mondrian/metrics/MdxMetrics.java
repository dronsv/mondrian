package mondrian.metrics;

import io.prometheus.client.Counter;

public class MdxMetrics {
    public static final Counter mdxRequests = Counter.build()
            .name("mdx_requests_total")
            .help("Total MDX requests per catalog, cube and user")
            .labelNames("catalog", "cube", "user")
            .register();
}