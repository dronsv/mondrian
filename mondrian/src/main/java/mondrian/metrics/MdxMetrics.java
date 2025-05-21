package mondrian.metrics;

import io.prometheus.client.Counter;

public class MdxMetrics {
    public static final Counter mdxRequests = Counter.build()
            .name("mdx_requests_total")
            .help("Total MDX requests per user and cube")
            .labelNames("user", "cube")
            .register();
}