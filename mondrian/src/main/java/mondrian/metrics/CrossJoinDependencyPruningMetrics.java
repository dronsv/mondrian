/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/

package mondrian.metrics;

import io.prometheus.client.Counter;

import java.util.Map;

/**
 * Prometheus counters for dependency-aware crossjoin pruning.
 *
 * <p>Labels are intentionally low-cardinality to keep runtime overhead small.
 */
public class CrossJoinDependencyPruningMetrics {

    public static final Counter pruningDeterminants = Counter.build()
            .name("crossjoin_dependency_pruning_determinants_total")
            .help("Total number of crossjoin determinant member lists pruned by dependency pruning")
            .labelNames("policy")
            .register();

    public static final Counter pruningRuleApplications = Counter.build()
            .name("crossjoin_dependency_pruning_rule_applications_total")
            .help("Total dependency rule applications by source and policy")
            .labelNames("policy", "source")
            .register();

    public static final Counter pruningRuleSkips = Counter.build()
            .name("crossjoin_dependency_pruning_rule_skips_total")
            .help("Total skipped dependency rules by reason and policy")
            .labelNames("policy", "reason")
            .register();

    private CrossJoinDependencyPruningMetrics() {
    }

    public static void recordDeterminantPrunes(String policy, int count) {
        if (count > 0) {
            pruningDeterminants.labels(normalize(policy)).inc(count);
        }
    }

    public static void recordRuleApplications(
            String policy,
            String source,
            int count)
    {
        if (count > 0) {
            pruningRuleApplications
                    .labels(normalize(policy), normalize(source))
                    .inc(count);
        }
    }

    public static void recordRuleSkips(
            String policy,
            Map<String, Integer> skipsByReason)
    {
        if (skipsByReason == null || skipsByReason.isEmpty()) {
            return;
        }
        final String normalizedPolicy = normalize(policy);
        for (Map.Entry<String, Integer> entry : skipsByReason.entrySet()) {
            final Integer count = entry.getValue();
            if (count == null || count.intValue() <= 0) {
                continue;
            }
            pruningRuleSkips
                    .labels(normalizedPolicy, normalize(entry.getKey()))
                    .inc(count.doubleValue());
        }
    }

    private static String normalize(String value) {
        return value == null || value.isEmpty()
                ? "unknown"
                : value.toLowerCase();
    }
}
