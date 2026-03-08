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

/**
 * Prometheus counters for mixed distinct/additive split planning.
 *
 * <p>All labels are intentionally low-cardinality to keep series count stable.
 */
public class SplitDistinctMetrics {

    public static final Counter splitPolicyResolved = Counter.build()
        .name("split_mixed_distinct_policy_resolved_total")
        .help("Resolved mixed distinct split policy by configured mode")
        .labelNames("configured_mode", "resolved", "merge_function_configured")
        .register();

    public static final Counter splitDecision = Counter.build()
        .name("split_distinct_decision_total")
        .help("Split decision outcomes for mixed distinct/additive planning")
        .labelNames("split", "reason", "mixed", "dialect_requires_split")
        .register();

    public static final Counter splitBranchLoads = Counter.build()
        .name("split_distinct_branch_load_total")
        .help("Branch SQL load executions for split planning")
        .labelNames("branch_type", "agg_candidate")
        .register();

    public static final Counter splitBranchMeasures = Counter.build()
        .name("split_distinct_branch_measures_total")
        .help("Measures routed through split branches")
        .labelNames("branch_type")
        .register();

    private SplitDistinctMetrics() {
    }

    public static void recordPolicyResolved(
        String configuredMode,
        boolean resolved,
        boolean mergeFunctionConfigured)
    {
        splitPolicyResolved
            .labels(
                normalize(configuredMode),
                asBoolLabel(resolved),
                asBoolLabel(mergeFunctionConfigured))
            .inc();
    }

    public static void recordDecision(
        boolean split,
        String reason,
        boolean mixed,
        boolean dialectRequiresSplit)
    {
        splitDecision
            .labels(
                asBoolLabel(split),
                normalize(reason),
                asBoolLabel(mixed),
                asBoolLabel(dialectRequiresSplit))
            .inc();
    }

    public static void recordBranchPlan(
        String branchType,
        boolean aggCandidate,
        int branchCount)
    {
        if (branchCount <= 0) {
            return;
        }
        splitBranchLoads
            .labels(normalize(branchType), asBoolLabel(aggCandidate))
            .inc(branchCount);
    }

    public static void recordBranchMeasures(
        String branchType,
        int measureCount)
    {
        if (measureCount <= 0) {
            return;
        }
        splitBranchMeasures
            .labels(normalize(branchType))
            .inc(measureCount);
    }

    private static String asBoolLabel(boolean value) {
        return value ? "true" : "false";
    }

    private static String normalize(String value) {
        if (value == null) {
            return "unknown";
        }
        final String trimmed = value.trim();
        if (trimmed.length() == 0) {
            return "unknown";
        }
        return trimmed.toLowerCase();
    }
}
