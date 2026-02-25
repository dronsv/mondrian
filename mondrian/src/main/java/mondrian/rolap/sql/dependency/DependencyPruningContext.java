/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql.dependency;

import mondrian.rolap.RolapEvaluator;

/**
 * Query-time context for dependency-driven crossjoin pruning.
 */
public final class DependencyPruningContext {
    private final RolapEvaluator evaluator;
    private final DependencyRegistry registry;
    private final DependencyRegistry.DependencyPruningPolicy policy;
    private final boolean hasRequiredTimeFilter;

    private DependencyPruningContext(
        RolapEvaluator evaluator,
        DependencyRegistry registry,
        DependencyRegistry.DependencyPruningPolicy policy,
        boolean hasRequiredTimeFilter)
    {
        this.evaluator = evaluator;
        this.registry = registry;
        this.policy = policy;
        this.hasRequiredTimeFilter = hasRequiredTimeFilter;
    }

    public static DependencyPruningContext fromEvaluator(RolapEvaluator evaluator) {
        DependencyRegistry registry =
            new DependencyRegistryBuilder().build(
                evaluator == null ? null : evaluator.getCube());
        return new DependencyPruningContext(
            evaluator,
            registry,
            registry.getPolicy(),
            false);
    }

    public RolapEvaluator getEvaluator() {
        return evaluator;
    }

    public DependencyRegistry getRegistry() {
        return registry;
    }

    public DependencyRegistry.DependencyPruningPolicy getPolicy() {
        return policy;
    }

    public boolean hasRequiredTimeFilter() {
        return hasRequiredTimeFilter;
    }
}

