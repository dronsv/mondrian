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

import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.DimensionType;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapEvaluator;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Query-time context for dependency-driven crossjoin pruning.
 */
public final class DependencyPruningContext {
    private static final Map<Cube, DependencyRegistry> REGISTRY_CACHE =
        Collections.synchronizedMap(new WeakHashMap<Cube, DependencyRegistry>());

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
        final Cube cube = evaluator == null ? null : evaluator.getCube();
        final DependencyRegistry registry = getOrBuildRegistry(cube);
        return of(
            evaluator,
            registry,
            resolvePolicy(registry),
            detectRequiredTimeFilter(evaluator));
    }

    public static DependencyPruningContext of(
        RolapEvaluator evaluator,
        DependencyRegistry registry,
        DependencyRegistry.DependencyPruningPolicy policy,
        boolean hasRequiredTimeFilter)
    {
        final DependencyRegistry safeRegistry =
            registry == null
                ? DependencyRegistry.builder("<unknown-cube>").build()
                : registry;
        final DependencyRegistry.DependencyPruningPolicy safePolicy =
            policy == null ? safeRegistry.getPolicy() : policy;
        return new DependencyPruningContext(
            evaluator,
            safeRegistry,
            safePolicy,
            hasRequiredTimeFilter);
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

    private static DependencyRegistry getOrBuildRegistry(Cube cube) {
        if (cube == null) {
            return new DependencyRegistryBuilder().build((Cube) null);
        }
        DependencyRegistry registry = REGISTRY_CACHE.get(cube);
        if (registry != null) {
            return registry;
        }
        registry = new DependencyRegistryBuilder().build(cube);
        REGISTRY_CACHE.put(cube, registry);
        return registry;
    }

    private static DependencyRegistry.DependencyPruningPolicy resolvePolicy(
        DependencyRegistry registry)
    {
        final String rawValue =
            MondrianProperties.instance().CrossJoinDependencyPruningPolicy.get();
        if (rawValue != null) {
            final String normalized = rawValue.trim();
            if (!normalized.isEmpty()) {
                try {
                    return DependencyRegistry.DependencyPruningPolicy.valueOf(
                        normalized.toUpperCase());
                } catch (IllegalArgumentException ignored) {
                    // Fall back to registry default if config value is invalid.
                }
            }
        }
        return registry == null
            ? DependencyRegistry.DependencyPruningPolicy.RELAXED
            : registry.getPolicy();
    }

    private static boolean detectRequiredTimeFilter(RolapEvaluator evaluator) {
        if (evaluator == null) {
            return false;
        }
        final Member[] members = evaluator.getMembers();
        if (members == null || members.length == 0) {
            return false;
        }
        for (Member member : members) {
            if (member == null || member.isAll() || member.isMeasure()) {
                continue;
            }
            final Dimension dimension =
                member.getHierarchy() == null
                    ? null
                    : member.getHierarchy().getDimension();
            if (dimension != null
                && dimension.getDimensionType() == DimensionType.TimeDimension)
            {
                return true;
            }
        }
        return false;
    }
}
