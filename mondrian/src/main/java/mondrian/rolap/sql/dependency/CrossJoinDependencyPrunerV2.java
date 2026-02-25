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
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.CrossJoinDependencyPruner;

/**
 * V2 wrapper for dependency-driven pruning.
 *
 * <p>Current behavior intentionally preserves V1 semantics by delegating to
 * {@link CrossJoinDependencyPruner}. Future versions will consume
 * {@link DependencyRegistry} and query-time safety checks.</p>
 */
public final class CrossJoinDependencyPrunerV2 {
    private CrossJoinDependencyPrunerV2() {
    }

    public static CrossJoinArg[] prune(
        CrossJoinArg[] args,
        RolapEvaluator evaluator)
    {
        if (evaluator == null) {
            return args;
        }
        // V2 skeleton: preserve current runtime behavior without adding
        // per-query registry build overhead yet.
        return CrossJoinDependencyPruner.prune(args, evaluator);
    }

    public static CrossJoinArg[] prune(
        CrossJoinArg[] args,
        DependencyPruningContext context)
    {
        if (context == null
            || context.getEvaluator() == null
            || context.getPolicy() == DependencyRegistry.DependencyPruningPolicy.OFF)
        {
            return args;
        }
        // V2 skeleton: preserve current runtime behavior.
        return CrossJoinDependencyPruner.prune(args, context.getEvaluator());
    }
}
