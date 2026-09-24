/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.Set;

import mondrian.olap.Hierarchy;
import mondrian.rolap.agg.PredicateCanonicalizer;

/**
 * A query's subselect restriction as one base cube sees it, with the canonical
 * form that cell caching keys on.
 *
 * <p>Neither is a property of the cell being read: the restriction is built
 * from the query's subselect, scoped to the plan's base cube and masked on the
 * hierarchies the cell escapes. A cell-cache probe asked for both once per
 * cell, so a query over a 100-member subselect rebuilt the same predicate tree
 * and the same 11 KB string thousands of times. {@link #of} builds one per
 * execution and distinct scope instead.</p>
 *
 * <p>What the memo may not do is answer with a restriction another cell would
 * not have built: see {@link #of} for the key and for the builds it refuses to
 * keep.</p>
 */
public final class SubcubeRestriction {
    /** No subselect, or none reaching this cube: restricts nothing. */
    public static final SubcubeRestriction NONE =
        new SubcubeRestriction(null);

    /**
     * Scopes kept per execution before the memo starts over, and their total
     * canonical length. A query has as many scopes as it has base cubes times
     * masked hierarchy sets, so neither bound is normally approached; they
     * exist so that a pathological plan cannot pin unbounded text. Not final
     * so that a test can shrink them: at zero nothing is kept, which is how
     * every build behaved before the memo, and what the cells of a query are
     * compared against.
     */
    static int memoCapacity = 256;
    static int memoWeightCapacity = 4 << 20;

    private final StarPredicate predicate;
    /** Built on first use: the paths that only need the tree never pay for it. */
    private String canonical;

    private SubcubeRestriction(StarPredicate predicate) {
        this.predicate = predicate;
    }

    /** Wraps a predicate that was not built through the memo. */
    public static SubcubeRestriction of(StarPredicate predicate) {
        return predicate == null ? NONE : new SubcubeRestriction(predicate);
    }

    /**
     * The restriction {@code baseCube} sees, with {@code masked} hierarchies
     * left unrestricted, built once per execution and scope.
     *
     * <p>The key is the scope: the base cube (a virtual cube restricts each of
     * its base cubes differently — PR #44/#49) and the masked hierarchy set.
     * The query itself is not in the key because the memo lives on
     * {@link RolapEvaluatorRoot}, which belongs to one execution of one query:
     * a different subselect, a different parameter binding or a re-execution
     * gets a different root and therefore an empty memo.</p>
     *
     * <p>Whatever is left is the build reading the evaluator's own context —
     * a subselect set evaluated per cell, or a member enumeration that failed
     * and left its axis unrestricted. {@link mondrian.olap.Query} counts every
     * such step, and a build during which the count moved is returned to its
     * caller and then dropped, never stored. So an entry in the memo is one
     * that every cell of this execution would have built identically.</p>
     */
    static SubcubeRestriction of(
        RolapEvaluator evaluator,
        RolapCube baseCube,
        Set<Hierarchy> masked)
    {
        final RolapEvaluatorRoot root = evaluator.root;
        final MemoKey key = new MemoKey(baseCube, masked);
        final SubcubeRestriction cached = root.subcubeRestrictions.get(key);
        if (cached != null) {
            return cached;
        }
        final long ticks = root.query.getSubcubeContextDependentTicks();
        final SubcubeRestriction built = of(
            root.query.getSubcubePredicates(baseCube, masked, evaluator));
        if (memoCapacity <= 0
            || ticks != root.query.getSubcubeContextDependentTicks())
        {
            return built;
        }
        // Materialize here rather than on first use: the weight bound has to
        // know what the entry pins, and the cell-cache probe asks for it
        // anyway. Nested builds may have filled the memo meanwhile.
        final int weight = built.canonical().length();
        if (weight > memoWeightCapacity) {
            // Eviction cannot make this entry fit. Return its value without
            // retaining it or discarding useful entries already in the memo.
            return built;
        }
        if (root.subcubeRestrictions.size() + 1 > memoCapacity
            || root.subcubeRestrictionWeight + weight > memoWeightCapacity)
        {
            root.clearSubcubeRestrictions();
        }
        root.subcubeRestrictions.put(key.copy(), built);
        root.subcubeRestrictionWeight += weight;
        root.query.countSubcubeRestrictionKept();
        return built;
    }

    /** The restriction itself; null when nothing is restricted. */
    public StarPredicate predicate() {
        return predicate;
    }

    /**
     * The canonical form cell caching keys on. Callers that share a
     * restriction share this string, so comparing two of them is a reference
     * check and hashing one is free after the first.
     */
    public String canonical() {
        String result = canonical;
        if (result == null) {
            // A race only ever recomputes the same value.
            result = PredicateCanonicalizer.canonicalize(predicate);
            canonical = result;
        }
        return result;
    }

    @Override
    public String toString() {
        return "SubcubeRestriction(" + canonical() + ")";
    }

    /**
     * Scope of a restriction. {@code masked} is compared by value, so a
     * lookup may pass the caller's own set; {@link #copy} takes the snapshot
     * that goes into the memo.
     */
    record MemoKey(RolapCube cube, Set<Hierarchy> masked) {
        MemoKey copy() {
            return new MemoKey(cube, Set.copyOf(masked));
        }
    }
}

// End SubcubeRestriction.java
