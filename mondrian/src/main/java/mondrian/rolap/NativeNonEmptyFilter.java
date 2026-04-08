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

import mondrian.calc.TupleList;
import mondrian.olap.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * SQL pre-pruning for NON EMPTY axis evaluation.
 *
 * <p>Generates SQL to find non-empty dimension key combinations,
 * then filters candidate tuples via HashSet lookup. This is a
 * PRUNE_ONLY optimization — legacy nonEmptyList evaluation
 * remains authoritative on the reduced list.
 */
public class NativeNonEmptyFilter {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeNonEmptyFilter.class);

    /** Reason codes for eligibility fallback. */
    public enum FallbackReason {
        DISABLED_BY_CONFIG,
        EMPTY_CANDIDATES,
        DIMENSION_CALC_MEMBER,
        CROSS_CUBE_MEASURES,
        UNSUPPORTED_MEASURE_SEMANTICS,
        UNRESOLVABLE_HIERARCHY,
        NO_STAR,
        RESULT_SHAPE_TOO_LARGE,
        SQL_EXECUTION_FAILED
    }

    private NativeNonEmptyFilter() {}

    /**
     * Attempts to prune obviously-empty tuples from the candidate list
     * via SQL pre-filter. Returns a reduced list if successful, or
     * null to signal fallback.
     *
     * <p>The caller MUST continue with legacy nonEmptyList evaluation
     * on the returned list — this method is PRUNE_ONLY.
     */
    public static TupleList tryPrune(
        RolapEvaluator evaluator,
        TupleList candidates,
        Set<Member> measures)
    {
        if (!MondrianProperties.instance()
                .NativeNonEmptyFilterEnable.get())
        {
            return null;
        }
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }

        FallbackReason reason = assessEligibility(
            evaluator, candidates, measures);
        if (reason != null) {
            return null;
        }

        // TODO: SQL generation, execution, filtering (Tasks 4-5)
        return null;
    }

    /**
     * Checks all preconditions for SQL pre-pruning.
     * Returns a FallbackReason if ineligible, or null if eligible.
     */
    static FallbackReason assessEligibility(
        RolapEvaluator evaluator,
        TupleList candidates,
        Set<Member> measures)
    {
        // 1. Check for dimension calculated members in any tuple
        for (List<Member> tuple : candidates) {
            for (Member m : tuple) {
                if (m.isMeasure()) {
                    continue;
                }
                if (m.isCalculated()) {
                    LOGGER.info(
                        "NativeNonEmptyFilter: fallback reason={}, member={}",
                        FallbackReason.DIMENSION_CALC_MEMBER,
                        m.getUniqueName());
                    return FallbackReason.DIMENSION_CALC_MEMBER;
                }
            }
        }

        // 2. Resolve base cube (single-cube check for VirtualCube)
        RolapCube baseCube = resolveBaseCube(evaluator, measures);
        if (baseCube == null) {
            return FallbackReason.CROSS_CUBE_MEASURES;
        }

        // 3. Get star — virtual cubes don't have one
        RolapStar star = baseCube.getStar();
        if (star == null) {
            LOGGER.info(
                "NativeNonEmptyFilter: fallback reason={}, cube={}",
                FallbackReason.NO_STAR, baseCube.getName());
            return FallbackReason.NO_STAR;
        }

        // All checks passed
        return null;
    }

    /**
     * Resolves the single base cube for the given measures.
     * Returns null if no non-virtual cube could be determined
     * (e.g. VirtualCube cross-cube scenario).
     */
    private static RolapCube resolveBaseCube(
        RolapEvaluator evaluator, Set<Member> measures)
    {
        // Try to find a stored measure to get the base cube
        if (measures != null) {
            for (Member m : measures) {
                Member unwrapped = m;
                while (unwrapped instanceof DelegatingRolapMember) {
                    unwrapped = ((DelegatingRolapMember) unwrapped).member;
                }
                if (unwrapped instanceof RolapStoredMeasure) {
                    return ((RolapStoredMeasure) unwrapped).getCube();
                }
            }
        }

        // Fallback to evaluator cube (works for non-virtual cubes)
        RolapCube evalCube = evaluator.getCube();
        if (evalCube != null && !evalCube.isVirtual()) {
            return evalCube;
        }

        LOGGER.info(
            "NativeNonEmptyFilter: fallback reason={}, "
            + "could not resolve non-virtual base cube",
            FallbackReason.CROSS_CUBE_MEASURES);
        return null;
    }
}
