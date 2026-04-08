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

import junit.framework.TestCase;
import mondrian.calc.TupleList;
import mondrian.calc.impl.ArrayTupleList;
import mondrian.olap.*;

import java.util.*;

import static org.mockito.Mockito.*;

public class NativeNonEmptyFilterTest extends TestCase {

    public void testReturnsNullWhenDisabled() {
        // Feature flag off (default) -> returns null (fallback)
        assertNull(NativeNonEmptyFilter.tryPrune(null, null, null));
    }

    public void testReturnsNullForEmptyCandidates() {
        MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
        try {
            RolapEvaluator evaluator = mock(RolapEvaluator.class);
            TupleList emptyList = mock(TupleList.class);
            when(emptyList.isEmpty()).thenReturn(true);

            assertNull(NativeNonEmptyFilter.tryPrune(
                evaluator, emptyList, Collections.<Member>emptySet()));
        } finally {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(false);
        }
    }

    /**
     * A tuple containing a calculated dimension member (not a measure)
     * should cause tryPrune to return null (fallback).
     */
    public void testFallsBackForDimensionCalcMember() {
        MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
        try {
            RolapEvaluator evaluator = mock(RolapEvaluator.class);

            // A calculated member that is NOT a measure
            Member calcMember = mock(Member.class);
            when(calcMember.isCalculated()).thenReturn(true);
            when(calcMember.isMeasure()).thenReturn(false);
            when(calcMember.getUniqueName()).thenReturn("[Dim].[CalcMbr]");

            // Regular measure member in the same tuple
            Member measureMember = mock(Member.class);
            when(measureMember.isMeasure()).thenReturn(true);

            // Build a 2-arity candidate list with one tuple
            ArrayTupleList candidates = new ArrayTupleList(2);
            candidates.addTuple(calcMember, measureMember);

            Set<Member> measures = Collections.<Member>singleton(measureMember);

            assertNull(
                "Should fall back when tuple contains a dimension calc member",
                NativeNonEmptyFilter.tryPrune(evaluator, candidates, measures));

            // Also check assessEligibility directly
            assertEquals(
                NativeNonEmptyFilter.FallbackReason.DIMENSION_CALC_MEMBER,
                NativeNonEmptyFilter.assessEligibility(
                    evaluator, candidates, measures));
        } finally {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(false);
        }
    }

    /**
     * A tuple with only regular (non-calc) dimension members and stored
     * measures should pass the calc-member check. Since we can't easily
     * create a real RolapStar in a unit test, we verify that:
     *   - the method doesn't crash
     *   - the reason is NOT DIMENSION_CALC_MEMBER
     *   - it falls back at the NO_STAR check (getStar() returns null from mock)
     */
    public void testEligibleForStoredMeasures() {
        MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
        try {
            // RolapEvaluator.getCube() is final -- avoid calling it by providing
            // a RolapStoredMeasure in the measures set so resolveBaseCube
            // returns early without touching the evaluator.
            RolapEvaluator evaluator = mock(RolapEvaluator.class);

            // Regular (non-calc) dimension member
            RolapMember regularMember = mock(RolapMember.class);
            when(regularMember.isCalculated()).thenReturn(false);
            when(regularMember.isMeasure()).thenReturn(false);

            // Stored measure that returns a mocked cube with no star
            RolapCube cube = mock(RolapCube.class);
            when(cube.isVirtual()).thenReturn(false);
            when(cube.getStar()).thenReturn(null);   // -> NO_STAR fallback
            when(cube.getName()).thenReturn("TestCube");

            RolapStoredMeasure storedMeasure = mock(RolapStoredMeasure.class);
            when(storedMeasure.isMeasure()).thenReturn(true);
            when(storedMeasure.isCalculated()).thenReturn(false);
            when(storedMeasure.getCube()).thenReturn(cube);

            ArrayTupleList candidates = new ArrayTupleList(2);
            candidates.addTuple(regularMember, storedMeasure);

            Set<Member> measures =
                Collections.<Member>singleton(storedMeasure);

            NativeNonEmptyFilter.FallbackReason reason =
                NativeNonEmptyFilter.assessEligibility(
                    evaluator, candidates, measures);

            // Must not be DIMENSION_CALC_MEMBER -- the calc check was passed
            assertFalse(
                "Should not fall back for DIMENSION_CALC_MEMBER",
                NativeNonEmptyFilter.FallbackReason.DIMENSION_CALC_MEMBER
                    .equals(reason));

            // Should fall back at NO_STAR (getStar() returned null)
            assertEquals(
                "Expected NO_STAR fallback when star is unavailable",
                NativeNonEmptyFilter.FallbackReason.NO_STAR,
                reason);
        } finally {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(false);
        }
    }

    // ------------------------------------------------------------------
    // Task 4 tests: SQL generation helpers
    // ------------------------------------------------------------------

    /**
     * collectSignatures extracts unique sets of active (non-All,
     * non-measure) hierarchies from candidate tuples.
     */
    public void testCollectSignatures() {
        Hierarchy h1 = mock(Hierarchy.class);
        Hierarchy h2 = mock(Hierarchy.class);

        // Non-All, non-measure member on h1
        Member m1 = mock(Member.class);
        when(m1.isMeasure()).thenReturn(false);
        when(m1.isAll()).thenReturn(false);
        when(m1.getHierarchy()).thenReturn(h1);

        // Non-All, non-measure member on h2
        Member m2 = mock(Member.class);
        when(m2.isMeasure()).thenReturn(false);
        when(m2.isAll()).thenReturn(false);
        when(m2.getHierarchy()).thenReturn(h2);

        // All-level member on h1
        Member allMember = mock(Member.class);
        when(allMember.isMeasure()).thenReturn(false);
        when(allMember.isAll()).thenReturn(true);
        when(allMember.getHierarchy()).thenReturn(h1);

        ArrayTupleList candidates = new ArrayTupleList(2);
        candidates.addTuple(m1, m2);         // sig = {h1, h2}
        candidates.addTuple(allMember, m2);  // sig = {h2}

        Set<Set<Hierarchy>> sigs =
            NativeNonEmptyFilter.collectSignatures(candidates);
        assertEquals(
            "Two distinct signatures expected (one 2-hierarchy, one 1-hierarchy)",
            2, sigs.size());
    }

    /**
     * collectSignatures with identical hierarchy sets across tuples
     * should yield one signature (deduplication).
     */
    public void testCollectSignaturesDeduplicate() {
        Hierarchy h1 = mock(Hierarchy.class);
        Hierarchy h2 = mock(Hierarchy.class);

        Member m1a = mock(Member.class);
        when(m1a.isMeasure()).thenReturn(false);
        when(m1a.isAll()).thenReturn(false);
        when(m1a.getHierarchy()).thenReturn(h1);

        Member m2a = mock(Member.class);
        when(m2a.isMeasure()).thenReturn(false);
        when(m2a.isAll()).thenReturn(false);
        when(m2a.getHierarchy()).thenReturn(h2);

        Member m1b = mock(Member.class);
        when(m1b.isMeasure()).thenReturn(false);
        when(m1b.isAll()).thenReturn(false);
        when(m1b.getHierarchy()).thenReturn(h1);

        Member m2b = mock(Member.class);
        when(m2b.isMeasure()).thenReturn(false);
        when(m2b.isAll()).thenReturn(false);
        when(m2b.getHierarchy()).thenReturn(h2);

        // Both tuples have the same set of hierarchies {h1, h2}
        ArrayTupleList candidates = new ArrayTupleList(2);
        candidates.addTuple(m1a, m2a);
        candidates.addTuple(m1b, m2b);

        Set<Set<Hierarchy>> sigs =
            NativeNonEmptyFilter.collectSignatures(candidates);
        assertEquals(
            "Same hierarchy set in both tuples -> one signature",
            1, sigs.size());
    }

    /**
     * resolveLeafMeasures returns null when a stored measure has no
     * star measure (getStarMeasure() returns null).
     */
    public void testResolveLeafMeasuresNullStarMeasure() {
        RolapCube cube = mock(RolapCube.class);

        RolapStoredMeasure measure = mock(RolapStoredMeasure.class);
        when(measure.getStarMeasure()).thenReturn(null);
        when(measure.getUniqueName()).thenReturn("[Measures].[Sales]");

        Set<Member> measures = Collections.<Member>singleton(measure);

        // Should return null because star measure is null
        assertNull(
            "resolveLeafMeasures should return null when star measure"
            + " is not available",
            NativeNonEmptyFilter.resolveLeafMeasures(measures, cube));
    }

    /**
     * resolveLeafMeasures correctly maps a stored measure to its
     * column name and aggregation kind.
     */
    public void testResolveLeafMeasuresStoredMeasure() {
        RolapCube cube = mock(RolapCube.class);

        // Build a star measure with a column expression
        RolapStar.Measure starMeasure = mock(RolapStar.Measure.class);
        MondrianDef.Column colExpr = new MondrianDef.Column();
        colExpr.name = "sales_amount";
        when(starMeasure.getExpression()).thenReturn(colExpr);

        RolapAggregator agg = mock(RolapAggregator.class);
        when(agg.isDistinct()).thenReturn(false);
        when(starMeasure.getAggregator()).thenReturn(agg);

        RolapStoredMeasure measure = mock(RolapStoredMeasure.class);
        when(measure.getStarMeasure()).thenReturn(starMeasure);

        Set<Member> measures = Collections.<Member>singleton(measure);

        Map<String, NativeNonEmptyFilter.AggKind> result =
            NativeNonEmptyFilter.resolveLeafMeasures(measures, cube);

        assertNotNull("resolveLeafMeasures should succeed", result);
        assertEquals(1, result.size());
        assertTrue(result.containsKey("sales_amount"));
        assertEquals(
            NativeNonEmptyFilter.AggKind.SUM,
            result.get("sales_amount"));
    }

    /**
     * resolveLeafMeasures maps a distinct-count measure to
     * COUNT_DISTINCT aggregation kind.
     */
    public void testResolveLeafMeasuresDistinctCount() {
        RolapCube cube = mock(RolapCube.class);

        RolapStar.Measure starMeasure = mock(RolapStar.Measure.class);
        MondrianDef.Column colExpr = new MondrianDef.Column();
        colExpr.name = "customer_id";
        when(starMeasure.getExpression()).thenReturn(colExpr);

        RolapAggregator agg = mock(RolapAggregator.class);
        when(agg.isDistinct()).thenReturn(true);
        when(starMeasure.getAggregator()).thenReturn(agg);

        RolapStoredMeasure measure = mock(RolapStoredMeasure.class);
        when(measure.getStarMeasure()).thenReturn(starMeasure);

        Set<Member> measures = Collections.<Member>singleton(measure);

        Map<String, NativeNonEmptyFilter.AggKind> result =
            NativeNonEmptyFilter.resolveLeafMeasures(measures, cube);

        assertNotNull(result);
        assertEquals(
            NativeNonEmptyFilter.AggKind.COUNT_DISTINCT,
            result.get("customer_id"));
    }

    /**
     * resolveLeafMeasures returns null for a calculated measure
     * that has no expression (cannot walk formula tree).
     */
    public void testResolveLeafMeasuresCalcNoExpression() {
        RolapCube cube = mock(RolapCube.class);

        Member calcMeasure = mock(Member.class);
        when(calcMeasure.isCalculated()).thenReturn(true);
        when(calcMeasure.getExpression()).thenReturn(null);
        when(calcMeasure.getUniqueName())
            .thenReturn("[Measures].[Calc]");

        Set<Member> measures = Collections.<Member>singleton(calcMeasure);

        assertNull(
            "Should return null for calc measure with no expression",
            NativeNonEmptyFilter.resolveLeafMeasures(measures, cube));
    }

    /**
     * resolveLeafMeasures with empty measures set returns null.
     */
    public void testResolveLeafMeasuresEmpty() {
        RolapCube cube = mock(RolapCube.class);
        Set<Member> measures = Collections.<Member>emptySet();

        assertNull(
            "Empty measures set should return null",
            NativeNonEmptyFilter.resolveLeafMeasures(measures, cube));
    }
}
