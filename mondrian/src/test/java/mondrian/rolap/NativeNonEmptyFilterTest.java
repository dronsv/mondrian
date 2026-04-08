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
        // Feature flag off (default) → returns null (fallback)
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
            // RolapEvaluator.getCube() is final — avoid calling it by providing
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
            when(cube.getStar()).thenReturn(null);   // → NO_STAR fallback
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

            // Must not be DIMENSION_CALC_MEMBER — the calc check was passed
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
}
