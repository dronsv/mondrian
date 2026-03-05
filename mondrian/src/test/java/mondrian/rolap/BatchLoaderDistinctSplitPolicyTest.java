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

public class BatchLoaderDistinctSplitPolicyTest extends TestCase {

    public void testDoesNotSplitWhenNoDistinctMeasures() {
        assertFalse(BatchLoader.shouldSplitDistinctMeasures(
            3, 0, true, true, true, true));
    }

    public void testDoesNotSplitMixedMeasuresByDefaultWhenDialectAllows() {
        assertFalse(BatchLoader.shouldSplitDistinctMeasures(
            2, 1, true, true, true, false));
    }

    public void testSplitsMixedMeasuresWhenFeatureFlagEnabled() {
        assertTrue(BatchLoader.shouldSplitDistinctMeasures(
            2, 1, true, true, true, true));
    }

    public void testStillDoesNotSplitPureDistinctSetWhenDialectAllows() {
        assertFalse(BatchLoader.shouldSplitDistinctMeasures(
            2, 2, true, true, true, true));
    }

    public void testSplitsWhenDialectDoesNotAllowDistinctWithOtherAggs() {
        assertTrue(BatchLoader.shouldSplitDistinctMeasures(
            2, 1, true, true, false, false));
    }

    public void testLoadsAllDistinctMeasuresTogetherWhenDialectSupportsIt() {
        assertTrue(BatchLoader.shouldLoadAllDistinctMeasuresTogether(
            true));
    }

    public void testDoesNotLoadAllDistinctMeasuresTogetherWhenCountDistinctIsLimited() {
        assertFalse(BatchLoader.shouldLoadAllDistinctMeasuresTogether(
            false));
    }

    public void testDoesNotSplitByAggCandidateWithoutMixedSplit() {
        assertFalse(BatchLoader.shouldSplitByAggCandidate(
            false, 3, true));
    }

    public void testDoesNotSplitByAggCandidateWhenFeatureDisabled() {
        assertFalse(BatchLoader.shouldSplitByAggCandidate(
            true, 3, false));
    }

    public void testSplitsByAggCandidateWhenMixedSplitAndFeatureEnabled() {
        assertTrue(BatchLoader.shouldSplitByAggCandidate(
            true, 3, true));
    }
}
