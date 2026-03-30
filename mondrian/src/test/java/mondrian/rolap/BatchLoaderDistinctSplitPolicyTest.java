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

    public void testSplitsByAggCandidateForAdditiveMeasuresWhenEnabled() {
        assertTrue(BatchLoader.shouldSplitByAggCandidate(
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

    public void testEnablesMixedSplitWhenConfiguredAndNoFormulas() {
        assertTrue(BatchLoader.shouldEnableMixedDistinctSplit(
            true, false));
    }

    public void testKeepsMixedSplitEnabledWhenQueryScopedFormulasPresent() {
        assertTrue(BatchLoader.shouldEnableMixedDistinctSplit(
            true, true));
    }

    public void testKeepsMixedSplitDisabledWhenNotConfigured() {
        assertFalse(BatchLoader.shouldEnableMixedDistinctSplit(
            false, false));
    }

    public void testResolveSplitMixedDistinctDefaultsToMergeFunctionWhenUnset() {
        assertTrue(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            null, true));
        assertFalse(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            null, false));
    }

    public void testResolveSplitMixedDistinctHonorsExplicitBoolean() {
        assertTrue(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "true", false));
        assertFalse(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "false", true));
    }

    public void testResolveSplitMixedDistinctSupportsAutoMode() {
        assertTrue(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "auto", true));
        assertFalse(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "auto", false));
    }

    public void testResolveSplitMixedDistinctTreatsBlankAsAuto() {
        assertTrue(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "", true));
        assertFalse(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "", false));
    }

    public void testResolveSplitMixedDistinctTreatsWhitespaceAsAuto() {
        assertTrue(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "   ", true));
        assertFalse(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "   ", false));
    }

    public void testResolveSplitMixedDistinctTreatsUnknownAsAuto() {
        assertTrue(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "maybe", true));
        assertFalse(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "maybe", false));
    }

    public void testResolveSplitMixedDistinctSupportsAutoCaseInsensitive() {
        assertTrue(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "AuTo", true));
        assertFalse(BatchLoader.resolveSplitMixedDistinctMeasureBatches(
            "AUTO", false));
    }

    public void testSplitsPureDistinctSetWhenMultipleDistinctNotAllowed() {
        assertTrue(BatchLoader.shouldSplitDistinctMeasures(
            3, 3, true, false, true, false));
    }

    public void testSplitsPureDistinctSetWhenCountDistinctNotAllowed() {
        assertTrue(BatchLoader.shouldSplitDistinctMeasures(
            2, 2, false, false, false, false));
    }

    public void testDoesNotSplitSingleDistinctMeasureWhenDialectAllows() {
        assertFalse(BatchLoader.shouldSplitDistinctMeasures(
            1, 1, true, true, true, false));
    }

    public void testDoesNotSplitByAggCandidateWithSingleAdditiveMeasure() {
        assertFalse(BatchLoader.shouldSplitByAggCandidate(
            true, 1, true));
    }

    public void testResolveConfiguredModeForUnsetAndBlank() {
        assertEquals(
            "unset",
            BatchLoader.resolveSplitMixedDistinctConfiguredMode(null));
        assertEquals(
            "unset",
            BatchLoader.resolveSplitMixedDistinctConfiguredMode("   "));
    }

    public void testResolveConfiguredModeForKnownValuesAndInvalid() {
        assertEquals(
            "auto",
            BatchLoader.resolveSplitMixedDistinctConfiguredMode("AUTO"));
        assertEquals(
            "true",
            BatchLoader.resolveSplitMixedDistinctConfiguredMode("true"));
        assertEquals(
            "false",
            BatchLoader.resolveSplitMixedDistinctConfiguredMode("false"));
        assertEquals(
            "invalid",
            BatchLoader.resolveSplitMixedDistinctConfiguredMode("foobar"));
    }

    public void testDeriveSplitReasonNoSplit() {
        assertEquals(
            "none",
            BatchLoader.deriveSplitReason(false, false, false));
    }

    public void testDeriveSplitReasonDialectOnly() {
        assertEquals(
            "dialect",
            BatchLoader.deriveSplitReason(true, true, false));
    }

    public void testDeriveSplitReasonMixedOnly() {
        assertEquals(
            "mixed_policy",
            BatchLoader.deriveSplitReason(true, false, true));
    }

    public void testDeriveSplitReasonDialectAndMixed() {
        assertEquals(
            "dialect_and_mixed",
            BatchLoader.deriveSplitReason(true, true, true));
    }
}
