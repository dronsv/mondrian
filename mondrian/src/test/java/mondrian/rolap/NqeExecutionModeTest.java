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

import mondrian.olap.Member;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static mondrian.rolap.MeasureClassifier.CandidateClass.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link NativeQueryEngine#classifyExecutionMode}.
 */
public class NqeExecutionModeTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static MeasureClassifier.Candidate candidate(
        MeasureClassifier.CandidateClass cls)
    {
        return new MeasureClassifier.Candidate(
            mock(Member.class), cls, null, null);
    }

    private static List<MeasureClassifier.Candidate> candidates(
        MeasureClassifier.CandidateClass... classes)
    {
        MeasureClassifier.Candidate[] arr =
            new MeasureClassifier.Candidate[classes.length];
        for (int i = 0; i < classes.length; i++) {
            arr[i] = candidate(classes[i]);
        }
        return Arrays.asList(arr);
    }

    // -----------------------------------------------------------------------
    // Test cases
    // -----------------------------------------------------------------------

    /** All stored → NQE owns the full result. */
    @Test
    public void testAllStored_returnsFullResult() {
        List<MeasureClassifier.Candidate> cs =
            candidates(DIRECT_PUSH_STORED, DIRECT_PUSH_STORED);
        assertEquals(
            NqeExecutionMode.FULL_RESULT,
            NativeQueryEngine.classifyExecutionMode(cs));
    }

    /** Stored + PostProcess → still NQE-ownable (PostProcess is ownable). */
    @Test
    public void testStoredPlusPostProcess_returnsFullResult() {
        List<MeasureClassifier.Candidate> cs =
            candidates(DIRECT_PUSH_STORED, POST_PROCESS_CANDIDATE);
        assertEquals(
            NqeExecutionMode.FULL_RESULT,
            NativeQueryEngine.classifyExecutionMode(cs));
    }

    /** Stored + native (non-ownable) → NQE prefetches stored, native has its own path. */
    @Test
    public void testStoredPlusNative_returnsPrefetchOnly() {
        List<MeasureClassifier.Candidate> cs =
            candidates(DIRECT_PUSH_STORED, DIRECT_PUSH_NATIVE);
        assertEquals(
            NqeExecutionMode.PREFETCH_ONLY,
            NativeQueryEngine.classifyExecutionMode(cs));
    }

    /** All native (no stored) → NQE cannot contribute at all. */
    @Test
    public void testAllNative_returnsBypass() {
        List<MeasureClassifier.Candidate> cs =
            candidates(DIRECT_PUSH_NATIVE, DIRECT_PUSH_NATIVE);
        assertEquals(
            NqeExecutionMode.BYPASS,
            NativeQueryEngine.classifyExecutionMode(cs));
    }

    /** Native + PostProcess (no stored) → non-ownable wins, no stored → BYPASS. */
    @Test
    public void testNativePlusPostProcess_noStored_returnsBypass() {
        List<MeasureClassifier.Candidate> cs =
            candidates(DIRECT_PUSH_NATIVE, POST_PROCESS_CANDIDATE);
        assertEquals(
            NqeExecutionMode.BYPASS,
            NativeQueryEngine.classifyExecutionMode(cs));
    }

    /** Stored + native + PostProcess → stored present, but non-ownable native too → PREFETCH_ONLY. */
    @Test
    public void testStoredPlusNativePlusPostProcess_returnsPrefetchOnly() {
        List<MeasureClassifier.Candidate> cs =
            candidates(DIRECT_PUSH_STORED, DIRECT_PUSH_NATIVE, POST_PROCESS_CANDIDATE);
        assertEquals(
            NqeExecutionMode.PREFETCH_ONLY,
            NativeQueryEngine.classifyExecutionMode(cs));
    }

    /** Single PostProcess only → NQE-ownable → FULL_RESULT. */
    @Test
    public void testOnlyPostProcess_returnsFullResult() {
        List<MeasureClassifier.Candidate> cs =
            candidates(POST_PROCESS_CANDIDATE);
        assertEquals(
            NqeExecutionMode.FULL_RESULT,
            NativeQueryEngine.classifyExecutionMode(cs));
    }
}

// End NqeExecutionModeTest.java
