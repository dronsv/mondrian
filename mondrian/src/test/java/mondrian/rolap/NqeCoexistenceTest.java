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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Sentinel tests for NQE coexistence: mode classification, prefetch
 * provider contracts, metrics record, and key symmetry.
 */
public class NqeCoexistenceTest {

    // -----------------------------------------------------------------------
    // Mode classification tests
    // -----------------------------------------------------------------------

    @Test
    public void allStored_fullResult() {
        assertEquals(NqeExecutionMode.FULL_RESULT,
            NativeQueryEngine.classifyExecutionMode(
                List.of(stored("S1"), stored("S2"))));
    }

    @Test
    public void storedPlusNative_prefetchOnly() {
        assertEquals(NqeExecutionMode.PREFETCH_ONLY,
            NativeQueryEngine.classifyExecutionMode(
                List.of(stored("S"), native_("N"))));
    }

    @Test
    public void allNative_bypass() {
        assertEquals(NqeExecutionMode.BYPASS,
            NativeQueryEngine.classifyExecutionMode(
                List.of(native_("N1"), native_("N2"))));
    }

    @Test
    public void storedPlusPostProcess_fullResult() {
        assertEquals(NqeExecutionMode.FULL_RESULT,
            NativeQueryEngine.classifyExecutionMode(
                List.of(stored("S"), postProcess("P"))));
    }

    @Test
    public void nativePlusPostProcess_noStored_bypass() {
        assertEquals(NqeExecutionMode.BYPASS,
            NativeQueryEngine.classifyExecutionMode(
                List.of(native_("N"), postProcess("P"))));
    }

    @Test
    public void storedNativePostProcess_prefetchOnly() {
        assertEquals(NqeExecutionMode.PREFETCH_ONLY,
            NativeQueryEngine.classifyExecutionMode(
                List.of(stored("S"), native_("N"), postProcess("P"))));
    }

    // -----------------------------------------------------------------------
    // Provider contract tests
    // -----------------------------------------------------------------------

    @Test
    public void provider_exactHit() {
        Map<PrefetchKey, Object> data = new HashMap<>();
        PrefetchKey key = new PrefetchKey(5, new Object[]{"Brand"});
        data.put(key, 42.0);
        PrefetchedCellProvider p = new ImmutablePrefetchProvider(data);
        assertEquals(42.0, p.lookup(key));
    }

    @Test
    public void provider_miss() {
        PrefetchedCellProvider p = ImmutablePrefetchProvider.empty();
        assertSame(PrefetchKey.MISS,
            p.lookup(new PrefetchKey(5, new Object[]{"X"})));
    }

    @Test
    public void provider_emptySize() {
        assertEquals(0, ImmutablePrefetchProvider.empty().size());
    }

    // -----------------------------------------------------------------------
    // Metrics record
    // -----------------------------------------------------------------------

    @Test
    public void metrics_allCounters() {
        PrefetchBuildMetrics m =
            new PrefetchBuildMetrics(100, 95, 5, 2, 93, 1, 2, 1, 1);
        assertEquals(100, m.nqeRowsProcessed());
        assertEquals(5,   m.rowsRejected());
        assertEquals(2,   m.duplicateKeys());
    }

    // -----------------------------------------------------------------------
    // Key symmetry
    // -----------------------------------------------------------------------

    @Test
    public void key_symmetry() {
        PrefetchKeyBuilder b = new PrefetchKeyBuilder();
        PrefetchKey a = b.fromNqeRow(5, new Object[]{"X", 2024L});
        PrefetchKey c = b.fromNqeRow(5, new Object[]{"X", 2024L});
        assertEquals(a, c);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private MeasureClassifier.Candidate stored(String name) {
        return makeCand(name, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED);
    }

    private MeasureClassifier.Candidate native_(String name) {
        return makeCand(name, MeasureClassifier.CandidateClass.DIRECT_PUSH_NATIVE);
    }

    private MeasureClassifier.Candidate postProcess(String name) {
        return makeCand(name, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE);
    }

    private MeasureClassifier.Candidate makeCand(
        String name,
        MeasureClassifier.CandidateClass cls)
    {
        Member m = mock(Member.class);
        when(m.getUniqueName()).thenReturn("[Measures].[" + name + "]");
        return new MeasureClassifier.Candidate(m, cls, null, null);
    }
}

// End NqeCoexistenceTest.java
