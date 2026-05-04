/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.nativesql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for {@link NativeSqlTelemetry} — test-queryable counters + log hooks. */
public class NativeSqlTelemetryTest {

    @BeforeEach public void setUp() {
        NativeSqlTelemetry.resetForTests();
    }

    @Test public void testExecutionCountStartsAtZero() {
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-unknown"));
    }

    @Test public void testIncrementExecutionCount() {
        NativeSqlTelemetry.incExecutionCount("fp-A");
        assertEquals(1, NativeSqlTelemetry.executionCount("fp-A"));
        NativeSqlTelemetry.incExecutionCount("fp-A");
        assertEquals(2, NativeSqlTelemetry.executionCount("fp-A"));
    }

    @Test public void testCountsAreIndependentPerFingerprint() {
        NativeSqlTelemetry.incExecutionCount("fp-A");
        NativeSqlTelemetry.incExecutionCount("fp-A");
        NativeSqlTelemetry.incExecutionCount("fp-B");
        assertEquals(2, NativeSqlTelemetry.executionCount("fp-A"));
        assertEquals(1, NativeSqlTelemetry.executionCount("fp-B"));
    }

    @Test public void testResetClearsAllCounters() {
        NativeSqlTelemetry.incExecutionCount("fp-X");
        NativeSqlTelemetry.resetForTests();
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-X"));
    }

    @Test public void testHookMethodsDoNotThrowOnValidInput() {
        // These hooks are advisory — they emit log events and (for counters)
        // increment internal state.  They must never throw on valid input.
        NativeSqlTelemetry.executionStart("fp-A");
        NativeSqlTelemetry.executionSuccess("fp-A", 42L);
        NativeSqlTelemetry.executionFailed(
            "fp-A", new RuntimeException("x"),
            NativeSqlError.Classification.PROPAGATE, 5L);
        NativeSqlTelemetry.cachedErrorHit(
            "fp-A", NativeSqlError.Classification.FALLBACK);
        NativeSqlTelemetry.onErrorBug(
            "fp-A", new NullPointerException("metrics bug"));
        NativeSqlTelemetry.fingerprintKindViolation(
            "fp-A", "SCALAR", "BATCH");
        NativeSqlTelemetry.reportUnauthorizedDowngrade(
            "fp-A", new RuntimeException("x"),
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.Classification.FALLBACK);
        // No assertion other than "did not throw".
    }

    @Test public void testSnapshotReturnsCopySortedByKey() {
        NativeSqlTelemetry.incExecutionCount("fp-z");
        NativeSqlTelemetry.incExecutionCount("fp-a");
        NativeSqlTelemetry.incExecutionCount("fp-a");

        SortedMap<String, Integer> snap = NativeSqlTelemetry.snapshot();

        assertEquals(
            List.of("fp-a", "fp-z"),
            new ArrayList<>(snap.keySet()));
        assertEquals(2, snap.get("fp-a").intValue());
        assertEquals(1, snap.get("fp-z").intValue());

        // Mutating the snapshot must NOT affect live counters.
        snap.put("fp-q", 99);
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-q"));
    }

    @Test public void testSnapshotEmptyWhenNoCounters() {
        assertEquals(0, NativeSqlTelemetry.snapshot().size());
    }

    @Test public void testHookMethodsToleratNull() {
        // Defensive: telemetry is advisory; it must not crash the drain loop
        // just because a caller passes in an unexpected null.
        NativeSqlTelemetry.executionStart(null);
        NativeSqlTelemetry.executionFailed(null, null, null, 0L);
        NativeSqlTelemetry.onErrorBug(null, null);
        // No assertion other than "did not throw".
    }

    @Test public void testCachedSuccessHitIncrementsCachedHitsSnapshotOnly() {
        NativeSqlTelemetry.executionSuccess("fp-A", 10L);
        NativeSqlTelemetry.cachedSuccessHit("fp-A");
        assertEquals(1, NativeSqlTelemetry.executionCount("fp-A"),
            "executionSuccess must bump COUNTERS only");
        assertEquals(1, NativeSqlTelemetry.cachedSuccessHitCount("fp-A"),
            "cachedSuccessHit must bump CACHED_HITS only");
        assertEquals(Integer.valueOf(1),
            NativeSqlTelemetry.snapshot().get("fp-A"));
        assertEquals(Integer.valueOf(1),
            NativeSqlTelemetry.cachedHitsSnapshot().get("fp-A"));
    }

    @Test public void testRepeatedCachedSuccessHitAccumulatesIndependently() {
        NativeSqlTelemetry.cachedSuccessHit("fp-X");
        NativeSqlTelemetry.cachedSuccessHit("fp-X");
        assertEquals(2, NativeSqlTelemetry.cachedSuccessHitCount("fp-X"));
        assertEquals(Integer.valueOf(2),
            NativeSqlTelemetry.cachedHitsSnapshot().get("fp-X"));
        // fp-X must NOT appear in fresh-execution snapshot.
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-X"));
        assertNull(NativeSqlTelemetry.snapshot().get("fp-X"));
    }

    @Test public void testCachedErrorHitDoesNotIncrementEitherCounter() {
        NativeSqlTelemetry.cachedErrorHit(
            "fp-E", NativeSqlError.Classification.FALLBACK);
        NativeSqlTelemetry.cachedErrorHit(
            "fp-E", NativeSqlError.Classification.PROPAGATE);
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-E"));
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount("fp-E"));
        assertNull(NativeSqlTelemetry.snapshot().get("fp-E"));
        assertNull(NativeSqlTelemetry.cachedHitsSnapshot().get("fp-E"));
    }

    @Test public void testResetClearsCachedHits() {
        NativeSqlTelemetry.cachedSuccessHit("fp-R");
        NativeSqlTelemetry.incExecutionCount("fp-R");
        NativeSqlTelemetry.resetForTests();
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount("fp-R"));
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-R"));
        assertEquals(0, NativeSqlTelemetry.cachedHitsSnapshot().size());
        assertEquals(0, NativeSqlTelemetry.snapshot().size());
    }
}
