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
}
