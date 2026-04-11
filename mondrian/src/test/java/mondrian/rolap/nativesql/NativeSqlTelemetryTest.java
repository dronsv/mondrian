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

import junit.framework.TestCase;

/** Tests for {@link NativeSqlTelemetry} — test-queryable counters + log hooks. */
public class NativeSqlTelemetryTest extends TestCase {

    @Override
    protected void setUp() {
        NativeSqlTelemetry.resetForTests();
    }

    public void testExecutionCountStartsAtZero() {
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-unknown"));
    }

    public void testIncrementExecutionCount() {
        NativeSqlTelemetry.incExecutionCount("fp-A");
        assertEquals(1, NativeSqlTelemetry.executionCount("fp-A"));
        NativeSqlTelemetry.incExecutionCount("fp-A");
        assertEquals(2, NativeSqlTelemetry.executionCount("fp-A"));
    }

    public void testCountsAreIndependentPerFingerprint() {
        NativeSqlTelemetry.incExecutionCount("fp-A");
        NativeSqlTelemetry.incExecutionCount("fp-A");
        NativeSqlTelemetry.incExecutionCount("fp-B");
        assertEquals(2, NativeSqlTelemetry.executionCount("fp-A"));
        assertEquals(1, NativeSqlTelemetry.executionCount("fp-B"));
    }

    public void testResetClearsAllCounters() {
        NativeSqlTelemetry.incExecutionCount("fp-X");
        NativeSqlTelemetry.resetForTests();
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-X"));
    }

    public void testHookMethodsDoNotThrowOnValidInput() {
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

    public void testHookMethodsToleratNull() {
        // Defensive: telemetry is advisory; it must not crash the drain loop
        // just because a caller passes in an unexpected null.
        NativeSqlTelemetry.executionStart(null);
        NativeSqlTelemetry.executionFailed(null, null, null, 0L);
        NativeSqlTelemetry.onErrorBug(null, null);
        // No assertion other than "did not throw".
    }
}
