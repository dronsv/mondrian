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

import mondrian.rolap.nativesql.NativeSqlTelemetryEvents.EventRecord;
import mondrian.rolap.nativesql.NativeSqlTelemetryEvents.EventType;

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
        NativeSqlTelemetryEvents.resetForTests();
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

    // ----- Phase 8e: split fresh-attempt counters --------------------

    @Test public void testExecutionSuccessCountStartsAtZero() {
        assertEquals(0, NativeSqlTelemetry.executionSuccessCount("fp-unknown"));
    }

    @Test public void testExecutionFailedCountStartsAtZero() {
        assertEquals(0, NativeSqlTelemetry.executionFailedCount("fp-unknown"));
    }

    @Test public void testExecutionSuccessIncrementsLegacyAndSplitOnly() {
        NativeSqlTelemetry.executionSuccess("fp-A", 12L);
        assertEquals(1, NativeSqlTelemetry.executionCount("fp-A"),
            "executionSuccess must bump COUNTERS");
        assertEquals(1, NativeSqlTelemetry.executionSuccessCount("fp-A"),
            "executionSuccess must bump EXECUTION_SUCCESSES");
        assertEquals(0, NativeSqlTelemetry.executionFailedCount("fp-A"),
            "executionSuccess must NOT bump EXECUTION_FAILURES");
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount("fp-A"),
            "executionSuccess must NOT bump CACHED_HITS");
    }

    @Test public void testExecutionFailedIncrementsLegacyAndSplitOnly() {
        NativeSqlTelemetry.executionFailed(
            "fp-B",
            new RuntimeException("simulated"),
            mondrian.rolap.nativesql.NativeSqlError.Classification.FALLBACK,
            5L);
        assertEquals(1, NativeSqlTelemetry.executionCount("fp-B"),
            "executionFailed must bump COUNTERS");
        assertEquals(1, NativeSqlTelemetry.executionFailedCount("fp-B"),
            "executionFailed must bump EXECUTION_FAILURES");
        assertEquals(0, NativeSqlTelemetry.executionSuccessCount("fp-B"),
            "executionFailed must NOT bump EXECUTION_SUCCESSES");
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount("fp-B"),
            "executionFailed must NOT bump CACHED_HITS");
    }

    @Test public void testQuiescentInvariantAttemptEqualsSuccessPlusFailed() {
        NativeSqlTelemetry.executionSuccess("fp-X", 1L);
        NativeSqlTelemetry.executionSuccess("fp-X", 1L);
        NativeSqlTelemetry.executionFailed(
            "fp-X",
            new RuntimeException(),
            mondrian.rolap.nativesql.NativeSqlError.Classification.PROPAGATE,
            1L);
        int attempt = NativeSqlTelemetry.executionCount("fp-X");
        int success = NativeSqlTelemetry.executionSuccessCount("fp-X");
        int failed = NativeSqlTelemetry.executionFailedCount("fp-X");
        assertEquals(3, attempt);
        assertEquals(2, success);
        assertEquals(1, failed);
        assertEquals(attempt, success + failed,
            "Quiescent invariant: attempt == success + failed");
    }

    @Test public void testIncExecutionCountOnlyBumpsFreshAttempts() {
        // Documented primitive bypass: callers of incExecutionCount opt
        // out of Phase 8e split tracking.  This test pins that contract.
        NativeSqlTelemetry.incExecutionCount("fp-raw");
        assertEquals(1, NativeSqlTelemetry.executionCount("fp-raw"),
            "incExecutionCount must bump COUNTERS");
        assertEquals(0, NativeSqlTelemetry.executionSuccessCount("fp-raw"),
            "incExecutionCount must NOT bump EXECUTION_SUCCESSES");
        assertEquals(0, NativeSqlTelemetry.executionFailedCount("fp-raw"),
            "incExecutionCount must NOT bump EXECUTION_FAILURES");
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount("fp-raw"),
            "incExecutionCount must NOT bump CACHED_HITS");
    }

    @Test public void testExecutionSuccessSnapshotSortedByKey() {
        NativeSqlTelemetry.executionSuccess("fp-zebra", 1L);
        NativeSqlTelemetry.executionSuccess("fp-apple", 1L);
        NativeSqlTelemetry.executionSuccess("fp-apple", 1L);
        SortedMap<String, Integer> snap =
            NativeSqlTelemetry.executionSuccessSnapshot();
        assertEquals(2, snap.size());
        assertEquals(Integer.valueOf(2), snap.get("fp-apple"));
        assertEquals(Integer.valueOf(1), snap.get("fp-zebra"));
        // SortedMap by natural order on String key
        assertEquals("fp-apple", snap.firstKey());
        assertEquals("fp-zebra", snap.lastKey());
    }

    @Test public void testExecutionFailedSnapshotSortedByKey() {
        NativeSqlTelemetry.executionFailed("fp-z", new RuntimeException(),
            mondrian.rolap.nativesql.NativeSqlError.Classification.FALLBACK, 1L);
        NativeSqlTelemetry.executionFailed("fp-a", new RuntimeException(),
            mondrian.rolap.nativesql.NativeSqlError.Classification.FALLBACK, 1L);
        SortedMap<String, Integer> snap =
            NativeSqlTelemetry.executionFailedSnapshot();
        assertEquals(2, snap.size());
        assertEquals("fp-a", snap.firstKey());
        assertEquals("fp-z", snap.lastKey());
    }

    @Test public void testCachedSuccessHitDoesNotIncrementSplitCounters() {
        NativeSqlTelemetry.cachedSuccessHit("fp-cache");
        assertEquals(0, NativeSqlTelemetry.executionSuccessCount("fp-cache"),
            "cachedSuccessHit must NOT bump EXECUTION_SUCCESSES");
        assertEquals(0, NativeSqlTelemetry.executionFailedCount("fp-cache"),
            "cachedSuccessHit must NOT bump EXECUTION_FAILURES");
        assertEquals(1, NativeSqlTelemetry.cachedSuccessHitCount("fp-cache"));
    }

    @Test public void testResetClearsSuccessAndFailedCounters() {
        NativeSqlTelemetry.executionSuccess("fp-1", 1L);
        NativeSqlTelemetry.executionFailed("fp-2", new RuntimeException(),
            mondrian.rolap.nativesql.NativeSqlError.Classification.FALLBACK, 1L);
        NativeSqlTelemetry.resetForTests();
        assertEquals(0, NativeSqlTelemetry.executionSuccessCount("fp-1"));
        assertEquals(0, NativeSqlTelemetry.executionFailedCount("fp-2"));
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-1"));
        assertEquals(0, NativeSqlTelemetry.executionCount("fp-2"));
        assertEquals(0,
            NativeSqlTelemetry.executionSuccessSnapshot().size());
        assertEquals(0,
            NativeSqlTelemetry.executionFailedSnapshot().size());
    }

    @Test public void testConcurrentEventsQuiescentInvariant() throws Exception {
        // Deterministic concurrency test: 8 threads * 1000 iterations,
        // even iterations success / odd iterations failure for the same fp.
        // Each thread therefore contributes 500 success + 500 failed.
        // Total expected: attempt=8000, success=4000, failed=4000.
        // Asserts only the post-join quiescent invariant — never the
        // mid-update transient (avoids scheduler-timing flake).
        final int threadCount = 8;
        final int iterPerThread = 1_000;
        final String fp = "fp-concurrent";
        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < iterPerThread; i++) {
                    if ((i & 1) == 0) {
                        NativeSqlTelemetry.executionSuccess(fp, 0L);
                    } else {
                        NativeSqlTelemetry.executionFailed(fp,
                            new RuntimeException(),
                            mondrian.rolap.nativesql.NativeSqlError
                                .Classification.FALLBACK,
                            0L);
                    }
                }
            });
        }
        for (Thread th : threads) th.start();
        for (Thread th : threads) th.join();

        int attempt = NativeSqlTelemetry.executionCount(fp);
        int success = NativeSqlTelemetry.executionSuccessCount(fp);
        int failed = NativeSqlTelemetry.executionFailedCount(fp);
        assertEquals(threadCount * iterPerThread, attempt,
            "attempt total must include every event");
        assertEquals(threadCount * iterPerThread / 2, success,
            "success total: half of all events");
        assertEquals(threadCount * iterPerThread / 2, failed,
            "failed total: half of all events");
        assertEquals(attempt, success + failed,
            "Quiescent invariant after join");
    }

    // ----- Phase 8f: event-buffer firing-site coverage --------------------

    @Test
    void executionStart_appendsExecutionStartEvent() {
        NativeSqlTelemetry.executionStart("fp1");
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.EXECUTION_START, snap.get(0).type());
        org.junit.jupiter.api.Assertions.assertEquals(
            "fp1", snap.get(0).fingerprintId());
        org.junit.jupiter.api.Assertions.assertNull(
            snap.get(0).classification());
        org.junit.jupiter.api.Assertions.assertNull(
            snap.get(0).durationMs());
        org.junit.jupiter.api.Assertions.assertNull(
            snap.get(0).message());
    }

    @Test
    void executionSuccess_appendsExecutionSuccessEventWithDuration() {
        NativeSqlTelemetry.executionSuccess("fp1", 42L);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.EXECUTION_SUCCESS, snap.get(0).type());
        org.junit.jupiter.api.Assertions.assertEquals(
            Long.valueOf(42L), snap.get(0).durationMs());
        org.junit.jupiter.api.Assertions.assertNull(
            snap.get(0).classification());
    }

    @Test
    void executionFailed_appendsExecutionFailedEventWithClassificationAndMessage() {
        java.sql.SQLException sqle = new java.sql.SQLException("boom");
        NativeSqlTelemetry.executionFailed(
            "fp1",
            sqle,
            NativeSqlError.Classification.FALLBACK,
            7L);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        EventRecord rec = snap.get(0);
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.EXECUTION_FAILED, rec.type());
        org.junit.jupiter.api.Assertions.assertSame(
            NativeSqlError.Classification.FALLBACK, rec.classification());
        org.junit.jupiter.api.Assertions.assertEquals(
            Long.valueOf(7L), rec.durationMs());
        org.junit.jupiter.api.Assertions.assertEquals(
            "SQLException: boom", rec.message());
    }

    @Test
    void cachedSuccessHit_appendsCachedSuccessHitEvent() {
        NativeSqlTelemetry.cachedSuccessHit("fp1");
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.CACHED_SUCCESS_HIT, snap.get(0).type());
        org.junit.jupiter.api.Assertions.assertNull(
            snap.get(0).classification());
        org.junit.jupiter.api.Assertions.assertNull(
            snap.get(0).message());
    }

    @Test
    void cachedErrorHit_2arg_appendsCachedErrorHitEventWithNullMessage() {
        NativeSqlTelemetry.cachedErrorHit(
            "fp1", NativeSqlError.Classification.FALLBACK);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        EventRecord rec = snap.get(0);
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.CACHED_ERROR_HIT, rec.type());
        org.junit.jupiter.api.Assertions.assertSame(
            NativeSqlError.Classification.FALLBACK, rec.classification());
        org.junit.jupiter.api.Assertions.assertNull(rec.message());
    }

    @Test
    void cachedErrorHit_3arg_appendsCachedErrorHitEventWithFormattedMessage() {
        java.sql.SQLException sqle = new java.sql.SQLException("transient");
        NativeSqlTelemetry.cachedErrorHit(
            "fp1", NativeSqlError.Classification.FALLBACK, sqle);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        EventRecord rec = snap.get(0);
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.CACHED_ERROR_HIT, rec.type());
        org.junit.jupiter.api.Assertions.assertEquals(
            "SQLException: transient", rec.message());
    }

    @Test
    void reportUnauthorizedDowngrade_appendsEventWithEffectivePropagateClassification() {
        java.sql.SQLException sqle = new java.sql.SQLException("hard");
        NativeSqlTelemetry.reportUnauthorizedDowngrade(
            "fp1",
            sqle,
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.Classification.FALLBACK);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        EventRecord rec = snap.get(0);
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.UNAUTHORIZED_DOWNGRADE, rec.type());
        // Effective classification: PROPAGATE, not the requested FALLBACK.
        org.junit.jupiter.api.Assertions.assertSame(
            NativeSqlError.Classification.PROPAGATE, rec.classification());
        org.junit.jupiter.api.Assertions.assertTrue(
            rec.message().contains("base=PROPAGATE"),
            "expected base=PROPAGATE; got: " + rec.message());
        org.junit.jupiter.api.Assertions.assertTrue(
            rec.message().contains("requested=FALLBACK"),
            "expected requested=FALLBACK; got: " + rec.message());
        org.junit.jupiter.api.Assertions.assertTrue(
            rec.message().contains("effective=PROPAGATE"),
            "expected effective=PROPAGATE; got: " + rec.message());
    }

    @Test
    void onErrorBug_appendsOnErrorBugEvent() {
        RuntimeException re = new RuntimeException("metrics-hook-bug");
        NativeSqlTelemetry.onErrorBug("fp1", re);
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        EventRecord rec = snap.get(0);
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.ON_ERROR_BUG, rec.type());
        org.junit.jupiter.api.Assertions.assertEquals(
            "RuntimeException: metrics-hook-bug", rec.message());
    }

    @Test
    void fingerprintKindViolation_appendsEventWithStructuredMessage() {
        NativeSqlTelemetry.fingerprintKindViolation(
            "fp1", "BATCH", "SCALAR");
        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        EventRecord rec = snap.get(0);
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.FINGERPRINT_KIND_VIOLATION, rec.type());
        org.junit.jupiter.api.Assertions.assertNull(rec.classification());
        org.junit.jupiter.api.Assertions.assertEquals(
            "existing=SCALAR, attempted=BATCH", rec.message());
    }

    @Test
    void telemetryHookUpdatesCounterAndAppendsEvent() {
        // Single hook call → both post-conditions hold. Does NOT claim to
        // prove ordering; bump-order is code-reviewed (Phase 8e style).
        NativeSqlTelemetry.executionSuccess("fp-combo", 11L);

        org.junit.jupiter.api.Assertions.assertEquals(
            1, NativeSqlTelemetry.executionCount("fp-combo"));
        org.junit.jupiter.api.Assertions.assertEquals(
            1, NativeSqlTelemetry.executionSuccessCount("fp-combo"));

        List<EventRecord> snap = NativeSqlTelemetryEvents.snapshot();
        org.junit.jupiter.api.Assertions.assertEquals(1, snap.size());
        org.junit.jupiter.api.Assertions.assertSame(
            EventType.EXECUTION_SUCCESS, snap.get(0).type());
    }
}
