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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Logging + test-queryable counters for cell-phase native registry events.
 *
 * <p>All methods are advisory.  They emit log records and (for counters)
 * update internal state.  They must NEVER throw on valid or malformed input,
 * because they are called from inside the registry drain loop and a crash
 * here would break orderly failure semantics (see Section 3 of design spec).
 *
 * <p>The {@link #executionCount(String)} counter exists specifically for
 * integration tests I1, I8, I10 in Section 7 of the spec.  Production code
 * should not rely on it for control flow.
 */
public final class NativeSqlTelemetry {

    private static final Logger LOGGER =
        LogManager.getLogger(NativeSqlTelemetry.class);

    private static final ConcurrentMap<String, AtomicInteger> COUNTERS =
        new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, AtomicInteger> CACHED_HITS =
        new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, AtomicInteger> EXECUTION_SUCCESSES =
        new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, AtomicInteger> EXECUTION_FAILURES =
        new ConcurrentHashMap<>();

    private NativeSqlTelemetry() { /* utility */ }

    // -- counters (test-queryable) --

    /**
     * Low-level primitive that increments only the legacy fresh-attempt
     * counter ({@link #executionCount(String)} / {@link #snapshot()}).
     *
     * <p>Production code should normally call
     * {@link #executionSuccess(String, long)} or
     * {@link #executionFailed(String, Throwable,
     * mondrian.rolap.nativesql.NativeSqlError.Classification, long)} so that
     * Phase 8e split counters
     * ({@link #executionSuccessCount(String)} /
     *  {@link #executionFailedCount(String)}) stay consistent with
     * {@link #executionCount(String)}.
     *
     * <p>Direct callers intentionally bypass success/failure split tracking;
     * test {@code testIncExecutionCountOnlyBumpsFreshAttempts} pins the
     * documented bypass behavior so the primitive's narrow semantics survive
     * future refactors.
     */
    public static void incExecutionCount(String fingerprintId) {
        if (fingerprintId == null) return;
        COUNTERS.computeIfAbsent(fingerprintId, k -> new AtomicInteger())
            .incrementAndGet();
    }

    public static int executionCount(String fingerprintId) {
        if (fingerprintId == null) return 0;
        AtomicInteger c = COUNTERS.get(fingerprintId);
        return c == null ? 0 : c.get();
    }

    /**
     * Returns the number of cached successful native results delivered for
     * {@code fingerprintId}.  Bumped by {@link #cachedSuccessHit(String)}
     * only.  Independent of {@link #executionCount(String)}, which counts
     * fresh native attempts (success + failure).
     */
    public static int cachedSuccessHitCount(String fingerprintId) {
        if (fingerprintId == null) return 0;
        AtomicInteger c = CACHED_HITS.get(fingerprintId);
        return c == null ? 0 : c.get();
    }

    /**
     * Returns the number of successful fresh native executions recorded
     * for {@code fingerprintId}.  Bumped by
     * {@link #executionSuccess(String, long)} only.  Independent of
     * {@link #executionFailedCount(String)}; their sum is the quiescent
     * value of {@link #executionCount(String)}.
     */
    public static int executionSuccessCount(String fingerprintId) {
        if (fingerprintId == null) return 0;
        AtomicInteger c = EXECUTION_SUCCESSES.get(fingerprintId);
        return c == null ? 0 : c.get();
    }

    /**
     * Returns the number of failed fresh native executions recorded for
     * {@code fingerprintId}.  Bumped by
     * {@link #executionFailed(String, Throwable,
     * mondrian.rolap.nativesql.NativeSqlError.Classification, long)} only.
     */
    public static int executionFailedCount(String fingerprintId) {
        if (fingerprintId == null) return 0;
        AtomicInteger c = EXECUTION_FAILURES.get(fingerprintId);
        return c == null ? 0 : c.get();
    }

    /**
     * Returns a deterministic snapshot of all execution counters.  Used by
     * the Phase 7 baseline-capture procedure and by integration tests that
     * assert per-fingerprint execution counts.  The returned map is a copy
     * sorted by key for stable JSON output; mutations on it do not affect
     * the live counters.
     */
    public static java.util.SortedMap<String, Integer> snapshot() {
        java.util.TreeMap<String, Integer> out = new java.util.TreeMap<>();
        for (java.util.Map.Entry<String, AtomicInteger> e
            : COUNTERS.entrySet())
        {
            out.put(e.getKey(), e.getValue().get());
        }
        return out;
    }

    /**
     * Deterministic snapshot of cached successful result hits, keyed by
     * fingerprint.  Sister method to {@link #snapshot()}; the two counter
     * families are intentionally separate so the Phase 7 baseline JSON
     * shape (consumers of {@link #snapshot()}) remains frozen.  See spec
     * docs/superpowers/specs/2026-05-04-phase-8c-cached-success-hit-telemetry-design.md
     * §3 for the contract.
     *
     * <p>Mutations on the returned map do not affect the live counters.
     */
    public static java.util.SortedMap<String, Integer> cachedHitsSnapshot() {
        java.util.TreeMap<String, Integer> out = new java.util.TreeMap<>();
        for (java.util.Map.Entry<String, AtomicInteger> e
            : CACHED_HITS.entrySet())
        {
            out.put(e.getKey(), e.getValue().get());
        }
        return out;
    }

    /**
     * Deterministic snapshot of fresh successful native executions, keyed
     * by fingerprint.  Sister method to {@link #snapshot()} and
     * {@link #cachedHitsSnapshot()}.  See spec
     * {@code docs/superpowers/specs/2026-05-06-phase-8e-fresh-success-failed-split-design.md}
     * §3 for the contract.
     *
     * <p>Concurrency note: the bump-order rule (legacy first, split second)
     * inside {@link #executionSuccess(String, long)} is a per-event
     * ordering constraint on two {@link AtomicInteger} increments.  It
     * does NOT guarantee {@code attempt &gt;= success + failed} across
     * multi-snapshot reads under concurrent load.  Only the quiescent
     * invariant is contractual.
     *
     * <p>Mutations on the returned map do not affect the live counters.
     */
    public static java.util.SortedMap<String, Integer> executionSuccessSnapshot() {
        java.util.TreeMap<String, Integer> out = new java.util.TreeMap<>();
        for (java.util.Map.Entry<String, AtomicInteger> e
            : EXECUTION_SUCCESSES.entrySet())
        {
            out.put(e.getKey(), e.getValue().get());
        }
        return out;
    }

    /**
     * Deterministic snapshot of fresh failed native executions, keyed by
     * fingerprint.  Sister method to {@link #executionSuccessSnapshot()};
     * same concurrency caveat applies: the bump-order rule (legacy first,
     * split second) inside {@link #executionFailed(String, Throwable,
     * mondrian.rolap.nativesql.NativeSqlError.Classification, long)} is a
     * per-event ordering constraint on two {@link AtomicInteger}
     * increments.  It does NOT guarantee {@code attempt &gt;= success +
     * failed} across multi-snapshot reads under concurrent load.  Only
     * the quiescent invariant is contractual.
     *
     * <p>Mutations on the returned map do not affect the live counters.
     */
    public static java.util.SortedMap<String, Integer> executionFailedSnapshot() {
        java.util.TreeMap<String, Integer> out = new java.util.TreeMap<>();
        for (java.util.Map.Entry<String, AtomicInteger> e
            : EXECUTION_FAILURES.entrySet())
        {
            out.put(e.getKey(), e.getValue().get());
        }
        return out;
    }

    /** Test-only: clear all counters. Called from {@code setUp}. */
    public static void resetForTests() {
        COUNTERS.clear();
        CACHED_HITS.clear();
        EXECUTION_SUCCESSES.clear();
        EXECUTION_FAILURES.clear();
    }

    // -- event hooks (advisory, never throw) --

    public static void executionStart(String fingerprintId) {
        safeLog("native-sql-start fp={}", fingerprintId);
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.EXECUTION_START,
            fingerprintId,
            /*classification*/ null,
            /*durationMs*/ null,
            /*message*/ null);
    }

    public static void executionSuccess(String fingerprintId, long durationMs) {
        if (fingerprintId == null) return;
        // Bump order MUST be legacy first, split second.  See spec §3
        // "Concurrency note": permits transient `attempt > success + failed`
        // observed by concurrent multi-snapshot readers; forbids the
        // reverse transient.
        COUNTERS.computeIfAbsent(fingerprintId, k -> new AtomicInteger())
            .incrementAndGet();
        EXECUTION_SUCCESSES.computeIfAbsent(fingerprintId, k -> new AtomicInteger())
            .incrementAndGet();
        safeLog("native-sql-success fp={} duration_ms={}",
            fingerprintId, durationMs);
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.EXECUTION_SUCCESS,
            fingerprintId,
            /*classification*/ null,
            durationMs,
            /*message*/ null);
    }

    public static void executionFailed(
        String fingerprintId,
        Throwable t,
        NativeSqlError.Classification classification,
        long durationMs)
    {
        if (fingerprintId == null) return;
        // Bump order: legacy first, split second (see executionSuccess).
        COUNTERS.computeIfAbsent(fingerprintId, k -> new AtomicInteger())
            .incrementAndGet();
        EXECUTION_FAILURES.computeIfAbsent(fingerprintId, k -> new AtomicInteger())
            .incrementAndGet();
        try {
            LOGGER.warn(
                "native-sql-failed fp={} classification={} duration_ms={}",
                fingerprintId, classification, durationMs, t);
        } catch (Throwable ignore) {
            // telemetry must never throw
        }
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.EXECUTION_FAILED,
            fingerprintId,
            classification,
            durationMs,
            formatThrowable(t));
    }

    /**
     * Records that a previously-cached successful native result was
     * delivered for {@code fingerprintId}.  Symmetric pair with
     * {@link #executionSuccess(String, long)}: the latter is fired
     * once per fresh native execution from {@link mondrian.rolap.nativesql.NativeSqlRegistry#drain()};
     * this method is fired once per cache hit from
     * {@link mondrian.rolap.nativesql.NativeSqlRegistry#lookup(NativeSqlFingerprint, NativeSqlWorkKind)}
     * (wired in Phase 8c Tasks 2/3).
     *
     * <p>Increments {@link #cachedSuccessHitCount(String)} only.  Does
     * NOT touch {@link #executionCount(String)} or {@link #snapshot()} —
     * the fresh-execution counter family stays frozen per spec §3.
     *
     * <p>Null-safe: a {@code null} {@code fingerprintId} is silently
     * ignored, mirroring {@link #incExecutionCount(String)}.
     */
    public static void cachedSuccessHit(String fingerprintId) {
        if (fingerprintId == null) return;
        CACHED_HITS.computeIfAbsent(fingerprintId, k -> new AtomicInteger())
            .incrementAndGet();
        safeLog("native-sql-cached-success-hit fp={}", fingerprintId);
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.CACHED_SUCCESS_HIT,
            fingerprintId,
            /*classification*/ null,
            /*durationMs*/ null,
            /*message*/ null);
    }

    public static void cachedErrorHit(
        String fingerprintId,
        NativeSqlError.Classification classification)
    {
        cachedErrorHit(fingerprintId, classification, /*t*/ null);
    }

    public static void cachedErrorHit(
        String fingerprintId,
        NativeSqlError.Classification classification,
        Throwable t)
    {
        safeLog("native-sql-cached-error-hit fp={} classification={}",
            fingerprintId, classification);
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.CACHED_ERROR_HIT,
            fingerprintId,
            classification,
            /*durationMs*/ null,
            formatThrowable(t));
    }

    public static void onErrorBug(String fingerprintId, Throwable metricsBug) {
        try {
            LOGGER.error(
                "native-sql-on-error-bug fp={} — consumer onError() itself threw",
                fingerprintId, metricsBug);
        } catch (Throwable ignore) {
            // telemetry must never throw
        }
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.ON_ERROR_BUG,
            fingerprintId,
            /*classification*/ null,
            /*durationMs*/ null,
            formatThrowable(metricsBug));
    }

    public static void fingerprintKindViolation(
        String fingerprintId,
        String attemptedKind,
        String existingKind)
    {
        try {
            LOGGER.error(
                "native-sql-kind-violation fp={} attempted={} existing={}",
                fingerprintId, attemptedKind, existingKind);
        } catch (Throwable ignore) {
            // telemetry must never throw
        }
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.FINGERPRINT_KIND_VIOLATION,
            fingerprintId,
            /*classification*/ null,
            /*durationMs*/ null,
            formatFingerprintKindViolation(existingKind, attemptedKind));
    }

    public static void reportUnauthorizedDowngrade(
        String fingerprintId,
        Throwable t,
        NativeSqlError.Classification base,
        NativeSqlError.Classification adjusted)
    {
        try {
            LOGGER.warn(
                "native-sql-unauthorized-downgrade fp={} base={} adjusted={}",
                fingerprintId, base, adjusted, t);
        } catch (Throwable ignore) {
            // telemetry must never throw
        }
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.UNAUTHORIZED_DOWNGRADE,
            fingerprintId,
            NativeSqlError.Classification.PROPAGATE,
            /*durationMs*/ null,
            formatUnauthorizedDowngrade(base, adjusted, t));
    }

    // -- Phase 8f event-format helpers --

    private static String formatThrowable(Throwable t) {
        if (t == null) {
            return null;
        }
        String simpleName = t.getClass().getSimpleName();
        String msg = t.getMessage();
        return msg == null ? simpleName : simpleName + ": " + msg;
    }

    private static String formatUnauthorizedDowngrade(
        NativeSqlError.Classification base,
        NativeSqlError.Classification adjusted,
        Throwable t)
    {
        String tStr = formatThrowable(t);
        String head = "base=" + base.name()
            + ", requested=" + adjusted.name()
            + ", effective=PROPAGATE";
        return tStr == null ? head : head + "; " + tStr;
    }

    private static String formatFingerprintKindViolation(
        String existing,
        String attempted)
    {
        return "existing=" + existing + ", attempted=" + attempted;
    }

    private static void safeLog(String pattern, Object... args) {
        try {
            LOGGER.debug(pattern, args);
        } catch (Throwable ignore) {
            // telemetry must never throw
        }
    }
}
