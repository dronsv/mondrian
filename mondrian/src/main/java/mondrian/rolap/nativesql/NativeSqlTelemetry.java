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

    private NativeSqlTelemetry() { /* utility */ }

    // -- counters (test-queryable) --

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

    /** Test-only: clear all counters. Called from {@code setUp}. */
    public static void resetForTests() {
        COUNTERS.clear();
        CACHED_HITS.clear();
    }

    // -- event hooks (advisory, never throw) --

    public static void executionStart(String fingerprintId) {
        safeLog("native-sql-start fp={}", fingerprintId);
    }

    public static void executionSuccess(String fingerprintId, long durationMs) {
        incExecutionCount(fingerprintId);
        safeLog("native-sql-success fp={} duration_ms={}",
            fingerprintId, durationMs);
    }

    public static void executionFailed(
        String fingerprintId,
        Throwable t,
        NativeSqlError.Classification classification,
        long durationMs)
    {
        incExecutionCount(fingerprintId);
        try {
            LOGGER.warn(
                "native-sql-failed fp={} classification={} duration_ms={}",
                fingerprintId, classification, durationMs, t);
        } catch (Throwable ignore) {
            // telemetry must never throw
        }
    }

    public static void cachedSuccessHit(String fingerprintId) {
        if (fingerprintId == null) return;
        CACHED_HITS.computeIfAbsent(fingerprintId, k -> new AtomicInteger())
            .incrementAndGet();
        safeLog("native-sql-cached-success-hit fp={}", fingerprintId);
    }

    public static void cachedErrorHit(
        String fingerprintId,
        NativeSqlError.Classification classification)
    {
        safeLog("native-sql-cached-error-hit fp={} classification={}",
            fingerprintId, classification);
    }

    public static void onErrorBug(String fingerprintId, Throwable metricsBug) {
        try {
            LOGGER.error(
                "native-sql-on-error-bug fp={} — consumer onError() itself threw",
                fingerprintId, metricsBug);
        } catch (Throwable ignore) {
            // telemetry must never throw
        }
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
    }

    private static void safeLog(String pattern, Object... args) {
        try {
            LOGGER.debug(pattern, args);
        } catch (Throwable ignore) {
            // telemetry must never throw
        }
    }
}
