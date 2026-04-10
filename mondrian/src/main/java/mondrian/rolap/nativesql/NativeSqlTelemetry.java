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

    /** Test-only: clear all counters. Called from {@code setUp}. */
    public static void resetForTests() {
        COUNTERS.clear();
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
