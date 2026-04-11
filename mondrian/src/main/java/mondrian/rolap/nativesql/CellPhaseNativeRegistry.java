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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cell-phase native work registry.  Owns the pending-work queue and
 * drain loop for {@link CellNativeWork} units produced by
 * {@code NativeSqlCalc} and {@code NativeQueryEngine} Phase D, plus a
 * process-wide cache of successful results for cross-statement reuse.
 *
 * <p><b>Cache lifetime split (Contract 1 revision):</b>
 * <ul>
 *   <li>{@code pending} — per-instance (per {@code RolapEvaluatorRoot}).
 *       Drain orchestration, phase-loop coordination, cancellation.</li>
 *   <li>{@link #GLOBAL_SUCCESS} — <b>process-wide static</b>.  Successful
 *       results survive statement teardown, giving cross-statement reuse
 *       matching legacy {@code NativeSqlCalc.SHARED_CACHE} semantics.
 *       Cleared on schema flush via {@link #clearGlobalCache}.</li>
 *   <li>{@link #FINGERPRINT_KIND_INDEX} — <b>process-wide static</b>.
 *       Contract 5 uniqueness is stronger when enforced process-wide.</li>
 *   <li>{@code localErrors} — per-instance.  Error state (classified as
 *       FALLBACK or PROPAGATE) does NOT leak across statements — a
 *       transient failure (connection refused, timeout) on one query
 *       does not poison subsequent queries.</li>
 * </ul>
 *
 * <p>Not thread-safe for the per-instance state — Mondrian statements are
 * single-threaded per phase loop.  The static state uses concurrent
 * collections.
 *
 * <p>Contract coverage (see design spec Section 2 for full definitions):
 * <ul>
 *   <li>Contract 1 — result identity keyed on {@code (fingerprint, kind)}.
 *       Lifetime: successful results process-wide, errors per-statement.</li>
 *   <li>Contract 2 — drain progress = terminal state advancement.</li>
 *   <li>Contract 3 — consumer re-entry dispatch via {@link CellLookupResult}.</li>
 *   <li>Contract 5 — fingerprint-kind uniqueness enforced fail-fast
 *       process-wide on {@link #register} and {@link #executeOrLookup}.</li>
 * </ul>
 */
public final class CellPhaseNativeRegistry {

    /**
     * Per-instance pending-work queue.  Drain orchestration is
     * statement-scoped so phase-loop ordering and cancellation stay
     * isolated across concurrent statements.
     */
    private final LinkedHashMap<CacheKey, CellNativeWork> pending = new LinkedHashMap<>();

    /**
     * Per-instance error cache.  Errors stay statement-local so a
     * transient failure on one query does not poison subsequent queries.
     * Within a single statement, however, a cached error prevents the
     * same work unit from being re-registered in a retry loop.
     */
    private final Map<CacheKey, CellLookupResult> localErrors = new HashMap<>();

    /**
     * Process-wide successful results cache.  Cross-statement reuse
     * based on stable {@code (fingerprint, kind)} identity.  Keyed on
     * {@link CacheKey} which is derived from
     * {@link NativeSqlFingerprint} (SQL text + bound params + DataSource
     * identity + session context).  Two statements with the same
     * identity legitimately share the same cached result.
     *
     * <p>Cleared by {@link #clearGlobalCache} on schema flush.
     */
    private static final ConcurrentMap<CacheKey, CellLookupResult> GLOBAL_SUCCESS =
        new ConcurrentHashMap<>();

    /**
     * Process-wide fingerprint → kind index for Contract 5 enforcement.
     * Once a fingerprint has been used with one {@link CellWorkKind},
     * subsequent registrations under a different kind fail fast across
     * all statements (not just within one).
     */
    private static final ConcurrentMap<NativeSqlFingerprint, CellWorkKind> FINGERPRINT_KIND_INDEX =
        new ConcurrentHashMap<>();

    /** Default query timeout in seconds for native work execution. */
    private static final int DEFAULT_TIMEOUT_SECONDS = 0;

    /**
     * Clears all process-wide cache state.  Called from
     * {@code NativeSqlCalc.clearCache()} on schema flush and from test
     * setUp methods to avoid cross-test pollution.
     */
    public static void clearGlobalCache() {
        GLOBAL_SUCCESS.clear();
        FINGERPRINT_KIND_INDEX.clear();
    }

    // -- public API --

    /**
     * Look up a cached result for {@code (fp, kind)}.  Checks local
     * errors first (transient, statement-scoped) then the global
     * success cache (process-wide, cross-statement reuse).  Returns
     * {@link CellLookupResult#MISS} if no entry exists in either cache.
     */
    public CellLookupResult lookup(NativeSqlFingerprint fp, CellWorkKind kind) {
        CacheKey ck = new CacheKey(fp, kind);
        // Local errors take precedence: if a work unit failed earlier in
        // this statement, subsequent lookups must see the error rather
        // than a potentially-stale cached success from before the
        // error happened.
        CellLookupResult err = localErrors.get(ck);
        if (err != null) return err;
        CellLookupResult ok = GLOBAL_SUCCESS.get(ck);
        return ok != null ? ok : CellLookupResult.MISS;
    }

    /**
     * Register a work unit for deferred execution by the next {@link #drain}
     * call.  Caller must return {@code RolapUtil.valueNotReadyException}
     * after this method returns (sentinel-re-entry path).
     *
     * @throws IllegalStateException if the work unit's fingerprint is already
     *         registered under a different {@link CellWorkKind} (Contract 5)
     */
    public void register(CellNativeWork work) {
        Objects.requireNonNull(work, "work");
        enforceKindUniqueness(work);

        CacheKey ck = new CacheKey(work.fingerprint(), work.kind());
        // Already terminal: skip silently.  Both caches are checked.
        if (localErrors.containsKey(ck)) return;
        if (GLOBAL_SUCCESS.containsKey(ck)) return;
        pending.putIfAbsent(ck, work);
    }

    /**
     * Synchronous single-unit path for call sites that cannot return a
     * sentinel (e.g. NQE Phase D).  Returns a cached result if present,
     * otherwise drains the work unit inline (and only that unit — does NOT
     * trigger a full {@link #drain} sweep).
     *
     * @throws IllegalStateException on Contract 5 violation
     */
    public CellLookupResult executeOrLookup(CellNativeWork work) {
        Objects.requireNonNull(work, "work");

        CellLookupResult cached = lookup(work.fingerprint(), work.kind());
        if (!cached.isMiss()) return cached;

        enforceKindUniqueness(work);

        CacheKey ck = new CacheKey(work.fingerprint(), work.kind());

        // If the same identity is already pending (registered via sentinel
        // path by an earlier consumer), drain THAT unit synchronously rather
        // than executing a fresh one.  This gives cross-entry-point coalescing.
        CellNativeWork alreadyPending = pending.remove(ck);
        if (alreadyPending != null) {
            drainOne(ck, alreadyPending);
        } else {
            drainOne(ck, work);
        }

        return lookup(work.fingerprint(), work.kind());
    }

    /** Current pending queue size. Used by tests. */
    public int pendingSize() {
        return pending.size();
    }

    // -- internals --

    private void enforceKindUniqueness(CellNativeWork work) {
        CellWorkKind existing = FINGERPRINT_KIND_INDEX.putIfAbsent(
            work.fingerprint(), work.kind());
        if (existing != null && existing != work.kind()) {
            NativeSqlTelemetry.fingerprintKindViolation(
                work.fingerprint().toString(),
                work.kind().name(),
                existing.name());
            throw new IllegalStateException(
                "Contract 5 violation: fingerprint " + work.fingerprint()
                + " is already registered with kind " + existing
                + ", attempted to register with kind " + work.kind());
        }
    }

    /**
     * Drain the pending queue.  Executes each pending work unit and
     * publishes success or classified error to the cache.
     *
     * @return {@code true} iff at least one identity moved from {@code pending}
     *         to a terminal state (Contract 2)
     */
    public boolean drain() {
        if (pending.isEmpty()) return false;

        // Snapshot per Section 2: drain sees only the currently-pending work.
        // Registrations that happen during drain go to next sweep.
        List<Map.Entry<CacheKey, CellNativeWork>> snapshot =
            new ArrayList<>(pending.entrySet());
        pending.clear();

        boolean progress = false;
        for (Map.Entry<CacheKey, CellNativeWork> entry : snapshot) {
            drainOne(entry.getKey(), entry.getValue());
            progress = true;
        }
        return progress;
    }

    private void drainOne(CacheKey ck, CellNativeWork work) {
        Throwable failure = null;
        Object result = null;

        try {
            result = NativeSqlExecutor.run(
                work.sql(),
                work.dataSource(),
                DEFAULT_TIMEOUT_SECONDS,
                (ResultSet rs) -> work.consume(rs));
        } catch (Throwable t) {
            failure = t;
        }

        if (failure == null) {
            // Successful result → GLOBAL_SUCCESS (process-wide reuse).
            GLOBAL_SUCCESS.put(ck, CellLookupResult.success(result));
            NativeSqlTelemetry.incExecutionCount(work.fingerprint().toString());
            return;
        }

        NativeSqlError.Classification base = NativeSqlError.classify(failure);
        NativeSqlError.Classification adjusted = work.policyAdjust(failure, base);

        if (base == NativeSqlError.Classification.PROPAGATE
            && adjusted == NativeSqlError.Classification.FALLBACK
            && !work.allowsPropagateDowngrade())
        {
            NativeSqlTelemetry.reportUnauthorizedDowngrade(
                work.fingerprint().toString(), failure, base, adjusted);
            adjusted = NativeSqlError.Classification.PROPAGATE;
        }

        CellLookupResult errResult =
            adjusted == NativeSqlError.Classification.FALLBACK
                ? CellLookupResult.errorFallback(failure)
                : CellLookupResult.errorPropagate(failure);
        // Errors go to per-instance localErrors only.  Transient
        // failures MUST NOT poison subsequent statements.
        localErrors.put(ck, errResult);

        try {
            work.onError(failure);
        } catch (Throwable metricsBug) {
            NativeSqlTelemetry.onErrorBug(
                work.fingerprint().toString(), metricsBug);
        }

        NativeSqlTelemetry.incExecutionCount(work.fingerprint().toString());
    }

    // -- cache key --

    private record CacheKey(NativeSqlFingerprint fingerprint, CellWorkKind kind) {}
}
