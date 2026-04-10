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

/**
 * Cell-phase native work registry.  Owns the pending-work queue, shared
 * result cache, and drain loop for {@link CellNativeWork} units produced by
 * {@code NativeSqlCalc} and {@code NativeQueryEngine} Phase D.
 *
 * <p>Constructed per {@code RolapEvaluatorRoot} (per statement).  Not
 * thread-safe — Mondrian statements are single-threaded per phase loop.
 *
 * <p>Contract coverage (see design spec Section 2 for full definitions):
 * <ul>
 *   <li>Contract 1 — result identity keyed on {@code (fingerprint, kind)}.</li>
 *   <li>Contract 2 — drain progress = terminal state advancement.</li>
 *   <li>Contract 3 — consumer re-entry dispatch via {@link CellLookupResult}.</li>
 *   <li>Contract 5 — fingerprint-kind uniqueness enforced fail-fast on
 *       {@link #register} and {@link #executeOrLookup}.</li>
 * </ul>
 */
public final class CellPhaseNativeRegistry {

    private final LinkedHashMap<CacheKey, CellNativeWork> pending = new LinkedHashMap<>();
    private final Map<CacheKey, CellLookupResult> cache = new HashMap<>();
    private final Map<NativeSqlFingerprint, CellWorkKind> fingerprintKindIndex = new HashMap<>();

    /** Default query timeout in seconds for native work execution. */
    private static final int DEFAULT_TIMEOUT_SECONDS = 0;

    // -- public API --

    /**
     * Look up a cached result for {@code (fp, kind)} without registering.
     * Returns {@link CellLookupResult#MISS} if no entry exists.
     */
    public CellLookupResult lookup(NativeSqlFingerprint fp, CellWorkKind kind) {
        CellLookupResult cached = cache.get(new CacheKey(fp, kind));
        return cached != null ? cached : CellLookupResult.MISS;
    }

    /**
     * Register a work unit for deferred execution by the next {@link #drain}
     * call.  Caller must throw {@code valueNotReadyException} after this
     * method returns (sentinel-re-entry path).
     *
     * @throws IllegalStateException if the work unit's fingerprint is already
     *         registered under a different {@link CellWorkKind} (Contract 5)
     */
    public void register(CellNativeWork work) {
        Objects.requireNonNull(work, "work");
        enforceKindUniqueness(work);

        CacheKey ck = new CacheKey(work.fingerprint(), work.kind());
        if (cache.containsKey(ck)) return;  // already terminal, no-op
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
        CellWorkKind existing = fingerprintKindIndex.get(work.fingerprint());
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
        fingerprintKindIndex.put(work.fingerprint(), work.kind());
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
            cache.put(ck, CellLookupResult.success(result));
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
        cache.put(ck, errResult);

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
