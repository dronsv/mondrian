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
 * Native SQL work registry — pending plane (cell-phase consumers via
 * {@code register}/{@code drain}/{@code executeOrLookup}) and one-shot plane
 * added in Phase 8a (see {@code NativeSqlOneShotWork}).  Owns the
 * pending-work queue and drain loop for {@link NativeSqlWork} units produced by
 * {@code NativeSqlCalc} and {@code NativeQueryEngine} Phase D, plus a
 * process-wide cache of successful results for cross-statement reuse.
 *
 * <p><b>Cache lifetime split (Contract 1 revision):</b>
 * <ul>
 *   <li>{@code pending} — per-instance (per {@code RolapEvaluatorRoot}).
 *       Drain orchestration, phase-loop coordination, cancellation.</li>
 *   <li>{@link #GLOBAL_SUCCESS} — <b>process-wide static</b>.  Successful
 *       results survive statement teardown, giving cross-statement reuse.
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
 *   <li>Contract 1 — result identity internally keyed on
 *       {@code (fingerprint, Bucket)}.  Pending-plane work derives its
 *       {@code Bucket} from {@link NativeSqlWorkKind}
 *       ({@code SCALAR → CELL_SCALAR}, {@code BATCH → CELL_BATCH}); the
 *       one-shot plane added in Phase 8a uses {@code Bucket.ONESHOT}.
 *       Lifetime: successful results process-wide, errors per-statement.</li>
 *   <li>Contract 2 — drain progress = terminal state advancement.</li>
 *   <li>Contract 3 — consumer re-entry dispatch via {@link NativeSqlLookupResult}.</li>
 *   <li>Contract 5 — fingerprint-kind uniqueness enforced fail-fast
 *       process-wide on {@link #register} and {@link #executeOrLookup}.</li>
 * </ul>
 */
public final class NativeSqlRegistry {

    /**
     * Per-instance pending-work queue.  Drain orchestration is
     * statement-scoped so phase-loop ordering and cancellation stay
     * isolated across concurrent statements.
     */
    private final LinkedHashMap<CacheKey, NativeSqlWork> pending = new LinkedHashMap<>();

    /**
     * Per-instance error cache.  Errors stay statement-local so a
     * transient failure on one query does not poison subsequent queries.
     * Within a single statement, however, a cached error prevents the
     * same work unit from being re-registered in a retry loop.
     */
    private final Map<CacheKey, NativeSqlLookupResult> localErrors = new HashMap<>();

    /**
     * Process-wide successful results cache.  Cross-statement reuse
     * based on stable {@code (fingerprint, Bucket)} identity, where
     * {@code Bucket} is the private internal discriminator that maps
     * from pending-plane {@link NativeSqlWorkKind} and reserves
     * {@code ONESHOT} for the one-shot plane.  Keyed on
     * {@link CacheKey} which is derived from
     * {@link NativeSqlFingerprint} (SQL text + bound params + DataSource
     * identity + session context).  Two statements with the same
     * identity legitimately share the same cached result.
     *
     * <p>Cleared by {@link #clearGlobalCache} on schema flush.
     */
    private static final ConcurrentMap<CacheKey, NativeSqlLookupResult> GLOBAL_SUCCESS =
        new ConcurrentHashMap<>();

    /**
     * Process-wide fingerprint → kind index for Contract 5 enforcement.
     * Once a fingerprint has been used with one {@link NativeSqlWorkKind},
     * subsequent registrations under a different kind fail fast across
     * all statements (not just within one).
     */
    private static final ConcurrentMap<NativeSqlFingerprint, NativeSqlWorkKind> FINGERPRINT_KIND_INDEX =
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
     * {@link NativeSqlLookupResult#MISS} if no entry exists in either cache.
     */
    public NativeSqlLookupResult lookup(NativeSqlFingerprint fp, NativeSqlWorkKind kind) {
        CacheKey ck = new CacheKey(fp, Bucket.forKind(kind));
        // Local errors take precedence: if a work unit failed earlier in
        // this statement, subsequent lookups must see the error rather
        // than a potentially-stale cached success from before the
        // error happened.
        NativeSqlLookupResult err = localErrors.get(ck);
        if (err != null) {
            NativeSqlTelemetry.cachedErrorHit(
                fp.toString(), classificationOf(err));
            return err;
        }
        NativeSqlLookupResult ok = GLOBAL_SUCCESS.get(ck);
        if (ok != null) {
            NativeSqlTelemetry.cachedSuccessHit(fp.toString());
            return ok;
        }
        return NativeSqlLookupResult.MISS;
    }

    /**
     * Maps a cached error {@link NativeSqlLookupResult} subtype to its
     * {@link NativeSqlError.Classification}.  The mapping is canonical:
     * the result subtype was chosen at original error classification
     * time in {@link #drain()}, so this is a lossless type → enum cast,
     * not a re-derivation from message text (per spec §4 implementation
     * constraint).
     *
     * <p>Fail-fast on an unrecognised subtype.  This helper is only reached
     * when {@code localErrors} contains a value — a path only entered after
     * {@link #drain()} has populated the entry — and {@code drain()}
     * exclusively constructs {@link NativeSqlLookupResult.ErrorFallback} and
     * {@link NativeSqlLookupResult.ErrorPropagate}.  So the {@code throw} is
     * unreachable in correct operation; it exists strictly as a future-change
     * guard.  An unknown subtype here means a new {@link NativeSqlLookupResult}
     * variant was added without updating this mapping — surface it loudly
     * rather than silently classifying as PROPAGATE.  Acceptable because
     * {@code lookup()} has no documented non-throwing contract, so a throw
     * propagating out of this helper correctly surfaces the bug at the
     * call site rather than masking it.
     */
    private static NativeSqlError.Classification classificationOf(
        NativeSqlLookupResult err)
    {
        if (err.isErrorFallback()) {
            return NativeSqlError.Classification.FALLBACK;
        }
        if (err.isErrorPropagate()) {
            return NativeSqlError.Classification.PROPAGATE;
        }
        throw new IllegalArgumentException(
            "cached local error result expected, got " + err.getClass().getName());
    }

    /**
     * Register a work unit for deferred execution by the next {@link #drain}
     * call.  Caller must return {@code RolapUtil.valueNotReadyException}
     * after this method returns (sentinel-re-entry path).
     *
     * @throws IllegalStateException if the work unit's fingerprint is already
     *         registered under a different {@link NativeSqlWorkKind} (Contract 5)
     */
    public void register(NativeSqlWork work) {
        Objects.requireNonNull(work, "work");
        enforceKindUniqueness(work);

        CacheKey ck = new CacheKey(work.fingerprint(), Bucket.forKind(work.kind()));
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
    public NativeSqlLookupResult executeOrLookup(NativeSqlWork work) {
        Objects.requireNonNull(work, "work");

        NativeSqlLookupResult cached = lookup(work.fingerprint(), work.kind());
        if (!cached.isMiss()) return cached;

        enforceKindUniqueness(work);

        CacheKey ck = new CacheKey(work.fingerprint(), Bucket.forKind(work.kind()));

        // If the same identity is already pending (registered via sentinel
        // path by an earlier consumer), drain THAT unit synchronously rather
        // than executing a fresh one.  This gives cross-entry-point coalescing.
        NativeSqlWork alreadyPending = pending.remove(ck);
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

    // -- one-shot plane (Phase 8a) --

    /**
     * Synchronous one-shot path. Does NOT participate in the pending plane.
     * No {@code Locus} dependency; safe to call with no live
     * {@link mondrian.rolap.RolapEvaluatorRoot}.
     *
     * <p>Lookup → execute → store contract:
     * <ol>
     *   <li>If GLOBAL_SUCCESS contains {@code (fingerprint, ONESHOT)}, fire
     *       {@link NativeSqlTelemetry#cachedSuccessHit} and return the
     *       cached payload.</li>
     *   <li>Otherwise, fire {@link NativeSqlTelemetry#executionStart},
     *       execute via {@link NativeSqlExecutor#run}, classify any
     *       throwable, and either:
     *       <ul>
     *         <li>fire {@link NativeSqlTelemetry#executionSuccess} (BEFORE
     *             the cache write), publish to GLOBAL_SUCCESS, return
     *             payload;</li>
     *         <li>fire {@link NativeSqlTelemetry#executionFailed} with the
     *             adjusted classification, then dispatch FALLBACK to
     *             {@link NativeSqlOneShotWork#fallbackValue} or PROPAGATE
     *             to a wrapped runtime exception preserving the original
     *             throwable as cause.</li>
     *       </ul></li>
     * </ol>
     *
     * <p>Errors are NOT cached (one-shot plane has no per-statement
     * localErrors). Cache-assisted, NOT single-flight: concurrent misses
     * for the same (fingerprint, ONESHOT) key may execute the SQL more than
     * once; payloads must be semantically equivalent.
     *
     * @param work non-null work descriptor
     * @return the cached or freshly-computed payload, or
     *         {@code work.fallbackValue(t)} for FALLBACK-classified failures
     * @throws RuntimeException for PROPAGATE-classified failures
     *         (Mondrian-style wrapper preserving the original throwable)
     */
    public static <R> R executeOneShot(NativeSqlOneShotWork<R> work) {
        Objects.requireNonNull(work, "work");

        final NativeSqlFingerprint fp = work.fingerprint();
        final CacheKey ck = new CacheKey(fp, Bucket.ONESHOT);
        final String fpId = fp.toString();

        // 1) cached hit?
        NativeSqlLookupResult cached = GLOBAL_SUCCESS.get(ck);
        if (cached != null && cached.isSuccess()) {
            NativeSqlTelemetry.cachedSuccessHit(fpId);
            @SuppressWarnings("unchecked")
            R payload = (R) cached.successPayload();
            return payload;
        }

        // 2) miss — execute fresh
        NativeSqlTelemetry.executionStart(fpId);
        final long startNanos = System.nanoTime();
        Throwable failure = null;
        R payload = null;
        try {
            payload = NativeSqlExecutor.run(
                work.sql(),
                work.dataSource(),
                DEFAULT_TIMEOUT_SECONDS,
                (ResultSet rs) -> work.consume(rs));
        } catch (Throwable t) {
            failure = t;
        }
        final long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;

        if (failure == null) {
            // 3a) success — telemetry FIRST (Phase 8e bump-order rule), then cache
            NativeSqlTelemetry.executionSuccess(fpId, durationMs);
            GLOBAL_SUCCESS.put(ck, NativeSqlLookupResult.success(payload));
            return payload;
        }

        // 3b) failure — classify
        NativeSqlError.Classification base = NativeSqlError.classify(failure);
        NativeSqlError.Classification adjusted = work.policyAdjust(failure, base);
        if (base == NativeSqlError.Classification.PROPAGATE
            && adjusted == NativeSqlError.Classification.FALLBACK
            && !work.allowsPropagateDowngrade())
        {
            NativeSqlTelemetry.reportUnauthorizedDowngrade(
                fpId, failure, base, adjusted);
            adjusted = NativeSqlError.Classification.PROPAGATE;
        }
        NativeSqlTelemetry.executionFailed(fpId, failure, adjusted, durationMs);

        // 4) deliver
        if (adjusted == NativeSqlError.Classification.FALLBACK) {
            return work.fallbackValue(failure);
        }
        // PROPAGATE — wrap and throw. Preserve original throwable as cause.
        throw mondrian.olap.Util.newError(
            failure, "fp=" + fpId + "; sql=[" + work.sql() + "]");
    }

    // -- internals --

    private void enforceKindUniqueness(NativeSqlWork work) {
        NativeSqlWorkKind existing = FINGERPRINT_KIND_INDEX.putIfAbsent(
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
        List<Map.Entry<CacheKey, NativeSqlWork>> snapshot =
            new ArrayList<>(pending.entrySet());
        pending.clear();

        boolean progress = false;
        for (Map.Entry<CacheKey, NativeSqlWork> entry : snapshot) {
            drainOne(entry.getKey(), entry.getValue());
            progress = true;
        }
        return progress;
    }

    private void drainOne(CacheKey ck, NativeSqlWork work) {
        Throwable failure = null;
        Object result = null;

        // Single shared anchor — durationMs covers executor + consume(rs)
        // even when the throwable originates inside executeQuery.  See spec
        // §3 "Single shared startNanos" rule.
        long startNanos = System.nanoTime();

        try {
            result = NativeSqlExecutor.run(
                work.sql(),
                work.dataSource(),
                DEFAULT_TIMEOUT_SECONDS,
                (ResultSet rs) -> work.consume(rs));
        } catch (Throwable t) {
            failure = t;
        }

        long durationMs = (System.nanoTime() - startNanos) / 1_000_000L;

        if (failure == null) {
            // Successful result → GLOBAL_SUCCESS (process-wide reuse).
            GLOBAL_SUCCESS.put(ck, NativeSqlLookupResult.success(result));
            NativeSqlTelemetry.executionSuccess(
                work.fingerprint().toString(), durationMs);
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

        NativeSqlLookupResult errResult =
            adjusted == NativeSqlError.Classification.FALLBACK
                ? NativeSqlLookupResult.errorFallback(failure)
                : NativeSqlLookupResult.errorPropagate(failure);
        // Errors go to per-instance localErrors only.  Transient
        // failures MUST NOT poison subsequent statements.
        localErrors.put(ck, errResult);

        try {
            work.onError(failure);
        } catch (Throwable metricsBug) {
            NativeSqlTelemetry.onErrorBug(
                work.fingerprint().toString(), metricsBug);
        }

        NativeSqlTelemetry.executionFailed(
            work.fingerprint().toString(), failure, adjusted, durationMs);
    }

    // -- cache key --

    /**
     * Internal cache-bucket discriminator. Pending-plane work maps to
     * {@code CELL_SCALAR} / {@code CELL_BATCH} based on
     * {@link NativeSqlWorkKind}; one-shot work uses {@code ONESHOT}.
     *
     * <p>Private to {@code NativeSqlRegistry}: never exposed in any public
     * API, parameter type, or telemetry tag. Keeps the cache key space
     * closed.
     */
    private enum Bucket {
        CELL_SCALAR,
        CELL_BATCH,
        ONESHOT;

        static Bucket forKind(NativeSqlWorkKind kind) {
            switch (kind) {
                case SCALAR: return CELL_SCALAR;
                case BATCH:  return CELL_BATCH;
                default:
                    throw new IllegalArgumentException(
                        "Unknown NativeSqlWorkKind: " + kind);
            }
        }
    }

    private record CacheKey(NativeSqlFingerprint fingerprint, Bucket bucket) {}
}
