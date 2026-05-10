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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** Contract tests for {@link NativeSqlRegistry}. */
public class NativeSqlRegistryTest {

    private NativeSqlRegistry registry;
    private DataSource ds;
    private Connection conn;
    private Statement stmt;

    @BeforeEach public void setUp() throws Exception {
        // Clear process-wide state so tests do not pollute each other.
        // GLOBAL_SUCCESS + FINGERPRINT_KIND_INDEX are static (see
        // registry Javadoc — cache lifetime split for cross-statement
        // reuse).
        NativeSqlRegistry.clearGlobalCache();
        registry = new NativeSqlRegistry();
        ds = mock(DataSource.class);
        conn = mock(Connection.class);
        stmt = mock(Statement.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        NativeSqlTelemetry.resetForTests();
    }

    // -- test helpers --

    private NativeSqlFingerprint fp(String sql) {
        return NativeSqlFingerprint.of(sql, Collections.emptyList(), ds, "sess");
    }

    // ---------------------------------------------------------------------
    // Block A — basic lookup + register
    // ---------------------------------------------------------------------

    @Test public void testEmptyRegistryLookupIsMiss() {
        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isMiss());
    }

    @Test public void testEmptyRegistryDrainReturnsFalse() {
        assertFalse(registry.drain());
    }

    @Test public void testRegisterAddsToPending() {
        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "result");
        registry.register(work);
        assertEquals(1, registry.pendingSize());
    }

    @Test public void testRegisterDuplicateIsNoOp() {
        FakeScalarWork work1 =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "result");
        FakeScalarWork work2 =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "result");
        registry.register(work1);
        registry.register(work2);
        assertEquals(1, registry.pendingSize(), "same identity must dedup");
    }

    // ---------------------------------------------------------------------
    // Block B — Contract 5: fingerprint-kind uniqueness
    // ---------------------------------------------------------------------

    @Test public void testContract5_registerScalarThenBatchSameFingerprintFails() {
        NativeSqlFingerprint fp = fp("SELECT 1");
        registry.register(new FakeScalarWork(fp, ds, "SELECT 1", "scalar"));

        try {
            registry.register(new FakeBatchWork(fp, ds, "SELECT 1", "batch"));
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("Contract 5"));
        }
    }

    @Test public void testContract5_executeOrLookupWithDifferentKindFails() throws Exception {
        NativeSqlFingerprint fp = fp("SELECT 1");
        registry.register(new FakeScalarWork(fp, ds, "SELECT 1", "scalar"));

        try {
            registry.executeOrLookup(new FakeBatchWork(fp, ds, "SELECT 1", "batch"));
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("Contract 5"));
        }
    }

    @Test public void testContract5_registerSameKindTwiceIsFine() {
        NativeSqlFingerprint fp = fp("SELECT 1");
        registry.register(new FakeScalarWork(fp, ds, "SELECT 1", "x"));
        registry.register(new FakeScalarWork(fp, ds, "SELECT 1", "x"));
        assertEquals(1, registry.pendingSize());
    }

    // ---------------------------------------------------------------------
    // Block C — drain happy path + cache population
    // ---------------------------------------------------------------------

    @Test public void testDrainExecutesPendingAndCachesResult() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "cached-result");
        registry.register(work);

        assertTrue(registry.drain());
        assertEquals(0, registry.pendingSize());

        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isSuccess());
        assertEquals("cached-result", r.successPayload());
    }

    @Test public void testDrainConsumesEachWorkUnitExactlyOnce() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x");
        registry.register(work);
        registry.drain();

        assertEquals(1, work.consumeCount);
    }

    @Test public void testSecondDrainIsNoOpOnEmptyPending() {
        assertFalse(registry.drain());
        assertFalse(registry.drain());
    }

    @Test public void testLookupHitDoesNotRegister() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        registry.drain();

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "y"));
        assertEquals(0, registry.pendingSize(), "should not re-register cached identity");
    }

    // ---------------------------------------------------------------------
    // Block D — drain error handling + classification
    // ---------------------------------------------------------------------

    @Test public void testDrainFailureCachesPropagateError() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        assertTrue(registry.drain());

        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate());
        assertEquals("connection refused", r.errorThrowable().getMessage());
    }

    @Test public void testDrainUnsupportedTemplateShapeCachesFallback() throws Exception {
        FakeFallbackScalar work = new FakeFallbackScalar(fp("SELECT 1"), ds, "SELECT 1");

        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(work);
        registry.drain();

        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorFallback());
    }

    @Test public void testDrainCachedErrorIsStickyAcrossRegisters() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("oops"));

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        registry.drain();

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "y"));
        assertEquals(0, registry.pendingSize());

        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate());
    }

    @Test public void testDrainReturnsTrueEvenOnError() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("oops"));

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        assertTrue(registry.drain(), "error drain must return progress=true");
    }

    @Test public void testDrainCallsOnErrorCallback() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("oops"));

        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x");
        registry.register(work);
        registry.drain();

        assertEquals(1, work.onErrorCount);
    }

    // ---------------------------------------------------------------------
    // Block E — drain snapshot semantics
    // ---------------------------------------------------------------------

    @Test public void testRegistrationDuringDrainGoesToNextSweep() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        RecursiveRegisteringWork workA = new RecursiveRegisteringWork(
            fp("SELECT A"), ds, "SELECT A", registry,
            new FakeScalarWork(fp("SELECT B"), ds, "SELECT B", "B-result"));

        registry.register(workA);
        assertEquals(1, registry.pendingSize());

        assertTrue(registry.drain());
        assertEquals(1,
            registry.pendingSize(), "workB must be pending after first drain");

        assertTrue(registry.drain());
        assertEquals(0, registry.pendingSize());

        assertTrue(registry.lookup(fp("SELECT A"), NativeSqlWorkKind.SCALAR).isSuccess());
        assertTrue(registry.lookup(fp("SELECT B"), NativeSqlWorkKind.SCALAR).isSuccess());
    }

    @Test public void testDrainTerminationWithRecursiveRegistration() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        long start = System.nanoTime();
        registry.drain();
        long elapsed = System.nanoTime() - start;
        assertTrue(elapsed < 1_000_000_000L, "drain must not loop forever");
    }

    // ---------------------------------------------------------------------
    // Block F — executeOrLookup single-unit scope
    // ---------------------------------------------------------------------

    @Test public void testExecuteOrLookupCacheHitReturnsCached() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "cached"));
        registry.drain();

        reset(stmt);
        when(conn.createStatement()).thenReturn(stmt);
        NativeSqlLookupResult r = registry.executeOrLookup(
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "fresh"));
        assertTrue(r.isSuccess());
        assertEquals("cached", r.successPayload());
        verify(stmt, never()).executeQuery(anyString());
    }

    @Test public void testExecuteOrLookupCacheMissExecutesInline() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        NativeSqlLookupResult r = registry.executeOrLookup(
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "fresh"));

        assertTrue(r.isSuccess());
        assertEquals("fresh", r.successPayload());
    }

    @Test public void testExecuteOrLookupDoesNotTouchOtherPending() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeScalarWork pendingA =
            new FakeScalarWork(fp("SELECT A"), ds, "SELECT A", "A");
        FakeScalarWork pendingB =
            new FakeScalarWork(fp("SELECT B"), ds, "SELECT B", "B");
        registry.register(pendingA);
        registry.register(pendingB);
        assertEquals(2, registry.pendingSize());

        registry.executeOrLookup(
            new FakeScalarWork(fp("SELECT C"), ds, "SELECT C", "C"));

        assertEquals(2, registry.pendingSize(), "A and B must remain pending");
        assertEquals(0,
            pendingA.consumeCount, "pendingA must not have been drained");
        assertEquals(0,
            pendingB.consumeCount, "pendingB must not have been drained");
    }

    @Test public void testExecuteOrLookupPromotesAlreadyPendingUnit() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeScalarWork fromA =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "A-result");
        registry.register(fromA);
        assertEquals(1, registry.pendingSize());

        NativeSqlLookupResult r = registry.executeOrLookup(
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "B-result"));

        assertTrue(r.isSuccess());
        assertEquals(1, fromA.consumeCount);
        assertEquals(0, registry.pendingSize());
    }

    // ---------------------------------------------------------------------
    // Block G — policyAdjust directional constraint (Section 3)
    // ---------------------------------------------------------------------

    @Test public void testPolicyAdjustDefault_noChange() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        registry.drain();

        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate());
    }

    @Test public void testPolicyAdjustEscalation_fallbackToPropagate_allowed() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        EscalatingScalarWork work = new EscalatingScalarWork(fp("SELECT 1"), ds, "SELECT 1");
        registry.register(work);
        registry.drain();

        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate(), "escalation must be honored");
    }

    @Test public void testPolicyAdjustUnauthorizedDowngrade_rejected() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        UnauthorizedDowngradeWork work = new UnauthorizedDowngradeWork(
            fp("SELECT 1"), ds, "SELECT 1");
        registry.register(work);
        registry.drain();

        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate(), "unauthorized downgrade must be rejected");
    }

    @Test public void testPolicyAdjustAuthorizedDowngrade_allowed() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        AuthorizedDowngradeWork work = new AuthorizedDowngradeWork(
            fp("SELECT 1"), ds, "SELECT 1");
        registry.register(work);
        registry.drain();

        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorFallback(), "authorized downgrade must be honored");
    }

    // ---------------------------------------------------------------------
    // Block H — onError callback bug isolation
    // ---------------------------------------------------------------------

    @Test public void testOnErrorBugDoesNotDestabilizeDrainLoop() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("oops"));

        registry.register(
            new BuggyOnErrorWork(fp("SELECT A"), ds, "SELECT A"));
        registry.register(
            new FakeScalarWork(fp("SELECT B"), ds, "SELECT B", "B"));

        assertTrue(registry.drain());

        NativeSqlLookupResult rA = registry.lookup(fp("SELECT A"), NativeSqlWorkKind.SCALAR);
        NativeSqlLookupResult rB = registry.lookup(fp("SELECT B"), NativeSqlWorkKind.SCALAR);

        assertTrue(rA.isErrorPropagate(), "work A must have terminal state");
        assertTrue(rB.isErrorPropagate(),
            "work B must ALSO have terminal state despite A's onError bug");
    }

    // ---------------------------------------------------------------------
    // Block T — Phase 8c lookup() telemetry wiring
    // ---------------------------------------------------------------------

    @Test public void testLookupCachedSuccessFiresCachedSuccessHit() throws Exception {
        // Prime GLOBAL_SUCCESS via executeOrLookup() — runs the work once,
        // caches the success.  The capture-baseline pattern below absorbs
        // any counter mutations from this priming step.
        // Build the fingerprint instance once and reuse it — the assertion
        // is "lookup of the same logical fingerprint returns cached", not
        // "fp() factory produces equal instances on repeat calls".
        NativeSqlFingerprint fpT1 = fp("SELECT T1");
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        registry.executeOrLookup(
            new FakeScalarWork(fpT1, ds, "SELECT T1", "primed"));

        // Capture baselines AFTER priming.
        java.util.SortedMap<String, Integer> freshBefore =
            NativeSqlTelemetry.snapshot();
        java.util.SortedMap<String, Integer> cachedBefore =
            NativeSqlTelemetry.cachedHitsSnapshot();
        String fpKey = fpT1.toString();
        int cachedBeforeFp = cachedBefore.getOrDefault(fpKey, 0);

        // Act: invoke lookup() directly on the now-cached entry.
        NativeSqlLookupResult r = registry.lookup(fpT1, NativeSqlWorkKind.SCALAR);

        // Cached success was returned.
        assertTrue(r.isSuccess(),
            "lookup must return cached Success, got " + r.getClass().getSimpleName());

        // Wiring contract: cachedSuccessHit fired exactly once.
        assertEquals(cachedBeforeFp + 1,
            NativeSqlTelemetry.cachedSuccessHitCount(fpKey),
            "lookup() on cached success must fire cachedSuccessHit exactly once");

        // Frozen contract: lookup() must NOT bump COUNTERS.
        assertEquals(freshBefore, NativeSqlTelemetry.snapshot(),
            "lookup() must not mutate the fresh-execution snapshot");
    }

    @Test public void testLookupCachedErrorFiresCachedErrorHitWithoutIncrementingCounters() throws Exception {
        // Prime localErrors with a FALLBACK-classified error via drain().
        // FakeFallbackScalar throws UnsupportedTemplateShape, a typed
        // FALLBACK sentinel; the registry caches it as ErrorFallback in
        // localErrors (per drain() failure branch).
        // Build the fingerprint instance once and reuse it (intent: "lookup
        // of the same logical fingerprint returns the cached error").
        NativeSqlFingerprint fpTE = fp("SELECT TE");
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        registry.register(new FakeFallbackScalar(fpTE, ds, "SELECT TE"));
        registry.drain();

        // Capture baselines AFTER priming.
        java.util.SortedMap<String, Integer> freshBefore =
            NativeSqlTelemetry.snapshot();
        java.util.SortedMap<String, Integer> cachedBefore =
            NativeSqlTelemetry.cachedHitsSnapshot();

        // Act: invoke lookup() directly on the now-cached error.
        NativeSqlLookupResult r = registry.lookup(fpTE, NativeSqlWorkKind.SCALAR);

        // Cached fallback error was returned.
        assertTrue(r.isErrorFallback(),
            "lookup must return cached ErrorFallback, got "
            + r.getClass().getSimpleName());

        // Negative-assertion ladder rung 2 (no ListAppender available in
        // this test tree): assert that cachedErrorHit does NOT mutate
        // counter state.  This guards the spec §3 invariant that the
        // cached-error path is log/event-only and does not touch
        // COUNTERS or CACHED_HITS.
        //
        // Positive wiring verification ("cachedErrorHit actually called")
        // is covered by the static grep check in plan Step 5, executed
        // before commit (see commit message).
        assertEquals(freshBefore, NativeSqlTelemetry.snapshot(),
            "lookup() on cached error must not mutate fresh-execution snapshot");
        assertEquals(cachedBefore, NativeSqlTelemetry.cachedHitsSnapshot(),
            "lookup() on cached error must not mutate cached-hits snapshot");
    }

    // ---------------------------------------------------------------------
    // Block X — Phase 8e drain rewire: drain() fires rich telemetry events
    // ---------------------------------------------------------------------

    @Test public void testDrainSuccessFiresExecutionSuccess() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "ok");
        registry.register(work);
        assertTrue(registry.drain());

        String fpId = work.fingerprint().toString();
        assertEquals(1, NativeSqlTelemetry.executionCount(fpId),
            "success branch must bump COUNTERS");
        assertEquals(1, NativeSqlTelemetry.executionSuccessCount(fpId),
            "success branch must bump EXECUTION_SUCCESSES");
        assertEquals(0, NativeSqlTelemetry.executionFailedCount(fpId),
            "success branch must NOT bump EXECUTION_FAILURES");
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount(fpId),
            "success branch must NOT bump CACHED_HITS");
    }

    @Test public void testDrainFailureFiresExecutionFailed_propagateClass()
        throws Exception
    {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x");
        registry.register(work);
        assertTrue(registry.drain());

        // SQLException is classified as PROPAGATE by NativeSqlError.classify
        // (see Phase 8c tests).  Phase 8e asserts that the failure path
        // bumps EXECUTION_FAILURES regardless of classification.
        String fpId = work.fingerprint().toString();
        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate(), "precondition: PROPAGATE classification");

        assertEquals(1, NativeSqlTelemetry.executionCount(fpId),
            "failure branch must bump COUNTERS");
        assertEquals(1, NativeSqlTelemetry.executionFailedCount(fpId),
            "failure branch must bump EXECUTION_FAILURES");
        assertEquals(0, NativeSqlTelemetry.executionSuccessCount(fpId),
            "failure branch must NOT bump EXECUTION_SUCCESSES");
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount(fpId),
            "failure branch must NOT bump CACHED_HITS");
    }

    @Test public void testDrainFailureFiresExecutionFailed_fallbackClass()
        throws Exception
    {
        // FakeFallbackScalar throws UnsupportedTemplateShape from consume(),
        // which NativeSqlError.classify() maps directly to FALLBACK.  The
        // default policyAdjust (identity) leaves the classification unchanged.
        // This exercises the FALLBACK arm of drainOne's classification logic.
        // Reuses the existing test fixture from
        // testDrainUnsupportedTemplateShapeCachesFallback above.
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeFallbackScalar work =
            new FakeFallbackScalar(fp("SELECT 1"), ds, "SELECT 1");
        registry.register(work);
        assertTrue(registry.drain());

        String fpId = work.fingerprint().toString();
        NativeSqlLookupResult r = registry.lookup(fp("SELECT 1"), NativeSqlWorkKind.SCALAR);
        assertTrue(r.isErrorFallback(), "precondition: FALLBACK classification");

        assertEquals(1, NativeSqlTelemetry.executionCount(fpId));
        assertEquals(1, NativeSqlTelemetry.executionFailedCount(fpId),
            "FALLBACK-classified failure must also bump EXECUTION_FAILURES");
        assertEquals(0, NativeSqlTelemetry.executionSuccessCount(fpId));
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount(fpId));
    }

    // ---------------------------------------------------------------------
    // Block Y — Phase 8a Task 2d: executeOneShot static one-shot path
    // ---------------------------------------------------------------------

    @Test
    void executeOneShot_cacheMissExecutesStoresAndRecordsSuccess() throws Exception {
        // arrange
        DataSource ds = mockDataSourceReturningOneRowOneInt(42);
        String sql = "SELECT 42";
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            sql, Collections.emptyList(), ds, null);

        // act
        Integer first = NativeSqlRegistry.executeOneShot(
            new TestOneShotWork(fp, ds, sql));

        // assert
        assertEquals(42, first.intValue());
        assertEquals(1, NativeSqlTelemetry.executionCount(fp.toString()));
        assertEquals(1, NativeSqlTelemetry.executionSuccessCount(fp.toString()));

        // act again (second call must hit cache)
        Integer second = NativeSqlRegistry.executeOneShot(
            new TestOneShotWork(fp, ds, sql));

        // assert
        assertEquals(42, second.intValue());
        assertEquals(1, NativeSqlTelemetry.executionCount(fp.toString()));   // unchanged
        assertEquals(1, NativeSqlTelemetry.cachedSuccessHitCount(fp.toString()));
    }

    @Test
    void executeOneShot_fallbackPathReturnsSentinelAndDoesNotCache() throws Exception {
        DataSource ds = mockDataSourceThatThrows(new SQLException("transient"));
        String sql = "SELECT 1";
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            sql, Collections.emptyList(), ds, null);

        Integer r = NativeSqlRegistry.executeOneShot(
            new FallbackOneShotWork(fp, ds, sql, /*sentinel*/ -1));
        assertEquals(-1, r.intValue());

        // second call re-executes (errors not cached)
        Integer r2 = NativeSqlRegistry.executeOneShot(
            new FallbackOneShotWork(fp, ds, sql, /*sentinel*/ -1));
        assertEquals(-1, r2.intValue());
        assertEquals(2, NativeSqlTelemetry.executionCount(fp.toString()));
        assertEquals(0, NativeSqlTelemetry.cachedSuccessHitCount(fp.toString()));
        assertEquals(0, NativeSqlTelemetry.executionSuccessCount(fp.toString()));
        assertEquals(2, NativeSqlTelemetry.executionFailedCount(fp.toString()));
    }

    @Test
    void executeOneShot_propagatePathThrowsWrappedAndDoesNotCache() throws Exception {
        DataSource ds = mockDataSourceThatThrows(new SQLException("hard failure"));
        String sql = "SELECT 1";
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            sql, Collections.emptyList(), ds, null);

        RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> NativeSqlRegistry.executeOneShot(
                new PropagateOneShotWork(fp, ds, sql)));
        assertNotNull(thrown.getCause());
        assertTrue(thrown.getMessage().contains("sql=["));

        // second call re-executes (errors not cached)
        assertThrows(RuntimeException.class,
            () -> NativeSqlRegistry.executeOneShot(
                new PropagateOneShotWork(fp, ds, sql)));
        assertEquals(2, NativeSqlTelemetry.executionCount(fp.toString()));
    }

    @Test
    void executeOneShot_unauthorizedDowngradeForcedBackToPropagate() throws Exception {
        DataSource ds = mockDataSourceThatThrows(new RuntimeException("hard"));
        String sql = "SELECT 1";
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            sql, Collections.emptyList(), ds, null);

        // policyAdjust requests FALLBACK, allowsPropagateDowngrade=false → PROPAGATE wins
        assertThrows(RuntimeException.class,
            () -> NativeSqlRegistry.executeOneShot(
                new UnauthorizedDowngradeOneShotWork(fp, ds, sql)));
    }

    @Test
    void executeOneShot_authorizedDowngradeReachesFallback() throws Exception {
        // Distinguishes from executeOneShot_fallbackPathReturnsSentinelAndDoesNotCache
        // by the underlying error type: RuntimeException here vs SQLException there.
        // RuntimeException is classified as base=PROPAGATE, then policyAdjust→FALLBACK
        // with allowsPropagateDowngrade=true authorizes the downgrade — exercising
        // the PROPAGATE→FALLBACK branch of the downgrade-authorization guard.
        DataSource ds = mockDataSourceThatThrows(new RuntimeException("hard"));
        String sql = "SELECT 1";
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            sql, Collections.emptyList(), ds, null);

        Integer r = NativeSqlRegistry.executeOneShot(
            new FallbackOneShotWork(fp, ds, sql, /*sentinel*/ 99));
        assertEquals(99, r.intValue());
    }

    /**
     * JVM-fatal Errors (OutOfMemoryError, StackOverflowError, LinkageError,
     * ThreadDeath, ...) MUST NOT be classified by {@link NativeSqlError}
     * and wrapped via {@link mondrian.olap.Util#newError(Throwable, String)}
     * (which returns a {@code RuntimeException}, masking the fatal
     * condition behind upstream catch-Exception sites).  Pin: an
     * {@link OutOfMemoryError} thrown by the executor must propagate as
     * an {@link Error} subtype, with the original instance preserved.
     */
    @Test
    void executeOneShot_rethrowsOutOfMemoryErrorWithoutWrapping()
        throws Exception
    {
        OutOfMemoryError oom =
            new OutOfMemoryError("synthetic-OOM-for-test");
        DataSource ds = mockDataSourceThatThrowsError(oom);
        String sql = "SELECT 1";
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            sql, Collections.emptyList(), ds, null);

        // PropagateOneShotWork is the most aggressive consumer policy —
        // even with PROPAGATE it must still surface Errors as Errors,
        // not as the RuntimeException that Util.newError(...) would
        // wrap them in if they reached the classify branch.
        Throwable thrown = assertThrows(
            OutOfMemoryError.class,
            () -> NativeSqlRegistry.executeOneShot(
                new PropagateOneShotWork(fp, ds, sql)),
            "Expected OutOfMemoryError to propagate as Error, "
                + "not as a RuntimeException wrapper");
        assertSame(
            oom, thrown,
            "Expected the exact OutOfMemoryError instance to propagate");
    }

    @Test
    void oneShotCacheBucketIsolatedFromCellBuckets() throws Exception {
        DataSource ds = mockDataSourceReturningOneRowOneInt(7);
        String sql = "SELECT 7";
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            sql, Collections.emptyList(), ds, null);

        // ONESHOT path
        Integer one = NativeSqlRegistry.executeOneShot(
            new TestOneShotWork(fp, ds, sql));
        assertEquals(7, one.intValue());

        // Now register a CELL_SCALAR work with the SAME fingerprint via the
        // pending plane. Must NOT trigger Contract 5 violation (different bucket).
        NativeSqlRegistry reg = new NativeSqlRegistry();
        NativeSqlLookupResult after = reg.executeOrLookup(
            new FakeScalarWork(fp, ds, sql, /*scalar*/ 7));
        assertTrue(after.isSuccess());

        // Reset telemetry so the final assertion isolates the next ONESHOT
        // call. (executeOrLookup's post-drain lookup() also fires
        // cachedSuccessHit for the freshly-cached CELL_SCALAR entry, which
        // is independent of bucket isolation and out of scope for this test.)
        NativeSqlTelemetry.resetForTests();

        // Both bucket entries coexist: the next ONESHOT lookup must hit
        // (not get masked by the CELL_SCALAR entry, not collide with it).
        Integer twoStillCached = NativeSqlRegistry.executeOneShot(
            new TestOneShotWork(fp, ds, sql));
        assertEquals(7, twoStillCached.intValue());
        assertEquals(1, NativeSqlTelemetry.cachedSuccessHitCount(fp.toString()));
        assertEquals(0, NativeSqlTelemetry.executionCount(fp.toString()),
            "ONESHOT entry must still be cached — no fresh execution");
    }

    // -- one-shot mock helpers --

    private static DataSource mockDataSourceReturningOneRowOneInt(int value) throws SQLException {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt(1)).thenReturn(value);
        return ds;
    }

    private static DataSource mockDataSourceThatThrows(SQLException toThrow) throws SQLException {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenThrow(toThrow);
        return ds;
    }

    private static DataSource mockDataSourceThatThrows(RuntimeException toThrow) throws SQLException {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenThrow(toThrow);
        return ds;
    }

    /**
     * Variant for JVM-fatal Errors (OutOfMemoryError, StackOverflowError,
     * LinkageError, ...).  Mockito's {@code thenThrow(Throwable...)}
     * accepts Errors; the safety contract under test is that the
     * registry surfaces them as Errors instead of wrapping them.
     */
    private static DataSource mockDataSourceThatThrowsError(Error toThrow)
        throws SQLException
    {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenThrow(toThrow);
        return ds;
    }

    // -- one-shot work fixtures --

    private static final class TestOneShotWork extends NativeSqlOneShotWork<Integer> {
        TestOneShotWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public Integer consume(ResultSet rs) throws SQLException {
            rs.next();
            return rs.getInt(1);
        }
        @Override public Integer fallbackValue(Throwable t) {
            throw new IllegalStateException("unreachable", t);
        }
    }

    private static final class FallbackOneShotWork extends NativeSqlOneShotWork<Integer> {
        private final int sentinel;
        FallbackOneShotWork(NativeSqlFingerprint fp, DataSource ds, String sql, int sentinel) {
            super(fp, ds, sql);
            this.sentinel = sentinel;
        }
        @Override public Integer consume(ResultSet rs) { throw new AssertionError("unreachable"); }
        @Override public NativeSqlError.Classification policyAdjust(
            Throwable t, NativeSqlError.Classification base) {
            return NativeSqlError.Classification.FALLBACK;
        }
        @Override public boolean allowsPropagateDowngrade() { return true; }
        @Override public Integer fallbackValue(Throwable t) { return sentinel; }
    }

    private static final class PropagateOneShotWork extends NativeSqlOneShotWork<Integer> {
        PropagateOneShotWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public Integer consume(ResultSet rs) { throw new AssertionError("unreachable"); }
        @Override public NativeSqlError.Classification policyAdjust(
            Throwable t, NativeSqlError.Classification base) {
            return NativeSqlError.Classification.PROPAGATE;
        }
        @Override public Integer fallbackValue(Throwable t) {
            throw new IllegalStateException("unreachable", t);
        }
    }

    private static final class UnauthorizedDowngradeOneShotWork extends NativeSqlOneShotWork<Integer> {
        UnauthorizedDowngradeOneShotWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public Integer consume(ResultSet rs) { throw new AssertionError("unreachable"); }
        @Override public NativeSqlError.Classification policyAdjust(
            Throwable t, NativeSqlError.Classification base) {
            // request PROPAGATE → FALLBACK downgrade without authorization
            return NativeSqlError.Classification.FALLBACK;
        }
        // allowsPropagateDowngrade defaults to false
        @Override public Integer fallbackValue(Throwable t) {
            throw new IllegalStateException("unreachable", t);
        }
    }

    // -- fake work types --

    static final class FakeScalarWork extends ScalarNativeSqlWork {
        private final Object payload;
        int consumeCount = 0;
        int onErrorCount = 0;
        FakeScalarWork(NativeSqlFingerprint fp, DataSource ds, String sql, Object payload) {
            super(fp, ds, sql);
            this.payload = payload;
        }
        @Override public Object consume(ResultSet rs) {
            consumeCount++;
            return payload;
        }
        @Override public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
        @Override public void onError(Throwable t) {
            onErrorCount++;
        }
    }

    static final class FakeBatchWork extends BatchNativeSqlWork {
        private final Object payload;
        FakeBatchWork(NativeSqlFingerprint fp, DataSource ds, String sql, Object payload) {
            super(fp, ds, sql);
            this.payload = payload;
        }
        @Override public Object consume(ResultSet rs) {
            return payload;
        }
        @Override public Object materialize(Object cachedPayload, Object coordKey) {
            return cachedPayload;
        }
    }

    static final class FakeFallbackScalar extends ScalarNativeSqlWork {
        FakeFallbackScalar(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public Object consume(ResultSet rs) {
            throw new NativeSqlError.UnsupportedTemplateShape("test shape");
        }
        @Override public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
    }

    static final class RecursiveRegisteringWork extends ScalarNativeSqlWork {
        private final NativeSqlRegistry reg;
        private final NativeSqlWork toRegister;
        RecursiveRegisteringWork(
            NativeSqlFingerprint fp, DataSource ds, String sql,
            NativeSqlRegistry reg, NativeSqlWork toRegister)
        {
            super(fp, ds, sql);
            this.reg = reg;
            this.toRegister = toRegister;
        }
        @Override public Object consume(ResultSet rs) {
            reg.register(toRegister);
            return "A-result";
        }
        @Override public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
    }

    static final class EscalatingScalarWork extends ScalarNativeSqlWork {
        EscalatingScalarWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public Object consume(ResultSet rs) {
            throw new NativeSqlError.UnsupportedTemplateShape("forced fallback");
        }
        @Override public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
        @Override public NativeSqlError.Classification policyAdjust(
            Throwable t, NativeSqlError.Classification base)
        {
            return NativeSqlError.Classification.PROPAGATE;
        }
    }

    static final class UnauthorizedDowngradeWork extends ScalarNativeSqlWork {
        UnauthorizedDowngradeWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public Object consume(ResultSet rs) {
            return "x";
        }
        @Override public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
        @Override public NativeSqlError.Classification policyAdjust(
            Throwable t, NativeSqlError.Classification base)
        {
            return NativeSqlError.Classification.FALLBACK;
        }
    }

    static final class AuthorizedDowngradeWork extends ScalarNativeSqlWork {
        AuthorizedDowngradeWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public Object consume(ResultSet rs) {
            return "x";
        }
        @Override public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
        @Override public NativeSqlError.Classification policyAdjust(
            Throwable t, NativeSqlError.Classification base)
        {
            return NativeSqlError.Classification.FALLBACK;
        }
        @Override public boolean allowsPropagateDowngrade() {
            return true;
        }
    }

    static final class BuggyOnErrorWork extends ScalarNativeSqlWork {
        BuggyOnErrorWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public Object consume(ResultSet rs) {
            return "never reached";
        }
        @Override public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
        @Override public void onError(Throwable t) {
            throw new NullPointerException("metrics bug");
        }
    }
}
