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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** Contract tests for {@link CellPhaseNativeRegistry}. */
public class CellPhaseNativeRegistryTest extends TestCase {

    private CellPhaseNativeRegistry registry;
    private DataSource ds;
    private Connection conn;
    private Statement stmt;

    @Override
    protected void setUp() throws Exception {
        // Clear process-wide state so tests do not pollute each other.
        // GLOBAL_SUCCESS + FINGERPRINT_KIND_INDEX are static (see
        // registry Javadoc — cache lifetime split for cross-statement
        // reuse).
        CellPhaseNativeRegistry.clearGlobalCache();
        registry = new CellPhaseNativeRegistry();
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

    public void testEmptyRegistryLookupIsMiss() {
        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue(r.isMiss());
    }

    public void testEmptyRegistryDrainReturnsFalse() {
        assertFalse(registry.drain());
    }

    public void testRegisterAddsToPending() {
        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "result");
        registry.register(work);
        assertEquals(1, registry.pendingSize());
    }

    public void testRegisterDuplicateIsNoOp() {
        FakeScalarWork work1 =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "result");
        FakeScalarWork work2 =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "result");
        registry.register(work1);
        registry.register(work2);
        assertEquals("same identity must dedup", 1, registry.pendingSize());
    }

    // ---------------------------------------------------------------------
    // Block B — Contract 5: fingerprint-kind uniqueness
    // ---------------------------------------------------------------------

    public void testContract5_registerScalarThenBatchSameFingerprintFails() {
        NativeSqlFingerprint fp = fp("SELECT 1");
        registry.register(new FakeScalarWork(fp, ds, "SELECT 1", "scalar"));

        try {
            registry.register(new FakeBatchWork(fp, ds, "SELECT 1", "batch"));
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("Contract 5"));
        }
    }

    public void testContract5_executeOrLookupWithDifferentKindFails() throws Exception {
        NativeSqlFingerprint fp = fp("SELECT 1");
        registry.register(new FakeScalarWork(fp, ds, "SELECT 1", "scalar"));

        try {
            registry.executeOrLookup(new FakeBatchWork(fp, ds, "SELECT 1", "batch"));
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("Contract 5"));
        }
    }

    public void testContract5_registerSameKindTwiceIsFine() {
        NativeSqlFingerprint fp = fp("SELECT 1");
        registry.register(new FakeScalarWork(fp, ds, "SELECT 1", "x"));
        registry.register(new FakeScalarWork(fp, ds, "SELECT 1", "x"));
        assertEquals(1, registry.pendingSize());
    }

    // ---------------------------------------------------------------------
    // Block C — drain happy path + cache population
    // ---------------------------------------------------------------------

    public void testDrainExecutesPendingAndCachesResult() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "cached-result");
        registry.register(work);

        assertTrue(registry.drain());
        assertEquals(0, registry.pendingSize());

        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue(r.isSuccess());
        assertEquals("cached-result", r.successPayload());
    }

    public void testDrainConsumesEachWorkUnitExactlyOnce() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeScalarWork work =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x");
        registry.register(work);
        registry.drain();

        assertEquals(1, work.consumeCount);
    }

    public void testSecondDrainIsNoOpOnEmptyPending() {
        assertFalse(registry.drain());
        assertFalse(registry.drain());
    }

    public void testLookupHitDoesNotRegister() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        registry.drain();

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "y"));
        assertEquals("should not re-register cached identity", 0, registry.pendingSize());
    }

    // ---------------------------------------------------------------------
    // Block D — drain error handling + classification
    // ---------------------------------------------------------------------

    public void testDrainFailureCachesPropagateError() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        assertTrue(registry.drain());

        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate());
        assertEquals("connection refused", r.errorThrowable().getMessage());
    }

    public void testDrainUnsupportedTemplateShapeCachesFallback() throws Exception {
        FakeFallbackScalar work = new FakeFallbackScalar(fp("SELECT 1"), ds, "SELECT 1");

        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(work);
        registry.drain();

        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue(r.isErrorFallback());
    }

    public void testDrainCachedErrorIsStickyAcrossRegisters() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("oops"));

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        registry.drain();

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "y"));
        assertEquals(0, registry.pendingSize());

        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate());
    }

    public void testDrainReturnsTrueEvenOnError() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("oops"));

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        assertTrue("error drain must return progress=true", registry.drain());
    }

    public void testDrainCallsOnErrorCallback() throws Exception {
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

    public void testRegistrationDuringDrainGoesToNextSweep() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        RecursiveRegisteringWork workA = new RecursiveRegisteringWork(
            fp("SELECT A"), ds, "SELECT A", registry,
            new FakeScalarWork(fp("SELECT B"), ds, "SELECT B", "B-result"));

        registry.register(workA);
        assertEquals(1, registry.pendingSize());

        assertTrue(registry.drain());
        assertEquals("workB must be pending after first drain",
            1, registry.pendingSize());

        assertTrue(registry.drain());
        assertEquals(0, registry.pendingSize());

        assertTrue(registry.lookup(fp("SELECT A"), CellWorkKind.SCALAR).isSuccess());
        assertTrue(registry.lookup(fp("SELECT B"), CellWorkKind.SCALAR).isSuccess());
    }

    public void testDrainTerminationWithRecursiveRegistration() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        long start = System.nanoTime();
        registry.drain();
        long elapsed = System.nanoTime() - start;
        assertTrue("drain must not loop forever", elapsed < 1_000_000_000L);
    }

    // ---------------------------------------------------------------------
    // Block F — executeOrLookup single-unit scope
    // ---------------------------------------------------------------------

    public void testExecuteOrLookupCacheHitReturnsCached() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "cached"));
        registry.drain();

        reset(stmt);
        when(conn.createStatement()).thenReturn(stmt);
        CellLookupResult r = registry.executeOrLookup(
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "fresh"));
        assertTrue(r.isSuccess());
        assertEquals("cached", r.successPayload());
        verify(stmt, never()).executeQuery(anyString());
    }

    public void testExecuteOrLookupCacheMissExecutesInline() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        CellLookupResult r = registry.executeOrLookup(
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "fresh"));

        assertTrue(r.isSuccess());
        assertEquals("fresh", r.successPayload());
    }

    public void testExecuteOrLookupDoesNotTouchOtherPending() throws Exception {
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

        assertEquals("A and B must remain pending", 2, registry.pendingSize());
        assertEquals("pendingA must not have been drained",
            0, pendingA.consumeCount);
        assertEquals("pendingB must not have been drained",
            0, pendingB.consumeCount);
    }

    public void testExecuteOrLookupPromotesAlreadyPendingUnit() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        FakeScalarWork fromA =
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "A-result");
        registry.register(fromA);
        assertEquals(1, registry.pendingSize());

        CellLookupResult r = registry.executeOrLookup(
            new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "B-result"));

        assertTrue(r.isSuccess());
        assertEquals(1, fromA.consumeCount);
        assertEquals(0, registry.pendingSize());
    }

    // ---------------------------------------------------------------------
    // Block G — policyAdjust directional constraint (Section 3)
    // ---------------------------------------------------------------------

    public void testPolicyAdjustDefault_noChange() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        registry.register(new FakeScalarWork(fp("SELECT 1"), ds, "SELECT 1", "x"));
        registry.drain();

        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue(r.isErrorPropagate());
    }

    public void testPolicyAdjustEscalation_fallbackToPropagate_allowed() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        EscalatingScalarWork work = new EscalatingScalarWork(fp("SELECT 1"), ds, "SELECT 1");
        registry.register(work);
        registry.drain();

        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue("escalation must be honored", r.isErrorPropagate());
    }

    public void testPolicyAdjustUnauthorizedDowngrade_rejected() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        UnauthorizedDowngradeWork work = new UnauthorizedDowngradeWork(
            fp("SELECT 1"), ds, "SELECT 1");
        registry.register(work);
        registry.drain();

        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue("unauthorized downgrade must be rejected", r.isErrorPropagate());
    }

    public void testPolicyAdjustAuthorizedDowngrade_allowed() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("connection refused"));

        AuthorizedDowngradeWork work = new AuthorizedDowngradeWork(
            fp("SELECT 1"), ds, "SELECT 1");
        registry.register(work);
        registry.drain();

        CellLookupResult r = registry.lookup(fp("SELECT 1"), CellWorkKind.SCALAR);
        assertTrue("authorized downgrade must be honored", r.isErrorFallback());
    }

    // ---------------------------------------------------------------------
    // Block H — onError callback bug isolation
    // ---------------------------------------------------------------------

    public void testOnErrorBugDoesNotDestabilizeDrainLoop() throws Exception {
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("oops"));

        registry.register(
            new BuggyOnErrorWork(fp("SELECT A"), ds, "SELECT A"));
        registry.register(
            new FakeScalarWork(fp("SELECT B"), ds, "SELECT B", "B"));

        assertTrue(registry.drain());

        CellLookupResult rA = registry.lookup(fp("SELECT A"), CellWorkKind.SCALAR);
        CellLookupResult rB = registry.lookup(fp("SELECT B"), CellWorkKind.SCALAR);

        assertTrue("work A must have terminal state", rA.isErrorPropagate());
        assertTrue("work B must ALSO have terminal state despite A's onError bug",
            rB.isErrorPropagate());
    }

    // -- fake work types --

    static final class FakeScalarWork extends ScalarCellWork {
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

    static final class FakeBatchWork extends BatchCellWork {
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

    static final class FakeFallbackScalar extends ScalarCellWork {
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

    static final class RecursiveRegisteringWork extends ScalarCellWork {
        private final CellPhaseNativeRegistry reg;
        private final CellNativeWork toRegister;
        RecursiveRegisteringWork(
            NativeSqlFingerprint fp, DataSource ds, String sql,
            CellPhaseNativeRegistry reg, CellNativeWork toRegister)
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

    static final class EscalatingScalarWork extends ScalarCellWork {
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

    static final class UnauthorizedDowngradeWork extends ScalarCellWork {
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

    static final class AuthorizedDowngradeWork extends ScalarCellWork {
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

    static final class BuggyOnErrorWork extends ScalarCellWork {
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
