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

import mondrian.olap.MondrianProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link NativeSqlRegistry} propagates the
 * {@code mondrian.rolap.queryTimeout} property to the JDBC layer
 * via {@link NativeSqlExecutor#run}.
 *
 * <p>Bug history: prior to this fix, {@code NativeSqlRegistry} hard-coded
 * {@code DEFAULT_TIMEOUT_SECONDS = 0}, ignoring the configured Mondrian
 * property.  That meant the native SQL execution path could run forever
 * while the legacy path respected the documented timeout — a config-honoring
 * regression detectable only in production.
 *
 * <p>Test seam: rather than introducing a public hook in production code,
 * we observe propagation by capturing the {@code setQueryTimeout(int)}
 * argument on a Mockito-mocked {@link Statement}.  This exercises the
 * full path through {@link NativeSqlExecutor#run} and asserts on the JDBC
 * call boundary.
 */
public class NativeSqlRegistryQueryTimeoutTest {

    private NativeSqlRegistry registry;
    private DataSource ds;
    private Connection conn;
    private Statement stmt;
    private int previousQueryTimeout;

    @BeforeEach public void setUp() throws Exception {
        // Process-wide caches must be empty so each test re-executes via
        // NativeSqlExecutor.run (cached results would skip the executor
        // entirely and defeat the timeout assertion).
        NativeSqlRegistry.clearGlobalCache();
        NativeSqlTelemetry.resetForTests();
        registry = new NativeSqlRegistry();
        ds = mock(DataSource.class);
        conn = mock(Connection.class);
        stmt = mock(Statement.class);
        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);

        previousQueryTimeout =
            MondrianProperties.instance().QueryTimeout.get();
    }

    @AfterEach public void tearDown() {
        // Restore prior value to avoid cross-test pollution; QueryTimeout
        // is a process-wide singleton.
        MondrianProperties.instance().QueryTimeout.set(previousQueryTimeout);
    }

    private NativeSqlFingerprint fp(String sql) {
        return NativeSqlFingerprint.of(sql, Collections.emptyList(), ds, "sess");
    }

    /**
     * drain() path: the configured QueryTimeout flows through to
     * {@link Statement#setQueryTimeout}.
     */
    @Test public void testDrainPropagatesQueryTimeoutFromMondrianProperty()
        throws Exception
    {
        MondrianProperties.instance().QueryTimeout.set(7);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(new TimeoutFakeScalarWork(
            fp("SELECT 1"), ds, "SELECT 1", "ok"));
        assertTrue(registry.drain());

        ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
        verify(stmt).setQueryTimeout(captor.capture());
        assertEquals(7, captor.getValue().intValue(),
            "drain() must pass MondrianProperties.QueryTimeout.get() "
            + "to Statement.setQueryTimeout");
    }

    /**
     * Late-set property values must be honored — i.e. the read happens at
     * execution time, not class-load time. Setting the property AFTER
     * NativeSqlRegistry has already been touched (and the constant would
     * have been initialized at class load if it were a constant) must still
     * propagate.
     */
    @Test public void testDrainHonorsLateSetQueryTimeoutValue()
        throws Exception
    {
        // First execution at one value …
        MondrianProperties.instance().QueryTimeout.set(0);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        registry.register(new TimeoutFakeScalarWork(
            fp("SELECT A"), ds, "SELECT A", "ok"));
        registry.drain();

        // … then a fresh registry sees the new value applied at execution time.
        MondrianProperties.instance().QueryTimeout.set(13);
        Statement stmt2 = mock(Statement.class);
        when(conn.createStatement()).thenReturn(stmt2);
        when(stmt2.executeQuery(anyString())).thenReturn(rs);

        NativeSqlRegistry registry2 = new NativeSqlRegistry();
        registry2.register(new TimeoutFakeScalarWork(
            fp("SELECT B"), ds, "SELECT B", "ok"));
        registry2.drain();

        verify(stmt2).setQueryTimeout(13);
    }

    /**
     * Negative property values must be clamped to zero so that
     * {@link NativeSqlExecutor#run}'s {@code timeoutSeconds >= 0}
     * precondition is not tripped by misconfiguration.
     */
    @Test public void testNegativeQueryTimeoutIsClampedToZero()
        throws Exception
    {
        MondrianProperties.instance().QueryTimeout.set(-1);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        registry.register(new TimeoutFakeScalarWork(
            fp("SELECT 1"), ds, "SELECT 1", "ok"));
        assertTrue(registry.drain(),
            "negative property must not blow up NativeSqlExecutor.run");

        verify(stmt).setQueryTimeout(0);
    }

    /**
     * executeOneShot() path: same propagation contract as drain().
     */
    @Test public void testExecuteOneShotPropagatesQueryTimeoutFromMondrianProperty()
        throws Exception
    {
        MondrianProperties.instance().QueryTimeout.set(11);
        ResultSet rs = mock(ResultSet.class);
        when(stmt.executeQuery(anyString())).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt(1)).thenReturn(42);

        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            "SELECT 42", Collections.emptyList(), ds, null);
        Integer result = NativeSqlRegistry.executeOneShot(
            new IntOneShotWork(fp, ds, "SELECT 42"));
        assertEquals(42, result.intValue());

        verify(stmt).setQueryTimeout(11);
    }

    // -- fixtures --

    static final class TimeoutFakeScalarWork extends ScalarNativeSqlWork {
        private final Object payload;
        TimeoutFakeScalarWork(
            NativeSqlFingerprint fp, DataSource ds, String sql, Object payload)
        {
            super(fp, ds, sql);
            this.payload = payload;
        }
        @Override public Object consume(ResultSet rs) {
            return payload;
        }
        @Override public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
    }

    static final class IntOneShotWork extends NativeSqlOneShotWork<Integer> {
        IntOneShotWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
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
}
