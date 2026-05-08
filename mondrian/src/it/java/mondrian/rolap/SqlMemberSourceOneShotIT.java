/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Result;
import mondrian.rolap.nativesql.NativeSqlRegistry;
import mondrian.rolap.nativesql.NativeSqlTelemetry;
import mondrian.test.FoodMartTestCase;

/**
 * Integration tests for Phase 8a SqlMemberSource one-shot migration.
 *
 * <p>Runs against the embedded-H2 FoodMart fixture (see
 * scripts/test-it-h2.sh and the it-h2-foodmart Maven profile).
 */
public class SqlMemberSourceOneShotIT extends FoodMartTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeSqlRegistry.clearGlobalCache();
        NativeSqlTelemetry.resetForTests();
    }

    /**
     * Verifies the cache-reuse contract for getMemberCount across two
     * statements: the second invocation must see a cached payload and
     * fire cachedSuccessHit instead of re-executing SQL.
     */
    public void testGetMemberCount_cachesAcrossStatements() {
        // First MDX query — triggers level-member-count SQL via
        // SqlMemberSource.getMemberCount when Mondrian needs cardinality.
        Result r1 = executeQuery(
            "SELECT [Measures].[Unit Sales] ON 0,"
                + " [Store].[Store City].Members ON 1"
                + " FROM [Sales]");
        assertNotNull(r1);

        java.util.SortedMap<String, Integer> snap1 = NativeSqlTelemetry.snapshot();

        // Second identical query — should reuse cached count.
        Result r2 = executeQuery(
            "SELECT [Measures].[Unit Sales] ON 0,"
                + " [Store].[Store City].Members ON 1"
                + " FROM [Sales]");
        assertNotNull(r2);

        // executionCount must not have grown for any fingerprint that the
        // first query established.
        java.util.SortedMap<String, Integer> snap2 = NativeSqlTelemetry.snapshot();
        for (java.util.Map.Entry<String, Integer> e : snap1.entrySet()) {
            assertEquals(
                "fingerprint " + e.getKey()
                    + " executed again on second identical query",
                e.getValue(),
                snap2.getOrDefault(e.getKey(), 0));
        }

        // The strict contract is "no fresh fingerprint executions on
        // identical workload", which the loop above already enforces.
        // Cached-hit count is intentionally not asserted: Mondrian's
        // higher-level member cache may serve the second query without
        // consulting the registry at all (zero hits), or it may consult
        // and hit (positive hits). Both outcomes satisfy the spec §9.3
        // soft contract, so a precise hit-count assertion would either
        // be flaky or a tautology.
    }

    /**
     * Caller-level propagation test for Phase 8a §9.3 follow-up.
     *
     * <p>Inject a failing {@link DataSource} and invoke the private
     * {@code SqlMemberSource.getMemberCount(level, dataSource)}
     * directly. Asserts that a Mondrian runtime exception bubbles to
     * the caller with the original {@link SQLException} preserved as
     * the throwable chain root cause — i.e., the substrate's PROPAGATE
     * wrapping (analogous to the legacy {@code SqlStatement.handle(e)})
     * is wired correctly end-to-end through {@code MemberCountWork}.
     *
     * <p>Uses a real {@code RolapHierarchy} loaded from the H2 FoodMart
     * fixture. The failing DataSource has a distinct identityHash from
     * the FoodMart DataSource, so its fingerprint slot in
     * {@code GLOBAL_SUCCESS} is fresh and cannot collide with any
     * cached success from earlier in this JVM.
     */
    public void testGetMemberCount_propagatesSqlExceptionAsMondrianError()
        throws Exception
    {
        // Get a real RolapLevel from the loaded FoodMart [Sales] cube,
        // [Store] hierarchy, [Store City] level (depth 3 — All Stores,
        // Country, State, City).
        mondrian.olap.Connection connection = getConnection();
        Cube cube = cubeByName(connection, "Sales");
        assertNotNull("Sales cube must be loaded", cube);
        Hierarchy storeHierarchy = null;
        for (Dimension d : cube.getDimensions()) {
            for (Hierarchy h : d.getHierarchies()) {
                if ("[Store]".equals(h.getUniqueName())) {
                    storeHierarchy = h;
                    break;
                }
            }
            if (storeHierarchy != null) {
                break;
            }
        }
        assertNotNull("[Store] hierarchy must be present", storeHierarchy);
        Level cityLevel = null;
        for (Level l : storeHierarchy.getLevels()) {
            if ("Store City".equals(l.getName())) {
                cityLevel = l;
                break;
            }
        }
        assertNotNull("[Store].[Store City] level must be present", cityLevel);
        RolapHierarchy rolapHierarchy = (RolapHierarchy) storeHierarchy;
        RolapLevel rolapLevel = (RolapLevel) cityLevel;

        // Build a SqlMemberSource bound to the real hierarchy.
        SqlMemberSource sms = new SqlMemberSource(rolapHierarchy);

        // Build a failing DataSource that throws SQLException at
        // executeQuery time. Its identityHash differs from the FoodMart
        // DataSource, so it gets its own (fingerprint, ONESHOT) slot in
        // GLOBAL_SUCCESS and cannot cache-hit on prior real results.
        SQLException synthetic = new SQLException("synthetic test failure");
        DataSource failingDs = failingDataSource(synthetic);

        // Reset the substrate state so this test's behavior is
        // independent of any cache state populated by earlier tests in
        // the same JVM.
        NativeSqlRegistry.clearGlobalCache();
        NativeSqlTelemetry.resetForTests();

        // Invoke the private getMemberCount(level, dataSource) directly.
        Method getMemberCount = SqlMemberSource.class.getDeclaredMethod(
            "getMemberCount", RolapLevel.class, DataSource.class);
        getMemberCount.setAccessible(true);

        try {
            getMemberCount.invoke(sms, rolapLevel, failingDs);
            fail("expected a Mondrian runtime exception to propagate");
        } catch (InvocationTargetException ite) {
            Throwable wrapped = ite.getCause();
            assertNotNull(
                "private method threw something but the cause was null",
                wrapped);
            assertTrue(
                "expected a runtime exception, got " + wrapped.getClass(),
                wrapped instanceof RuntimeException);

            // The synthetic SQLException must be reachable somewhere in
            // the cause chain. Mondrian's wrapping helpers may add one
            // or more wrapper frames between the runtime exception and
            // the raw SQLException, so walk the chain.
            boolean foundOriginal = false;
            for (Throwable t = wrapped; t != null; t = t.getCause()) {
                if (t == synthetic) {
                    foundOriginal = true;
                    break;
                }
            }
            assertTrue(
                "synthetic SQLException not found in cause chain of "
                    + wrapped,
                foundOriginal);
        }
    }

    /**
     * Anonymous {@link DataSource} whose {@link DataSource#getConnection()}
     * throws the supplied {@link SQLException}. The substrate's
     * {@link mondrian.rolap.nativesql.NativeSqlExecutor#run} calls
     * {@code getConnection()} before any other JDBC operation, so the
     * throw lands inside the substrate's classifier exactly as a real
     * connection-acquisition failure would.
     *
     * <p>Implemented as a plain anonymous class rather than via Mockito
     * because the IT classloader does not enable
     * {@code net.bytebuddy.experimental} and ByteBuddy 1.x does not
     * formally support Java 25 yet.
     */
    private static DataSource failingDataSource(SQLException toThrow) {
        return new DataSource() {
            @Override
            public java.sql.Connection getConnection() throws SQLException {
                throw toThrow;
            }
            @Override
            public java.sql.Connection getConnection(
                String username, String password) throws SQLException
            {
                throw toThrow;
            }
            @Override public PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(PrintWriter out) { }
            @Override public void setLoginTimeout(int seconds) { }
            @Override public int getLoginTimeout() { return 0; }
            @Override public Logger getParentLogger() { return null; }
            @Override public <T> T unwrap(Class<T> iface) { return null; }
            @Override public boolean isWrapperFor(Class<?> iface) {
                return false;
            }
        };
    }
}
