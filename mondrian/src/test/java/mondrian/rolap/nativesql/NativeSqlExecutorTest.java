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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** Tests for {@link NativeSqlExecutor} — JDBC envelope + timeout/cancel. */
public class NativeSqlExecutorTest extends TestCase {

    public void testHappyPathReturnsHandlerResult() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        Integer result = NativeSqlExecutor.run(
            "SELECT 1 FROM t",
            ds,
            30,
            resultSet -> 42);

        assertEquals(Integer.valueOf(42), result);

        verify(stmt).setQueryTimeout(30);
        verify(stmt).executeQuery("SELECT 1 FROM t");
        verify(rs).close();
        verify(stmt).close();
        verify(conn).close();
    }

    public void testHandlerSqlExceptionPropagates() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        try {
            NativeSqlExecutor.run(
                "SELECT 1",
                ds,
                10,
                resultSet -> { throw new SQLException("handler fail"); });
            fail("expected SQLException");
        } catch (SQLException expected) {
            assertEquals("handler fail", expected.getMessage());
        }

        // Even on handler failure, resources must close.
        verify(rs).close();
        verify(stmt).close();
        verify(conn).close();
    }

    public void testExecuteQuerySqlExceptionPropagates() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString()))
            .thenThrow(new SQLException("syntax error"));

        try {
            NativeSqlExecutor.run("BROKEN SQL", ds, 10, resultSet -> 1);
            fail("expected SQLException");
        } catch (SQLException expected) {
            assertEquals("syntax error", expected.getMessage());
        }

        verify(stmt).close();
        verify(conn).close();
    }

    public void testConnectionAcquisitionFailure() throws Exception {
        DataSource ds = mock(DataSource.class);
        when(ds.getConnection())
            .thenThrow(new SQLException("pool exhausted"));

        try {
            NativeSqlExecutor.run("SELECT 1", ds, 10, rs -> 1);
            fail("expected SQLException");
        } catch (SQLException expected) {
            assertEquals("pool exhausted", expected.getMessage());
        }
    }

    public void testZeroTimeoutIsPassedThrough() throws Exception {
        // Zero means "no timeout" in JDBC.  Executor should pass it through
        // rather than substitute a default.
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        Statement stmt = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(anyString())).thenReturn(rs);

        NativeSqlExecutor.run("SELECT 1", ds, 0, r -> 1);

        verify(stmt).setQueryTimeout(0);
    }

    public void testNegativeTimeoutRejected() {
        DataSource ds = mock(DataSource.class);
        try {
            NativeSqlExecutor.run("SELECT 1", ds, -5, r -> 1);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // pass
        } catch (SQLException e) {
            fail("should reject before touching JDBC");
        }
    }

    public void testNullSqlRejected() {
        DataSource ds = mock(DataSource.class);
        try {
            NativeSqlExecutor.run(null, ds, 10, r -> 1);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) {
            // pass
        } catch (SQLException e) {
            fail("should reject before touching JDBC");
        }
    }

    public void testNullDataSourceRejected() {
        try {
            NativeSqlExecutor.run("SELECT 1", null, 10, r -> 1);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) {
            // pass
        } catch (SQLException e) {
            fail("should reject before touching JDBC");
        }
    }

    public void testNullHandlerRejected() {
        DataSource ds = mock(DataSource.class);
        try {
            NativeSqlExecutor.run("SELECT 1", ds, 10, null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) {
            // pass
        } catch (SQLException e) {
            fail("should reject before touching JDBC");
        }
    }
}
