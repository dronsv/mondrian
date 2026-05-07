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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import mondrian.rolap.nativesql.NativeSqlError;
import mondrian.rolap.nativesql.NativeSqlFingerprint;

/**
 * Unit tests for the work-object subclasses introduced by Phase 8a's
 * SqlMemberSource one-shot migration. Exercises consume(), policy hooks,
 * and fallback semantics in isolation from FoodMartTestCase.
 */
class SqlMemberSourceOneShotUnitTest {

    private static NativeSqlFingerprint fp(String sql, DataSource ds) {
        return NativeSqlFingerprint.of(sql, Collections.emptyList(), ds, null);
    }

    // -- MemberCountWork --

    @Test
    void memberCountWork_consume_mustCountFalse_readsScalarFromFirstRow() throws Exception {
        DataSource ds = mock(DataSource.class);
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(7);

        SqlMemberSource.MemberCountWork w = new SqlMemberSource.MemberCountWork(
            fp("SELECT count(*) FROM customer", ds), ds,
            "SELECT count(*) FROM customer", /*mustCount*/ false);
        Integer result = w.consume(rs);
        assertEquals(7, result.intValue());
    }

    @Test
    void memberCountWork_consume_mustCountTrue_countsDistinctOrderedRows() throws Exception {
        DataSource ds = mock(DataSource.class);
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(2);
        // Row sequence (ordered): (USA, CA), (USA, CA), (USA, OR), (CAN, BC)
        // Expected distinct count: 3 (rows 1, 3, 4 differ from predecessor;
        //   row 2 equals row 1 across all columns)
        when(rs.next()).thenReturn(true, true, true, true, false);
        when(rs.getString(1)).thenReturn("USA", "USA", "USA", "CAN");
        when(rs.getString(2)).thenReturn("CA",  "CA",  "OR",  "BC");

        SqlMemberSource.MemberCountWork w = new SqlMemberSource.MemberCountWork(
            fp("SELECT country, state FROM ... ORDER BY 1, 2", ds), ds,
            "SELECT country, state FROM ... ORDER BY 1, 2", /*mustCount*/ true);
        Integer result = w.consume(rs);
        assertEquals(3, result.intValue());
    }

    @Test
    void memberCountWork_policyAdjustForcesPropagate() {
        DataSource ds = mock(DataSource.class);
        SqlMemberSource.MemberCountWork w = new SqlMemberSource.MemberCountWork(
            fp("SELECT count(*) FROM x", ds), ds,
            "SELECT count(*) FROM x", false);
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            w.policyAdjust(new SQLException("x"), NativeSqlError.Classification.FALLBACK));
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            w.policyAdjust(new SQLException("x"), NativeSqlError.Classification.PROPAGATE));
    }

    @Test
    void memberCountWork_fallbackValue_throwsIllegalStateException() {
        DataSource ds = mock(DataSource.class);
        SqlMemberSource.MemberCountWork w = new SqlMemberSource.MemberCountWork(
            fp("SELECT count(*) FROM x", ds), ds,
            "SELECT count(*) FROM x", false);
        SQLException sqle = new SQLException("nope");
        IllegalStateException ise = assertThrows(IllegalStateException.class,
            () -> w.fallbackValue(sqle));
        assertEquals(sqle, ise.getCause());
    }
}
