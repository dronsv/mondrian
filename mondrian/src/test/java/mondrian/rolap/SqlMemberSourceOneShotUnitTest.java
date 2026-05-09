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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
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

    // -- ApproxRowCountWork --

    @Test
    void approxRowCountWork_consume_returnsImmutableListOfLongsPerColumn() throws Exception {
        DataSource ds = mock(DataSource.class);
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.next()).thenReturn(true);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(3);
        when(rs.getLong(1)).thenReturn(100_000L);
        when(rs.getLong(2)).thenReturn(0L);
        when(rs.getLong(3)).thenReturn(50_000L);
        when(rs.wasNull()).thenReturn(false, false, false);

        SqlMemberSource.ApproxRowCountWork w = new SqlMemberSource.ApproxRowCountWork(
            fp("SELECT uniqHLL12(a), uniqHLL12(b), uniqHLL12(c) FROM t", ds), ds,
            "SELECT uniqHLL12(a), uniqHLL12(b), uniqHLL12(c) FROM t",
            "[Hierarchy]");
        java.util.List<Long> result = w.consume(rs);
        assertEquals(3, result.size());
        assertEquals(Long.valueOf(100_000L), result.get(0));
        assertEquals(Long.valueOf(0L),       result.get(1));
        assertEquals(Long.valueOf(50_000L),  result.get(2));
        assertThrows(UnsupportedOperationException.class, () -> result.add(1L));
    }

    @Test
    void approxRowCountWork_consume_filtersSqlNullsAndNegatives() throws Exception {
        DataSource ds = mock(DataSource.class);
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(rs.next()).thenReturn(true);
        when(rs.getMetaData()).thenReturn(md);
        when(md.getColumnCount()).thenReturn(3);
        // col 1: 200, col 2: SQL NULL, col 3: -1 (negative — filtered)
        when(rs.getLong(1)).thenReturn(200L);
        when(rs.getLong(2)).thenReturn(0L);
        when(rs.getLong(3)).thenReturn(-1L);
        when(rs.wasNull()).thenReturn(false, true, false);

        SqlMemberSource.ApproxRowCountWork w = new SqlMemberSource.ApproxRowCountWork(
            fp("SELECT a, b, c FROM t", ds), ds, "SELECT a, b, c FROM t",
            "[Hierarchy]");
        java.util.List<Long> result = w.consume(rs);
        assertEquals(3, result.size());
        assertEquals(Long.valueOf(200L), result.get(0));
        assertNull(result.get(1));   // SQL NULL → null slot
        assertNull(result.get(2));   // negative   → null slot
    }

    @Test
    void approxRowCountWork_consume_emptyResultSetReturnsEmptyImmutableList()
        throws Exception
    {
        DataSource ds = mock(DataSource.class);
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(false);

        SqlMemberSource.ApproxRowCountWork w = new SqlMemberSource.ApproxRowCountWork(
            fp("SELECT 1 WHERE FALSE", ds), ds, "SELECT 1 WHERE FALSE",
            "[Hierarchy]");
        java.util.List<Long> result = w.consume(rs);
        assertEquals(0, result.size());
        assertThrows(UnsupportedOperationException.class, () -> result.add(1L));
    }

    @Test
    void approxRowCountWork_policyAdjustForcesFallback() {
        DataSource ds = mock(DataSource.class);
        SqlMemberSource.ApproxRowCountWork w = new SqlMemberSource.ApproxRowCountWork(
            fp("SELECT 1", ds), ds, "SELECT 1", "[Hierarchy]");
        assertEquals(
            NativeSqlError.Classification.FALLBACK,
            w.policyAdjust(new SQLException(), NativeSqlError.Classification.FALLBACK));
        assertEquals(
            NativeSqlError.Classification.FALLBACK,
            w.policyAdjust(new RuntimeException(), NativeSqlError.Classification.PROPAGATE));
    }

    @Test
    void approxRowCountWork_authorizesPropagateDowngrade() {
        DataSource ds = mock(DataSource.class);
        SqlMemberSource.ApproxRowCountWork w = new SqlMemberSource.ApproxRowCountWork(
            fp("SELECT 1", ds), ds, "SELECT 1", "[Hierarchy]");
        assertTrue(w.allowsPropagateDowngrade());
    }

    @Test
    void approxRowCountWork_fallbackValue_returnsEmptyImmutableList() {
        DataSource ds = mock(DataSource.class);
        SqlMemberSource.ApproxRowCountWork w = new SqlMemberSource.ApproxRowCountWork(
            fp("SELECT 1", ds), ds, "SELECT 1", "[Hierarchy]");
        java.util.List<Long> result = w.fallbackValue(new SQLException("boom"));
        assertEquals(0, result.size());
        assertThrows(UnsupportedOperationException.class, () -> result.add(1L));
    }

    // -- applyApproxRowCounts (extracted side-effect loop) --

    @Test
    void applyApproxRowCounts_emptyValuesListAppliesNothing() {
        RolapLevel l1 = mock(RolapLevel.class);
        RolapLevel l2 = mock(RolapLevel.class);

        int populated = SqlMemberSource.applyApproxRowCounts(
            Arrays.asList(l1, l2),
            Collections.<Long>emptyList());

        assertEquals(0, populated);
        verify(l1, never()).setApproxRowCount(anyInt());
        verify(l2, never()).setApproxRowCount(anyInt());
    }

    @Test
    void applyApproxRowCounts_nullSlotsSkipped() {
        RolapLevel l1 = mock(RolapLevel.class);
        RolapLevel l2 = mock(RolapLevel.class);

        int populated = SqlMemberSource.applyApproxRowCounts(
            Arrays.asList(l1, l2),
            Arrays.asList(100L, null));

        assertEquals(1, populated);
        verify(l1).setApproxRowCount(100);
        verify(l2, never()).setApproxRowCount(anyInt());
    }

    @Test
    void applyApproxRowCounts_clampsLongValuesToIntegerMaxValue() {
        RolapLevel l = mock(RolapLevel.class);
        long large = (long) Integer.MAX_VALUE + 1L;

        int populated = SqlMemberSource.applyApproxRowCounts(
            Collections.singletonList(l),
            Collections.singletonList(large));

        assertEquals(1, populated);
        verify(l).setApproxRowCount(Integer.MAX_VALUE);
    }

    @Test
    void applyApproxRowCounts_levelsLongerThanValuesIteratesOnlyMin() {
        RolapLevel l1 = mock(RolapLevel.class);
        RolapLevel l2 = mock(RolapLevel.class);
        RolapLevel l3 = mock(RolapLevel.class);

        int populated = SqlMemberSource.applyApproxRowCounts(
            Arrays.asList(l1, l2, l3),
            Arrays.asList(10L, 20L));

        assertEquals(2, populated);
        verify(l1).setApproxRowCount(10);
        verify(l2).setApproxRowCount(20);
        verify(l3, never()).setApproxRowCount(anyInt());
    }

    @Test
    void applyApproxRowCounts_valuesLongerThanLevelsIteratesOnlyMin() {
        RolapLevel l1 = mock(RolapLevel.class);

        int populated = SqlMemberSource.applyApproxRowCounts(
            Collections.singletonList(l1),
            Arrays.asList(10L, 20L, 30L));

        assertEquals(1, populated);
        verify(l1).setApproxRowCount(10);
    }
}
