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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import static org.mockito.Mockito.*;

/** Tests for {@link ScalarCellWork} — scalar subclass of CellNativeWork. */
public class ScalarCellWorkTest extends TestCase {

    private final DataSource ds = mock(DataSource.class);

    private NativeSqlFingerprint fp() {
        return NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds, "sess");
    }

    public void testKindIsScalar() {
        ScalarCellWork work = new TestScalarWork(fp(), ds, "SELECT 1 FROM t");
        assertEquals(CellWorkKind.SCALAR, work.kind());
    }

    public void testConsumeReturnsCachedPayload() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true);
        when(rs.getInt(1)).thenReturn(42);

        ScalarCellWork work = new TestScalarWork(fp(), ds, "SELECT 1 FROM t");
        Object payload = work.consume(rs);
        assertEquals(Integer.valueOf(42), payload);
    }

    public void testMaterializeReturnsPayloadDirectly() {
        ScalarCellWork work = new TestScalarWork(fp(), ds, "SELECT 1 FROM t");
        assertEquals(Integer.valueOf(99), work.materialize(99));
    }

    public void testMaterializeWithNullPayload() {
        ScalarCellWork work = new TestScalarWork(fp(), ds, "SELECT 1 FROM t");
        assertNull(work.materialize(null));
    }

    public void testDefaultPolicyAdjustReturnsBase() {
        ScalarCellWork work = new TestScalarWork(fp(), ds, "SELECT 1 FROM t");
        assertEquals(
            NativeSqlError.Classification.FALLBACK,
            work.policyAdjust(
                new RuntimeException(),
                NativeSqlError.Classification.FALLBACK));
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            work.policyAdjust(
                new RuntimeException(),
                NativeSqlError.Classification.PROPAGATE));
    }

    public void testDefaultAllowsPropagateDowngradeIsFalse() {
        ScalarCellWork work = new TestScalarWork(fp(), ds, "SELECT 1 FROM t");
        assertFalse(work.allowsPropagateDowngrade());
    }

    public void testDefaultOnErrorIsNoOp() {
        ScalarCellWork work = new TestScalarWork(fp(), ds, "SELECT 1 FROM t");
        // Must not throw.
        work.onError(new RuntimeException());
    }

    // -- test double --

    private static final class TestScalarWork extends ScalarCellWork {
        TestScalarWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }

        @Override
        public Object consume(ResultSet rs) throws SQLException {
            rs.next();
            return rs.getInt(1);
        }

        @Override
        public Object materialize(Object cachedPayload) {
            return cachedPayload;
        }
    }
}
