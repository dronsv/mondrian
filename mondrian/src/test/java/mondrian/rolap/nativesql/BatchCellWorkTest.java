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
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

/** Tests for {@link BatchCellWork} — batch subclass of CellNativeWork. */
public class BatchCellWorkTest extends TestCase {

    private final DataSource ds = mock(DataSource.class);

    private NativeSqlFingerprint fp() {
        return NativeSqlFingerprint.of(
            "SELECT k, v FROM t", Collections.emptyList(), ds, "sess");
    }

    public void testKindIsBatch() {
        BatchCellWork work = new TestBatchWork(fp(), ds, "SELECT k, v FROM t");
        assertEquals(CellWorkKind.BATCH, work.kind());
    }

    public void testMaterializeReturnsPerCoordValue() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("coord-A", 100);
        payload.put("coord-B", 200);

        BatchCellWork work = new TestBatchWork(fp(), ds, "SELECT k, v FROM t");

        assertEquals(Integer.valueOf(100), work.materialize(payload, "coord-A"));
        assertEquals(Integer.valueOf(200), work.materialize(payload, "coord-B"));
    }

    public void testMaterializeMissingCoordReturnsNull() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("coord-A", 100);

        BatchCellWork work = new TestBatchWork(fp(), ds, "SELECT k, v FROM t");
        assertNull(work.materialize(payload, "coord-missing"));
    }

    public void testKindIdentityPreservedAcrossInstances() {
        BatchCellWork a = new TestBatchWork(fp(), ds, "SELECT 1");
        BatchCellWork b = new TestBatchWork(fp(), ds, "SELECT 2");
        assertEquals(CellWorkKind.BATCH, a.kind());
        assertEquals(CellWorkKind.BATCH, b.kind());
    }

    // -- test double --

    @SuppressWarnings("unchecked")
    private static final class TestBatchWork extends BatchCellWork {
        TestBatchWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }

        @Override
        public Object consume(ResultSet rs) throws SQLException {
            // Test harness — returns an empty map. Real implementations
            // walk the ResultSet and build the coordinate map.
            return new HashMap<String, Object>();
        }

        @Override
        public Object materialize(Object cachedPayload, Object coordKey) {
            return ((Map<Object, Object>) cachedPayload).get(coordKey);
        }
    }
}
