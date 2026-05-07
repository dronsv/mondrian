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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

class NativeSqlOneShotWorkTest {

    private static NativeSqlFingerprint fp(String sql, DataSource ds) {
        return NativeSqlFingerprint.of(
            sql, Collections.emptyList(), ds, /*session*/ null);
    }

    private static final class NoopOneShotWork extends NativeSqlOneShotWork<String> {
        NoopOneShotWork(NativeSqlFingerprint fp, DataSource ds, String sql) {
            super(fp, ds, sql);
        }
        @Override public String consume(ResultSet rs) throws SQLException { return "ok"; }
        @Override public String fallbackValue(Throwable t) { return "fallback:" + t.getClass().getSimpleName(); }
    }

    @Test
    void defaultPolicyAdjustReturnsBase() {
        DataSource ds = mock(DataSource.class);
        NativeSqlOneShotWork<String> w = new NoopOneShotWork(fp("SELECT 1", ds), ds, "SELECT 1");
        assertSame(
            NativeSqlError.Classification.FALLBACK,
            w.policyAdjust(new RuntimeException(), NativeSqlError.Classification.FALLBACK));
        assertSame(
            NativeSqlError.Classification.PROPAGATE,
            w.policyAdjust(new RuntimeException(), NativeSqlError.Classification.PROPAGATE));
    }

    @Test
    void defaultAllowsPropagateDowngradeFalse() {
        DataSource ds = mock(DataSource.class);
        NativeSqlOneShotWork<String> w = new NoopOneShotWork(fp("SELECT 1", ds), ds, "SELECT 1");
        assertFalse(w.allowsPropagateDowngrade());
    }

    @Test
    void constructorRejectsNullFingerprint() {
        DataSource ds = mock(DataSource.class);
        assertThrows(NullPointerException.class,
            () -> new NoopOneShotWork(null, ds, "SELECT 1"));
    }

    @Test
    void constructorRejectsNullDataSource() {
        assertThrows(NullPointerException.class,
            () -> {
                NativeSqlFingerprint f = NativeSqlFingerprint.of(
                    "SELECT 1", Collections.emptyList(), mock(DataSource.class), null);
                new NoopOneShotWork(f, null, "SELECT 1");
            });
    }

    @Test
    void constructorRejectsNullSql() {
        DataSource ds = mock(DataSource.class);
        NativeSqlFingerprint f = fp("SELECT 1", ds);
        assertThrows(NullPointerException.class, () -> new NoopOneShotWork(f, ds, null));
    }

    @Test
    void accessorsRoundTrip() {
        DataSource ds = mock(DataSource.class);
        String sql = "SELECT 1 FROM dual";
        NativeSqlFingerprint f = fp(sql, ds);
        NoopOneShotWork w = new NoopOneShotWork(f, ds, sql);
        assertSame(f, w.fingerprint());
        assertSame(ds, w.dataSource());
        assertEquals(sql, w.sql());
    }
}
