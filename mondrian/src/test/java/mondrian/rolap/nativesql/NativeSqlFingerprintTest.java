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
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/** Tests for {@link NativeSqlFingerprint} — Contract 1 identity participants. */
public class NativeSqlFingerprintTest extends TestCase {

    private final DataSource ds1 = mock(DataSource.class);
    private final DataSource ds2 = mock(DataSource.class);

    public void testEqualInputsProduceEqualFingerprints() {
        NativeSqlFingerprint a = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, "session-A");
        NativeSqlFingerprint b = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, "session-A");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testWhitespaceCanonicalization() {
        NativeSqlFingerprint a = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, "sess");
        NativeSqlFingerprint b = NativeSqlFingerprint.of(
            "SELECT  1\n  FROM   t", Collections.emptyList(), ds1, "sess");
        assertEquals("whitespace variants must canonicalize", a, b);
    }

    public void testDifferentSqlProducesDifferentFingerprints() {
        NativeSqlFingerprint a = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, "sess");
        NativeSqlFingerprint b = NativeSqlFingerprint.of(
            "SELECT 2 FROM t", Collections.emptyList(), ds1, "sess");
        assertFalse(a.equals(b));
    }

    public void testDifferentParamsProduceDifferentFingerprints() {
        NativeSqlFingerprint a = NativeSqlFingerprint.of(
            "SELECT ? FROM t", Arrays.asList((Object) 1), ds1, "sess");
        NativeSqlFingerprint b = NativeSqlFingerprint.of(
            "SELECT ? FROM t", Arrays.asList((Object) 2), ds1, "sess");
        assertFalse(a.equals(b));
    }

    public void testDifferentDataSourceProducesDifferentFingerprints() {
        NativeSqlFingerprint a = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, "sess");
        NativeSqlFingerprint b = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds2, "sess");
        assertFalse(a.equals(b));
    }

    public void testDifferentSessionProducesDifferentFingerprints() {
        NativeSqlFingerprint a = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, "sess-A");
        NativeSqlFingerprint b = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, "sess-B");
        assertFalse(a.equals(b));
    }

    public void testNullSessionAllowed() {
        NativeSqlFingerprint a = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, null);
        NativeSqlFingerprint b = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Collections.emptyList(), ds1, null);
        assertEquals("null session must equal null session", a, b);
        assertEquals("hashCode must be stable with null session",
            a.hashCode(), b.hashCode());
        assertEquals("describe must render null session as sentinel",
            "<null>", a.describe().get("session"));
    }

    public void testDescribeListsAllParticipants() {
        NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            "SELECT 1 FROM t", Arrays.asList((Object) 42), ds1, "sess");
        Map<String, String> described = fp.describe();
        assertTrue(described.containsKey("sql"));
        assertTrue(described.containsKey("params"));
        assertTrue(described.containsKey("dataSource"));
        assertTrue(described.containsKey("session"));
    }

    public void testNullSqlRejected() {
        try {
            NativeSqlFingerprint.of(null, Collections.emptyList(), ds1, "sess");
            fail("expected NullPointerException");
        } catch (NullPointerException expected) {
            // pass
        }
    }

    public void testNullParamsRejected() {
        try {
            NativeSqlFingerprint.of("SELECT 1", null, ds1, "sess");
            fail("expected NullPointerException");
        } catch (NullPointerException expected) {
            // pass
        }
    }

    public void testNullDataSourceRejected() {
        try {
            NativeSqlFingerprint.of("SELECT 1", Collections.emptyList(), null, "sess");
            fail("expected NullPointerException");
        } catch (NullPointerException expected) {
            // pass
        }
    }
}
