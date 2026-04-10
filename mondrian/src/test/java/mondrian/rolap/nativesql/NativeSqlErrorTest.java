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
import java.sql.SQLException;

/** Tests for {@link NativeSqlError} — narrow FALLBACK taxonomy. */
public class NativeSqlErrorTest extends TestCase {

    public void testUnsupportedTemplateShapeIsFallback() {
        Throwable t = new NativeSqlError.UnsupportedTemplateShape("axis combo");
        assertEquals(
            NativeSqlError.Classification.FALLBACK,
            NativeSqlError.classify(t));
    }

    public void testMissingOptionalArtifactIsFallback() {
        Throwable t = new NativeSqlError.MissingOptionalArtifact("agg_sku_month");
        assertEquals(
            NativeSqlError.Classification.FALLBACK,
            NativeSqlError.classify(t));
    }

    public void testMissingOptionalArtifactPreservesArtifactId() {
        NativeSqlError.MissingOptionalArtifact t =
            new NativeSqlError.MissingOptionalArtifact("agg_sku_month");
        assertEquals("agg_sku_month", t.artifactId());
    }

    public void testGenericSqlExceptionIsPropagate() {
        Throwable t = new SQLException("connection refused");
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.classify(t));
    }

    public void testSqlExceptionWithState42S02IsPropagateByDefault() {
        // Broad object-missing SQL state WITHOUT explicit opt-in → PROPAGATE.
        // Opt-in is via MissingOptionalArtifact typed sentinel only.
        Throwable t = new SQLException("no such table", "42S02");
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.classify(t));
    }

    public void testInterruptedExceptionIsPropagate() {
        Throwable t = new InterruptedException("cancelled");
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.classify(t));
    }

    public void testRuntimeExceptionIsPropagate() {
        Throwable t = new RuntimeException("consumer bug");
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.classify(t));
    }

    public void testNullPointerExceptionIsPropagate() {
        Throwable t = new NullPointerException();
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.classify(t));
    }

    public void testErrorSubclassIsPropagate() {
        Throwable t = new StackOverflowError();
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.classify(t));
    }

    public void testNullThrowableIsPropagate() {
        // Defensive: the classifier is pure and total, including on null.
        assertEquals(
            NativeSqlError.Classification.PROPAGATE,
            NativeSqlError.classify(null));
    }

    public void testUnsupportedTemplateShapeWithCause() {
        Throwable cause = new SQLException("underlying");
        NativeSqlError.UnsupportedTemplateShape t =
            new NativeSqlError.UnsupportedTemplateShape("reason", cause);
        assertSame(cause, t.getCause());
        assertEquals(
            NativeSqlError.Classification.FALLBACK,
            NativeSqlError.classify(t));
    }
}
