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

/** Tests for {@link CellLookupResult} — tagged-result state machine. */
public class CellLookupResultTest extends TestCase {

    public void testMissState() {
        CellLookupResult r = CellLookupResult.MISS;
        assertTrue(r.isMiss());
        assertFalse(r.isSuccess());
        assertFalse(r.isErrorFallback());
        assertFalse(r.isErrorPropagate());
    }

    public void testMissPayloadAccessThrows() {
        try {
            CellLookupResult.MISS.successPayload();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* pass */ }
    }

    public void testMissErrorAccessThrows() {
        try {
            CellLookupResult.MISS.errorThrowable();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* pass */ }
    }

    public void testSuccessState() {
        CellLookupResult r = CellLookupResult.success("payload-A");
        assertFalse(r.isMiss());
        assertTrue(r.isSuccess());
        assertFalse(r.isErrorFallback());
        assertFalse(r.isErrorPropagate());
        assertEquals("payload-A", r.successPayload());
    }

    public void testSuccessErrorAccessThrows() {
        CellLookupResult r = CellLookupResult.success("x");
        try {
            r.errorThrowable();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* pass */ }
    }

    public void testSuccessWithNullPayloadAllowed() {
        // Consumers may legitimately cache a null scalar result.  Null
        // payload is not the same as MISS.
        CellLookupResult r = CellLookupResult.success(null);
        assertTrue(r.isSuccess());
        assertNull(r.successPayload());
    }

    public void testErrorFallbackState() {
        RuntimeException cause = new RuntimeException("optional missing");
        CellLookupResult r = CellLookupResult.errorFallback(cause);
        assertFalse(r.isMiss());
        assertFalse(r.isSuccess());
        assertTrue(r.isErrorFallback());
        assertFalse(r.isErrorPropagate());
        assertSame(cause, r.errorThrowable());
    }

    public void testErrorFallbackPayloadAccessThrows() {
        CellLookupResult r = CellLookupResult.errorFallback(new RuntimeException());
        try {
            r.successPayload();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* pass */ }
    }

    public void testErrorPropagateState() {
        RuntimeException cause = new RuntimeException("connection refused");
        CellLookupResult r = CellLookupResult.errorPropagate(cause);
        assertFalse(r.isMiss());
        assertFalse(r.isSuccess());
        assertFalse(r.isErrorFallback());
        assertTrue(r.isErrorPropagate());
        assertSame(cause, r.errorThrowable());
    }

    public void testMissSingletonIdentity() {
        // MISS is a static constant.  Repeat reads return the same instance.
        assertSame(CellLookupResult.MISS, CellLookupResult.MISS);
    }

    public void testErrorFallbackWithNullThrowableRejected() {
        try {
            CellLookupResult.errorFallback(null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* pass */ }
    }

    public void testErrorPropagateWithNullThrowableRejected() {
        try {
            CellLookupResult.errorPropagate(null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* pass */ }
    }
}
