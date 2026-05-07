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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for {@link NativeSqlLookupResult} — tagged-result state machine. */
public class NativeSqlLookupResultTest {

    @Test public void testMissState() {
        NativeSqlLookupResult r = NativeSqlLookupResult.MISS;
        assertTrue(r.isMiss());
        assertFalse(r.isSuccess());
        assertFalse(r.isErrorFallback());
        assertFalse(r.isErrorPropagate());
    }

    @Test public void testMissPayloadAccessThrows() {
        try {
            NativeSqlLookupResult.MISS.successPayload();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* pass */ }
    }

    @Test public void testMissErrorAccessThrows() {
        try {
            NativeSqlLookupResult.MISS.errorThrowable();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* pass */ }
    }

    @Test public void testSuccessState() {
        NativeSqlLookupResult r = NativeSqlLookupResult.success("payload-A");
        assertFalse(r.isMiss());
        assertTrue(r.isSuccess());
        assertFalse(r.isErrorFallback());
        assertFalse(r.isErrorPropagate());
        assertEquals("payload-A", r.successPayload());
    }

    @Test public void testSuccessErrorAccessThrows() {
        NativeSqlLookupResult r = NativeSqlLookupResult.success("x");
        try {
            r.errorThrowable();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* pass */ }
    }

    @Test public void testSuccessWithNullPayloadAllowed() {
        // Consumers may legitimately cache a null scalar result.  Null
        // payload is not the same as MISS.
        NativeSqlLookupResult r = NativeSqlLookupResult.success(null);
        assertTrue(r.isSuccess());
        assertNull(r.successPayload());
    }

    @Test public void testErrorFallbackState() {
        RuntimeException cause = new RuntimeException("optional missing");
        NativeSqlLookupResult r = NativeSqlLookupResult.errorFallback(cause);
        assertFalse(r.isMiss());
        assertFalse(r.isSuccess());
        assertTrue(r.isErrorFallback());
        assertFalse(r.isErrorPropagate());
        assertSame(cause, r.errorThrowable());
    }

    @Test public void testErrorFallbackPayloadAccessThrows() {
        NativeSqlLookupResult r = NativeSqlLookupResult.errorFallback(new RuntimeException());
        try {
            r.successPayload();
            fail("expected IllegalStateException");
        } catch (IllegalStateException expected) { /* pass */ }
    }

    @Test public void testErrorPropagateState() {
        RuntimeException cause = new RuntimeException("connection refused");
        NativeSqlLookupResult r = NativeSqlLookupResult.errorPropagate(cause);
        assertFalse(r.isMiss());
        assertFalse(r.isSuccess());
        assertFalse(r.isErrorFallback());
        assertTrue(r.isErrorPropagate());
        assertSame(cause, r.errorThrowable());
    }

    @Test public void testMissSingletonIdentity() {
        // MISS is a static constant.  Repeat reads return the same instance.
        assertSame(NativeSqlLookupResult.MISS, NativeSqlLookupResult.MISS);
    }

    @Test public void testErrorFallbackWithNullThrowableRejected() {
        try {
            NativeSqlLookupResult.errorFallback(null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* pass */ }
    }

    @Test public void testErrorPropagateWithNullThrowableRejected() {
        try {
            NativeSqlLookupResult.errorPropagate(null);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) { /* pass */ }
    }
}
