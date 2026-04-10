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

/** Tests for {@link StatementLocalCache} — generic typed cache primitive. */
public class StatementLocalCacheTest extends TestCase {

    public void testInitiallyEmpty() {
        StatementLocalCache<String, Integer> cache = new StatementLocalCache<>();
        assertEquals(0, cache.size());
        assertNull(cache.get("anything"));
        assertFalse(cache.contains("anything"));
    }

    public void testPutAndGetRoundTrip() {
        StatementLocalCache<String, Integer> cache = new StatementLocalCache<>();
        cache.put("a", 1);
        assertEquals(Integer.valueOf(1), cache.get("a"));
        assertTrue(cache.contains("a"));
        assertEquals(1, cache.size());
    }

    public void testMultipleEntries() {
        StatementLocalCache<String, String> cache = new StatementLocalCache<>();
        cache.put("one", "alpha");
        cache.put("two", "beta");
        cache.put("three", "gamma");
        assertEquals(3, cache.size());
        assertEquals("alpha", cache.get("one"));
        assertEquals("beta", cache.get("two"));
        assertEquals("gamma", cache.get("three"));
    }

    public void testPutReplacesExistingValue() {
        StatementLocalCache<String, Integer> cache = new StatementLocalCache<>();
        cache.put("a", 1);
        cache.put("a", 2);
        assertEquals(Integer.valueOf(2), cache.get("a"));
        assertEquals(1, cache.size());
    }

    public void testClear() {
        StatementLocalCache<String, Integer> cache = new StatementLocalCache<>();
        cache.put("a", 1);
        cache.put("b", 2);
        cache.clear();
        assertEquals(0, cache.size());
        assertNull(cache.get("a"));
    }

    public void testTwoInstancesAreIndependent() {
        // Contract 7: each cache instance owns its own state.  Creating two
        // caches and putting the same key into one must NOT affect the other.
        StatementLocalCache<String, Integer> cache1 = new StatementLocalCache<>();
        StatementLocalCache<String, Integer> cache2 = new StatementLocalCache<>();
        cache1.put("shared-key", 100);
        assertEquals(Integer.valueOf(100), cache1.get("shared-key"));
        assertNull("cache2 must not see cache1's entries", cache2.get("shared-key"));
        assertFalse(cache2.contains("shared-key"));
    }

    public void testNullKeyRejected() {
        StatementLocalCache<String, Integer> cache = new StatementLocalCache<>();
        try {
            cache.put(null, 1);
            fail("expected NullPointerException");
        } catch (NullPointerException expected) {
            // pass
        }
    }

    public void testNullValueAllowedForAbsenceEncoding() {
        // Null value is allowed — caller decides if they want to encode "known
        // absent" via null or via a sentinel.  We intentionally don't enforce.
        StatementLocalCache<String, Integer> cache = new StatementLocalCache<>();
        cache.put("key", null);
        assertTrue(cache.contains("key"));
        assertNull(cache.get("key"));
        assertEquals(1, cache.size());
    }
}
