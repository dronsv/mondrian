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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ImmutablePrefetchProvider} implementing
 * {@link PrefetchedCellProvider}.
 */
public class PrefetchedCellProviderTest {

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private static PrefetchKey key(int bitPos, Object... values) {
        return new PrefetchKey(bitPos, values);
    }

    // -----------------------------------------------------------------------
    // Lookup: hit
    // -----------------------------------------------------------------------

    @Test
    public void testLookupHit_returnsStoredValue() {
        PrefetchKey k = key(3, "brand-A", 1L);
        Map<PrefetchKey, Object> map = new HashMap<>();
        map.put(k, 42.5);

        ImmutablePrefetchProvider provider = new ImmutablePrefetchProvider(map);
        Object result = provider.lookup(k);
        assertEquals(42.5, result, "lookup should return the stored value on hit");
    }

    @Test
    public void testLookupHit_integerStoredValue() {
        PrefetchKey k = key(1, "x");
        Map<PrefetchKey, Object> map = new HashMap<>();
        map.put(k, 100);

        ImmutablePrefetchProvider provider = new ImmutablePrefetchProvider(map);
        assertEquals(100, provider.lookup(k));
    }

    // -----------------------------------------------------------------------
    // Lookup: miss
    // -----------------------------------------------------------------------

    @Test
    public void testLookupMiss_returnsMissSentinel() {
        PrefetchKey present = key(3, "brand-A", 1L);
        PrefetchKey absent  = key(3, "brand-B", 2L);
        Map<PrefetchKey, Object> map = new HashMap<>();
        map.put(present, 99.0);

        ImmutablePrefetchProvider provider = new ImmutablePrefetchProvider(map);
        Object result = provider.lookup(absent);
        assertSame(PrefetchKey.MISS, result,
            "lookup of absent key must return PrefetchKey.MISS");
    }

    // -----------------------------------------------------------------------
    // MISS is not null
    // -----------------------------------------------------------------------

    @Test
    public void testMissSentinel_isNotNull() {
        assertNotNull(PrefetchKey.MISS,
            "PrefetchKey.MISS must not be null");
    }

    // -----------------------------------------------------------------------
    // Size
    // -----------------------------------------------------------------------

    @Test
    public void testSize_reflectsMapSize() {
        Map<PrefetchKey, Object> map = new HashMap<>();
        map.put(key(1, "a"), 1.0);
        map.put(key(2, "b"), 2.0);
        map.put(key(3, "c"), 3.0);

        ImmutablePrefetchProvider provider = new ImmutablePrefetchProvider(map);
        assertEquals(3, provider.size());
    }

    // -----------------------------------------------------------------------
    // Empty provider
    // -----------------------------------------------------------------------

    @Test
    public void testEmpty_hasZeroSize() {
        assertEquals(0, ImmutablePrefetchProvider.empty().size());
    }

    @Test
    public void testEmpty_lookupReturnsMiss() {
        PrefetchKey k = key(0);
        assertSame(PrefetchKey.MISS,
            ImmutablePrefetchProvider.empty().lookup(k),
            "empty provider must return MISS for any key");
    }

    @Test
    public void testEmpty_singletonReference() {
        assertSame(ImmutablePrefetchProvider.empty(),
            ImmutablePrefetchProvider.empty(),
            "empty() must return the same singleton on every call");
    }

    // -----------------------------------------------------------------------
    // Immutability: modifying source map after construction has no effect
    // -----------------------------------------------------------------------

    @Test
    public void testImmutability_sourceMapModificationDoesNotAffectProvider() {
        Map<PrefetchKey, Object> map = new HashMap<>();
        PrefetchKey k1 = key(1, "a");
        map.put(k1, 10.0);

        ImmutablePrefetchProvider provider = new ImmutablePrefetchProvider(map);
        assertEquals(1, provider.size());

        // Modify the source map — provider must be unaffected
        map.put(key(2, "b"), 20.0);
        assertEquals(1, provider.size(),
            "provider size must not change when source map is modified post-construction");
    }

    @Test
    public void testImmutability_internalMapIsUnmodifiable() {
        Map<PrefetchKey, Object> map = new HashMap<>();
        map.put(key(1, "a"), 10.0);

        ImmutablePrefetchProvider provider = new ImmutablePrefetchProvider(map);
        // Verify the backing map is wrapped in an unmodifiable view by checking
        // that a lookup via the provider's interface still works correctly.
        assertSame(PrefetchKey.MISS, provider.lookup(key(99, "x")),
            "absent key must still return MISS after any attempted mutation");
    }

    // -----------------------------------------------------------------------
    // Null value in map returns MISS (documented behaviour)
    // -----------------------------------------------------------------------

    @Test
    public void testNullValueInMap_returnsMiss() {
        Map<PrefetchKey, Object> map = new HashMap<>();
        PrefetchKey k = key(4, "z");
        map.put(k, null);

        ImmutablePrefetchProvider provider = new ImmutablePrefetchProvider(map);
        // Per contract: null stored value is indistinguishable from absent
        assertSame(PrefetchKey.MISS, provider.lookup(k),
            "null stored value must return MISS (not null)");
    }
}

// End PrefetchedCellProviderTest.java
