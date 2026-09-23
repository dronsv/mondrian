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

import mondrian.olap.Dimension;
import mondrian.olap.LevelType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

/**
 * Counter-based regression test for the cost of a case-insensitive
 * member-property lookup.
 *
 * <p>{@code getPropertyValue(name, false)} used to walk the member's
 * whole property map comparing every key with
 * {@code String.equalsIgnoreCase}, even when the caller spelled the
 * property exactly as the schema declares it — which is what every
 * engine-internal caller does. On a wide level that is a linear scan
 * plus a Unicode case-fold per key, per access, and the crossjoin
 * orderer performs one such access per member per comparison.</p>
 *
 * <p>These tests pin the fast path by counting how often the map's
 * key set is enumerated, rather than by timing anything.</p>
 */
public class RolapMemberBasePropertyLookupCostTest {

    /** A HashMap that records how often its key set is enumerated. */
    private static final class CountingMap extends HashMap<String, Object> {
        private static final long serialVersionUID = 1L;
        private int keySetScans;

        @Override
        public Set<String> keySet() {
            keySetScans++;
            return super.keySet();
        }
    }

    private RolapLevel mockLevel() {
        RolapLevel level = mock(RolapLevel.class);
        RolapHierarchy hier = mock(RolapHierarchy.class);
        Dimension dim = mock(Dimension.class);
        lenient().when(dim.isMeasures()).thenReturn(false);
        lenient().when(hier.getDimension()).thenReturn(dim);
        lenient().when(level.getHierarchy()).thenReturn(hier);
        lenient().when(level.getLevelType()).thenReturn(LevelType.Regular);
        lenient().when(level.isAll()).thenReturn(false);
        return level;
    }

    /**
     * Builds a member carrying {@code properties} and swaps its value
     * map for a {@link CountingMap} holding the same entries.
     */
    private CountingMap instrument(RolapMemberBase member) throws Exception {
        final Field field =
            RolapMemberBase.class.getDeclaredField("mapPropertyNameToValue");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        final Map<String, Object> existing =
            (Map<String, Object>) field.get(member);
        final CountingMap counting = new CountingMap();
        counting.putAll(existing);
        field.set(member, counting);
        return counting;
    }

    private RolapMemberBase memberWithWideLevel() {
        final RolapMemberBase member =
            new RolapMemberBase(null, mockLevel(), "key1");
        // A realistically wide level: the address level in the house
        // query pack declares two dozen properties.
        for (int i = 0; i < 24; i++) {
            member.setProperty("Prop" + i, "value" + i);
        }
        return member;
    }

    @Test
    public void exactlySpelledNameIsFoundWithoutScanningTheKeySet()
        throws Exception
    {
        final RolapMemberBase member = memberWithWideLevel();
        final CountingMap counting = instrument(member);

        assertEquals("value7", member.getPropertyValue("Prop7", false));
        assertEquals(
            0,
            counting.keySetScans,
            "an exactly-spelled property must not fall back to the "
            + "case-insensitive key-set scan");

        assertTrue(member.isPropertyLoaded("Prop7", false));
        assertEquals(
            0,
            counting.keySetScans,
            "isPropertyLoaded must take the same fast path");
    }

    @Test
    public void differentlyCasedNameStillResolvesViaTheScan()
        throws Exception
    {
        final RolapMemberBase member = memberWithWideLevel();
        final CountingMap counting = instrument(member);

        assertEquals("value7", member.getPropertyValue("PROP7", false));
        assertEquals(
            1,
            counting.keySetScans,
            "a differently-cased name still needs exactly one scan");

        assertTrue(member.isPropertyLoaded("prop7", false));
        assertEquals(2, counting.keySetScans);
    }

    @Test
    public void unknownNameScansOnceAndReturnsNull() throws Exception {
        final RolapMemberBase member = memberWithWideLevel();
        final CountingMap counting = instrument(member);

        assertNull(member.getPropertyValue("NoSuchProperty", false));
        assertEquals(1, counting.keySetScans);
        assertFalse(member.isPropertyLoaded("NoSuchProperty", false));
        assertEquals(2, counting.keySetScans);
    }

    /**
     * A property whose loaded value is null must still be found by the
     * fast path — "loaded and null" is not "absent" (see
     * {@link RolapMemberBaseIsPropertyLoadedTest}).
     */
    @Test
    public void loadedNullValueIsFoundWithoutScanningTheKeySet()
        throws Exception
    {
        final RolapMemberBase member = memberWithWideLevel();
        member.setProperty("Phone", null);
        final CountingMap counting = instrument(member);

        assertNull(member.getPropertyValue("Phone", false));
        assertEquals(
            0,
            counting.keySetScans,
            "a loaded null must short-circuit the scan too");
        assertTrue(member.isPropertyLoaded("Phone", false));
        assertEquals(0, counting.keySetScans);
    }

    /**
     * When a level declares two properties differing only in case, the
     * exactly-spelled one wins. The old key-set scan returned whichever
     * the map happened to iterate first.
     */
    @Test
    public void exactSpellingWinsOverACaseVariant() throws Exception {
        final RolapMemberBase member =
            new RolapMemberBase(null, mockLevel(), "key1");
        member.setProperty("Region", "exact");
        member.setProperty("REGION", "variant");
        instrument(member);

        assertEquals("exact", member.getPropertyValue("Region", false));
        assertEquals("variant", member.getPropertyValue("REGION", false));
    }
}

// End RolapMemberBasePropertyLookupCostTest.java
