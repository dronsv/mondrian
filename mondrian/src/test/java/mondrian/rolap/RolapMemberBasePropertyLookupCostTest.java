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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

/**
 * Cost and case-collision parity of member-property lookup.
 *
 * <p>Case-insensitive values retain the first matching entry in map order,
 * even when the caller spells a later case variant exactly. Restoring that
 * contract deliberately removes the zero-scan value shortcut. The crossjoin
 * orderer's per-sort determinant memo still limits value scans to once per
 * distinct determinant/member pair. Exact presence checks can safely avoid
 * a scan because they return only a boolean.</p>
 *
 * <p>These tests count key-set scans rather than elapsed time, and use a
 * fixed map order to give case-collision results a literal oracle.</p>
 */
public class RolapMemberBasePropertyLookupCostTest {

    /** A map with fixed iteration order that counts key-set scans. */
    private static final class CountingMap extends LinkedHashMap<String, Object> {
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
    public void caseInsensitiveValueLookupScansButExactPresenceDoesNot()
        throws Exception
    {
        final RolapMemberBase member = memberWithWideLevel();
        final CountingMap counting = instrument(member);

        assertEquals("value7", member.getPropertyValue("Prop7", true));
        assertEquals(0, counting.keySetScans, "case-sensitive lookup stays direct");
        assertEquals("value7", member.getPropertyValue("Prop7", false));
        assertEquals(
            1,
            counting.keySetScans,
            "case-insensitive values retain the original first-match scan");

        assertTrue(member.isPropertyLoaded("Prop7", false));
        assertEquals(
            1,
            counting.keySetScans,
            "an exact presence check needs no additional scan");
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
     * A property whose loaded value is null is still present —
     * "loaded and null" is not "absent" (see
     * {@link RolapMemberBaseIsPropertyLoadedTest}).
     */
    @Test
    public void loadedNullRetainsLookupOrderAndFastPresenceCheck()
        throws Exception
    {
        final RolapMemberBase member = memberWithWideLevel();
        member.setProperty("Phone", null);
        final CountingMap counting = instrument(member);

        assertNull(member.getPropertyValue("Phone", false));
        assertEquals(
            1,
            counting.keySetScans,
            "loaded null values use the same first-match scan");
        assertTrue(member.isPropertyLoaded("Phone", false));
        assertEquals(1, counting.keySetScans);
    }

    /** Case-insensitive lookup must choose the same first match for every spelling. */
    @Test
    public void caseVariantsKeepTheFirstCaseInsensitiveMatch() throws Exception {
        final RolapMemberBase member =
            new RolapMemberBase(null, mockLevel(), "key1");
        member.setProperty("Region", "first");
        instrument(member);
        member.setProperty("REGION", "second");

        assertEquals("first", member.getPropertyValue("Region", false));
        assertEquals("first", member.getPropertyValue("REGION", false));
        assertEquals("first", member.getPropertyValue("region", false));
        assertEquals("first", member.getPropertyValue("Region", true));
        assertEquals("second", member.getPropertyValue("REGION", true));
        assertNull(member.getPropertyValue("region", true));
    }

    @Test
    public void addingAndUpdatingCaseVariantsPreservesLookupOrder() throws Exception {
        final RolapMemberBase member =
            new RolapMemberBase(null, mockLevel(), "key1");
        member.setProperty("Region", "first");
        instrument(member);
        assertEquals("first", member.getPropertyValue("REGION", false));

        member.setProperty("REGION", "second");
        member.setProperty("Region", "updated");
        assertEquals("updated", member.getPropertyValue("Region", false));
        assertEquals("updated", member.getPropertyValue("REGION", false));
        assertEquals("updated", member.getPropertyValue("region", false));
        assertEquals("second", member.getPropertyValue("REGION", true));
    }

    @Test
    public void aNullFirstCaseVariantStillWins() throws Exception {
        final RolapMemberBase member =
            new RolapMemberBase(null, mockLevel(), "key1");
        member.setProperty("Region", null);
        instrument(member);
        member.setProperty("REGION", "second");

        assertNull(member.getPropertyValue("Region", false));
        assertNull(member.getPropertyValue("REGION", false));
        assertNull(member.getPropertyValue("region", false));
        assertTrue(member.isPropertyLoaded("Region", false));
        assertTrue(member.isPropertyLoaded("REGION", false));
        assertTrue(member.isPropertyLoaded("region", false));
        assertEquals("second", member.getPropertyValue("REGION", true));
    }
}

// End RolapMemberBasePropertyLookupCostTest.java
