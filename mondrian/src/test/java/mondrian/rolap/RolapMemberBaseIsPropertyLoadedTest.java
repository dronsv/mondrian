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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RolapMemberBase#isPropertyLoaded(String)} — V2-M1
 * foundation for the lazy-fetch decision in M4 (dronsv/mondrian#22).
 *
 * <p>The method distinguishes "property was loaded and the value is
 * null" (returns true) from "property was never loaded" (returns
 * false). Both cases still return null from {@code getPropertyValue}
 * — M1 does not change that observable. The point is to give M4 a
 * statically-decidable signal for whether to trigger a one-shot SQL
 * fetch when V2's per-query projection plan deliberately skipped this
 * property at load time.
 */
public class RolapMemberBaseIsPropertyLoadedTest {

    /** A fully-mocked RolapLevel sufficient to satisfy
     *  RolapMemberBase's constructor invariants. The constructor
     *  reads the level's hierarchy/name/etc., so we wire them with
     *  permissive mocks. */
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

    @Test
    public void neverLoaded_returnsFalse_andGetPropertyValueIsNull() {
        RolapMemberBase m = new RolapMemberBase(
            null, mockLevel(), "key1");
        assertFalse(m.isPropertyLoaded("Phone"));
        assertNull(m.getPropertyValue("Phone"));
    }

    @Test
    public void loadedNonNullValue_returnsTrue() {
        RolapMemberBase m = new RolapMemberBase(
            null, mockLevel(), "key1");
        m.setProperty("Phone", "555-1234");
        assertTrue(m.isPropertyLoaded("Phone"));
        // And the value is what was set.
        org.junit.jupiter.api.Assertions.assertEquals(
            "555-1234", m.getPropertyValue("Phone"));
    }

    @Test
    public void loadedNullValue_returnsTrue_butGetPropertyValueIsNull() {
        // This is the core distinction M1 enables. SQL-NULL values
        // arrive via setProperty(name, null) — the property is loaded
        // and the value happens to be null. The lazy-fetch path in M4
        // must NOT re-fetch in this case.
        RolapMemberBase m = new RolapMemberBase(
            null, mockLevel(), "key1");
        m.setProperty("Phone", null);
        assertTrue(
            m.isPropertyLoaded("Phone"),
            "Property explicitly set to null is loaded");
        assertNull(
            m.getPropertyValue("Phone"),
            "getPropertyValue returns the loaded null");
    }

    @Test
    public void caseInsensitiveLookup() {
        RolapMemberBase m = new RolapMemberBase(
            null, mockLevel(), "key1");
        m.setProperty("Phone", "x");
        assertTrue(m.isPropertyLoaded("Phone"));
        assertTrue(m.isPropertyLoaded("phone", false));
        assertTrue(m.isPropertyLoaded("PHONE", false));
        // Strict (default) does NOT match different case.
        assertFalse(m.isPropertyLoaded("phone"));
        assertFalse(m.isPropertyLoaded("phone", true));
    }

    @Test
    public void multipleProperties_eachLoadedIndependently() {
        RolapMemberBase m = new RolapMemberBase(
            null, mockLevel(), "key1");
        m.setProperty("Phone", "555-1234");
        m.setProperty("Address", null);
        assertTrue(m.isPropertyLoaded("Phone"));
        assertTrue(m.isPropertyLoaded("Address"));
        assertFalse(m.isPropertyLoaded("Email"));
    }

    @Test
    public void delegatingMemberDelegatesLoadedPropertyCheck() {
        RolapMemberBase base = new RolapMemberBase(
            null, mockLevel(), "key1");
        base.setProperty("Phone", "555-1234");

        DelegatingRolapMember delegating = new DelegatingRolapMember(base);

        assertTrue(delegating.isPropertyLoaded("Phone"));
        assertFalse(delegating.isPropertyLoaded("Email"));
    }

    @Test
    public void delegatingMemberEmptyMapDoesNotThrow() {
        RolapMemberBase base = new RolapMemberBase(
            null, mockLevel(), "key1");
        DelegatingRolapMember delegating = new DelegatingRolapMember(base);

        assertFalse(delegating.isPropertyLoaded("Phone"));
        assertNull(delegating.getPropertyFromMap("Phone", true));
    }
}

// End RolapMemberBaseIsPropertyLoadedTest.java
