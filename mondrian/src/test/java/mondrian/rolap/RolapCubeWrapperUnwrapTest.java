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

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

/**
 * Pins behaviour of {@link RolapCubeHierarchy#unwrap} and
 * {@link RolapCubeLevel#unwrap}. These helpers replace the inline
 * {@code instanceof RolapCubeHierarchy + getRolapHierarchy()} (resp.
 * {@code RolapCubeLevel + getRolapLevel()}) idiom that previously
 * appeared across NativeQueryEngine, NativeNonEmptyFilter,
 * SqlMemberSource, SyntheticFlatHierarchySupport and ExplicitRecognizer.
 */
public class RolapCubeWrapperUnwrapTest {

    @Test
    public void hierarchyUnwrap_returnsInputForNonCubeHierarchy() {
        // Behaviour for non-cube hierarchies is "return as-is" so callers
        // that follow up with `instanceof SyntheticFlatHierarchy` or
        // `instanceof RolapHierarchy` checks see the original object.
        Hierarchy h = mock(Hierarchy.class);
        assertSame(h, RolapCubeHierarchy.unwrap(h));
    }

    @Test
    public void hierarchyUnwrap_nullPassesThrough() {
        assertNull(RolapCubeHierarchy.unwrap(null));
    }

    @Test
    public void levelUnwrap_returnsRolapLevelInputUnchanged() {
        RolapLevel rl = mock(RolapLevel.class);
        assertSame(rl, RolapCubeLevel.unwrap(rl));
    }

    @Test
    public void levelUnwrap_returnsNullForUnrelatedLevel() {
        // The Level interface admits non-Rolap implementations; the
        // helper returns null rather than throwing, matching the
        // original inline pattern's "give me a RolapLevel or nothing"
        // semantics at the ExplicitRecognizer call site.
        Level l = mock(Level.class);
        assertNull(RolapCubeLevel.unwrap(l));
    }

    @Test
    public void levelUnwrap_nullPassesThrough() {
        assertNull(RolapCubeLevel.unwrap(null));
    }
}

// End RolapCubeWrapperUnwrapTest.java
