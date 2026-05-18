/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap4j.MondrianOlap4jHierarchy;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.SyntheticFlatHierarchy;
import org.olap4j.metadata.Hierarchy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression for dronsv/mondrian#17 —
 * {@code MDSCHEMA_HIERARCHIES} must honor {@code showHierarchy="false"}.
 *
 * <p>The existing guard in {@code RowsetDefinition} attempted to skip
 * hierarchies via {@code hierarchy instanceof RolapCubeHierarchy} but the
 * hierarchy reaching {@code populate*} is an {@link MondrianOlap4jHierarchy}
 * wrapper, so the {@code instanceof} check was always false and the guard
 * was dead code. The extracted helper {@code isSchemaHidden} unwraps the
 * wrapper via {@link MondrianOlap4jHierarchy#getHierarchy()} and inspects
 * {@link RolapHierarchy#isShowHierarchy()} on the underlying mondrian
 * hierarchy.
 */
public class RowsetDefinitionSchemaHiddenHierarchyTest {

    @Test public void
    testOlap4jWrappedHiddenRolapHierarchyIsSchemaHidden() {
        RolapHierarchy underlying = mock(RolapHierarchy.class);
        when(underlying.isShowHierarchy()).thenReturn(false);
        MondrianOlap4jHierarchy wrapper = mock(MondrianOlap4jHierarchy.class);
        when(wrapper.getHierarchy()).thenReturn(underlying);

        assertTrue(RowsetDefinition.isSchemaHidden(wrapper));
    }

    @Test public void
    testOlap4jWrappedVisibleRolapHierarchyIsNotSchemaHidden() {
        RolapHierarchy underlying = mock(RolapHierarchy.class);
        when(underlying.isShowHierarchy()).thenReturn(true);
        MondrianOlap4jHierarchy wrapper = mock(MondrianOlap4jHierarchy.class);
        when(wrapper.getHierarchy()).thenReturn(underlying);

        assertFalse(RowsetDefinition.isSchemaHidden(wrapper));
    }

    @Test public void
    testOlap4jWrappedSyntheticFlatIsNotSchemaHidden() {
        // SyntheticFlatHierarchy is always shown — its
        // isShowHierarchy() override returns true even if the source
        // hierarchy is hidden. The helper must respect that.
        SyntheticFlatHierarchy synth = mock(SyntheticFlatHierarchy.class);
        when(synth.isShowHierarchy()).thenReturn(true);
        MondrianOlap4jHierarchy wrapper = mock(MondrianOlap4jHierarchy.class);
        when(wrapper.getHierarchy()).thenReturn(synth);

        assertFalse(RowsetDefinition.isSchemaHidden(wrapper));
    }

    @Test public void
    testNullHierarchyIsNotSchemaHidden() {
        assertFalse(RowsetDefinition.isSchemaHidden(null));
    }

    @Test public void
    testNonMondrianOlap4jWrapperIsNotSchemaHidden() {
        // A foreign olap4j Hierarchy implementation that is not the
        // Mondrian wrapper cannot expose the underlying RolapHierarchy,
        // so the helper returns false (no schema-hidden interpretation).
        Hierarchy foreign = mock(Hierarchy.class);
        assertFalse(RowsetDefinition.isSchemaHidden(foreign));
    }
}
