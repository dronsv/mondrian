/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import junit.framework.TestCase;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CrossJoinDependencyPruner}.
 */
public class CrossJoinDependencyPrunerTest extends TestCase {

    public void testParseDependencyRules() {
        List<CrossJoinDependencyPruner.DependencyRule> rules =
            CrossJoinDependencyPruner.parseDependencyRules(
                "[Product].[Category]|ancestor; "
                + "[Product].[Brand]|property:CategoryKey");

        assertEquals(2, rules.size());
        assertEquals("[Product].[Category]", rules.get(0).determinantLevel);
        assertEquals(
            CrossJoinDependencyPruner.MappingType.ANCESTOR,
            rules.get(0).mappingType);
        assertEquals("[Product].[Brand]", rules.get(1).determinantLevel);
        assertEquals(
            CrossJoinDependencyPruner.MappingType.PROPERTY,
            rules.get(1).mappingType);
        assertEquals("CategoryKey", rules.get(1).mappingProperty);
    }

    public void testHierarchyAncestorDependency() {
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel determinant = mock(RolapLevel.class);
        RolapLevel dependent = mock(RolapLevel.class);

        when(determinant.getHierarchy()).thenReturn(hierarchy);
        when(dependent.getHierarchy()).thenReturn(hierarchy);
        when(determinant.getDepth()).thenReturn(2);
        when(dependent.getDepth()).thenReturn(4);

        assertTrue(
            CrossJoinDependencyPruner.isHierarchyAncestorDependency(
                dependent,
                determinant));
    }

    public void testCollectAncestorKeys() {
        RolapLevel determinantLevel = mock(RolapLevel.class);
        RolapLevel dependentLevel = mock(RolapLevel.class);
        RolapMember determinant1 = mock(RolapMember.class);
        RolapMember determinant2 = mock(RolapMember.class);
        RolapMember dependent1 = mock(RolapMember.class);
        RolapMember dependent2 = mock(RolapMember.class);

        when(determinant1.getLevel()).thenReturn(determinantLevel);
        when(determinant1.getKey()).thenReturn("cat_1");
        when(determinant2.getLevel()).thenReturn(determinantLevel);
        when(determinant2.getKey()).thenReturn("cat_2");

        when(dependent1.getLevel()).thenReturn(dependentLevel);
        when(dependent1.getParentMember()).thenReturn(determinant1);
        when(dependent2.getLevel()).thenReturn(dependentLevel);
        when(dependent2.getParentMember()).thenReturn(determinant2);

        Set<Object> keys =
            CrossJoinDependencyPruner.collectAncestorKeys(
                Arrays.asList(dependent1, dependent2),
                determinantLevel);

        assertEquals(2, keys.size());
        assertTrue(keys.contains("cat_1"));
        assertTrue(keys.contains("cat_2"));
    }

    public void testCollectPropertyKeys() {
        RolapMember dependent1 = mock(RolapMember.class);
        RolapMember dependent2 = mock(RolapMember.class);
        when(dependent1.getPropertyValue("CategoryKey")).thenReturn(10);
        when(dependent2.getPropertyValue("CategoryKey")).thenReturn(20);

        Set<Object> keys =
            CrossJoinDependencyPruner.collectPropertyKeys(
                Arrays.asList(dependent1, dependent2),
                "CategoryKey");

        assertEquals(2, keys.size());
        assertTrue(keys.contains(10));
        assertTrue(keys.contains(20));
    }
}

// End CrossJoinDependencyPrunerTest.java
