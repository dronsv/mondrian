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
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.sql.dependency.CrossJoinDependencyPrunerV2;
import mondrian.rolap.sql.dependency.DependencyPruningContext;
import mondrian.rolap.sql.dependency.DependencyRegistry;

import java.util.Arrays;
import java.util.Collections;
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

    public void testV2StrictDoesNotApplyAncestorFallbackWithoutExplicitRule() {
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel determinantLevel = mockLevel(hierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel dependentLevel = mockLevel(hierarchy, "Sku", "[Product].[Sku]", 4);

        MemberListCrossJoinArg determinantArg = memberListArg(
            evaluator,
            determinantLevel,
            member(determinantLevel, "cat_1"),
            member(determinantLevel, "cat_2"),
            member(determinantLevel, "cat_3"));

        RolapMember dependent1 = memberWithParent(dependentLevel, "sku_1", member(determinantLevel, "cat_1"));
        RolapMember dependent2 = memberWithParent(dependentLevel, "sku_2", member(determinantLevel, "cat_2"));
        MemberListCrossJoinArg dependentArg =
            memberListArg(evaluator, dependentLevel, dependent1, dependent2);

        DependencyRegistry registry = DependencyRegistry
            .builder("[Cube]")
            .policy(DependencyRegistry.DependencyPruningPolicy.RELAXED)
            .addLevelDescriptor(new DependencyRegistry.LevelDependencyDescriptor(
                dependentLevel.getUniqueName(),
                hierarchy.getUniqueName(),
                dependentLevel.getDepth(),
                Collections.<DependencyRegistry.CompiledDependencyRule>emptyList(),
                false))
            .build();

        DependencyPruningContext context = DependencyPruningContext.of(
            evaluator,
            registry,
            DependencyRegistry.DependencyPruningPolicy.STRICT,
            false);

        CrossJoinArg[] original = new CrossJoinArg[] { determinantArg, dependentArg };
        CrossJoinArg[] pruned = CrossJoinDependencyPrunerV2.prune(original, context);

        assertSame(original, pruned);
    }

    public void testV2RelaxedAppliesAncestorFallback() {
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel determinantLevel = mockLevel(hierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel dependentLevel = mockLevel(hierarchy, "Sku", "[Product].[Sku]", 4);

        MemberListCrossJoinArg determinantArg = memberListArg(
            evaluator,
            determinantLevel,
            member(determinantLevel, "cat_1"),
            member(determinantLevel, "cat_2"),
            member(determinantLevel, "cat_3"));

        RolapMember dependent1 = memberWithParent(dependentLevel, "sku_1", member(determinantLevel, "cat_1"));
        RolapMember dependent2 = memberWithParent(dependentLevel, "sku_2", member(determinantLevel, "cat_2"));
        MemberListCrossJoinArg dependentArg =
            memberListArg(evaluator, dependentLevel, dependent1, dependent2);

        DependencyRegistry registry = DependencyRegistry
            .builder("[Cube]")
            .addLevelDescriptor(new DependencyRegistry.LevelDependencyDescriptor(
                dependentLevel.getUniqueName(),
                hierarchy.getUniqueName(),
                dependentLevel.getDepth(),
                Collections.<DependencyRegistry.CompiledDependencyRule>emptyList(),
                false))
            .build();

        DependencyPruningContext context = DependencyPruningContext.of(
            evaluator,
            registry,
            DependencyRegistry.DependencyPruningPolicy.RELAXED,
            false);

        CrossJoinArg[] original = new CrossJoinArg[] { determinantArg, dependentArg };
        CrossJoinArg[] pruned = CrossJoinDependencyPrunerV2.prune(original, context);

        assertNotSame(original, pruned);
        assertTrue(pruned[0] instanceof MemberListCrossJoinArg);
        MemberListCrossJoinArg prunedDeterminant = (MemberListCrossJoinArg) pruned[0];
        assertEquals(2, prunedDeterminant.getMembers().size());
        assertEquals("cat_1", prunedDeterminant.getMembers().get(0).getKey());
        assertEquals("cat_2", prunedDeterminant.getMembers().get(1).getKey());
    }

    private static RolapLevel mockLevel(
        RolapHierarchy hierarchy,
        String name,
        String uniqueName,
        int depth)
    {
        RolapLevel level = mock(RolapLevel.class);
        when(level.getHierarchy()).thenReturn(hierarchy);
        when(level.getName()).thenReturn(name);
        when(level.getUniqueName()).thenReturn(uniqueName);
        when(level.getDepth()).thenReturn(depth);
        when(level.isSimple()).thenReturn(true);
        when(level.isParentChild()).thenReturn(false);
        return level;
    }

    private static RolapMember member(RolapLevel level, Object key) {
        RolapMember member = mock(RolapMember.class);
        when(member.getLevel()).thenReturn(level);
        when(member.getKey()).thenReturn(key);
        when(member.isNull()).thenReturn(false);
        when(member.isAll()).thenReturn(false);
        when(member.isCalculated()).thenReturn(false);
        when(member.isParentChildLeaf()).thenReturn(false);
        return member;
    }

    private static RolapMember memberWithParent(
        RolapLevel level,
        Object key,
        RolapMember parent)
    {
        RolapMember member = member(level, key);
        when(member.getParentMember()).thenReturn(parent);
        return member;
    }

    private static MemberListCrossJoinArg memberListArg(
        RolapEvaluator evaluator,
        RolapLevel level,
        RolapMember... members)
    {
        for (RolapMember member : members) {
            when(member.getLevel()).thenReturn(level);
        }
        CrossJoinArg arg = MemberListCrossJoinArg.create(
            evaluator,
            Arrays.asList(members),
            false,
            false);
        assertTrue(arg instanceof MemberListCrossJoinArg);
        return (MemberListCrossJoinArg) arg;
    }
}

// End CrossJoinDependencyPrunerTest.java
