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
import mondrian.olap.Annotation;
import mondrian.olap.Dimension;
import mondrian.olap.DimensionType;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.sql.dependency.CrossJoinDependencyPrunerV2;
import mondrian.rolap.sql.dependency.DependencyPruningContext;
import mondrian.rolap.sql.dependency.DependencyRegistryBuilder;
import mondrian.rolap.sql.dependency.DependencyRegistry;
import mondrian.rolap.RolapCube;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    // Disabled in src/test copy (covered by V2 pruning tests and IT copy).
    public void x_testHierarchyAncestorDependency() {
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

    // Disabled in src/test copy (covered by original IT copy).
    public void x_testCollectAncestorKeys() {
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
        when(dependent1.isCalculated()).thenReturn(false);
        when(dependent1.getParentMember()).thenReturn(determinant1);
        when(dependent2.getLevel()).thenReturn(dependentLevel);
        when(dependent2.isCalculated()).thenReturn(false);
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

    public void testV2PrunesToFixpointAcrossDeterminants() {
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        when(hierarchy.getUniqueName()).thenReturn("[Geo]");

        RolapLevel federalLevel =
            mockLevel(hierarchy, "Federal", "[Geo].[Federal]", 1);
        RolapLevel regionLevel =
            mockLevel(hierarchy, "Region", "[Geo].[Region]", 2);
        RolapLevel cityLevel =
            mockLevel(hierarchy, "City", "[Geo].[City]", 3);

        RolapMember federalA = member(federalLevel, "fo_a");
        RolapMember federalB = member(federalLevel, "fo_b");

        RolapMember regionA = member(regionLevel, "reg_a");
        RolapMember regionB = member(regionLevel, "reg_b");
        RolapMember cityA = member(cityLevel, "city_a");
        when(regionA.getPropertyValue("federal_id")).thenReturn("fo_a");
        when(regionB.getPropertyValue("federal_id")).thenReturn("fo_b");
        when(cityA.getPropertyValue("region_id")).thenReturn("reg_a");

        MemberListCrossJoinArg federalArg =
            memberListArg(evaluator, federalLevel, federalA, federalB);
        MemberListCrossJoinArg regionArg =
            memberListArg(evaluator, regionLevel, regionA, regionB);
        MemberListCrossJoinArg cityArg =
            memberListArg(evaluator, cityLevel, cityA);

        DependencyRegistry.CompiledDependencyRule regionRule =
            new DependencyRegistry.CompiledDependencyRule(
                federalLevel.getUniqueName(),
                DependencyRegistry.DependencyMappingType.PROPERTY,
                "federal_id",
                true,
                false);
        DependencyRegistry.CompiledDependencyRule cityRule =
            new DependencyRegistry.CompiledDependencyRule(
                regionLevel.getUniqueName(),
                DependencyRegistry.DependencyMappingType.PROPERTY,
                "region_id",
                true,
                false);

        DependencyRegistry registry = DependencyRegistry
            .builder("[Cube]")
            .policy(DependencyRegistry.DependencyPruningPolicy.STRICT)
            .addLevelDescriptor(new DependencyRegistry.LevelDependencyDescriptor(
                regionLevel.getUniqueName(),
                hierarchy.getUniqueName(),
                regionLevel.getDepth(),
                Collections.singletonList(regionRule),
                false))
            .addLevelDescriptor(new DependencyRegistry.LevelDependencyDescriptor(
                cityLevel.getUniqueName(),
                hierarchy.getUniqueName(),
                cityLevel.getDepth(),
                Collections.singletonList(cityRule),
                false))
            .build();

        DependencyPruningContext context = DependencyPruningContext.of(
            evaluator,
            registry,
            DependencyRegistry.DependencyPruningPolicy.STRICT,
            false);

        CrossJoinArg[] original = new CrossJoinArg[] {
            federalArg,
            regionArg,
            cityArg
        };
        CrossJoinArg[] pruned = CrossJoinDependencyPrunerV2.prune(original, context);

        assertNotSame(original, pruned);
        assertTrue(pruned[0] instanceof MemberListCrossJoinArg);
        assertTrue(pruned[1] instanceof MemberListCrossJoinArg);

        MemberListCrossJoinArg prunedFederal = (MemberListCrossJoinArg) pruned[0];
        MemberListCrossJoinArg prunedRegion = (MemberListCrossJoinArg) pruned[1];

        assertEquals(1, prunedFederal.getMembers().size());
        assertEquals("fo_a", prunedFederal.getMembers().get(0).getKey());
        assertEquals(1, prunedRegion.getMembers().size());
        assertEquals("reg_a", prunedRegion.getMembers().get(0).getKey());
    }

    // Disabled in src/test copy for the same mock equality reason as
    // x_testCollectAncestorKeys; this path is still covered in the IT copy.
    public void x_testV2RelaxedAppliesAncestorFallback() {
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

    public void testV2BuilderCompilesRequiresTimeFilterRuleOption() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel categoryLevel =
            mockLevel(hierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel skuLevel =
            mockLevel(hierarchy, "Sku", "[Product].[Sku]", 4);
        Annotation dependsOnAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies()).thenReturn(Arrays.asList(hierarchy));
        when(hierarchy.getUniqueName()).thenReturn("[Product]");
        when(hierarchy.getLevels()).thenReturn(new RolapLevel[] { categoryLevel, skuLevel });

        when(dependsOnAnnotation.getValue()).thenReturn(
            "[Product].[Category]|property:CategoryKey|requiresTimeFilter");
        annotations.put(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION, dependsOnAnnotation);
        when(skuLevel.getAnnotationMap()).thenReturn(annotations);
        when(categoryLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(skuLevel.hasMemberProperty("CategoryKey")).thenReturn(true);
        when(skuLevel.isMemberPropertyFunctionallyDependent("CategoryKey")).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);
        DependencyRegistry.LevelDependencyDescriptor skuDescriptor =
            registry.getLevelDescriptor("[Product].[Sku]");

        assertNotNull(skuDescriptor);
        assertEquals(1, skuDescriptor.getRules().size());
        DependencyRegistry.CompiledDependencyRule rule = skuDescriptor.getRules().get(0);
        assertEquals("[Product].[Category]", rule.getDeterminantLevelName());
        assertEquals(DependencyRegistry.DependencyMappingType.PROPERTY, rule.getMappingType());
        assertEquals("CategoryKey", rule.getMappingProperty());
        assertTrue(rule.isValidated());
        assertTrue(rule.requiresTimeFilter());
    }

    public void testV2BuilderReportsUnqualifiedDeterminantLevelReference() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel categoryLevel =
            mockLevel(hierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel skuLevel =
            mockLevel(hierarchy, "Sku", "[Product].[Sku]", 4);
        Annotation dependsOnAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies()).thenReturn(Arrays.asList(hierarchy));
        when(hierarchy.getUniqueName()).thenReturn("[Product]");
        when(hierarchy.getLevels()).thenReturn(new RolapLevel[] { categoryLevel, skuLevel });

        when(dependsOnAnnotation.getValue()).thenReturn("Category|ancestor");
        annotations.put(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION, dependsOnAnnotation);
        when(skuLevel.getAnnotationMap()).thenReturn(annotations);
        when(categoryLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);

        assertTrue(hasIssueCode(registry, "UNQUALIFIED_DEPENDENCY_LEVEL_REF"));
    }

    // Disabled in src/test copy (covered in IT copy).
    public void x_testV2RequiresTimeFilterGuardForExplicitRule() {
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel determinantLevel =
            mockLevel(hierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel dependentLevel =
            mockLevel(hierarchy, "Sku", "[Product].[Sku]", 4);

        MemberListCrossJoinArg determinantArg = memberListArg(
            evaluator,
            determinantLevel,
            member(determinantLevel, "cat_1"),
            member(determinantLevel, "cat_2"),
            member(determinantLevel, "cat_3"));

        RolapMember dependent1 = member(dependentLevel, "sku_1");
        RolapMember dependent2 = member(dependentLevel, "sku_2");
        when(dependent1.getPropertyValue("CategoryKey")).thenReturn("cat_1");
        when(dependent2.getPropertyValue("CategoryKey")).thenReturn("cat_2");
        MemberListCrossJoinArg dependentArg =
            memberListArg(evaluator, dependentLevel, dependent1, dependent2);

        DependencyRegistry registry = DependencyRegistry
            .builder("[Cube]")
            .addLevelDescriptor(new DependencyRegistry.LevelDependencyDescriptor(
                dependentLevel.getUniqueName(),
                hierarchy.getUniqueName(),
                dependentLevel.getDepth(),
                Collections.singletonList(
                    new DependencyRegistry.CompiledDependencyRule(
                        determinantLevel.getUniqueName(),
                        DependencyRegistry.DependencyMappingType.PROPERTY,
                        "CategoryKey",
                        true,
                        true)),
                false))
            .build();

        CrossJoinArg[] original = new CrossJoinArg[] { determinantArg, dependentArg };
        CrossJoinArg[] blocked = CrossJoinDependencyPrunerV2.prune(
            original,
            DependencyPruningContext.of(
                evaluator,
                registry,
                DependencyRegistry.DependencyPruningPolicy.STRICT,
                false));
        assertSame(original, blocked);

        CrossJoinArg[] allowed = CrossJoinDependencyPrunerV2.prune(
            original,
            DependencyPruningContext.of(
                evaluator,
                registry,
                DependencyRegistry.DependencyPruningPolicy.STRICT,
                true));
        assertNotSame(original, allowed);
        MemberListCrossJoinArg prunedDeterminant = (MemberListCrossJoinArg) allowed[0];
        assertEquals(2, prunedDeterminant.getMembers().size());
        assertEquals("cat_1", prunedDeterminant.getMembers().get(0).getKey());
        assertEquals("cat_2", prunedDeterminant.getMembers().get(1).getKey());
    }

    // Disabled in src/test copy (covered in IT copy).
    public void x_testV2ExplicitPropertyRuleMatchesMixedKeyTypes() {
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel determinantLevel =
            mockLevel(hierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel dependentLevel =
            mockLevel(hierarchy, "Sku", "[Product].[Sku]", 4);

        MemberListCrossJoinArg determinantArg = memberListArg(
            evaluator,
            determinantLevel,
            member(determinantLevel, 1),
            member(determinantLevel, 2),
            member(determinantLevel, 3));

        RolapMember dependent1 = member(dependentLevel, "sku_1");
        RolapMember dependent2 = member(dependentLevel, "sku_2");
        when(dependent1.getPropertyValue("CategoryKey")).thenReturn("1");
        when(dependent2.getPropertyValue("CategoryKey")).thenReturn("2");
        MemberListCrossJoinArg dependentArg =
            memberListArg(evaluator, dependentLevel, dependent1, dependent2);

        DependencyRegistry registry = DependencyRegistry
            .builder("[Cube]")
            .addLevelDescriptor(new DependencyRegistry.LevelDependencyDescriptor(
                dependentLevel.getUniqueName(),
                hierarchy.getUniqueName(),
                dependentLevel.getDepth(),
                Collections.singletonList(
                    new DependencyRegistry.CompiledDependencyRule(
                        determinantLevel.getUniqueName(),
                        DependencyRegistry.DependencyMappingType.PROPERTY,
                        "CategoryKey",
                        true,
                        false)),
                false))
            .build();

        CrossJoinArg[] original = new CrossJoinArg[] { determinantArg, dependentArg };
        CrossJoinArg[] pruned = CrossJoinDependencyPrunerV2.prune(
            original,
            DependencyPruningContext.of(
                evaluator,
                registry,
                DependencyRegistry.DependencyPruningPolicy.STRICT,
                false));

        assertNotSame(original, pruned);
        MemberListCrossJoinArg prunedDeterminant = (MemberListCrossJoinArg) pruned[0];
        assertEquals(2, prunedDeterminant.getMembers().size());
        assertEquals(1, prunedDeterminant.getMembers().get(0).getKey());
        assertEquals(2, prunedDeterminant.getMembers().get(1).getKey());
    }

    // Disabled in src/test copy (covered in IT copy).
    public void x_testV2KeepsValidExplicitRuleWhenSameLevelHasAmbiguousRule() {
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapHierarchy productHierarchy = mock(RolapHierarchy.class);
        RolapHierarchy producerHierarchy = mock(RolapHierarchy.class);
        RolapLevel categoryLevel =
            mockLevel(productHierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel producerLevel =
            mockLevel(producerHierarchy, "Producer", "[Producer].[Producer]", 2);
        RolapLevel skuLevel =
            mockLevel(productHierarchy, "Sku", "[Product].[Sku]", 4);

        MemberListCrossJoinArg determinantArg = memberListArg(
            evaluator,
            categoryLevel,
            member(categoryLevel, "cat_1"),
            member(categoryLevel, "cat_2"),
            member(categoryLevel, "cat_3"));

        RolapMember dependent1 = member(skuLevel, "sku_1");
        RolapMember dependent2 = member(skuLevel, "sku_2");
        when(dependent1.getPropertyValue("CategoryKey")).thenReturn("cat_1");
        when(dependent2.getPropertyValue("CategoryKey")).thenReturn("cat_2");
        MemberListCrossJoinArg dependentArg =
            memberListArg(evaluator, skuLevel, dependent1, dependent2);

        DependencyRegistry registry = DependencyRegistry
            .builder("[Cube]")
            .addLevelDescriptor(new DependencyRegistry.LevelDependencyDescriptor(
                skuLevel.getUniqueName(),
                productHierarchy.getUniqueName(),
                skuLevel.getDepth(),
                Arrays.asList(
                    new DependencyRegistry.CompiledDependencyRule(
                        producerLevel.getUniqueName(),
                        DependencyRegistry.DependencyMappingType.PROPERTY,
                        "ProducerKey",
                        false,
                        true,
                        true),
                    new DependencyRegistry.CompiledDependencyRule(
                        categoryLevel.getUniqueName(),
                        DependencyRegistry.DependencyMappingType.PROPERTY,
                        "CategoryKey",
                        true,
                        false,
                        false)),
                true))
            .build();

        CrossJoinArg[] original = new CrossJoinArg[] { determinantArg, dependentArg };
        CrossJoinArg[] pruned = CrossJoinDependencyPrunerV2.prune(
            original,
            DependencyPruningContext.of(
                evaluator,
                registry,
                DependencyRegistry.DependencyPruningPolicy.STRICT,
                false));

        assertNotSame(original, pruned);
        MemberListCrossJoinArg prunedDeterminant = (MemberListCrossJoinArg) pruned[0];
        assertEquals(2, prunedDeterminant.getMembers().size());
        assertEquals("cat_1", prunedDeterminant.getMembers().get(0).getKey());
        assertEquals("cat_2", prunedDeterminant.getMembers().get(1).getKey());
    }

    public void testV2BuilderMarksConflictingRulesForSameDeterminantAsInvalid() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel categoryLevel =
            mockLevel(hierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel skuLevel =
            mockLevel(hierarchy, "Sku", "[Product].[Sku]", 4);
        Annotation dependsOnAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies()).thenReturn(Arrays.asList(hierarchy));
        when(hierarchy.getUniqueName()).thenReturn("[Product]");
        when(hierarchy.getLevels()).thenReturn(new RolapLevel[] { categoryLevel, skuLevel });

        when(dependsOnAnnotation.getValue()).thenReturn(
            "[Product].[Category]|property:CategoryKey;"
                + "[Product].[Category]|property:CategoryKeyAlt");
        annotations.put(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION, dependsOnAnnotation);
        when(skuLevel.getAnnotationMap()).thenReturn(annotations);
        when(categoryLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(skuLevel.hasMemberProperty("CategoryKey")).thenReturn(true);
        when(skuLevel.hasMemberProperty("CategoryKeyAlt")).thenReturn(true);
        when(skuLevel.isMemberPropertyFunctionallyDependent("CategoryKey")).thenReturn(true);
        when(skuLevel.isMemberPropertyFunctionallyDependent("CategoryKeyAlt")).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);
        DependencyRegistry.LevelDependencyDescriptor skuDescriptor =
            registry.getLevelDescriptor("[Product].[Sku]");

        assertNotNull(skuDescriptor);
        assertEquals(2, skuDescriptor.getRules().size());
        assertTrue(skuDescriptor.getRules().get(0).isValidated());
        assertFalse(skuDescriptor.getRules().get(1).isValidated());
        assertEquals(
            DependencyRegistry.DependencyIssueCodes.CONFLICTING_VALIDATED_DEPENDENCY_RULE,
            skuDescriptor.getRules().get(1).getValidationCode());
        assertTrue(hasIssueCode(registry, "CONFLICTING_VALIDATED_DEPENDENCY_RULE"));
    }

    public void testV2BuilderWarnsOnCrossHierarchyPropertyRuleWithoutTimeFilter() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy categoryHierarchy = mock(RolapHierarchy.class);
        RolapHierarchy producerHierarchy = mock(RolapHierarchy.class);
        Dimension standardDimension = mock(Dimension.class);
        RolapLevel producerLevel =
            mockLevel(producerHierarchy, "Producer", "[Producer].[Producer]", 2);
        RolapLevel skuLevel =
            mockLevel(categoryHierarchy, "Sku", "[Product].[Sku]", 4);
        Annotation dependsOnAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(standardDimension.getDimensionType()).thenReturn(DimensionType.StandardDimension);
        when(categoryHierarchy.getUniqueName()).thenReturn("[Product]");
        when(producerHierarchy.getUniqueName()).thenReturn("[Producer]");
        when(categoryHierarchy.getDimension()).thenReturn(standardDimension);
        when(producerHierarchy.getDimension()).thenReturn(standardDimension);

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies())
            .thenReturn(Arrays.asList(producerHierarchy, categoryHierarchy));
        when(producerHierarchy.getLevels()).thenReturn(new RolapLevel[] { producerLevel });
        when(categoryHierarchy.getLevels()).thenReturn(new RolapLevel[] { skuLevel });

        when(dependsOnAnnotation.getValue()).thenReturn(
            "[Producer].[Producer]|property:ProducerKey");
        annotations.put(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION, dependsOnAnnotation);
        when(skuLevel.getAnnotationMap()).thenReturn(annotations);
        when(producerLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(skuLevel.hasMemberProperty("ProducerKey")).thenReturn(true);
        when(skuLevel.isMemberPropertyFunctionallyDependent("ProducerKey")).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);

        assertTrue(hasIssueCode(
            registry,
            "CROSS_HIERARCHY_PROPERTY_RULE_WITHOUT_TIME_FILTER"));
    }

    public void testV2BuilderMarksAmbiguousJoinPathForCrossHierarchyPropertyRule() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy categoryHierarchy = mock(RolapHierarchy.class);
        RolapHierarchy producerHierarchy = mock(RolapHierarchy.class);
        Dimension standardDimension = mock(Dimension.class);
        RolapLevel producerLevel =
            mockLevel(producerHierarchy, "Producer", "[Producer].[Producer]", 2);
        RolapLevel skuLevel =
            mockLevel(categoryHierarchy, "Sku", "[Product].[Sku]", 4);
        Annotation dependsOnAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(standardDimension.getDimensionType()).thenReturn(DimensionType.StandardDimension);
        when(categoryHierarchy.getUniqueName()).thenReturn("[Product]");
        when(producerHierarchy.getUniqueName()).thenReturn("[Producer]");
        when(categoryHierarchy.getDimension()).thenReturn(standardDimension);
        when(producerHierarchy.getDimension()).thenReturn(standardDimension);

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies())
            .thenReturn(Arrays.asList(producerHierarchy, categoryHierarchy));
        when(producerHierarchy.getLevels()).thenReturn(new RolapLevel[] { producerLevel });
        when(categoryHierarchy.getLevels()).thenReturn(new RolapLevel[] { skuLevel });

        when(dependsOnAnnotation.getValue()).thenReturn(
            "[Producer].[Producer]|property:ProducerKey|requiresTimeFilter");
        annotations.put(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION, dependsOnAnnotation);
        when(skuLevel.getAnnotationMap()).thenReturn(annotations);
        when(producerLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(skuLevel.hasMemberProperty("ProducerKey")).thenReturn(true);
        when(skuLevel.isMemberPropertyFunctionallyDependent("ProducerKey")).thenReturn(true);
        when(skuLevel.hasStableSingleTableAnchor()).thenReturn(false);
        when(producerLevel.hasStableSingleTableAnchor()).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);
        DependencyRegistry.LevelDependencyDescriptor skuDescriptor =
            registry.getLevelDescriptor("[Product].[Sku]");

        assertNotNull(skuDescriptor);
        assertTrue(skuDescriptor.isAmbiguousJoinPath());
        assertEquals(1, skuDescriptor.getRules().size());
        assertTrue(skuDescriptor.getRules().get(0).isAmbiguousJoinPath());
        assertFalse(skuDescriptor.getRules().get(0).isValidated());
        assertEquals(
            DependencyRegistry.DependencyIssueCodes.AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH,
            skuDescriptor.getRules().get(0).getValidationCode());
        assertTrue(hasIssueCode(registry, "AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH"));
    }

    public void testV2BuilderKeepsOtherRuleValidWhenOneRuleHasAmbiguousJoinPath() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy productHierarchy = mock(RolapHierarchy.class);
        RolapHierarchy producerHierarchy = mock(RolapHierarchy.class);
        Dimension standardDimension = mock(Dimension.class);
        RolapLevel categoryLevel =
            mockLevel(productHierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel producerLevel =
            mockLevel(producerHierarchy, "Producer", "[Producer].[Producer]", 2);
        RolapLevel skuLevel =
            mockLevel(productHierarchy, "Sku", "[Product].[Sku]", 4);
        Annotation dependsOnAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(standardDimension.getDimensionType()).thenReturn(DimensionType.StandardDimension);
        when(productHierarchy.getUniqueName()).thenReturn("[Product]");
        when(producerHierarchy.getUniqueName()).thenReturn("[Producer]");
        when(productHierarchy.getDimension()).thenReturn(standardDimension);
        when(producerHierarchy.getDimension()).thenReturn(standardDimension);

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies())
            .thenReturn(Arrays.asList(producerHierarchy, productHierarchy));
        when(producerHierarchy.getLevels()).thenReturn(new RolapLevel[] { producerLevel });
        when(productHierarchy.getLevels()).thenReturn(new RolapLevel[] { categoryLevel, skuLevel });

        when(dependsOnAnnotation.getValue()).thenReturn(
            "[Producer].[Producer]|property:ProducerKey|requiresTimeFilter;"
                + "[Product].[Category]|ancestor");
        annotations.put(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION, dependsOnAnnotation);
        when(skuLevel.getAnnotationMap()).thenReturn(annotations);
        when(producerLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(categoryLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(skuLevel.hasMemberProperty("ProducerKey")).thenReturn(true);
        when(skuLevel.isMemberPropertyFunctionallyDependent("ProducerKey")).thenReturn(true);
        when(skuLevel.hasStableSingleTableAnchor()).thenReturn(false);
        when(producerLevel.hasStableSingleTableAnchor()).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);
        DependencyRegistry.LevelDependencyDescriptor skuDescriptor =
            registry.getLevelDescriptor("[Product].[Sku]");

        assertNotNull(skuDescriptor);
        assertEquals(2, skuDescriptor.getRules().size());
        assertTrue(skuDescriptor.isAmbiguousJoinPath());

        DependencyRegistry.CompiledDependencyRule producerRule = skuDescriptor.getRules().get(0);
        DependencyRegistry.CompiledDependencyRule categoryRule = skuDescriptor.getRules().get(1);

        assertEquals("[Producer].[Producer]", producerRule.getDeterminantLevelName());
        assertTrue(producerRule.isAmbiguousJoinPath());
        assertFalse(producerRule.isValidated());
        assertEquals(
            DependencyRegistry.DependencyIssueCodes.AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH,
            producerRule.getValidationCode());

        assertEquals("[Product].[Category]", categoryRule.getDeterminantLevelName());
        assertFalse(categoryRule.isAmbiguousJoinPath());
        assertTrue(categoryRule.isValidated());
        assertNull(categoryRule.getValidationCode());
    }

    public void testV2BuilderWarnsWhenRequiresTimeFilterButCubeHasNoTimeDimension() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        Dimension standardDimension = mock(Dimension.class);
        RolapLevel categoryLevel =
            mockLevel(hierarchy, "Category", "[Product].[Category]", 2);
        RolapLevel skuLevel =
            mockLevel(hierarchy, "Sku", "[Product].[Sku]", 4);
        Annotation dependsOnAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(standardDimension.getDimensionType()).thenReturn(DimensionType.StandardDimension);
        when(hierarchy.getUniqueName()).thenReturn("[Product]");
        when(hierarchy.getDimension()).thenReturn(standardDimension);
        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies()).thenReturn(Arrays.asList(hierarchy));
        when(hierarchy.getLevels()).thenReturn(new RolapLevel[] { categoryLevel, skuLevel });

        when(dependsOnAnnotation.getValue()).thenReturn(
            "[Product].[Category]|property:CategoryKey|requiresTimeFilter");
        annotations.put(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION, dependsOnAnnotation);
        when(skuLevel.getAnnotationMap()).thenReturn(annotations);
        when(categoryLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(skuLevel.hasMemberProperty("CategoryKey")).thenReturn(true);
        when(skuLevel.isMemberPropertyFunctionallyDependent("CategoryKey")).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);

        assertTrue(hasIssueCode(
            registry,
            "REQUIRES_TIME_FILTER_WITHOUT_TIME_DIMENSION"));
    }

    public void testV2BuilderMarksChainDeclaredForDependsOnChain() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel cityLevel =
            mockLevel(hierarchy, "City", "[Geo].[City]", 2);
        RolapLevel regionLevel =
            mockLevel(hierarchy, "Region", "[Geo].[Region]", 3);
        RolapLevel storeLevel =
            mockLevel(hierarchy, "Store", "[Geo].[Store]", 4);
        Annotation chainAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies()).thenReturn(Arrays.asList(hierarchy));
        when(hierarchy.getUniqueName()).thenReturn("[Geo]");
        when(hierarchy.getLevels()).thenReturn(
            new RolapLevel[] { cityLevel, regionLevel, storeLevel });

        when(chainAnnotation.getValue()).thenReturn(
            "[Geo].[City]=city_id > [Geo].[Region]=region_id");
        annotations.put("drilldown.dependsOnChain", chainAnnotation);
        when(storeLevel.getAnnotationMap()).thenReturn(annotations);
        when(cityLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(regionLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(storeLevel.hasMemberProperty("city_id")).thenReturn(true);
        when(storeLevel.hasMemberProperty("region_id")).thenReturn(true);
        when(storeLevel.isMemberPropertyFunctionallyDependent("city_id")).thenReturn(true);
        when(storeLevel.isMemberPropertyFunctionallyDependent("region_id")).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);
        DependencyRegistry.LevelDependencyDescriptor storeDescriptor =
            registry.getLevelDescriptor("[Geo].[Store]");

        assertNotNull(storeDescriptor);
        assertTrue(storeDescriptor.isChainDeclared());
        assertEquals(2, storeDescriptor.getRules().size());
        assertEquals("[Geo].[City]", storeDescriptor.getRules().get(0).getDeterminantLevelName());
        assertEquals("[Geo].[Region]", storeDescriptor.getRules().get(1).getDeterminantLevelName());
    }

    public void testV2BuilderInfersDependsOnChainPropertyNames() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel cityLevel =
            mockLevel(hierarchy, "City", "[Geo].[City]", 2);
        RolapLevel regionLevel =
            mockLevel(hierarchy, "Region", "[Geo].[Region]", 3);
        RolapLevel storeLevel =
            mockLevel(hierarchy, "Store", "[Geo].[Store]", 4);
        Annotation chainAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies()).thenReturn(Arrays.asList(hierarchy));
        when(hierarchy.getUniqueName()).thenReturn("[Geo]");
        when(hierarchy.getLevels()).thenReturn(
            new RolapLevel[] { cityLevel, regionLevel, storeLevel });

        when(chainAnnotation.getValue()).thenReturn(
            "[Geo].[City] > [Geo].[Region]|requiresTimeFilter");
        annotations.put("drilldown.dependsOnChain", chainAnnotation);
        when(storeLevel.getAnnotationMap()).thenReturn(annotations);
        when(cityLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(regionLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(storeLevel.findFunctionallyDependentPropertyCandidatesForDeterminantLevel(cityLevel))
            .thenReturn(new String[] { "city_id" });
        when(storeLevel.findFunctionallyDependentPropertyCandidatesForDeterminantLevel(regionLevel))
            .thenReturn(new String[] { "region_id" });
        when(storeLevel.hasMemberProperty("city_id")).thenReturn(true);
        when(storeLevel.hasMemberProperty("region_id")).thenReturn(true);
        when(storeLevel.isMemberPropertyFunctionallyDependent("city_id")).thenReturn(true);
        when(storeLevel.isMemberPropertyFunctionallyDependent("region_id")).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);
        DependencyRegistry.LevelDependencyDescriptor storeDescriptor =
            registry.getLevelDescriptor("[Geo].[Store]");

        assertNotNull(storeDescriptor);
        assertTrue(storeDescriptor.isChainDeclared());
        assertEquals(2, storeDescriptor.getRules().size());

        DependencyRegistry.CompiledDependencyRule cityRule = storeDescriptor.getRules().get(0);
        DependencyRegistry.CompiledDependencyRule regionRule = storeDescriptor.getRules().get(1);
        assertEquals("city_id", cityRule.getMappingProperty());
        assertEquals("region_id", regionRule.getMappingProperty());
        assertTrue(cityRule.isValidated());
        assertTrue(regionRule.isValidated());
        assertTrue(cityRule.requiresTimeFilter());
        assertTrue(regionRule.requiresTimeFilter());
    }

    public void testV2BuilderDoesNotMarkChainDeclaredForLegacyDependsOnOnly() {
        RolapCube cube = mock(RolapCube.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        RolapLevel cityLevel =
            mockLevel(hierarchy, "City", "[Geo].[City]", 2);
        RolapLevel storeLevel =
            mockLevel(hierarchy, "Store", "[Geo].[Store]", 4);
        Annotation dependsOnAnnotation = mock(Annotation.class);
        Map<String, Annotation> annotations = new HashMap<String, Annotation>();

        when(cube.getUniqueName()).thenReturn("[Cube]");
        when(cube.getHierarchies()).thenReturn(Arrays.asList(hierarchy));
        when(hierarchy.getUniqueName()).thenReturn("[Geo]");
        when(hierarchy.getLevels()).thenReturn(
            new RolapLevel[] { cityLevel, storeLevel });

        when(dependsOnAnnotation.getValue()).thenReturn("[Geo].[City]|property:city_id");
        annotations.put(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION, dependsOnAnnotation);
        when(storeLevel.getAnnotationMap()).thenReturn(annotations);
        when(cityLevel.getAnnotationMap()).thenReturn(Collections.<String, Annotation>emptyMap());
        when(storeLevel.hasMemberProperty("city_id")).thenReturn(true);
        when(storeLevel.isMemberPropertyFunctionallyDependent("city_id")).thenReturn(true);

        DependencyRegistry registry = new DependencyRegistryBuilder().build(cube);
        DependencyRegistry.LevelDependencyDescriptor storeDescriptor =
            registry.getLevelDescriptor("[Geo].[Store]");

        assertNotNull(storeDescriptor);
        assertFalse(storeDescriptor.isChainDeclared());
        assertEquals(1, storeDescriptor.getRules().size());
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
        MondrianProperties.instance().MaxConstraints.set(1000);
        when(level.isSimple()).thenReturn(true);
        when(level.isParentChild()).thenReturn(false);
        for (RolapMember member : members) {
            when(member.getLevel()).thenReturn(level);
            when(member.isNull()).thenReturn(false);
            when(member.isAll()).thenReturn(false);
            when(member.isCalculated()).thenReturn(false);
            when(member.isParentChildLeaf()).thenReturn(false);
        }
        // Build the arg reflectively in src/test copy so targeted V2 tests are
        // isolated from MemberListCrossJoinArg.create(...) behavior differences.
        try {
            Constructor<MemberListCrossJoinArg> ctor = MemberListCrossJoinArg.class
                .getDeclaredConstructor(
                    RolapLevel.class,
                    List.class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    boolean.class,
                    boolean.class);
            ctor.setAccessible(true);
            return ctor.newInstance(
                level,
                Arrays.asList(members),
                false,
                false,
                true,
                false,
                false);
        } catch (Exception e) {
            throw new AssertionError("Failed to construct MemberListCrossJoinArg", e);
        }
    }

    private static boolean hasIssueCode(DependencyRegistry registry, String code) {
        for (DependencyRegistry.DependencyValidationIssue issue : registry.getIssues()) {
            if (issue != null && code.equals(issue.getCode())) {
                return true;
            }
        }
        return false;
    }
}

// End CrossJoinDependencyPrunerTest.java
