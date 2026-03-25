/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql.dependency;

import junit.framework.TestCase;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.sql.CrossJoinArg;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrossJoinDependsOnChainOrdererTest extends TestCase {
    private boolean previousFlagValue;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        previousFlagValue =
            MondrianProperties.instance().CrossJoinOrderByDependsOnChain.get();
        MondrianProperties.instance().CrossJoinOrderByDependsOnChain.set(true);
    }

    @Override
    protected void tearDown() throws Exception {
        MondrianProperties.instance().CrossJoinOrderByDependsOnChain.set(
            previousFlagValue);
        super.tearDown();
    }

    public void testMaybeOrderClustersRowsWhenValidatedChainIsPresent() {
        final RolapLevel manufacturerLevel =
            mockLevel("[Product.Manufacturer].[Manufacturer]");
        final RolapLevel brandLevel =
            mockLevel("[Product.Brand].[Brand]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(manufacturerLevel),
            mockArg(brandLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        brandLevel.getUniqueName(),
                        "[Product.Brand]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                manufacturerLevel.getUniqueName(),
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "manufacturer_key",
                                true,
                                false)),
                        false,
                        true))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final RolapMember manufacturerA = mockMember(manufacturerLevel, "mfr-a", 1);
        final RolapMember manufacturerB = mockMember(manufacturerLevel, "mfr-b", 2);
        final RolapMember brandBuket = mockMember(brandLevel, "brand-b", 1);
        final RolapMember brandKlassika = mockMember(brandLevel, "brand-k", 2);
        final RolapMember brandTraditsiya = mockMember(brandLevel, "brand-t", 3);
        final RolapMember brandNaslazhdenie = mockMember(brandLevel, "brand-m", 4);

        final TupleList tupleList = TupleCollections.createList(2, 4);
        tupleList.addTuple(manufacturerA, brandBuket);
        tupleList.addTuple(manufacturerA, brandKlassika);
        tupleList.addTuple(manufacturerB, brandNaslazhdenie);
        tupleList.addTuple(manufacturerA, brandTraditsiya);

        final TupleList ordered =
            CrossJoinDependsOnChainOrderer.maybeOrder(tupleList, args, context);

        assertTuple(ordered, 0, manufacturerA, brandBuket);
        assertTuple(ordered, 1, manufacturerA, brandKlassika);
        assertTuple(ordered, 2, manufacturerA, brandTraditsiya);
        assertTuple(ordered, 3, manufacturerB, brandNaslazhdenie);
    }

    public void testMaybeOrderDoesNothingWithoutChainRule() {
        final RolapLevel manufacturerLevel =
            mockLevel("[Product.Manufacturer].[Manufacturer]");
        final RolapLevel brandLevel =
            mockLevel("[Product.Brand].[Brand]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(manufacturerLevel),
            mockArg(brandLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        brandLevel.getUniqueName(),
                        "[Product.Brand]",
                        1,
                        Collections.<DependencyRegistry.CompiledDependencyRule>emptyList(),
                        false,
                        false))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final RolapMember manufacturerA = mockMember(manufacturerLevel, "mfr-a", 1);
        final RolapMember manufacturerB = mockMember(manufacturerLevel, "mfr-b", 2);
        final RolapMember brandBuket = mockMember(brandLevel, "brand-b", 1);
        final RolapMember brandNaslazhdenie = mockMember(brandLevel, "brand-m", 2);

        final TupleList tupleList = TupleCollections.createList(2, 2);
        tupleList.addTuple(manufacturerB, brandNaslazhdenie);
        tupleList.addTuple(manufacturerA, brandBuket);

        final TupleList ordered =
            CrossJoinDependsOnChainOrderer.maybeOrder(tupleList, args, context);

        assertTuple(ordered, 0, manufacturerB, brandNaslazhdenie);
        assertTuple(ordered, 1, manufacturerA, brandBuket);
    }

    public void testMaybeOrderDoesNothingWhenFlagIsDisabled() {
        MondrianProperties.instance().CrossJoinOrderByDependsOnChain.set(false);

        final RolapLevel manufacturerLevel =
            mockLevel("[Product.Manufacturer].[Manufacturer]");
        final RolapLevel brandLevel =
            mockLevel("[Product.Brand].[Brand]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(manufacturerLevel),
            mockArg(brandLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        brandLevel.getUniqueName(),
                        "[Product.Brand]",
                        1,
                        Arrays.asList(
                            new DependencyRegistry.CompiledDependencyRule(
                                manufacturerLevel.getUniqueName(),
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "manufacturer_key",
                                true,
                                false)),
                        false,
                        true))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final RolapMember manufacturerB = mockMember(manufacturerLevel, "mfr-b", 2);
        final RolapMember manufacturerA = mockMember(manufacturerLevel, "mfr-a", 1);
        final RolapMember brandNaslazhdenie = mockMember(brandLevel, "brand-m", 4);
        final RolapMember brandBuket = mockMember(brandLevel, "brand-b", 1);

        final TupleList tupleList = TupleCollections.createList(2, 2);
        tupleList.addTuple(manufacturerB, brandNaslazhdenie);
        tupleList.addTuple(manufacturerA, brandBuket);

        final TupleList ordered =
            CrossJoinDependsOnChainOrderer.maybeOrder(tupleList, args, context);

        assertTuple(ordered, 0, manufacturerB, brandNaslazhdenie);
        assertTuple(ordered, 1, manufacturerA, brandBuket);
    }

    public void testOrderingPlanUsesNonAdjacentEarlierDeterminants() {
        final RolapLevel manufacturerLevel =
            mockLevel("[Product.Manufacturer].[Manufacturer]");
        final RolapLevel weightLevel =
            mockLevel("[Product.Weight].[Weight]");
        final RolapLevel skuLevel =
            mockLevel("[Product.SKU].[SKU]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(manufacturerLevel),
            mockArg(weightLevel),
            mockArg(skuLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        skuLevel.getUniqueName(),
                        "[Product.SKU]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                manufacturerLevel.getUniqueName(),
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "manufacturer_key",
                                true,
                                false)),
                        false,
                        true))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final DynamicDrilldepHierarchyPlan hierarchyPlan =
            CrossJoinDependsOnChainOrderer.buildHierarchyPlan(args, context);

        assertTrue(hierarchyPlan.hasApplicableChain());
        assertEquals(
            DynamicDrilldepHierarchyPlan.ShapeClass.PROJECTED_PREFIX,
            hierarchyPlan.getShapeClass());
        assertEquals(0, hierarchyPlan.getDeterminantColumns(1).length);
        assertEquals(1, hierarchyPlan.getDeterminantColumns(2).length);
        assertEquals(0, hierarchyPlan.getDeterminantColumns(2)[0]);
        assertEquals(0, hierarchyPlan.getHiddenDeterminantProperties(2).length);
    }

    public void testMaybeOrderUsesCompositeSignatureForOverlappingChains() {
        final RolapLevel manufacturerLevel =
            mockLevel("[Product.Manufacturer].[Manufacturer]");
        final RolapLevel categoryLevel =
            mockLevel("[Product.Category].[Category]");
        final RolapLevel skuLevel =
            mockLevel("[Product.SKU].[SKU]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(manufacturerLevel),
            mockArg(categoryLevel),
            mockArg(skuLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        skuLevel.getUniqueName(),
                        "[Product.SKU]",
                        1,
                        Arrays.asList(
                            new DependencyRegistry.CompiledDependencyRule(
                                manufacturerLevel.getUniqueName(),
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "manufacturer_key",
                                true,
                                false),
                            new DependencyRegistry.CompiledDependencyRule(
                                categoryLevel.getUniqueName(),
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "category_key",
                                true,
                                false)),
                        false,
                        true))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final RolapMember manufacturerA = mockMember(manufacturerLevel, "mfr-a", 1);
        final RolapMember manufacturerB = mockMember(manufacturerLevel, "mfr-b", 2);
        final RolapMember categoryA = mockMember(categoryLevel, "cat-a", 1);
        final RolapMember categoryB = mockMember(categoryLevel, "cat-b", 2);
        final RolapMember sku1 = mockMember(skuLevel, "sku-1", 1);
        final RolapMember sku2 = mockMember(skuLevel, "sku-2", 2);
        final RolapMember sku3 = mockMember(skuLevel, "sku-3", 3);
        final RolapMember sku4 = mockMember(skuLevel, "sku-4", 4);

        final TupleList tupleList = TupleCollections.createList(3, 4);
        tupleList.addTuple(manufacturerB, categoryA, sku4);
        tupleList.addTuple(manufacturerA, categoryB, sku3);
        tupleList.addTuple(manufacturerA, categoryA, sku2);
        tupleList.addTuple(manufacturerA, categoryA, sku1);

        final TupleList ordered =
            CrossJoinDependsOnChainOrderer.maybeOrder(tupleList, args, context);

        assertTuple(ordered, 0, manufacturerA, categoryA, sku1);
        assertTuple(ordered, 1, manufacturerA, categoryA, sku2);
        assertTuple(ordered, 2, manufacturerA, categoryB, sku3);
        assertTuple(ordered, 3, manufacturerB, categoryA, sku4);
    }

    public void testOrderingPlanUsesHiddenPropertyWhenDeterminantIsNotProjected() {
        final RolapLevel brandLevel =
            mockLevel("[Product.Brand].[Brand]");
        final RolapLevel skuLevel =
            mockLevel("[Product.SKU].[SKU]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(brandLevel),
            mockArg(skuLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        brandLevel.getUniqueName(),
                        "[Product.Brand]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                "[Product.Manufacturer].[Manufacturer]",
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "manufacturer_group",
                                true,
                                false)),
                        false))
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        skuLevel.getUniqueName(),
                        "[Product.SKU]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                brandLevel.getUniqueName(),
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "brand_key",
                                true,
                                false)),
                        false))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final DynamicDrilldepHierarchyPlan hierarchyPlan =
            CrossJoinDependsOnChainOrderer.buildHierarchyPlan(args, context);

        assertTrue(hierarchyPlan.hasApplicableChain());
        assertEquals(
            DynamicDrilldepHierarchyPlan.ShapeClass.MIXED,
            hierarchyPlan.getShapeClass());
        assertEquals(0, hierarchyPlan.getDeterminantColumns(0).length);
        assertEquals(1, hierarchyPlan.getHiddenDeterminantProperties(0).length);
        assertEquals(
            "manufacturer_group",
            hierarchyPlan.getHiddenDeterminantProperties(0)[0]);
        assertEquals(1, hierarchyPlan.getDeterminantColumns(1).length);
        assertEquals(0, hierarchyPlan.getDeterminantColumns(1)[0]);
    }

    public void testHierarchyPlanClassifiesPureHiddenDeterminantShape() {
        final RolapLevel brandLevel =
            mockLevel("[Product.Brand].[Brand]");
        final RolapLevel weightLevel =
            mockLevel("[Product.Weight].[Weight]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(brandLevel),
            mockArg(weightLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        brandLevel.getUniqueName(),
                        "[Product.Brand]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                "[Product.Manufacturer].[Manufacturer]",
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "manufacturer_group",
                                true,
                                false)),
                        false))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final DynamicDrilldepHierarchyPlan hierarchyPlan =
            DynamicDrilldepHierarchyPlan.build(args, context);

        assertTrue(hierarchyPlan.hasApplicableChain());
        assertEquals(
            DynamicDrilldepHierarchyPlan.ShapeClass.HIDDEN_DETERMINANT,
            hierarchyPlan.getShapeClass());
    }

    public void testMaybeOrderUsesHiddenPropertyForFirstVisibleLevel() {
        final RolapLevel brandLevel =
            mockLevel("[Product.Brand].[Brand]");
        final RolapLevel skuLevel =
            mockLevel("[Product.SKU].[SKU]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(brandLevel),
            mockArg(skuLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        brandLevel.getUniqueName(),
                        "[Product.Brand]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                "[Product.Manufacturer].[Manufacturer]",
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "manufacturer_group",
                                true,
                                false)),
                        false))
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        skuLevel.getUniqueName(),
                        "[Product.SKU]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                brandLevel.getUniqueName(),
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "brand_key",
                                true,
                                false)),
                        false))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final RolapMember brandSharm =
            mockMember(
                brandLevel,
                "brand-sharm",
                1,
                propertyMap("manufacturer_group", Integer.valueOf(2)));
        final RolapMember brandTraditsiya =
            mockMember(
                brandLevel,
                "brand-traditsiya",
                2,
                propertyMap("manufacturer_group", Integer.valueOf(1)));
        final RolapMember brandZefir =
            mockMember(
                brandLevel,
                "brand-zefir",
                3,
                propertyMap("manufacturer_group", Integer.valueOf(1)));
        final RolapMember skuA = mockMember(skuLevel, "sku-a", 1);
        final RolapMember skuB = mockMember(skuLevel, "sku-b", 2);
        final RolapMember skuC = mockMember(skuLevel, "sku-c", 3);

        final TupleList tupleList = TupleCollections.createList(2, 3);
        tupleList.addTuple(brandSharm, skuA);
        tupleList.addTuple(brandTraditsiya, skuB);
        tupleList.addTuple(brandZefir, skuC);

        final TupleList ordered =
            CrossJoinDependsOnChainOrderer.maybeOrder(tupleList, args, context);

        assertTuple(ordered, 0, brandTraditsiya, skuB);
        assertTuple(ordered, 1, brandZefir, skuC);
        assertTuple(ordered, 2, brandSharm, skuA);
    }

    public void testMaybeOrderUsesHiddenPropertyWhenPruningMarkedRuleAmbiguous() {
        final RolapLevel brandLevel =
            mockLevel("[Product.Brand].[Brand]");
        final RolapLevel skuLevel =
            mockLevel("[Product.SKU].[SKU]");
        final CrossJoinArg[] args = new CrossJoinArg[] {
            mockArg(brandLevel),
            mockArg(skuLevel)
        };
        final DependencyRegistry registry =
            DependencyRegistry.builder("[Cube]")
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        brandLevel.getUniqueName(),
                        "[Product.Brand]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                "[Product.Manufacturer].[Manufacturer]",
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "manufacturer_group",
                                false,
                                false,
                                true,
                                DependencyRegistry.DependencyIssueCodes
                                    .AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH)),
                        true))
                .addLevelDescriptor(
                    new DependencyRegistry.LevelDependencyDescriptor(
                        skuLevel.getUniqueName(),
                        "[Product.SKU]",
                        1,
                        Collections.singletonList(
                            new DependencyRegistry.CompiledDependencyRule(
                                brandLevel.getUniqueName(),
                                DependencyRegistry.DependencyMappingType.PROPERTY,
                                "brand_key",
                                true,
                                false)),
                        false))
                .build();
        final DependencyPruningContext context =
            DependencyPruningContext.of(
                null,
                registry,
                DependencyRegistry.DependencyPruningPolicy.RELAXED,
                false);

        final RolapMember brandSharm =
            mockMember(
                brandLevel,
                "brand-sharm",
                1,
                propertyMap("manufacturer_group", Integer.valueOf(2)));
        final RolapMember brandTraditsiya =
            mockMember(
                brandLevel,
                "brand-traditsiya",
                2,
                propertyMap("manufacturer_group", Integer.valueOf(1)));
        final RolapMember brandZefir =
            mockMember(
                brandLevel,
                "brand-zefir",
                3,
                propertyMap("manufacturer_group", Integer.valueOf(1)));
        final RolapMember skuA = mockMember(skuLevel, "sku-a", 1);
        final RolapMember skuB = mockMember(skuLevel, "sku-b", 2);
        final RolapMember skuC = mockMember(skuLevel, "sku-c", 3);

        final TupleList tupleList = TupleCollections.createList(2, 3);
        tupleList.addTuple(brandSharm, skuA);
        tupleList.addTuple(brandTraditsiya, skuB);
        tupleList.addTuple(brandZefir, skuC);

        final TupleList ordered =
            CrossJoinDependsOnChainOrderer.maybeOrder(tupleList, args, context);

        assertTuple(ordered, 0, brandTraditsiya, skuB);
        assertTuple(ordered, 1, brandZefir, skuC);
        assertTuple(ordered, 2, brandSharm, skuA);
    }

    private static RolapLevel mockLevel(String uniqueName) {
        final RolapLevel level = mock(RolapLevel.class);
        when(level.getUniqueName()).thenReturn(uniqueName);
        return level;
    }

    private static CrossJoinArg mockArg(RolapLevel level) {
        final CrossJoinArg arg = mock(CrossJoinArg.class);
        when(arg.getLevel()).thenReturn(level);
        when(arg.getMembers()).thenReturn(Collections.<RolapMember>emptyList());
        return arg;
    }

    private static RolapMember mockMember(
        RolapLevel level,
        Object key,
        int order)
    {
        return mockMember(level, key, order, Collections.<String, Object>emptyMap());
    }

    private static RolapMember mockMember(
        RolapLevel level,
        Object key,
        int order,
        Map<String, Object> properties)
    {
        final RolapMember member = mock(RolapMember.class);
        when(member.getLevel()).thenReturn(level);
        when(member.getKey()).thenReturn(key);
        when(member.getUniqueName()).thenReturn(String.valueOf(key));
        when(member.getOrderKey()).thenReturn(Integer.valueOf(order));
        when(member.getOrdinal()).thenReturn(order);
        when(member.isCalculatedInQuery()).thenReturn(false);
        if (properties != null) {
            for (Map.Entry<String, Object> entry : properties.entrySet()) {
                when(member.getPropertyValue(entry.getKey(), false))
                    .thenReturn(entry.getValue());
            }
        }
        return member;
    }

    private static Map<String, Object> propertyMap(String name, Object value) {
        final Map<String, Object> properties = new LinkedHashMap<String, Object>();
        properties.put(name, value);
        return properties;
    }

    private static void assertTuple(
        TupleList tupleList,
        int row,
        Member... expectedMembers)
    {
        for (int column = 0; column < expectedMembers.length; column++) {
            assertSame(expectedMembers[column], tupleList.get(column, row));
        }
    }
}
