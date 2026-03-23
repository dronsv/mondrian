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

        final CrossJoinDependsOnChainOrderer.OrderingPlan orderingPlan =
            CrossJoinDependsOnChainOrderer.buildOrderingPlan(args, context);

        assertTrue(orderingPlan.hasApplicableChain());
        assertEquals(0, orderingPlan.getDeterminantColumns(1).length);
        assertEquals(1, orderingPlan.getDeterminantColumns(2).length);
        assertEquals(0, orderingPlan.getDeterminantColumns(2)[0]);
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
        final RolapMember member = mock(RolapMember.class);
        when(member.getLevel()).thenReturn(level);
        when(member.getKey()).thenReturn(key);
        when(member.getUniqueName()).thenReturn(String.valueOf(key));
        when(member.getOrderKey()).thenReturn(Integer.valueOf(order));
        when(member.getOrdinal()).thenReturn(order);
        when(member.isCalculatedInQuery()).thenReturn(false);
        return member;
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
