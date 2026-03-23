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

import junit.framework.TestCase;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Syntax;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShareMeasurePeerHierarchyTupleNormalizerTest extends TestCase {
    public void testNormalizeInjectsMissingPeersBeforeMeasure() {
        final RolapDimension productDimension = mockDimension("Product", false);
        final RolapDimension measuresDimension = mockDimension("Measures", true);
        final RolapHierarchy familyHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Family]");
        final RolapHierarchy brandHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Brand]");
        final RolapHierarchy skuHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Sku]");
        final RolapHierarchy measuresHierarchy =
            mockHierarchy(measuresDimension, "[Measures]");

        final Exp tuple =
            tuple(
                memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Drink]")),
                memberExpr(mockMember(brandHierarchy, "[Product Flat].[Brand].[All Brands]")),
                memberExpr(mockMember(measuresHierarchy, "[Measures].[Unit Sales]")));

        final String normalized =
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedTupleMdx(
                tuple,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.asList(
                        mockMember(skuHierarchy, "[Product Flat].[Sku].[All Skus]"))));

        assertEquals(
            "([Product Flat].[Family].[Drink], [Product Flat].[Brand].[All Brands], "
                + "[Product Flat].[Sku].[All Skus], [Measures].[Unit Sales])",
            normalized);
    }

    public void testNormalizeReturnsOriginalExpressionForNonTupleShape() {
        final Exp nonTuple = memberExpr(mockMember(
            mockHierarchy(mockDimension("Product", false), "[Product Flat].[Brand]"),
            "[Product Flat].[Brand].[All Brands]"));

        assertNull(
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedTupleMdx(
                nonTuple,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.<Member>asList(mock(Member.class)))));
    }

    public void testNormalizeSkipsInjectionWhenHierarchyAlreadyPresentInTuple() {
        final RolapDimension productDimension = mockDimension("Product", false);
        final RolapHierarchy familyHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Family]");
        final RolapHierarchy brandHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Brand]");
        final RolapHierarchy skuHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Sku]");

        final Member skuAll = mockMember(skuHierarchy, "[Product Flat].[Sku].[All Skus]");
        final Exp tuple =
            tuple(
                memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Drink]")),
                memberExpr(mockMember(brandHierarchy, "[Product Flat].[Brand].[All Brands]")),
                memberExpr(skuAll));

        assertNull(
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedTupleMdx(
                tuple,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.asList(skuAll))));
    }

    public void testNormalizeInjectsDefaultMemberPropertyForHierarchyDefault() {
        final RolapDimension productDimension = mockDimension("Product", false);
        final RolapDimension measuresDimension = mockDimension("Measures", true);
        final RolapHierarchy familyHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Family]");
        final RolapHierarchy cityHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[City]");
        final RolapHierarchy measuresHierarchy =
            mockHierarchy(measuresDimension, "[Measures]");
        final RolapMember cityDefault =
            mockMember(cityHierarchy, "[Product Flat].[City].[All Cities]");
        when(cityHierarchy.getDefaultMember()).thenReturn(cityDefault);

        final Exp tuple =
            tuple(
                memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Drink]")),
                memberExpr(mockMember(measuresHierarchy, "[Measures].[Unit Sales]")));

        final String normalized =
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedTupleMdx(
                tuple,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.<Member>asList(cityDefault)));

        assertEquals(
            "([Product Flat].[Family].[Drink], [Product Flat].[City].DefaultMember, [Measures].[Unit Sales])",
            normalized);
    }

    private static MemberExpr memberExpr(Member member) {
        return new MemberExpr(member);
    }

    private static Exp tuple(Exp... args) {
        return new UnresolvedFunCall("()", Syntax.Parentheses, args);
    }

    private static RolapDimension mockDimension(String name, boolean isMeasures) {
        final RolapDimension dimension = mock(RolapDimension.class);
        when(dimension.getName()).thenReturn(name);
        when(dimension.isMeasures()).thenReturn(isMeasures);
        return dimension;
    }

    private static RolapHierarchy mockHierarchy(
        RolapDimension dimension,
        String uniqueName)
    {
        final RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        when(hierarchy.getDimension()).thenReturn(dimension);
        when(hierarchy.getUniqueName()).thenReturn(uniqueName);
        when(hierarchy.getName()).thenReturn(
            uniqueName.substring(uniqueName.lastIndexOf('.') + 1, uniqueName.length() - 1));
        return hierarchy;
    }

    private static RolapMember mockMember(Hierarchy hierarchy, String uniqueName) {
        final RolapMember member = mock(RolapMember.class);
        final RolapLevel level = mock(RolapLevel.class);
        final RolapDimension dimension = (RolapDimension) hierarchy.getDimension();
        when(level.getHierarchy()).thenReturn((RolapHierarchy) hierarchy);
        when(member.getHierarchy()).thenReturn((RolapHierarchy) hierarchy);
        when(member.getLevel()).thenReturn(level);
        when(member.getDimension()).thenReturn(dimension);
        when(member.getUniqueName()).thenReturn(uniqueName);
        return member;
    }
}
