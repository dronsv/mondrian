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
import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.Category;
import mondrian.olap.Exp;
import mondrian.olap.ExpBase;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;
import mondrian.mdx.MdxVisitor;

import java.io.PrintWriter;
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

    public void testNormalizeRewritesTupleBranchInsideIif() {
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
        final Exp expression =
            new UnresolvedFunCall(
                "IIf",
                Syntax.Function,
                new Exp[] {
                    memberExpr(mockMember(brandHierarchy, "[Product Flat].[Brand].[Current]")),
                    memberExpr(mockMember(measuresHierarchy, "[Measures].[Zero]")),
                    tuple
                });

        final String normalized =
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedExpressionMdx(
                expression,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.asList(
                        mockMember(skuHierarchy, "[Product Flat].[Sku].[All Skus]"))));

        assertEquals(
            "IIf([Product Flat].[Brand].[Current], [Measures].[Zero], "
                + "([Product Flat].[Family].[Drink], [Product Flat].[Brand].[All Brands], "
                + "[Product Flat].[Sku].[All Skus], [Measures].[Unit Sales]))",
            normalized);
    }

    public void testNormalizeRewritesTupleBranchInThenClause() {
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
        final Exp expression =
            new UnresolvedFunCall(
                "IIf",
                Syntax.Function,
                new Exp[] {
                    memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Current]")),
                    tuple,
                    memberExpr(mockMember(measuresHierarchy, "[Measures].[Zero]"))
                });

        final String normalized =
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedExpressionMdx(
                expression,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.asList(
                        mockMember(skuHierarchy, "[Product Flat].[Sku].[All Skus]"))));

        assertEquals(
            "IIf([Product Flat].[Family].[Current], "
                + "([Product Flat].[Family].[Drink], [Product Flat].[Brand].[All Brands], "
                + "[Product Flat].[Sku].[All Skus], [Measures].[Unit Sales]), "
                + "[Measures].[Zero])",
            normalized);
    }

    public void testNormalizeRewritesTupleBranchesOnBothSidesOfIif() {
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

        final Exp thenTuple =
            tuple(
                memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Drink]")),
                memberExpr(mockMember(brandHierarchy, "[Product Flat].[Brand].[All Brands]")),
                memberExpr(mockMember(measuresHierarchy, "[Measures].[Unit Sales]")));
        final Exp elseTuple =
            tuple(
                memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Food]")),
                memberExpr(mockMember(brandHierarchy, "[Product Flat].[Brand].[Alt Brands]")),
                memberExpr(mockMember(measuresHierarchy, "[Measures].[Store Sales]")));
        final Exp expression =
            new UnresolvedFunCall(
                "IIf",
                Syntax.Function,
                new Exp[] {
                    memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Current]")),
                    thenTuple,
                    elseTuple
                });

        final String normalized =
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedExpressionMdx(
                expression,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.asList(
                        mockMember(skuHierarchy, "[Product Flat].[Sku].[All Skus]"))));

        assertEquals(
            "IIf([Product Flat].[Family].[Current], "
                + "([Product Flat].[Family].[Drink], [Product Flat].[Brand].[All Brands], "
                + "[Product Flat].[Sku].[All Skus], [Measures].[Unit Sales]), "
                + "([Product Flat].[Family].[Food], [Product Flat].[Brand].[Alt Brands], "
                + "[Product Flat].[Sku].[All Skus], [Measures].[Store Sales]))",
            normalized);
    }

    public void testNormalizePreservesExplicitChildPinInsideTupleBranch() {
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
            new UnresolvedFunCall(
                "()",
                Syntax.Parentheses,
                new Exp[] {
                    new UnresolvedFunCall(
                        "CurrentMember",
                        Syntax.Property,
                        new Exp[] {
                            new HierarchyExpr(brandHierarchy) }),
                    memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Drink]")),
                    memberExpr(mockMember(measuresHierarchy, "[Measures].[Unit Sales]"))
                });
        final Exp expression =
            new UnresolvedFunCall(
                "IIf",
                Syntax.Function,
                new Exp[] {
                    memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Current]")),
                    tuple,
                    memberExpr(mockMember(measuresHierarchy, "[Measures].[Zero]"))
                });

        final String normalized =
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedExpressionMdx(
                expression,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.asList(
                        mockMember(brandHierarchy, "[Product Flat].[Brand].[All Brands]"),
                        mockMember(skuHierarchy, "[Product Flat].[Sku].[All Skus]"))));

        assertEquals(
            "IIf([Product Flat].[Family].[Current], "
                + "([Product Flat].[Brand].CurrentMember, [Product Flat].[Family].[Drink], "
                + "[Product Flat].[Sku].[All Skus], [Measures].[Unit Sales]), "
                + "[Measures].[Zero])",
            normalized);
    }

    public void testNormalizeCanonicalizesSupportedOrPropertiesGuard() {
        final RolapDimension productDimension = mockDimension("Product", false);
        final RolapDimension measuresDimension = mockDimension("Measures", true);
        final RolapHierarchy familyHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Family]");
        final RolapHierarchy brandHierarchy =
            mockHierarchy(productDimension, "[Product Flat].[Brand]");
        final RolapHierarchy measuresHierarchy =
            mockHierarchy(measuresDimension, "[Measures]");

        final Exp tuple =
            tuple(
                memberExpr(mockMember(familyHierarchy, "[Product Flat].[Family].[Drink]")),
                memberExpr(mockMember(measuresHierarchy, "[Measures].[Unit Sales]")));
        final Exp condition =
            new UnresolvedFunCall(
                "OR",
                Syntax.Infix,
                new Exp[] {
                    mdxLiteral(
                        "[Product Flat].[Brand].CurrentMember IS "
                            + "[Product Flat].[Brand].[All Brands]"),
                    mdxLiteral(
                        "VBA!Len([Product Flat].[Brand].CurrentMember"
                            + ".Properties(\"FamilyKey\")) = 0")
                });
        final Exp expression =
            new UnresolvedFunCall(
                "IIf",
                Syntax.Function,
                new Exp[] {
                    condition,
                    memberExpr(mockMember(measuresHierarchy, "[Measures].[Zero]")),
                    tuple
                });

        assertEquals(
            "IIf([Product Flat].[Brand].CurrentMember IS "
                + "[Product Flat].[Brand].[All Brands], [Measures].[Zero], "
                + "IIf(VBA!Len([Product Flat].[Brand].CurrentMember"
                + ".Properties(\"FamilyKey\")) = 0, [Measures].[Zero], "
                + "([Product Flat].[Family].[Drink], [Product Flat].[Brand].[All Brands], "
                + "[Measures].[Unit Sales])))",
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedExpressionMdx(
                expression,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.asList(
                        mockMember(brandHierarchy, "[Product Flat].[Brand].[All Brands]")))));
    }

    public void testNormalizeSkipsIifRewriteForMixedHierarchyGuardCondition() {
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
                memberExpr(mockMember(measuresHierarchy, "[Measures].[Unit Sales]")));
        final Exp condition =
            new UnresolvedFunCall(
                "OR",
                Syntax.Infix,
                new Exp[] {
                    mdxLiteral(
                        "[Product Flat].[Brand].CurrentMember IS "
                            + "[Product Flat].[Brand].[All Brands]"),
                    mdxLiteral(
                        "[Product Flat].[Sku].CurrentMember IS "
                            + "[Product Flat].[Sku].[All Skus]")
                });
        final Exp expression =
            new UnresolvedFunCall(
                "IIf",
                Syntax.Function,
                new Exp[] {
                    condition,
                    memberExpr(mockMember(measuresHierarchy, "[Measures].[Zero]")),
                    tuple
                });

        assertNull(
            ShareMeasurePeerHierarchyTupleNormalizer.toNormalizedExpressionMdx(
                expression,
                new ShareMeasurePeerHierarchyResetPlanner.InjectionPlan(
                    Arrays.asList(
                        mockMember(brandHierarchy, "[Product Flat].[Brand].[All Brands]"),
                        mockMember(skuHierarchy, "[Product Flat].[Sku].[All Skus]")))));
    }

    private static MemberExpr memberExpr(Member member) {
        return new MemberExpr(member);
    }

    private static Exp tuple(Exp... args) {
        return new UnresolvedFunCall("()", Syntax.Parentheses, args);
    }

    private static Exp mdxLiteral(String mdx) {
        return new MdxLiteralExp(mdx);
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

    private static final class MdxLiteralExp extends ExpBase {
        private final String mdx;

        private MdxLiteralExp(String mdx) {
            this.mdx = mdx;
        }

        public Exp clone() {
            return new MdxLiteralExp(mdx);
        }

        public int getCategory() {
            return Category.Unknown;
        }

        public Type getType() {
            return null;
        }

        public void unparse(PrintWriter pw) {
            pw.print(mdx);
        }

        public Exp accept(Validator validator) {
            return this;
        }

        public Object accept(MdxVisitor visitor) {
            return null;
        }
    }
}
