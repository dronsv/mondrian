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
import mondrian.olap.Annotation;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.SchemaReader;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShareMeasurePeerHierarchyResetPlannerTest extends TestCase {
    public void testExplicitHierarchyFinderCollectsMemberAndDimensionHierarchies() {
        final RolapDimension productDimension = mockDimension("Product");
        final RolapHierarchy brandHierarchy =
            mockHierarchy(productDimension, "[Product.Brand]", true);
        final RolapHierarchy skuHierarchy =
            mockHierarchy(productDimension, "[Product.SKU]", true);
        when(productDimension.getHierarchies()).thenReturn(
            new Hierarchy[] {brandHierarchy, skuHierarchy});

        final RolapMember explicitSkuMember = mockMember(skuHierarchy, "[Product.SKU].[A]");
        final Set<Hierarchy> explicitHierarchies =
            ExplicitHierarchyReferenceFinder.find(new MemberExpr(explicitSkuMember));

        assertTrue(explicitHierarchies.contains(skuHierarchy));
        assertFalse(explicitHierarchies.contains(brandHierarchy));
    }

    public void testCreatePlanResetsNonExplicitPeerHierarchies() {
        final RolapDimension productDimension = mockDimension("Product");
        final RolapHierarchy manufacturerHierarchy =
            mockHierarchy(productDimension, "[Product.Manufacturer]", true);
        final RolapHierarchy brandHierarchy =
            mockHierarchy(productDimension, "[Product.Brand]", true);
        final RolapHierarchy skuHierarchy =
            mockHierarchy(productDimension, "[Product.SKU]", true);
        final RolapHierarchy categoryHierarchy =
            mockHierarchy(productDimension, "[Product.Category]", true);
        when(productDimension.getHierarchies()).thenReturn(
            new Hierarchy[] {
                manufacturerHierarchy,
                brandHierarchy,
                skuHierarchy,
                categoryHierarchy
            });

        final RolapCube cube = mock(RolapCube.class);
        when(cube.getHierarchies()).thenReturn(
            Arrays.asList(
                manufacturerHierarchy,
                brandHierarchy,
                skuHierarchy,
                categoryHierarchy));

        final RolapCalculatedMember calculatedMember = mock(RolapCalculatedMember.class);
        when(calculatedMember.getAnnotationMap()).thenReturn(
            annotationMap(
                "semantics.kind", "companion_denominator",
                "semantics.childHierarchy", "Product.Brand",
                "semantics.topHierarchy", "Product.Manufacturer"));
        final RolapMember explicitBrandAll =
            mockMember(brandHierarchy, "[Product.Brand].[All]");
        when(calculatedMember.getExpression()).thenReturn(new MemberExpr(explicitBrandAll));

        final ShareMeasurePeerHierarchyResetPlanner.ResetPlan resetPlan =
            ShareMeasurePeerHierarchyResetPlanner.createPlan(
                mock(SchemaReader.class),
                cube,
                calculatedMember);

        assertEquals(2, resetPlan.getResetMembers().length);
        assertSame(skuHierarchy.getAllMember(), resetPlan.getResetMembers()[0]);
        assertSame(categoryHierarchy.getAllMember(), resetPlan.getResetMembers()[1]);
    }

    public void testCreatePlanKeepsExplicitPeerHierarchyUntouched() {
        final RolapDimension productDimension = mockDimension("Product");
        final RolapHierarchy manufacturerHierarchy =
            mockHierarchy(productDimension, "[Product.Manufacturer]", true);
        final RolapHierarchy brandHierarchy =
            mockHierarchy(productDimension, "[Product.Brand]", true);
        final RolapHierarchy skuHierarchy =
            mockHierarchy(productDimension, "[Product.SKU]", true);
        when(productDimension.getHierarchies()).thenReturn(
            new Hierarchy[] {manufacturerHierarchy, brandHierarchy, skuHierarchy});

        final RolapCube cube = mock(RolapCube.class);
        when(cube.getHierarchies()).thenReturn(
            Arrays.asList(manufacturerHierarchy, brandHierarchy, skuHierarchy));

        final RolapCalculatedMember calculatedMember = mock(RolapCalculatedMember.class);
        when(calculatedMember.getAnnotationMap()).thenReturn(
            annotationMap(
                "semantics.kind", "companion_denominator",
                "semantics.childHierarchy", "Product.Brand",
                "semantics.topHierarchy", "Product.Manufacturer"));
        final RolapMember explicitSkuMember =
            mockMember(skuHierarchy, "[Product.SKU].[Pinned]");
        when(calculatedMember.getExpression()).thenReturn(new MemberExpr(explicitSkuMember));

        final ShareMeasurePeerHierarchyResetPlanner.ResetPlan resetPlan =
            ShareMeasurePeerHierarchyResetPlanner.createPlan(
                mock(SchemaReader.class),
                cube,
                calculatedMember);

        assertTrue(resetPlan.isEmpty());
    }

    public void testCreatePlanUsesDefaultMemberWhenHierarchyHasNoAll() {
        final RolapDimension productDimension = mockDimension("Product");
        final RolapHierarchy manufacturerHierarchy =
            mockHierarchy(productDimension, "[Product.Manufacturer]", true);
        final RolapHierarchy brandHierarchy =
            mockHierarchy(productDimension, "[Product.Brand]", true);
        final RolapHierarchy weightHierarchy =
            mockHierarchy(productDimension, "[Product.Weight]", false);
        when(productDimension.getHierarchies()).thenReturn(
            new Hierarchy[] {manufacturerHierarchy, brandHierarchy, weightHierarchy});

        final RolapCube cube = mock(RolapCube.class);
        when(cube.getHierarchies()).thenReturn(
            Arrays.asList(manufacturerHierarchy, brandHierarchy, weightHierarchy));

        final RolapCalculatedMember calculatedMember = mock(RolapCalculatedMember.class);
        when(calculatedMember.getAnnotationMap()).thenReturn(
            annotationMap(
                "semantics.kind", "companion_denominator",
                "semantics.childHierarchy", "Product.Brand",
                "semantics.topHierarchy", "Product.Manufacturer"));
        when(calculatedMember.getExpression()).thenReturn(mock(Exp.class));

        final ShareMeasurePeerHierarchyResetPlanner.ResetPlan resetPlan =
            ShareMeasurePeerHierarchyResetPlanner.createPlan(
                mock(SchemaReader.class),
                cube,
                calculatedMember);

        assertEquals(1, resetPlan.getResetMembers().length);
        assertSame(weightHierarchy.getDefaultMember(), resetPlan.getResetMembers()[0]);
    }

    public void testCreatePlanReturnsEmptyForMalformedAnnotations() {
        final RolapCube cube = mock(RolapCube.class);
        when(cube.getHierarchies()).thenReturn(Collections.<RolapHierarchy>emptyList());

        final RolapCalculatedMember calculatedMember = mock(RolapCalculatedMember.class);
        when(calculatedMember.getAnnotationMap()).thenReturn(
            annotationMap("semantics.kind", "companion_denominator"));

        final ShareMeasurePeerHierarchyResetPlanner.ResetPlan resetPlan =
            ShareMeasurePeerHierarchyResetPlanner.createPlan(
                mock(SchemaReader.class),
                cube,
                calculatedMember);

        assertTrue(resetPlan.isEmpty());
    }

    private static Map<String, Annotation> annotationMap(String... values) {
        final Map<String, Annotation> map =
            new LinkedHashMap<String, Annotation>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i], annotation(values[i], values[i + 1]));
        }
        return map;
    }

    private static Annotation annotation(final String name, final String value) {
        return new Annotation() {
            public String getName() {
                return name;
            }

            public Object getValue() {
                return value;
            }
        };
    }

    private static RolapDimension mockDimension(String name) {
        final RolapDimension dimension = mock(RolapDimension.class);
        when(dimension.getName()).thenReturn(name);
        return dimension;
    }

    private static RolapHierarchy mockHierarchy(
        RolapDimension dimension,
        String uniqueName,
        boolean hasAll)
    {
        final RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        final RolapMember replacementMember =
            mockMember(hierarchy, uniqueName + ".[All]");
        when(hierarchy.getDimension()).thenReturn(dimension);
        when(hierarchy.getUniqueName()).thenReturn(uniqueName);
        when(hierarchy.getName()).thenReturn(uniqueName.substring(uniqueName.lastIndexOf('.') + 1, uniqueName.length() - 1));
        when(hierarchy.hasAll()).thenReturn(hasAll);
        if (hasAll) {
            when(hierarchy.getAllMember()).thenReturn(replacementMember);
        } else {
            when(hierarchy.getAllMember()).thenReturn(null);
        }
        when(hierarchy.getDefaultMember()).thenReturn(replacementMember);
        return hierarchy;
    }

    private static RolapMember mockMember(Hierarchy hierarchy, String uniqueName) {
        final RolapMember member = mock(RolapMember.class);
        when(member.getHierarchy()).thenReturn((RolapHierarchy) hierarchy);
        when(member.getUniqueName()).thenReturn(uniqueName);
        return member;
    }
}
