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

import mondrian.olap.AxisOrdinal;
import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.Query;
import mondrian.olap.QueryAxis;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.Type;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NativeQueryEngine} eligibility (Phase A classification).
 * Tests operate at the MeasureClassifier level so no full engine wiring is needed.
 */
public class NativeQueryEngineEligibilityTest {

    @Test
    public void testAllStoredMeasuresEligible() {
        Set<Member> measures = new LinkedHashSet<Member>();
        measures.add(mockStoredMeasure("sales_qty"));
        measures.add(mockStoredMeasure("sales_rub"));

        List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        assertNotNull(candidates, "All stored measures should be eligible");
        assertEquals(2, candidates.size());
    }

    @Test
    public void testUnsupportedCalcPoisonsQuery() {
        Set<Member> measures = new LinkedHashSet<Member>();
        measures.add(mockStoredMeasure("sales_qty"));
        measures.add(mockEvaluatorMeasure("complex"));

        List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        assertNull(candidates, "Evaluator measure should poison entire query");
    }

    @Test
    public void testAxisGuardHandlesVisibleTupleAxis() {
        Hierarchy format = mockHierarchy(Hierarchy.class, "[ТТ.Формат]");
        Level formatLevel =
            mockLevel(format, "[ТТ.Формат].[Формат]", 0, false);
        when(format.getLevels()).thenReturn(new Level[] {formatLevel});

        Hierarchy product = mockHierarchy(Hierarchy.class, "[Продукт.Товар]");
        Level productLevel =
            mockLevel(product, "[Продукт.Товар].[Товар]", 0, false);
        when(product.getLevels()).thenReturn(new Level[] {productLevel});

        Type axisType =
            setOfTuple(
                MemberType.forLevel(formatLevel),
                MemberType.forLevel(productLevel));

        assertNull(
            NativeQueryEngine.checkAxisHierarchyGuard(
                queryWithAxis(axisType)));
    }

    @Test
    public void testAxisGuardFindsHiddenHierarchyInsideTupleAxis() {
        Hierarchy format = mockHierarchy(Hierarchy.class, "[ТТ.Формат]");
        Level formatLevel =
            mockLevel(format, "[ТТ.Формат].[Формат]", 0, false);
        when(format.getLevels()).thenReturn(new Level[] {formatLevel});

        RolapHierarchy hidden =
            mockHierarchy(RolapHierarchy.class, "[Продукт.Категория]");
        when(hidden.isVisible()).thenReturn(false);
        Level hiddenLevel =
            mockLevel(hidden, "[Продукт.Категория].[Категория1]", 1, false);
        when(hidden.getLevels()).thenReturn(new Level[] {hiddenLevel});

        Type axisType =
            setOfTuple(
                MemberType.forLevel(formatLevel),
                MemberType.forLevel(hiddenLevel));

        assertEquals(
            FallbackReason.HIDDEN_HIERARCHY_ON_AXIS,
            NativeQueryEngine.checkAxisHierarchyGuard(
                queryWithAxis(axisType)));
    }

    @Test
    public void testAxisGuardFindsSyntheticFlatOnSingleAxis() {
        SyntheticFlatHierarchy flat =
            mockHierarchy(
                SyntheticFlatHierarchy.class, "[Продукт.Категория1]");
        Level flatLevel =
            mockLevel(flat, "[Продукт.Категория1].[Категория1]", 1, false);

        Type axisType = setOfMember(MemberType.forLevel(flatLevel));

        assertEquals(
            FallbackReason.SYNTHETIC_FLAT_ON_AXIS,
            NativeQueryEngine.checkAxisHierarchyGuard(
                queryWithAxis(axisType)));
    }

    @Test
    public void testAxisGuardUnwrapsRolapCubeHierarchyWrappingSyntheticFlat() {
        SyntheticFlatHierarchy underlyingFlat =
            mockHierarchy(
                SyntheticFlatHierarchy.class, "[Продукт.Категория1]");
        RolapCubeHierarchy wrapper =
            mockHierarchy(
                RolapCubeHierarchy.class, "[Продукт.Категория1]");
        when(wrapper.getRolapHierarchy()).thenReturn(underlyingFlat);
        Level wrapperLevel =
            mockLevel(
                wrapper, "[Продукт.Категория1].[Категория1]", 1, false);

        Type axisType = setOfMember(MemberType.forLevel(wrapperLevel));

        assertEquals(
            FallbackReason.SYNTHETIC_FLAT_ON_AXIS,
            NativeQueryEngine.checkAxisHierarchyGuard(
                queryWithAxis(axisType)));
    }

    @Test
    public void testAxisGuardFindsHiddenHierarchyOnSingleAxis() {
        RolapHierarchy hidden =
            mockHierarchy(RolapHierarchy.class, "[Продукт.Категория]");
        when(hidden.isVisible()).thenReturn(false);
        Level hiddenLevel =
            mockLevel(
                hidden, "[Продукт.Категория].[Категория1]", 1, false);

        Type axisType = setOfMember(MemberType.forLevel(hiddenLevel));

        assertEquals(
            FallbackReason.HIDDEN_HIERARCHY_ON_AXIS,
            NativeQueryEngine.checkAxisHierarchyGuard(
                queryWithAxis(axisType)));
    }

    @Test
    public void testAxisGuardFindsMultilevelFirstLevelInsideTupleAxis() {
        Hierarchy category =
            mockHierarchy(Hierarchy.class, "[Продукт.Категория]");
        Level all =
            mockLevel(category, "[Продукт.Категория].[All]", 0, true);
        Level category1 =
            mockLevel(category, "[Продукт.Категория].[Категория1]", 1, false);
        Level category2 =
            mockLevel(category, "[Продукт.Категория].[Категория2]", 2, false);
        when(category.getLevels())
            .thenReturn(new Level[] {all, category1, category2});

        Hierarchy format = mockHierarchy(Hierarchy.class, "[ТТ.Формат]");
        Level formatLevel =
            mockLevel(format, "[ТТ.Формат].[Формат]", 0, false);
        when(format.getLevels()).thenReturn(new Level[] {formatLevel});

        Type axisType =
            setOfTuple(
                MemberType.forLevel(category1),
                MemberType.forLevel(formatLevel));

        assertEquals(
            FallbackReason.MULTILEVEL_FIRST_LEVEL_ON_AXIS,
            NativeQueryEngine.checkAxisHierarchyGuard(
                queryWithAxis(axisType)));
    }

    private Member mockStoredMeasure(String name) {
        Member m = mock(Member.class);
        when(m.getName()).thenReturn(name);
        when(m.isMeasure()).thenReturn(true);
        when(m.isCalculated()).thenReturn(false);
        return m;
    }

    private Member mockEvaluatorMeasure(String name) {
        Member m = mock(Member.class);
        when(m.getName()).thenReturn(name);
        when(m.isMeasure()).thenReturn(true);
        when(m.isCalculated()).thenReturn(true);
        when(m.getExpression()).thenReturn(null);
        return m;
    }

    private <T extends Hierarchy> T mockHierarchy(
        Class<T> hierarchyClass,
        String uniqueName)
    {
        Dimension dimension = mock(Dimension.class);
        when(dimension.getUniqueName()).thenReturn(uniqueName + ".Dimension");

        T hierarchy = mock(hierarchyClass);
        when(hierarchy.getUniqueName()).thenReturn(uniqueName);
        when(hierarchy.getDimension()).thenReturn(dimension);
        return hierarchy;
    }

    private Level mockLevel(
        Hierarchy hierarchy,
        String uniqueName,
        int depth,
        boolean all)
    {
        Dimension dimension = hierarchy.getDimension();
        Level level = mock(Level.class);
        when(level.getUniqueName()).thenReturn(uniqueName);
        when(level.getHierarchy()).thenReturn(hierarchy);
        when(level.getDimension()).thenReturn(dimension);
        when(level.getDepth()).thenReturn(depth);
        when(level.isAll()).thenReturn(all);
        return level;
    }

    private Type setOfTuple(Type... elementTypes) {
        return new SetType(new TupleType(elementTypes));
    }

    private Type setOfMember(Type memberType) {
        return new SetType(memberType);
    }

    private Query queryWithAxis(Type axisType) {
        Exp exp = mock(Exp.class);
        when(exp.getType()).thenReturn(axisType);

        QueryAxis axis =
            new QueryAxis(
                false,
                exp,
                AxisOrdinal.StandardAxisOrdinal.ROWS,
                QueryAxis.SubtotalVisibility.Undefined);

        Query query = mock(Query.class);
        when(query.getAxes()).thenReturn(new QueryAxis[] {axis});
        when(query.getSlicerAxis()).thenReturn(null);
        return query;
    }
}

// End NativeQueryEngineEligibilityTest.java
