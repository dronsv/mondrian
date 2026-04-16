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

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the NativeQueryShape value object hierarchy:
 * {@link MemberCardinality}, {@link QueryLocation}, {@link LevelRef},
 * {@link HierarchyPresence}, and {@link NativeQueryShape}.
 */
class NativeQueryShapeTest {

    // =======================================================================
    // MemberCardinality
    // =======================================================================

    @Test
    void allMemberIsDistinctFromSingleMember() {
        assertNotEquals(MemberCardinality.ALL_MEMBER,
            MemberCardinality.SINGLE_MEMBER);
    }

    @Test
    void memberCardinalityHasFiveValues() {
        assertEquals(5, MemberCardinality.values().length);
    }

    // =======================================================================
    // LevelRef
    // =======================================================================

    @Test
    void levelRefNoneHasNullName() {
        assertNull(LevelRef.NONE.levelUniqueName());
        assertFalse(LevelRef.NONE.isPresent());
        assertEquals(-1, LevelRef.NONE.depth());
    }

    @Test
    void levelRefOfNullReturnsNone() {
        assertSame(LevelRef.NONE, LevelRef.of(null));
    }

    @Test
    void levelRefEqualityByUniqueName() {
        LevelRef a = LevelRef.of("[Time].[Year]", 1);
        LevelRef b = LevelRef.of("[Time].[Year]", 2);
        // Equality is by unique name, not depth
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void levelRefInequalityForDifferentNames() {
        LevelRef a = LevelRef.of("[Time].[Year]");
        LevelRef b = LevelRef.of("[Time].[Quarter]");
        assertNotEquals(a, b);
    }

    @Test
    void levelRefPresentForNonNullName() {
        assertTrue(LevelRef.of("[Store].[Country]").isPresent());
    }

    // =======================================================================
    // HierarchyPresence
    // =======================================================================

    @Test
    void hierarchyPresenceAllMemberIsUnconstrained() {
        HierarchyPresence hp = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.ALL_MEMBER)
            .constrained(false)
            .addLocation(QueryLocation.SLICER)
            .build();

        assertTrue(hp.isAllMember());
        assertFalse(hp.constrained());
        assertTrue(hp.onSlicer());
        assertFalse(hp.onVisibleAxis());
    }

    @Test
    void hierarchyPresenceSingleMemberIsConstrained() {
        HierarchyPresence hp = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.SINGLE_MEMBER)
            .constrained(true)
            .activeLevel(LevelRef.of("[Time].[Year]", 1))
            .addLocation(QueryLocation.SLICER)
            .build();

        assertFalse(hp.isAllMember());
        assertTrue(hp.constrained());
        assertEquals(
            MemberCardinality.SINGLE_MEMBER,
            hp.memberCardinality());
        assertTrue(hp.activeLevel().isPresent());
        assertEquals("[Time].[Year]", hp.activeLevel().levelUniqueName());
    }

    @Test
    void hierarchyPresenceSetExpressionMayHaveNoLevel() {
        HierarchyPresence hp = HierarchyPresence
            .builder("[Product]")
            .memberCardinality(MemberCardinality.SET_EXPRESSION)
            .constrained(true)
            .addLocation(QueryLocation.ROWS)
            .projected(true)
            .build();

        assertTrue(hp.isSetExpression());
        assertFalse(hp.activeLevel().isPresent());
        assertTrue(hp.projected());
        assertTrue(hp.onVisibleAxis());
    }

    @Test
    void hierarchyPresenceMultipleLocations() {
        HierarchyPresence hp = HierarchyPresence
            .builder("[Store]")
            .memberCardinality(MemberCardinality.SINGLE_MEMBER)
            .constrained(true)
            .addLocation(QueryLocation.ROWS)
            .addLocation(QueryLocation.CALCULATED_MEMBER_FORMULA)
            .projected(true)
            .build();

        assertEquals(2, hp.locations().size());
        assertTrue(hp.locations().contains(QueryLocation.ROWS));
        assertTrue(
            hp.locations().contains(QueryLocation.CALCULATED_MEMBER_FORMULA));
    }

    @Test
    void hierarchyPresenceDefaultsToUnknownCardinality() {
        HierarchyPresence hp = HierarchyPresence
            .builder("[Geo]")
            .build();

        assertEquals(MemberCardinality.UNKNOWN, hp.memberCardinality());
        assertFalse(hp.activeLevel().isPresent());
        assertFalse(hp.projected());
        assertFalse(hp.constrained());
    }

    @Test
    void hierarchyPresenceEquality() {
        HierarchyPresence a = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.SINGLE_MEMBER)
            .constrained(true)
            .activeLevel(LevelRef.of("[Time].[Year]"))
            .addLocation(QueryLocation.SLICER)
            .build();
        HierarchyPresence b = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.SINGLE_MEMBER)
            .constrained(true)
            .activeLevel(LevelRef.of("[Time].[Year]"))
            .addLocation(QueryLocation.SLICER)
            .build();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void hierarchyPresenceInequalityOnConstraint() {
        HierarchyPresence constrained = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.SINGLE_MEMBER)
            .constrained(true)
            .build();
        HierarchyPresence unconstrained = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.ALL_MEMBER)
            .constrained(false)
            .build();
        assertNotEquals(constrained, unconstrained);
    }

    // =======================================================================
    // NativeQueryShape
    // =======================================================================

    @Test
    void emptyShapeHasNoHierarchies() {
        NativeQueryShape shape = NativeQueryShape.builder().build();
        assertEquals(0, shape.hierarchyCount());
        assertFalse(shape.hasMeasures());
        assertFalse(shape.hasConstrainedHierarchy());
        assertFalse(shape.hasSetExpression());
        assertEquals(0, shape.nonEmptyAxisCount());
    }

    @Test
    void shapeWithHierarchiesAndMeasures() {
        HierarchyPresence timeOnSlicer = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.SINGLE_MEMBER)
            .constrained(true)
            .activeLevel(LevelRef.of("[Time].[Year]", 1))
            .addLocation(QueryLocation.SLICER)
            .build();

        HierarchyPresence productOnRows = HierarchyPresence
            .builder("[Product]")
            .memberCardinality(MemberCardinality.SET_EXPRESSION)
            .constrained(true)
            .addLocation(QueryLocation.ROWS)
            .projected(true)
            .build();

        NativeQueryShape shape = NativeQueryShape.builder()
            .addHierarchy(timeOnSlicer)
            .addHierarchy(productOnRows)
            .addMeasure("[Measures].[Sales]")
            .addMeasure("[Measures].[Cost]")
            .nonEmptyAxisCount(1)
            .build();

        assertEquals(2, shape.hierarchyCount());
        assertTrue(shape.hasMeasures());
        assertEquals(2, shape.requestedMeasureUniqueNames().size());
        assertTrue(shape.hasConstrainedHierarchy());
        assertTrue(shape.hasSetExpression());
        assertEquals(1, shape.nonEmptyAxisCount());

        // Lookup by hierarchy name
        assertSame(timeOnSlicer, shape.hierarchy("[Time]"));
        assertSame(productOnRows, shape.hierarchy("[Product]"));
        assertNull(shape.hierarchy("[Store]"));
    }

    @Test
    void shapeEquality() {
        NativeQueryShape a = buildSampleShape();
        NativeQueryShape b = buildSampleShape();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void shapeInequalityOnMeasures() {
        NativeQueryShape a = NativeQueryShape.builder()
            .addMeasure("[Measures].[Sales]")
            .build();
        NativeQueryShape b = NativeQueryShape.builder()
            .addMeasure("[Measures].[Cost]")
            .build();
        assertNotEquals(a, b);
    }

    @Test
    void shapeLastWriteWinsForDuplicateHierarchy() {
        HierarchyPresence first = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.ALL_MEMBER)
            .constrained(false)
            .build();
        HierarchyPresence second = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.SINGLE_MEMBER)
            .constrained(true)
            .build();

        NativeQueryShape shape = NativeQueryShape.builder()
            .addHierarchy(first)
            .addHierarchy(second)
            .build();

        assertEquals(1, shape.hierarchyCount());
        assertEquals(
            MemberCardinality.SINGLE_MEMBER,
            shape.hierarchy("[Time]").memberCardinality());
    }

    @Test
    void shapeUnconstrainedHierarchyNotReportedAsConstrained() {
        HierarchyPresence allMember = HierarchyPresence
            .builder("[Time]")
            .memberCardinality(MemberCardinality.ALL_MEMBER)
            .constrained(false)
            .build();

        NativeQueryShape shape = NativeQueryShape.builder()
            .addHierarchy(allMember)
            .build();

        assertFalse(shape.hasConstrainedHierarchy());
    }

    // -- helpers --

    private NativeQueryShape buildSampleShape() {
        return NativeQueryShape.builder()
            .addHierarchy(
                HierarchyPresence.builder("[Time]")
                    .memberCardinality(MemberCardinality.SINGLE_MEMBER)
                    .constrained(true)
                    .addLocation(QueryLocation.SLICER)
                    .build())
            .addMeasure("[Measures].[Sales]")
            .nonEmptyAxisCount(1)
            .build();
    }
}

// End NativeQueryShapeTest.java
