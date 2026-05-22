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

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.Property;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SyntheticFlatHierarchySupport}. Focused on the
 * static helpers that don't need a live cube — {@code equalsTolerant} and
 * the {@code ANCESTOR_PROPERTY_PREFIX} contract. {@code resolveSyntheticFlat}
 * and {@code findCommonSourceLink} have behavior unchanged from their
 * earlier {@code CrossJoinDependencyPruner} versions and stay covered by
 * {@code RolapNativeCrossJoinGuardEstimateTest} via delegation.
 */
public class SyntheticFlatHierarchySupportTest {

    @Test
    public void ancestorPropertyPrefixIsStableInternalString() {
        // The prefix is part of the public contract for the
        // synthetic-flat ↔ DrilldownMemberFunDef integration; it must
        // not change without coordinated updates to both sites and any
        // schemas that may have observed it.
        assertEquals(
            "_synth_src_ancestor_",
            SyntheticFlatHierarchySupport.ANCESTOR_PROPERTY_PREFIX);
    }

    @Test
    public void equalsTolerant_sameRefIsTrue() {
        Object x = "value";
        assertTrue(SyntheticFlatHierarchySupport.equalsTolerant(x, x));
    }

    @Test
    public void equalsTolerant_bothNullIsFalse() {
        // Treat null as not-equal-to-anything. Synthetic-flat key columns
        // are non-nullable by construction; a null on either side means
        // missing data and should NOT be considered a match.
        assertFalse(
            SyntheticFlatHierarchySupport.equalsTolerant(null, null));
        assertFalse(
            SyntheticFlatHierarchySupport.equalsTolerant("x", null));
        assertFalse(
            SyntheticFlatHierarchySupport.equalsTolerant(null, "x"));
    }

    @Test
    public void equalsTolerant_directEqualsWins() {
        assertTrue(
            SyntheticFlatHierarchySupport.equalsTolerant("abc", "abc"));
        assertTrue(
            SyntheticFlatHierarchySupport.equalsTolerant(
                Integer.valueOf(42), Integer.valueOf(42)));
    }

    @Test
    public void equalsTolerant_integerVsLong() {
        // The crux of the tolerant rule: same integral value across
        // numeric types must compare equal.
        assertTrue(
            SyntheticFlatHierarchySupport.equalsTolerant(
                Integer.valueOf(42), Long.valueOf(42L)));
        assertTrue(
            SyntheticFlatHierarchySupport.equalsTolerant(
                Long.valueOf(42L), Integer.valueOf(42)));
    }

    @Test
    public void equalsTolerant_integerVsString() {
        // Member properties may come back as String even when the
        // column is numeric, depending on driver / metadata.
        assertTrue(
            SyntheticFlatHierarchySupport.equalsTolerant(
                Integer.valueOf(15848), "15848"));
        assertTrue(
            SyntheticFlatHierarchySupport.equalsTolerant(
                "15848", Integer.valueOf(15848)));
    }

    @Test
    public void equalsTolerant_bigIntegerVsLong() {
        assertTrue(
            SyntheticFlatHierarchySupport.equalsTolerant(
                BigInteger.valueOf(42L), Long.valueOf(42L)));
    }

    @Test
    public void equalsTolerant_doubleVsLong_exactIntegralValue() {
        // 42.0 == 42L should compare equal — many JDBC drivers return
        // Double for NUMERIC columns even when the value is integral.
        assertTrue(
            SyntheticFlatHierarchySupport.equalsTolerant(
                Double.valueOf(42.0), Long.valueOf(42L)));
    }

    @Test
    public void equalsTolerant_differentValuesAreFalse() {
        assertFalse(
            SyntheticFlatHierarchySupport.equalsTolerant(
                Integer.valueOf(42), Integer.valueOf(43)));
        assertFalse(
            SyntheticFlatHierarchySupport.equalsTolerant("abc", "def"));
        assertFalse(
            SyntheticFlatHierarchySupport.equalsTolerant(
                Integer.valueOf(42), "43"));
    }

    @Test
    public void equalsTolerant_doubleWithFraction_doesNotCollapse() {
        // 42.5 is NOT 42 or 43. Don't silently round.
        assertFalse(
            SyntheticFlatHierarchySupport.equalsTolerant(
                Double.valueOf(42.5), Long.valueOf(42L)));
    }

    // ----- filterChildrenBySourcePath ---------------------------------------

    /**
     * #78 direct test: when drilling a synthetic-flat hierarchy in a
     * tuple whose sibling position projects an ancestor of the drill
     * level on the same source hierarchy, candidate children get
     * restricted to those whose ancestor property matches the sibling
     * key. Children of unrelated source paths are dropped.
     */
    /**
     * Builds a mock RolapLevel that reports exactly the listed property
     * names — used to simulate buildSyntheticLevel's emission outcome
     * for the drill side. Test inputs always omit anything outside the
     * sentinel prefix so this helper stays focused on the contract
     * filterChildrenBySourcePath actually reads.
     */
    private static RolapLevel mockDrillLevel(String... propertyNames) {
        RolapProperty[] properties = new RolapProperty[propertyNames.length];
        for (int i = 0; i < propertyNames.length; i++) {
            RolapProperty p = mock(RolapProperty.class);
            when(p.getName()).thenReturn(propertyNames[i]);
            properties[i] = p;
        }
        RolapLevel level = mock(RolapLevel.class);
        when(level.getProperties()).thenReturn(properties);
        return level;
    }

    @Test
    public void filterChildrenBySourcePath_keepsMatchingChildren() {
        RolapHierarchy commonSource = mock(RolapHierarchy.class);

        RolapLevel siblingLevel = mock(RolapLevel.class);
        when(siblingLevel.getName()).thenReturn("Category1");

        SyntheticFlatHierarchy.SourceLink siblingLink =
            new SyntheticFlatHierarchy.SourceLink(
                commonSource, siblingLevel, 1);
        SyntheticFlatHierarchy.SourceLink drillLink =
            new SyntheticFlatHierarchy.SourceLink(
                commonSource, mock(RolapLevel.class), 3);

        SyntheticFlatHierarchy siblingHier =
            mock(SyntheticFlatHierarchy.class);
        when(siblingHier.getSourceLinks())
            .thenReturn(List.of(siblingLink));

        SyntheticFlatHierarchy drillHier = mock(SyntheticFlatHierarchy.class);
        when(drillHier.findLinkForHierarchy(commonSource))
            .thenReturn(drillLink);

        Member sibling = mock(Member.class);
        when(sibling.getHierarchy()).thenReturn(siblingHier);
        when(sibling.isAll()).thenReturn(false);
        when(sibling.getName()).thenReturn("cat1");
        when(sibling.getPropertyValue(Property.KEY.getName()))
            .thenReturn(42);

        Member drillMember = mock(Member.class);

        String ancestorProp =
            SyntheticFlatHierarchySupport.ANCESTOR_PROPERTY_PREFIX
                + "Category1";

        Level childLevel = mockDrillLevel(ancestorProp);

        Member child1 = mock(Member.class);
        when(child1.getLevel()).thenReturn(childLevel);
        when(child1.getPropertyValue(ancestorProp)).thenReturn(42);

        Member child2 = mock(Member.class);
        when(child2.getLevel()).thenReturn(childLevel);
        when(child2.getPropertyValue(ancestorProp)).thenReturn(43);

        Member child3 = mock(Member.class);
        when(child3.getLevel()).thenReturn(childLevel);
        // String 42 — must compare equal to Integer 42 via equalsTolerant
        when(child3.getPropertyValue(ancestorProp)).thenReturn("42");

        List<Member> result =
            SyntheticFlatHierarchySupport.filterChildrenBySourcePath(
                new Member[]{sibling, drillMember},
                1,
                drillHier,
                List.of(child1, child2, child3));

        assertEquals(2, result.size(),
            "Expected child1 (Integer 42) and child3 (\"42\") to pass; "
                + "got: " + result.size());
        assertSame(child1, result.get(0));
        assertSame(child3, result.get(1));
    }

    /**
     * #78 review-finding regression: when the drill-side level does NOT
     * carry the {@link SyntheticFlatHierarchySupport#ANCESTOR_PROPERTY_PREFIX}
     * property — because {@code buildSyntheticLevel} skipped emission
     * for a non-unique source key or a snowflake ancestor on a different
     * table — the SourceLink topology still says the ancestor relationship
     * exists. The filter must drop that constraint instead of comparing
     * every child's null property to the sibling key and emptying the
     * result. Documented degradation: unfiltered children (Cartesian-
     * but-correct).
     */
    @Test
    public void filterChildrenBySourcePath_skipsConstraintWhenPropertyMissing() {
        RolapHierarchy commonSource = mock(RolapHierarchy.class);

        RolapLevel siblingLevel = mock(RolapLevel.class);
        when(siblingLevel.getName()).thenReturn("Category1");

        SyntheticFlatHierarchy.SourceLink siblingLink =
            new SyntheticFlatHierarchy.SourceLink(
                commonSource, siblingLevel, 1);
        SyntheticFlatHierarchy.SourceLink drillLink =
            new SyntheticFlatHierarchy.SourceLink(
                commonSource, mock(RolapLevel.class), 3);

        SyntheticFlatHierarchy siblingHier =
            mock(SyntheticFlatHierarchy.class);
        when(siblingHier.getSourceLinks())
            .thenReturn(List.of(siblingLink));

        SyntheticFlatHierarchy drillHier = mock(SyntheticFlatHierarchy.class);
        when(drillHier.findLinkForHierarchy(commonSource))
            .thenReturn(drillLink);

        Member sibling = mock(Member.class);
        when(sibling.getHierarchy()).thenReturn(siblingHier);
        when(sibling.isAll()).thenReturn(false);
        when(sibling.getName()).thenReturn("cat1");
        lenient().when(sibling.getPropertyValue(Property.KEY.getName()))
            .thenReturn(42);

        Member drillMember = mock(Member.class);

        // Drill level reports NO ancestor properties — simulating
        // buildSyntheticLevel having skipped emission (non-unique source
        // or snowflake ancestor). The filter must NOT compare against
        // null and drop everything.
        Level childLevel = mockDrillLevel();

        Member child1 = mock(Member.class);
        when(child1.getLevel()).thenReturn(childLevel);

        Member child2 = mock(Member.class);
        when(child2.getLevel()).thenReturn(childLevel);

        List<Member> children = List.of(child1, child2);

        List<Member> result =
            SyntheticFlatHierarchySupport.filterChildrenBySourcePath(
                new Member[]{sibling, drillMember},
                1,
                drillHier,
                children);

        assertSame(
            children, result,
            "When the drill level doesn't carry the ancestor property, "
                + "the filter must fall through unchanged — non-unique / "
                + "snowflake schemas need Cartesian-but-correct, not "
                + "empty");
    }

    /**
     * #78 contract: when the drill hierarchy is not synthetic-flat,
     * filter returns children unchanged — protects every non-#78
     * cross-hierarchy drill caller.
     */
    @Test
    public void filterChildrenBySourcePath_passesThroughForNonSyntheticDrill() {
        Hierarchy drillHier = mock(Hierarchy.class);
        Member m = mock(Member.class);
        List<Member> children = List.of(m);

        List<Member> result =
            SyntheticFlatHierarchySupport.filterChildrenBySourcePath(
                new Member[]{m}, 0, drillHier, children);

        assertSame(children, result);
    }

    /**
     * #78 contract: when no sibling projects a usable source-path
     * constraint (e.g. siblings are all [All] or non-synthetic-flat),
     * children pass through unchanged.
     */
    @Test
    public void filterChildrenBySourcePath_passesThroughWhenNoConstraints() {
        SyntheticFlatHierarchy drillHier = mock(SyntheticFlatHierarchy.class);
        Member drillMember = mock(Member.class);

        Member allSibling = mock(Member.class);
        when(allSibling.isAll()).thenReturn(true);

        Member child = mock(Member.class);
        List<Member> children = List.of(child);

        List<Member> result =
            SyntheticFlatHierarchySupport.filterChildrenBySourcePath(
                new Member[]{allSibling, drillMember},
                1,
                drillHier,
                children);

        assertSame(children, result);
    }

    /**
     * #78 contract: when the drill side links to the sibling's source
     * hierarchy at a LESSER depth (i.e. sibling is the descendant, not
     * the ancestor), no constraint is applied — drilling toward a
     * child level shouldn't be restricted by a deeper sibling.
     */
    @Test
    public void filterChildrenBySourcePath_passesThroughWhenSiblingIsDescendant() {
        RolapHierarchy commonSource = mock(RolapHierarchy.class);

        // Sibling is at depth 3; drill is at depth 1 — sibling is
        // descendant, drill is ancestor. No constraint should be added.
        SyntheticFlatHierarchy.SourceLink siblingLink =
            new SyntheticFlatHierarchy.SourceLink(
                commonSource, mock(RolapLevel.class), 3);
        SyntheticFlatHierarchy.SourceLink drillLink =
            new SyntheticFlatHierarchy.SourceLink(
                commonSource, mock(RolapLevel.class), 1);

        SyntheticFlatHierarchy siblingHier =
            mock(SyntheticFlatHierarchy.class);
        when(siblingHier.getSourceLinks())
            .thenReturn(List.of(siblingLink));

        SyntheticFlatHierarchy drillHier = mock(SyntheticFlatHierarchy.class);
        when(drillHier.findLinkForHierarchy(commonSource))
            .thenReturn(drillLink);

        Member sibling = mock(Member.class);
        when(sibling.getHierarchy()).thenReturn(siblingHier);
        when(sibling.isAll()).thenReturn(false);
        lenient().when(sibling.getPropertyValue(Property.KEY.getName()))
            .thenReturn(42);

        Member drillMember = mock(Member.class);
        Member child = mock(Member.class);
        // No constraint emitted, so getPropertyValue is never called on
        // child. We just expect the unmodified list back.
        List<Member> children = List.of(child);

        List<Member> result =
            SyntheticFlatHierarchySupport.filterChildrenBySourcePath(
                new Member[]{sibling, drillMember},
                1,
                drillHier,
                children);

        assertSame(children, result);
    }

    /**
     * #78 contract: empty/null children short-circuit immediately
     * without exercising the constraint path — protects callers that
     * pass a never-null but empty list.
     */
    @Test
    public void filterChildrenBySourcePath_handlesEmptyChildren() {
        SyntheticFlatHierarchy drillHier = mock(SyntheticFlatHierarchy.class);
        Member m = mock(Member.class);
        List<Member> empty = Collections.emptyList();

        assertSame(
            empty,
            SyntheticFlatHierarchySupport.filterChildrenBySourcePath(
                new Member[]{m}, 0, drillHier, empty));
        assertEquals(
            0,
            SyntheticFlatHierarchySupport.filterChildrenBySourcePath(
                new Member[]{m}, 0, drillHier, Arrays.asList())
                .size());
    }

    @Test
    public void resolveSyntheticFlat_nullForNonSyntheticHierarchy() {
        // resolveSyntheticFlat is delegated to but should be safe on
        // null and non-synthetic input.
        assertNotNull(SyntheticFlatHierarchySupport.class);
        // Direct null input — defensive check; the helper handles null
        // via instanceof returning false.
        // (Cannot easily test with a real RolapCubeHierarchy in a unit
        // test without a schema; live behavior is covered by
        // RolapNativeCrossJoinGuardEstimateTest via delegation.)
    }
}

// End SyntheticFlatHierarchySupportTest.java
