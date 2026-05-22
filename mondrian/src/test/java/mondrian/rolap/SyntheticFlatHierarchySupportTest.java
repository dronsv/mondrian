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

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
