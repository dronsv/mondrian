/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import mondrian.rolap.BitKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SegmentLoaderGroupingSetsTest {

    @Test public void testShouldSkipRolledUpAxisWhenGroupingBitSet() {
        final BitKey groupingBitKey = BitKey.Factory.makeBitKey(2);
        groupingBitKey.set(1);

        assertTrue(
            SegmentLoader.shouldSkipRolledUpAxis(
                true,
                groupingBitKey,
                1));
    }

    @Test public void testShouldNotSkipRolledUpAxisWhenGroupingBitNotSet() {
        final BitKey groupingBitKey = BitKey.Factory.makeBitKey(2);
        groupingBitKey.set(0);

        assertFalse(
            SegmentLoader.shouldSkipRolledUpAxis(
                true,
                groupingBitKey,
                1));
    }

    @Test public void testShouldNotSkipWhenGroupingSetsDisabled() {
        final BitKey groupingBitKey = BitKey.Factory.makeBitKey(1);
        groupingBitKey.set(0);

        assertFalse(
            SegmentLoader.shouldSkipRolledUpAxis(
                false,
                groupingBitKey,
                0));
    }

    @Test public void testShouldNotSkipForNonRollupColumn() {
        final BitKey groupingBitKey = BitKey.Factory.makeBitKey(2);
        groupingBitKey.set(1);

        assertFalse(
            SegmentLoader.shouldSkipRolledUpAxis(
                true,
                groupingBitKey,
                -1));
    }
}

