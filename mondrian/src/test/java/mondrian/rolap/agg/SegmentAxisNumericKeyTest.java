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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SegmentAxisNumericKeyTest {

    @Test public void testOffsetMatchesEquivalentIntegralNumericTypes() {
        SegmentAxis axis =
            new SegmentAxis(
                LiteralStarPredicate.TRUE,
                new Comparable[] {10L, 20L, 30L});

        assertEquals(1, axis.getOffset(Integer.valueOf(20)));
    }

    @Test public void testOffsetMatchesEquivalentDecimalNumericTypes() {
        SegmentAxis axis =
            new SegmentAxis(
                LiteralStarPredicate.TRUE,
                new Comparable[] {new BigDecimal("20.000")});

        assertEquals(0, axis.getOffset(Long.valueOf(20)));
    }

    @Test public void testOffsetDoesNotGuessAmbiguousNumericKeys() {
        AmbiguousNumber first = new AmbiguousNumber(0);
        AmbiguousNumber second = new AmbiguousNumber(1);
        SegmentAxis axis =
            new SegmentAxis(
                LiteralStarPredicate.TRUE,
                new Comparable[] {
                    first,
                    second
                });

        assertEquals(-1, axis.getOffset(Long.valueOf(20)));
        assertEquals(0, axis.getOffset(first));
        assertEquals(1, axis.getOffset(second));
    }

    private static class AmbiguousNumber
        extends Number
        implements Comparable<AmbiguousNumber>
    {
        private final int ordinal;

        private AmbiguousNumber(int ordinal) {
            this.ordinal = ordinal;
        }

        public int compareTo(AmbiguousNumber other) {
            return Integer.compare(ordinal, other.ordinal);
        }

        public int intValue() {
            return 20;
        }

        public long longValue() {
            return 20L;
        }

        public float floatValue() {
            return 20F;
        }

        public double doubleValue() {
            return 20D;
        }

        public String toString() {
            return "20";
        }
    }
}

// End SegmentAxisNumericKeyTest.java
