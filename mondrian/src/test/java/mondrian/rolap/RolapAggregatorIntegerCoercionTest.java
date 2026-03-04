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
import mondrian.spi.Dialect;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class RolapAggregatorIntegerCoercionTest extends TestCase {

    public void testSumIntegerAggregatorAcceptsMixedNumberTypes() {
        final List<Object> rawData = Arrays.asList(
            Integer.valueOf(1),
            Long.valueOf(2L),
            Double.valueOf(3.0d),
            new BigDecimal("4")
        );

        final Object aggregated =
            RolapAggregator.Sum.aggregate(rawData, Dialect.Datatype.Integer);

        assertEquals(Integer.valueOf(10), aggregated);
    }

    public void testMinIntegerAggregatorAcceptsMixedNumberTypes() {
        final List<Object> rawData = Arrays.asList(
            Long.valueOf(9L),
            Double.valueOf(2.0d),
            Integer.valueOf(5)
        );

        final Object aggregated =
            RolapAggregator.Min.aggregate(rawData, Dialect.Datatype.Integer);

        assertEquals(Integer.valueOf(2), aggregated);
    }

    public void testMaxIntegerAggregatorAcceptsMixedNumberTypes() {
        final List<Object> rawData = Arrays.asList(
            Integer.valueOf(1),
            Double.valueOf(7.0d),
            Long.valueOf(3L)
        );

        final Object aggregated =
            RolapAggregator.Max.aggregate(rawData, Dialect.Datatype.Integer);

        assertEquals(Integer.valueOf(7), aggregated);
    }

    public void testIntegerAggregatorAcceptsNumericStrings() {
        final List<Object> rawData = Arrays.asList("5", "2", null);

        final Object sum =
            RolapAggregator.Sum.aggregate(rawData, Dialect.Datatype.Integer);
        final Object min =
            RolapAggregator.Min.aggregate(rawData, Dialect.Datatype.Integer);
        final Object max =
            RolapAggregator.Max.aggregate(rawData, Dialect.Datatype.Integer);

        assertEquals(Integer.valueOf(7), sum);
        assertEquals(Integer.valueOf(2), min);
        assertEquals(Integer.valueOf(5), max);
    }
}
