/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JdbcSchemaClickHouseTypeTest {

    @Test public void testSimpleAggregateFunctionUInt64IsNumeric() {
        assertTrue(
            JdbcSchema.isClickHouseNumericTypeName(
                "SimpleAggregateFunction(sum, UInt64)"));
    }

    @Test public void testWrappedSimpleAggregateFunctionDecimalIsNumeric() {
        assertTrue(
            JdbcSchema.isClickHouseNumericTypeName(
                "LowCardinality(Nullable(SimpleAggregateFunction(sum, Decimal(18, 2))))"));
    }

    @Test public void testStringTypeIsNotNumeric() {
        assertFalse(
            JdbcSchema.isClickHouseNumericTypeName(
                "LowCardinality(Nullable(String))"));
    }

    @Test public void testAggregateFunctionStateIsNotTreatedAsNumeric() {
        assertFalse(
            JdbcSchema.isClickHouseNumericTypeName(
                "AggregateFunction(sum, UInt64)"));
    }

    @Test public void testUnwrapClickHouseTypeName() {
        assertEquals(
            "UInt64",
            JdbcSchema.unwrapClickHouseTypeName(
                "Nullable(SimpleAggregateFunction(sum, UInt64))"));
    }
}
