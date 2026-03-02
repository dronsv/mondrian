/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.spi.impl;

import junit.framework.TestCase;
import mondrian.rolap.SqlStatement;

public class ClickHouseDialectTypeNameMappingTest extends TestCase {

    public void testUnwrapSimpleAggregateFunctionType() {
        assertEquals(
            "UInt64",
            ClickHouseDialect.unwrapClickHouseType(
                "SimpleAggregateFunction(sum, UInt64)"));
    }

    public void testUnwrapNestedWrappers() {
        assertEquals(
            "Decimal(18, 2)",
            ClickHouseDialect.unwrapClickHouseType(
                "LowCardinality(Nullable(SimpleAggregateFunction(sum, Decimal(18, 2))))"));
    }

    public void testMapSimpleAggregateFunctionUInt64AsObject() {
        assertEquals(
            SqlStatement.Type.OBJECT,
            ClickHouseDialect.mapClickHouseOtherType(
                ClickHouseDialect.unwrapClickHouseType(
                    "SimpleAggregateFunction(sum, UInt64)")));
    }

    public void testMapUInt64AsObject() {
        assertEquals(
            SqlStatement.Type.OBJECT,
            ClickHouseDialect.mapClickHouseOtherType(
                ClickHouseDialect.unwrapClickHouseType("UInt64")));
    }

    public void testMapIpv4AndIpv6AsString() {
        assertEquals(
            SqlStatement.Type.STRING,
            ClickHouseDialect.mapClickHouseOtherType(
                ClickHouseDialect.unwrapClickHouseType("IPv4")));
        assertEquals(
            SqlStatement.Type.STRING,
            ClickHouseDialect.mapClickHouseOtherType(
                ClickHouseDialect.unwrapClickHouseType("Nullable(IPv6)")));
    }

    public void testUnknownOtherTypeReturnsNull() {
        assertNull(
            ClickHouseDialect.mapClickHouseOtherType(
                ClickHouseDialect.unwrapClickHouseType(
                    "Map(String, UInt64)")));
    }
}
