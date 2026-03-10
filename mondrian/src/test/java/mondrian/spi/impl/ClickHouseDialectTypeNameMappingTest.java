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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    public void testVersionAtLeastForGroupingSetsSupported() {
        assertTrue(ClickHouseDialect.isVersionAtLeast("24.1.8.22", 22, 1));
        assertTrue(ClickHouseDialect.isVersionAtLeast("22.1.0.0", 22, 1));
    }

    public void testVersionAtLeastForGroupingSetsUnsupported() {
        assertFalse(ClickHouseDialect.isVersionAtLeast("21.12.0.0", 22, 1));
        assertFalse(ClickHouseDialect.isVersionAtLeast("22.0.9.1", 22, 1));
        assertFalse(ClickHouseDialect.isVersionAtLeast("", 22, 1));
    }

    public void testGetTypePrefersTypeNameForLowCardinalityString() throws Exception {
        final ClickHouseDialect dialect = newDialect();
        final ResultSetMetaData metaData = mock(ResultSetMetaData.class);
        when(metaData.getColumnType(1)).thenReturn(Types.NUMERIC);
        when(metaData.getPrecision(1)).thenReturn(18);
        when(metaData.getScale(1)).thenReturn(4);
        when(metaData.getColumnTypeName(1)).thenReturn("LowCardinality(String)");

        assertEquals(SqlStatement.Type.STRING, dialect.getType(metaData, 0));
    }

    private ClickHouseDialect newDialect() throws SQLException {
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getIdentifierQuoteString()).thenReturn("`");
        when(databaseMetaData.getDatabaseProductName()).thenReturn("ClickHouse");
        when(databaseMetaData.getDatabaseProductVersion()).thenReturn("24.1.8.22");
        when(databaseMetaData.getDriverName()).thenReturn("clickhouse-jdbc");
        when(databaseMetaData.isReadOnly()).thenReturn(true);
        when(databaseMetaData.getMaxColumnNameLength()).thenReturn(256);
        when(databaseMetaData.supportsResultSetConcurrency(anyInt(), anyInt())).thenReturn(false);
        return new ClickHouseDialect(connection);
    }
}
