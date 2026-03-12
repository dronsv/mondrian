/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import junit.framework.TestCase;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.SqlStatement;
import mondrian.spi.Dialect;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClickHouseSqlSettingsSupportTest extends TestCase {
    private final String[] trackedProperties = new String[] {
        ClickHouseSqlSettingsSupport.PROP_ENABLED,
        ClickHouseSqlSettingsSupport.PROP_GLOBAL,
        ClickHouseSqlSettingsSupport.PROP_BY_CATALOG,
        ClickHouseSqlSettingsSupport.PROP_ALLOWLIST
    };

    private Map<String, String> previousValues;

    @Override
    protected void setUp() {
        previousValues = new LinkedHashMap<String, String>();
        final MondrianProperties properties = MondrianProperties.instance();
        for (String property : trackedProperties) {
            previousValues.put(property, properties.getProperty(property));
            properties.remove(property);
        }
    }

    @Override
    protected void tearDown() {
        final MondrianProperties properties = MondrianProperties.instance();
        for (Map.Entry<String, String> entry : previousValues.entrySet()) {
            if (entry.getValue() == null) {
                properties.remove(entry.getKey());
            } else {
                properties.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    public void testBuildSettingsClauseDisabledByDefault() {
        MondrianProperties.instance().setProperty(
            ClickHouseSqlSettingsSupport.PROP_GLOBAL,
            "max_threads=8");
        assertNull(
            ClickHouseSqlSettingsSupport.buildSettingsClause(
                clickHouseDialect(),
                "Konfet"));
    }

    public void testBuildSettingsClauseMergesGlobalAndCatalogOverride() {
        final MondrianProperties properties = MondrianProperties.instance();
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_ENABLED,
            "true");
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_GLOBAL,
            "max_threads=8,max_execution_time=20,readonly=1");
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_BY_CATALOG,
            "Konfet=max_threads=4,max_memory_usage=1000000");
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_ALLOWLIST,
            "max_threads,max_execution_time,max_memory_usage");

        assertEquals(
            "SETTINGS max_threads=4, max_execution_time=20, max_memory_usage=1000000",
            ClickHouseSqlSettingsSupport.buildSettingsClause(
                clickHouseDialect(),
                "konfet"));
    }

    public void testBuildSettingsClauseSkipsInvalidOrUnsafeValues() {
        final MondrianProperties properties = MondrianProperties.instance();
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_ENABLED,
            "true");
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_GLOBAL,
            "max_threads=8,bad_setting=1,max_memory_usage=1 OR 1=1,max_execution_time='30'");
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_ALLOWLIST,
            "max_threads,max_memory_usage,max_execution_time");

        assertEquals(
            "SETTINGS max_threads=8, max_execution_time='30'",
            ClickHouseSqlSettingsSupport.buildSettingsClause(
                clickHouseDialect(),
                "Konfet"));
    }

    public void testBuildSettingsClauseDoesNotApplyOnNonClickHouseDialect() {
        final MondrianProperties properties = MondrianProperties.instance();
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_ENABLED,
            "true");
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_GLOBAL,
            "max_threads=8");

        assertNull(
            ClickHouseSqlSettingsSupport.buildSettingsClause(
                postgresDialect(),
                "Konfet"));
    }

    public void testSqlQueryAppendsSettingsAfterLimit() {
        final MondrianProperties properties = MondrianProperties.instance();
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_ENABLED,
            "true");
        properties.setProperty(
            ClickHouseSqlSettingsSupport.PROP_GLOBAL,
            "max_threads=6,max_execution_time=25");

        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        sqlQuery.addSelect("1", SqlStatement.Type.INT);
        sqlQuery.addRowLimit(10);

        final String sql = sqlQuery.toString();
        assertTrue(sql.contains("LIMIT 10"));
        assertTrue(sql.contains("SETTINGS"));
        assertTrue(sql.contains("max_threads=6"));
        assertTrue(sql.contains("max_execution_time=25"));
    }

    private Dialect clickHouseDialect() {
        final Dialect dialect = mock(Dialect.class);
        when(dialect.getDatabaseProduct())
            .thenReturn(Dialect.DatabaseProduct.CLICKHOUSE);
        when(dialect.allowsAs()).thenReturn(true);
        when(dialect.requiresDrillthroughMaxRowsInLimit()).thenReturn(true);
        doAnswer(invocation -> {
            final String identifier = invocation.getArgument(0, String.class);
            final StringBuilder buf =
                invocation.getArgument(1, StringBuilder.class);
            buf.append('`').append(identifier).append('`');
            return null;
        }).when(dialect).quoteIdentifier(anyString(), any(StringBuilder.class));
        return dialect;
    }

    private Dialect postgresDialect() {
        final Dialect dialect = mock(Dialect.class);
        when(dialect.getDatabaseProduct())
            .thenReturn(Dialect.DatabaseProduct.POSTGRESQL);
        return dialect;
    }
}
