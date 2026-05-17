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

import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapStar;
import mondrian.rolap.SqlStatement;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.spi.Dialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClickHousePrewhereSupportTest {
    private Map<String, String> previousValues;

    @BeforeEach public void setUp() {
        previousValues = new LinkedHashMap<String, String>();
        final MondrianProperties properties = MondrianProperties.instance();
        previousValues.put(
            ClickHousePrewhereSupport.PROP_ENABLED,
            properties.getProperty(ClickHousePrewhereSupport.PROP_ENABLED));
        properties.remove(ClickHousePrewhereSupport.PROP_ENABLED);
    }

    @AfterEach public void tearDown() {
        final MondrianProperties properties = MondrianProperties.instance();
        final String value =
            previousValues.get(ClickHousePrewhereSupport.PROP_ENABLED);
        if (value == null) {
            properties.remove(ClickHousePrewhereSupport.PROP_ENABLED);
        } else {
            properties.setProperty(
                ClickHousePrewhereSupport.PROP_ENABLED,
                value);
        }
    }

    @Test public void testDefaultEnabledWhenPropertyUnset() {
        MondrianProperties.instance().remove(
            ClickHousePrewhereSupport.PROP_ENABLED);
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);

        assertTrue(
            ClickHousePrewhereSupport.shouldUsePrewhere(
                sqlQuery,
                fixture.baseCube,
                null,
                fixture.column),
            "PREWHERE must be enabled by default when the property is unset");
    }

    @Test public void testExplicitFalseDisablesPrewhere() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "false");
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);

        assertFalse(
            ClickHousePrewhereSupport.shouldUsePrewhere(
                sqlQuery,
                fixture.baseCube,
                null,
                fixture.column),
            "explicit enabled=false must disable PREWHERE (opt-out)");
        assertEquals(
            ClickHousePrewhereSupport.REASON_DISABLED,
            sqlQuery.getPreWhereFallbackReason());
    }

    @Test public void testAddsEligibleFactPredicateToPrewhere() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        sqlQuery.addSelect("1", SqlStatement.Type.INT);
        sqlQuery.addFromQuery("select * from fact", "f", false);

        final Fixture fixture = fixture(true);
        assertTrue(
            ClickHousePrewhereSupport.addSimplePredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                fixture.column,
                "f.manufacturer_group = 'Acme'"));

        final String sql = sqlQuery.toString();
        assertTrue(sqlQuery.hasPreWhere());
        assertNull(sqlQuery.getPreWhereFallbackReason());
        assertTrue(sql.contains(" prewhere "));
        assertTrue(sql.contains("f.manufacturer_group = 'Acme'"));
        assertFalse(sql.contains(" where f.manufacturer_group = 'Acme'"));
    }

    @Test public void testLeavesNonFactPredicateInWhere() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        sqlQuery.addSelect("1", SqlStatement.Type.INT);
        sqlQuery.addFromQuery("select * from fact", "f", false);

        final Fixture fixture = fixture(false);
        assertFalse(
            ClickHousePrewhereSupport.addSimplePredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                fixture.column,
                "d.manufacturer_group = 'Acme'"));
        sqlQuery.addWhere("d.manufacturer_group = 'Acme'");

        final String sql = sqlQuery.toString();
        assertFalse(sqlQuery.hasPreWhere());
        assertEquals(
            ClickHousePrewhereSupport.REASON_NON_FACT_COLUMN,
            sqlQuery.getPreWhereFallbackReason());
        assertFalse(sql.contains(" prewhere "));
        assertTrue(sql.contains(" where d.manufacturer_group = 'Acme'"));
    }

    @Test public void testDoesNotUsePrewhereForAggQueries() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final Fixture fixture = fixture(true);

        assertFalse(
            ClickHousePrewhereSupport.shouldUsePrewhere(
                new SqlQuery(clickHouseDialect(), "Konfet"),
                fixture.baseCube,
                mock(AggStar.class),
                fixture.column));
    }

    @Test public void testRecordsAggQueryFallbackReason() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final Fixture fixture = fixture(true);
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");

        assertFalse(
            ClickHousePrewhereSupport.shouldUsePrewhere(
                sqlQuery,
                fixture.baseCube,
                mock(AggStar.class),
                fixture.column));
        assertEquals(
            ClickHousePrewhereSupport.REASON_AGG_QUERY,
            sqlQuery.getPreWhereFallbackReason());
    }

    @Test public void testAddsTerminalLevelInPredicateToPrewhere() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        sqlQuery.addSelect("1", SqlStatement.Type.INT);
        sqlQuery.addFromQuery("select * from fact", "f", false);

        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        final RolapLevel fromLevel = level;

        assertTrue(
            ClickHousePrewhereSupport.addLevelConstraintPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                fixture.column,
                "f.manufacturer_group in ('Acme','Beta')",
                false,
                true,
                level,
                fromLevel));

        final String sql = sqlQuery.toString();
        assertTrue(sql.contains(" prewhere "));
        assertTrue(sql.contains("f.manufacturer_group in ('Acme','Beta')"));
    }

    @Test public void testDoesNotUsePrewhereForNonTerminalLevelConstraint() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        final RolapLevel fromLevel = mock(RolapLevel.class);

        assertFalse(
            ClickHousePrewhereSupport.addLevelConstraintPredicate(
                new SqlQuery(clickHouseDialect(), "Konfet"),
                fixture.baseCube,
                null,
                fixture.column,
                "f.region in ('A','B')",
                false,
                true,
                level,
                fromLevel));
    }

    @Test public void testRecordsNonTerminalLevelFallbackReason() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        final RolapLevel fromLevel = mock(RolapLevel.class);
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");

        assertFalse(
            ClickHousePrewhereSupport.addLevelConstraintPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                fixture.column,
                "f.region in ('A','B')",
                false,
                true,
                level,
                fromLevel));
        assertEquals(
            ClickHousePrewhereSupport.REASON_NON_TERMINAL_LEVEL_CONSTRAINT,
            sqlQuery.getPreWhereFallbackReason());
    }

    @Test public void testDoesNotUsePrewhereForExcludedLevelConstraint() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);

        assertFalse(
            ClickHousePrewhereSupport.addLevelConstraintPredicate(
                new SqlQuery(clickHouseDialect(), "Konfet"),
                fixture.baseCube,
                null,
                fixture.column,
                "not (f.region in ('A','B'))",
                true,
                true,
                level,
                level));
    }

    @Test public void testAddsWholeMultiLevelConditionToPrewhereWhenAllColumnsAreFact() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        sqlQuery.addSelect("1", SqlStatement.Type.INT);
        sqlQuery.addFromQuery("select * from fact", "f", false);

        final Fixture fixture = fixture(true);
        final RolapStar.Column secondFactColumn = mock(RolapStar.Column.class);
        when(secondFactColumn.getTable()).thenReturn(fixture.factTable);

        assertTrue(
            ClickHousePrewhereSupport.addConditionPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                Arrays.asList(fixture.column, secondFactColumn),
                "f.region = 'A' and f.city in ('X','Y')",
                false));

        final String sql = sqlQuery.toString();
        assertTrue(sql.contains(" prewhere "));
        assertTrue(sql.contains("f.region = 'A' and f.city in ('X','Y')"));
    }

    @Test public void testDoesNotUsePrewhereForMixedTableMultiLevelCondition() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final Fixture fixture = fixture(true);
        final RolapStar.Column nonFactColumn = mock(RolapStar.Column.class);
        when(nonFactColumn.getTable()).thenReturn(fixture.otherTable);

        assertFalse(
            ClickHousePrewhereSupport.addConditionPredicate(
                new SqlQuery(clickHouseDialect(), "Konfet"),
                fixture.baseCube,
                null,
                Arrays.asList(fixture.column, nonFactColumn),
                "f.region = 'A' and d.city in ('X','Y')",
                false));
    }

    @Test public void testRecordsFirstRejectedColumnFallbackReasonForMixedTableCondition() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED,
            "true");
        final Fixture fixture = fixture(true);
        final RolapStar.Column nonFactColumn = mock(RolapStar.Column.class);
        when(nonFactColumn.getTable()).thenReturn(fixture.otherTable);
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");

        assertFalse(
            ClickHousePrewhereSupport.addConditionPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                Arrays.asList(fixture.column, nonFactColumn),
                "f.region = 'A' and d.city in ('X','Y')",
                false));
        assertEquals(
            ClickHousePrewhereSupport.REASON_NON_FACT_COLUMN,
            sqlQuery.getPreWhereFallbackReason());
    }

    @Test public void testAddJoinedDimPredicateEmitsSubqueryForSingleEqOnUniqueLeaf() {
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        when(level.isUnique()).thenReturn(true);

        assertTrue(
            ClickHousePrewhereSupport.addJoinedDimPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                level,
                fixture.column,
                "`f`.`sku_key`",
                "`dim_konfet_product`",
                "`sku_key`",
                "`dim_konfet_product`.`brand` = 'X'"));

        assertTrue(sqlQuery.hasPreWhere());
        final String pre = sqlQuery.toString();
        assertTrue(
            pre.contains(
                "`f`.`sku_key` in"
                + " (select `sku_key` from `dim_konfet_product`"
                + " where `dim_konfet_product`.`brand` = 'X')"),
            "expected fact-FK IN (subquery) PREWHERE; got: " + pre);
    }

    @Test public void testAddJoinedDimPredicateEmitsSubqueryForMultiValueIn() {
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        when(level.isUnique()).thenReturn(true);

        assertTrue(
            ClickHousePrewhereSupport.addJoinedDimPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                level,
                fixture.column,
                "`f`.`sku_key`",
                "`dim_konfet_product`",
                "`sku_key`",
                "`dim_konfet_product`.`brand` in ('X','Y','Z')"));

        assertTrue(sqlQuery.hasPreWhere());
        assertTrue(
            sqlQuery.toString().contains("in ('X','Y','Z')"));
    }

    @Test public void testAddJoinedDimPredicateAllowsNullLevelForSimpleSingleValue() {
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);

        assertTrue(
            ClickHousePrewhereSupport.addJoinedDimPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                null,
                fixture.column,
                "`f`.`sku_key`",
                "`dim_konfet_product`",
                "`sku_key`",
                "`dim_konfet_product`.`brand` = 'X'"));
        assertTrue(sqlQuery.hasPreWhere());
    }

    @Test public void testAddJoinedDimPredicateDeclinesNonUniqueLevel() {
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        when(level.isUnique()).thenReturn(false);

        assertFalse(
            ClickHousePrewhereSupport.addJoinedDimPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                level,
                fixture.column,
                "`f`.`fk`",
                "`dim`",
                "`pk`",
                "`dim`.`col` = 'X'"));
        assertFalse(sqlQuery.hasPreWhere());
        assertEquals(
            ClickHousePrewhereSupport.REASON_NON_UNIQUE_LEVEL,
            sqlQuery.getPreWhereFallbackReason());
    }

    @Test public void testAddJoinedDimPredicateDeclinesAggQuery() {
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        when(level.isUnique()).thenReturn(true);

        assertFalse(
            ClickHousePrewhereSupport.addJoinedDimPredicate(
                sqlQuery,
                fixture.baseCube,
                mock(AggStar.class),
                level,
                fixture.column,
                "`f`.`fk`",
                "`dim`",
                "`pk`",
                "`dim`.`col` = 'X'"));
        assertEquals(
            ClickHousePrewhereSupport.REASON_AGG_QUERY,
            sqlQuery.getPreWhereFallbackReason());
    }

    @Test public void testAddJoinedDimPredicateDeclinesWhenExplicitlyDisabled() {
        MondrianProperties.instance().setProperty(
            ClickHousePrewhereSupport.PROP_ENABLED, "false");
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        when(level.isUnique()).thenReturn(true);

        assertFalse(
            ClickHousePrewhereSupport.addJoinedDimPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                level,
                fixture.column,
                "`f`.`fk`",
                "`dim`",
                "`pk`",
                "`dim`.`col` = 'X'"));
        assertEquals(
            ClickHousePrewhereSupport.REASON_DISABLED,
            sqlQuery.getPreWhereFallbackReason());
    }

    @Test public void testAddJoinedDimPredicateDeclinesNullForeignKeyColumn() {
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        when(level.isUnique()).thenReturn(true);

        assertFalse(
            ClickHousePrewhereSupport.addJoinedDimPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                level,
                null,
                "`f`.`fk`",
                "`dim`",
                "`pk`",
                "`dim`.`col` = 'X'"));
        assertEquals(
            ClickHousePrewhereSupport.REASON_NULL_FK,
            sqlQuery.getPreWhereFallbackReason());
    }

    @Test public void testAddJoinedDimPredicateDeclinesEmptyPredicate() {
        final SqlQuery sqlQuery = new SqlQuery(clickHouseDialect(), "Konfet");
        final Fixture fixture = fixture(true);
        final RolapLevel level = mock(RolapLevel.class);
        when(level.isUnique()).thenReturn(true);

        assertFalse(
            ClickHousePrewhereSupport.addJoinedDimPredicate(
                sqlQuery,
                fixture.baseCube,
                null,
                level,
                fixture.column,
                "`f`.`fk`",
                "`dim`",
                "`pk`",
                ""));
        assertEquals(
            ClickHousePrewhereSupport.REASON_EMPTY_PREDICATE,
            sqlQuery.getPreWhereFallbackReason());
    }

    private Fixture fixture(boolean factColumn) {
        final Fixture fixture = new Fixture();
        fixture.baseCube = mock(RolapCube.class);
        fixture.star = mock(RolapStar.class);
        fixture.factTable = mock(RolapStar.Table.class);
        fixture.otherTable = mock(RolapStar.Table.class);
        fixture.column = mock(RolapStar.Column.class);

        when(fixture.baseCube.getStar()).thenReturn(fixture.star);
        when(fixture.star.getFactTable()).thenReturn(fixture.factTable);
        when(fixture.column.getTable())
            .thenReturn(factColumn ? fixture.factTable : fixture.otherTable);

        return fixture;
    }

    private Dialect clickHouseDialect() {
        final Dialect dialect = mock(Dialect.class);
        when(dialect.getDatabaseProduct())
            .thenReturn(Dialect.DatabaseProduct.CLICKHOUSE);
        when(dialect.allowsAs()).thenReturn(true);
        doAnswer(invocation -> {
            final String identifier = invocation.getArgument(0, String.class);
            final StringBuilder buf =
                invocation.getArgument(1, StringBuilder.class);
            buf.append('`').append(identifier).append('`');
            return null;
        }).when(dialect).quoteIdentifier(anyString(), any(StringBuilder.class));
        return dialect;
    }

    private static class Fixture {
        private RolapCube baseCube;
        private RolapStar star;
        private RolapStar.Table factTable;
        private RolapStar.Table otherTable;
        private RolapStar.Column column;
    }
}
