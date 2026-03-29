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
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapStar;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.spi.Dialect;

import java.util.List;
import java.util.Locale;

/**
 * Routes eligible ClickHouse fact-table predicates into PREWHERE.
 *
 * <p>The initial implementation is intentionally narrow: it only targets
 * simple predicates on the base fact table for fact SQL. Aggregate queries and
 * predicates that require joined dimension tables stay in WHERE.</p>
 */
public final class ClickHousePrewhereSupport {
    public static final String PROP_ENABLED =
        "mondrian.clickhouse.prewhere.enabled";
    static final String REASON_DISABLED = "disabled";
    static final String REASON_NULL_BASE_CUBE = "null_base_cube";
    static final String REASON_AGG_QUERY = "agg_query";
    static final String REASON_NULL_COLUMN = "null_column";
    static final String REASON_NON_CLICKHOUSE_DIALECT =
        "non_clickhouse_dialect";
    static final String REASON_NON_FACT_COLUMN = "non_fact_column";
    static final String REASON_EXCLUDED = "excluded";
    static final String REASON_NULL_LEVEL = "null_level";
    static final String REASON_NON_TERMINAL_LEVEL_CONSTRAINT =
        "non_terminal_level_constraint";
    static final String REASON_EMPTY_COLUMNS = "empty_columns";
    static final String REASON_EMPTY_PREDICATE = "empty_predicate";

    private ClickHousePrewhereSupport() {
    }

    public static boolean addSimplePredicate(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapStar.Column column,
        String predicate)
    {
        if (!shouldUsePrewhere(sqlQuery, baseCube, aggStar, column)) {
            return false;
        }
        sqlQuery.addPreWhere(predicate);
        return true;
    }

    public static boolean addLevelConstraintPredicate(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapStar.Column column,
        String predicate,
        boolean exclude,
        boolean includeParentLevels,
        RolapLevel level,
        RolapLevel fromLevel)
    {
        if (exclude) {
            noteFallback(sqlQuery, REASON_EXCLUDED);
            return false;
        }
        if (level == null) {
            noteFallback(sqlQuery, REASON_NULL_LEVEL);
            return false;
        }
        if (!isTerminalLevelConstraint(level, fromLevel, includeParentLevels)) {
            noteFallback(sqlQuery, REASON_NON_TERMINAL_LEVEL_CONSTRAINT);
            return false;
        }
        return addSimplePredicate(
            sqlQuery,
            baseCube,
            aggStar,
            column,
            predicate);
    }

    public static boolean addConditionPredicate(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        List<RolapStar.Column> columns,
        String predicate,
        boolean exclude)
    {
        if (exclude) {
            noteFallback(sqlQuery, REASON_EXCLUDED);
            return false;
        }
        if (columns == null || columns.isEmpty()) {
            noteFallback(sqlQuery, REASON_EMPTY_COLUMNS);
            return false;
        }
        if (predicate == null || predicate.length() == 0) {
            noteFallback(sqlQuery, REASON_EMPTY_PREDICATE);
            return false;
        }
        for (RolapStar.Column column : columns) {
            if (!shouldUsePrewhere(sqlQuery, baseCube, aggStar, column)) {
                return false;
            }
        }
        sqlQuery.addPreWhere(predicate);
        return true;
    }

    static boolean shouldUsePrewhere(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar,
        RolapStar.Column column)
    {
        if (!isEnabled()) {
            noteFallback(sqlQuery, REASON_DISABLED);
            return false;
        }
        if (sqlQuery == null) {
            return false;
        }
        if (baseCube == null) {
            noteFallback(sqlQuery, REASON_NULL_BASE_CUBE);
            return false;
        }
        if (aggStar != null) {
            noteFallback(sqlQuery, REASON_AGG_QUERY);
            return false;
        }
        if (column == null) {
            noteFallback(sqlQuery, REASON_NULL_COLUMN);
            return false;
        }
        if (!isClickHouseDialect(sqlQuery.getDialect())) {
            noteFallback(sqlQuery, REASON_NON_CLICKHOUSE_DIALECT);
            return false;
        }
        if (column.getTable() != baseCube.getStar().getFactTable()) {
            noteFallback(sqlQuery, REASON_NON_FACT_COLUMN);
            return false;
        }
        return true;
    }

    private static boolean isEnabled() {
        final String value =
            MondrianProperties.instance().getProperty(PROP_ENABLED);
        if (value == null) {
            return false;
        }
        final String normalized = value.trim().toLowerCase(Locale.ROOT);
        return "true".equals(normalized)
            || "1".equals(normalized)
            || "on".equals(normalized)
            || "yes".equals(normalized);
    }

    private static boolean isClickHouseDialect(Dialect dialect) {
        return dialect != null
            && dialect.getDatabaseProduct()
            == Dialect.DatabaseProduct.CLICKHOUSE;
    }

    private static boolean isTerminalLevelConstraint(
        RolapLevel level,
        RolapLevel fromLevel,
        boolean includeParentLevels)
    {
        return level.isUnique()
            || level == fromLevel
            || !includeParentLevels;
    }

    private static void noteFallback(SqlQuery sqlQuery, String reason) {
        if (sqlQuery != null) {
            sqlQuery.setPreWhereFallbackReason(reason);
        }
    }
}
