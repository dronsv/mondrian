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
        if (exclude
            || level == null
            || !isTerminalLevelConstraint(level, fromLevel, includeParentLevels))
        {
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
        if (exclude
            || columns == null
            || columns.isEmpty()
            || predicate == null
            || predicate.length() == 0)
        {
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
        if (!isEnabled()
            || sqlQuery == null
            || baseCube == null
            || aggStar != null
            || column == null)
        {
            return false;
        }
        if (!isClickHouseDialect(sqlQuery.getDialect())) {
            return false;
        }
        return column.getTable() == baseCube.getStar().getFactTable();
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
}
