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

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.MondrianDef;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link ResolvedTable} implementation that wraps an {@link AggStar}.
 *
 * <p>Delegates to AggStar's existing rollup/expression logic to prevent drift
 * for HLL merges, distinct-count rollups, and custom aggregate behaviour.
 *
 * <h2>Measure resolution</h2>
 * <ul>
 *   <li>If {@code MeasureRef.bitPosition() >= 0}, looks up the column
 *       directly via {@link AggStar#lookupColumn(int)}.</li>
 *   <li>If {@code bitPosition < 0}, falls back to a name-based scan of the
 *       star's fact-table measures to obtain the bit position.</li>
 *   <li>When {@link #needsRollup()} is {@code true}, delegates to
 *       {@link AggStar.FactTable.Measure#generateRollupString} (handles
 *       {@code uniqCombinedMerge}, FK-based distinct rollup, etc.).</li>
 *   <li>When {@link #needsRollup()} is {@code false}, delegates to
 *       {@link AggStar.FactTable.Measure#generateExprString} (still applies
 *       merge function for distinct-count state columns).</li>
 * </ul>
 *
 * <h2>Level resolution</h2>
 * <ul>
 *   <li>Collapsed levels live in the agg fact table → no JOIN needed.</li>
 *   <li>Non-collapsed levels live in child DimTables → JOIN clauses are built
 *       from the {@link AggStar.Table.JoinCondition}.</li>
 * </ul>
 */
public class AggResolvedTable implements ResolvedTable {

    private static final Logger LOGGER =
        LogManager.getLogger(AggResolvedTable.class);

    private final AggStar aggStar;
    private final boolean rollup;

    /**
     * Creates an {@code AggResolvedTable}.
     *
     * @param aggStar  the aggregate-star being wrapped
     * @param rollup   whether the SQL generator must apply a rollup/merge step
     */
    public AggResolvedTable(AggStar aggStar, boolean rollup) {
        this.aggStar = aggStar;
        this.rollup = rollup;
    }

    // -----------------------------------------------------------------------
    // ResolvedTable contract
    // -----------------------------------------------------------------------

    @Override
    public String tableName() {
        return aggStar.getFactTable().getName();
    }

    @Override
    public boolean isAggregate() {
        return true;
    }

    @Override
    public boolean needsRollup() {
        return rollup;
    }

    /**
     * Returns the wrapped {@link AggStar} (for diagnostics / logging).
     *
     * @return the underlying agg-star, never {@code null}
     */
    public AggStar getAggStar() {
        return aggStar;
    }

    /**
     * Resolves a measure against the agg table.
     *
     * <p>Looks up the {@link AggStar.FactTable.Measure} by bit position (when
     * {@code >= 0}) or by name (fallback), then calls either
     * {@code generateRollupString} or {@code generateExprString} depending on
     * {@link #needsRollup()}.
     *
     * @return the resolved SQL expression, or {@code null} if the measure
     *         cannot be found in this agg table
     */
    @Override
    public MeasureSql resolveMeasure(MeasureRef measure, String alias) {
        AggStar.FactTable.Measure aggMeasure =
            findAggMeasure(measure);
        if (aggMeasure == null) {
            LOGGER.debug(
                "AggResolvedTable.resolveMeasure: cannot find agg measure"
                + " for: {} in agg table={}",
                measure.uniqueName(), tableName());
            return null;
        }

        SqlQuery sqlQuery = aggStar.getStar().getSqlQuery();
        String expr = rollup
            ? aggMeasure.generateRollupString(sqlQuery)
            : aggMeasure.generateExprString(sqlQuery);
        return new MeasureSql(expr);
    }

    /**
     * Resolves a hierarchy level against the agg table.
     *
     * <p>When the level column lives in the agg fact table (collapsed), returns
     * a simple {@code alias.colName} expression with no join clauses.  When it
     * lives in a child {@link AggStar.DimTable} (non-collapsed), builds JOIN
     * predicates from the dim table's join condition.
     *
     * @return the resolved column expression plus any join clauses, or
     *         {@code null} when the level is not resolvable
     */
    @Override
    public LevelSql resolveLevel(LevelRef level, String alias) {
        if (!(level.hierarchy() instanceof RolapHierarchy)) {
            LOGGER.debug(
                "AggResolvedTable.resolveLevel:"
                + " not RolapHierarchy: {}",
                level.hierarchy().getClass().getSimpleName());
            return null;
        }
        RolapHierarchy rolapHier = (RolapHierarchy) level.hierarchy();

        // Find the leaf (lowest non-All) level
        Level[] levels = rolapHier.getLevels();
        RolapLevel leafLevel = null;
        for (int i = levels.length - 1; i >= 0; i--) {
            if (!levels[i].isAll()) {
                leafLevel = (RolapLevel) levels[i];
                break;
            }
        }
        if (leafLevel == null) {
            LOGGER.debug(
                "AggResolvedTable.resolveLevel:"
                + " no non-All level for {}",
                level.hierarchy().getUniqueName());
            return null;
        }

        MondrianDef.Expression keyExp = leafLevel.getKeyExp();
        if (!(keyExp instanceof MondrianDef.Column)) {
            LOGGER.debug(
                "AggResolvedTable.resolveLevel:"
                + " keyExp is not a Column for level {} in {}",
                leafLevel.getName(),
                level.hierarchy().getUniqueName());
            return null;
        }
        MondrianDef.Column keyColumn = (MondrianDef.Column) keyExp;

        // Resolve the bit position from the star
        RolapStar star = level.star();
        RolapStar.Column starCol =
            star.lookupColumn(keyColumn.table, keyColumn.name);
        if (starCol == null) {
            LOGGER.debug(
                "AggResolvedTable.resolveLevel:"
                + " star column not found for {}.{} in hierarchy {}",
                keyColumn.table, keyColumn.name,
                level.hierarchy().getUniqueName());
            return null;
        }
        int bitPos = starCol.getBitPosition();

        // Look up the agg column
        AggStar.Table.Column aggCol = aggStar.lookupColumn(bitPos);
        if (aggCol == null) {
            LOGGER.debug(
                "AggResolvedTable.resolveLevel:"
                + " agg column not found at bitPos={} for {}",
                bitPos, level.hierarchy().getUniqueName());
            return null;
        }

        String colName = aggCol.getName();
        AggStar.Table colTable = aggCol.getTable();

        if (colTable == aggStar.getFactTable()) {
            // Collapsed: column is in the agg fact table directly
            return new LevelSql(alias + "." + colName);
        }

        // Non-collapsed: column lives in a child DimTable — build JOIN
        List<String> joinClauses = new ArrayList<>();
        buildDimJoin(colTable, alias, joinClauses);
        String dimAlias = colTable.getName();
        return new LevelSql(dimAlias + "." + colName, joinClauses);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Finds the {@link AggStar.FactTable.Measure} for the given
     * {@link MeasureRef}.
     *
     * <p>First tries the bit position; if negative, falls back to a name scan
     * using {@link FactResolvedTable#extractSimpleName(String)}.
     */
    private AggStar.FactTable.Measure findAggMeasure(MeasureRef measure) {
        int bitPos = measure.bitPosition();

        if (bitPos >= 0) {
            AggStar.Table.Column col = aggStar.lookupColumn(bitPos);
            if (col instanceof AggStar.FactTable.Measure) {
                return (AggStar.FactTable.Measure) col;
            }
            if (col != null) {
                LOGGER.debug(
                    "AggResolvedTable.findAggMeasure:"
                    + " column at bitPos={} is {} not a Measure",
                    bitPos, col.getClass().getSimpleName());
            }
            return null;
        }

        // bitPosition < 0: name-based fallback via the star
        String simpleName =
            FactResolvedTable.extractSimpleName(measure.uniqueName());
        RolapStar.Table starFactTable =
            aggStar.getStar().getFactTable();
        RolapStar.Measure starMeasure =
            starFactTable.lookupMeasureByName(measure.cubeName(), simpleName);
        if (starMeasure == null) {
            // Try simple-name scan
            for (RolapStar.Column col : starFactTable.getColumns()) {
                if (col instanceof RolapStar.Measure
                    && col.getName().equals(simpleName))
                {
                    starMeasure = (RolapStar.Measure) col;
                    break;
                }
            }
        }
        if (starMeasure == null) {
            LOGGER.debug(
                "AggResolvedTable.findAggMeasure: star measure not found"
                + " for uniqueName={} cube={}",
                measure.uniqueName(), measure.cubeName());
            return null;
        }

        int resolvedBitPos = starMeasure.getBitPosition();
        AggStar.Table.Column col = aggStar.lookupColumn(resolvedBitPos);
        if (col instanceof AggStar.FactTable.Measure) {
            return (AggStar.FactTable.Measure) col;
        }
        LOGGER.debug(
            "AggResolvedTable.findAggMeasure:"
            + " resolved bitPos={} but column is {}",
            resolvedBitPos,
            col == null ? "null" : col.getClass().getSimpleName());
        return null;
    }

    /**
     * Recursively builds JOIN clauses from a dim table back to the agg fact
     * table, walking the parent chain of {@link AggStar.Table}.
     */
    private void buildDimJoin(
        AggStar.Table dimTable,
        String factAlias,
        List<String> joinClauses)
    {
        if (!dimTable.hasJoinCondition()) {
            return;
        }
        // Recursively ensure the parent is joined first
        if (dimTable.hasParent()
            && dimTable.getParent() != aggStar.getFactTable())
        {
            buildDimJoin(dimTable.getParent(), factAlias, joinClauses);
        }

        AggStar.Table.JoinCondition jc = dimTable.getJoinCondition();
        SqlQuery sqlQuery = aggStar.getStar().getSqlQuery();

        // Determine the parent-side alias (may be fact table alias or an
        // intermediate dim table)
        String parentAlias;
        AggStar.Table parent = dimTable.getParent();
        if (parent == null || parent == aggStar.getFactTable()) {
            parentAlias = factAlias;
        } else {
            parentAlias = parent.getName();
        }

        // Build: JOIN dimTable dimAlias ON parentAlias.leftCol = dimAlias.rightCol
        MondrianDef.Expression leftExpr = jc.getLeft();
        MondrianDef.Expression rightExpr = jc.getRight();

        String leftColName = (leftExpr instanceof MondrianDef.Column)
            ? ((MondrianDef.Column) leftExpr).name
            : leftExpr.getExpression(sqlQuery);
        String rightColName = (rightExpr instanceof MondrianDef.Column)
            ? ((MondrianDef.Column) rightExpr).name
            : rightExpr.getExpression(sqlQuery);

        String dimName = dimTable.getName();
        String join = "JOIN " + dimName
            + " " + dimName
            + " ON " + parentAlias + "." + leftColName
            + " = " + dimName + "." + rightColName;
        joinClauses.add(join);
    }
}

// End AggResolvedTable.java
