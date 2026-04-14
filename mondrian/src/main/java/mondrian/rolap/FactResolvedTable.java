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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link ResolvedTable} implementation that wraps the fact table of a
 * {@link RolapStar}.
 *
 * <p>Extracts the fact-table SQL logic previously embedded in
 * {@link NativeQuerySqlGenerator}: measure lookup, column name derivation, and
 * dimension-table JOIN generation.
 *
 * <p>This is always a live (non-aggregated) table, so both {@link #isAggregate}
 * and {@link #needsRollup} return {@code false}.
 */
public class FactResolvedTable implements ResolvedTable {

    private static final Logger LOGGER =
        LogManager.getLogger(FactResolvedTable.class);

    private final RolapStar star;
    private final RolapCube cube;

    /**
     * Creates a {@code FactResolvedTable} for the given star and cube.
     *
     * @param star  the {@link RolapStar} whose fact table this wraps
     * @param cube  the {@link RolapCube} that owns the measures/hierarchies
     */
    public FactResolvedTable(RolapStar star, RolapCube cube) {
        this.star = star;
        this.cube = cube;
    }

    // -----------------------------------------------------------------------
    // ResolvedTable contract
    // -----------------------------------------------------------------------

    @Override
    public String tableName() {
        return star.getFactTable().getTableName();
    }

    @Override
    public boolean isAggregate() {
        return false;
    }

    @Override
    public boolean needsRollup() {
        return false;
    }

    /**
     * Resolves a measure against the fact table.
     *
     * <p>Looks up the {@link RolapStar.Measure} by the measure's unique name
     * (parsed to a simple name) and the cube name from the supplied
     * {@link MeasureRef}, then applies the aggregator expression.
     *
     * @return the resolved SQL expression, or {@code null} if the measure
     *         cannot be found in this star (rather than throwing, to preserve
     *         the existing NQE fallback behaviour)
     */
    @Override
    public MeasureSql resolveMeasure(MeasureRef measure, String alias) {
        RolapStar.Table factTable = star.getFactTable();
        RolapStar.Measure starMeasure =
            findStarMeasure(factTable, measure.uniqueName(), measure.cubeName());
        if (starMeasure == null) {
            LOGGER.debug(
                "FactResolvedTable.resolveMeasure: cannot find star measure"
                + " for: {} in cube={}",
                measure.uniqueName(), measure.cubeName());
            return null;
        }
        String colExpr = alias + "." + getColumnName(starMeasure);
        RolapAggregator agg = starMeasure.getAggregator();
        return new MeasureSql(agg.getExpression(colExpr));
    }

    /**
     * Resolves a hierarchy level against the fact table, adding any required
     * JOIN clauses to dimension tables.
     *
     * @return the resolved SQL column + join clauses, or {@code null} when the
     *         level cannot be resolved (non-RolapHierarchy, all-level, etc.)
     */
    @Override
    public LevelSql resolveLevel(LevelRef level, String alias) {
        List<String> joinClauses = new ArrayList<String>();
        Set<String> seenJoins = new HashSet<String>();

        String colExpr = resolveHierarchyColumn(
            level.hierarchy(), alias, joinClauses, seenJoins);
        if (colExpr == null) {
            return null;
        }
        return new LevelSql(colExpr, joinClauses);
    }

    // -----------------------------------------------------------------------
    // Internal helpers (package-visible for testability)
    // -----------------------------------------------------------------------

    /**
     * Resolves the leaf-level column expression for a hierarchy.
     *
     * <p>Extracted from {@link NativeQuerySqlGenerator#resolveHierarchyColumn}.
     */
    String resolveHierarchyColumn(
        Hierarchy hierarchy,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        if (!(hierarchy instanceof RolapHierarchy)) {
            LOGGER.debug(
                "FactResolvedTable.resolveHierarchyColumn:"
                + " not RolapHierarchy: {}",
                hierarchy.getClass().getSimpleName());
            return null;
        }
        RolapHierarchy rolapHier = (RolapHierarchy) hierarchy;
        Level[] levels = rolapHier.getLevels();

        // Find lowest non-All level
        RolapLevel leafLevel = null;
        for (int i = levels.length - 1; i >= 0; i--) {
            if (!levels[i].isAll()) {
                leafLevel = (RolapLevel) levels[i];
                break;
            }
        }
        if (leafLevel == null) {
            LOGGER.debug(
                "FactResolvedTable.resolveHierarchyColumn:"
                + " no non-All level for {}",
                hierarchy.getUniqueName());
            return null;
        }

        MondrianDef.Expression keyExp = leafLevel.getKeyExp();
        if (keyExp == null) {
            LOGGER.debug(
                "FactResolvedTable.resolveHierarchyColumn:"
                + " null keyExp for level {} in {}",
                leafLevel.getName(), hierarchy.getUniqueName());
            return null;
        }

        if (keyExp instanceof MondrianDef.Column) {
            MondrianDef.Column keyColumn = (MondrianDef.Column) keyExp;

            // Try star-backed column resolution first
            NativeSqlCalc.ResolvedColumnSql resolved =
                NativeSqlCalc.resolveLevelColumnSql(
                    keyColumn, star, factAlias, joinClauses, seenJoins);
            if (resolved != null) {
                return resolved.qualifiedColumn;
            }

            // Fallback: resolve via dimension table JOIN
            String dimCol = resolveDimensionColumn(
                keyColumn, rolapHier, factAlias, joinClauses, seenJoins);
            if (dimCol == null) {
                LOGGER.debug(
                    "FactResolvedTable.resolveHierarchyColumn:"
                    + " both star and dim resolution failed for {}"
                    + " (keyCol={}.{}, hierType={}, cube={})",
                    hierarchy.getUniqueName(),
                    keyColumn.table, keyColumn.name,
                    hierarchy.getClass().getSimpleName(),
                    cube.getName());
            }
            return dimCol;
        }

        LOGGER.debug(
            "FactResolvedTable.resolveHierarchyColumn:"
            + " keyExp is {} (not Column) for {}",
            keyExp.getClass().getSimpleName(),
            hierarchy.getUniqueName());
        return null;
    }

    /**
     * Resolves a dimension column by building a JOIN to the dimension table.
     *
     * <p>Extracted from {@link NativeQuerySqlGenerator#resolveDimensionColumn}.
     */
    String resolveDimensionColumn(
        MondrianDef.Column keyColumn,
        RolapHierarchy hierarchy,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        final String columnName = keyColumn.name;
        final MondrianDef.RelationOrJoin relation = hierarchy.getRelation();
        if (!(relation instanceof MondrianDef.Table)) {
            return null;
        }

        final MondrianDef.Table dimTable = (MondrianDef.Table) relation;
        final String dimAlias = dimTable.getAlias() != null
            ? dimTable.getAlias()
            : dimTable.name;
        final String dimTableName = dimTable.name;

        // Find foreign key from cube usages
        String foreignKey = null;
        final HierarchyUsage[] usages = cube.getUsages(hierarchy);
        if (usages != null && usages.length > 0) {
            foreignKey = usages[0].getForeignKey();
        }

        final MondrianDef.Hierarchy xmlHier = hierarchy.getXmlHierarchy();
        String primaryKey = xmlHier != null ? xmlHier.primaryKey : null;
        if (primaryKey == null) {
            primaryKey = columnName;
        }

        if (foreignKey == null) {
            LOGGER.debug(
                "FactResolvedTable: no foreign key for dim {} in {}"
                + " — skipping from GROUP BY",
                hierarchy.getName(), cube.getName());
            return null;
        }

        final String join = "JOIN " + dimTableName
            + " " + dimAlias
            + " ON " + factAlias + "." + foreignKey
            + " = " + dimAlias + "." + primaryKey;
        if (seenJoins.add(join)) {
            joinClauses.add(join);
        }
        return dimAlias + "." + columnName;
    }

    /**
     * Finds the {@link RolapStar.Measure} for the given measure unique name,
     * scoped to {@code cubeName}.
     *
     * <p>First tries the cube-qualified lookup; if that fails, falls back to
     * a simple-name scan across all columns in the fact table.
     *
     * <p>Extracted from {@link NativeQuerySqlGenerator#findStarMeasure}.
     */
    RolapStar.Measure findStarMeasure(
        RolapStar.Table factTable,
        String measureUniqueName,
        String cubeName)
    {
        String simpleName = extractSimpleName(measureUniqueName);

        // 1. Cube-qualified lookup
        RolapStar.Measure m = factTable.lookupMeasureByName(cubeName, simpleName);
        if (m != null) {
            return m;
        }

        // 2. Fallback: match by simple name across all cubes in this star
        for (RolapStar.Column col : factTable.getColumns()) {
            if (col instanceof RolapStar.Measure) {
                if (col.getName().equals(simpleName)) {
                    return (RolapStar.Measure) col;
                }
            }
        }

        LOGGER.debug(
            "FactResolvedTable.findStarMeasure: no match for"
            + " uniqueName={}, simpleName={}, cube={}",
            measureUniqueName, simpleName, cubeName);
        return null;
    }

    /**
     * Extracts the simple (unqualified) name from an MDX unique name.
     *
     * <p>Example: {@code [Measures].[Sales]} → {@code Sales}.
     */
    static String extractSimpleName(String uniqueName) {
        if (uniqueName == null) {
            return null;
        }
        int lastDot = uniqueName.lastIndexOf(".[");
        if (lastDot >= 0 && uniqueName.endsWith("]")) {
            return uniqueName.substring(lastDot + 2, uniqueName.length() - 1);
        }
        return uniqueName;
    }

    /**
     * Returns the physical column name from a star measure's expression.
     *
     * <p>Mirrors {@code NativeQuerySqlGenerator#getColumnName}.
     */
    private String getColumnName(RolapStar.Measure measure) {
        MondrianDef.Expression expr = measure.getExpression();
        if (expr instanceof MondrianDef.Column) {
            return ((MondrianDef.Column) expr).name;
        }
        return measure.getName();
    }
}
