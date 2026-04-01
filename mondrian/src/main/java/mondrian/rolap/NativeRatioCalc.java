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

import mondrian.calc.Calc;
import mondrian.calc.impl.GenericCalc;
import mondrian.olap.*;
import mondrian.spi.Dialect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import javax.sql.DataSource;

/**
 * Evaluates a ratio calculated measure via a single batch SQL query.
 *
 * <p>Instead of per-tuple MDX evaluation (which requires segment loading
 * for each tuple), generates one SQL that computes
 * {@code numerator / denominator * multiplier} for all axis members,
 * caches the result, and returns values from cache.
 *
 * <p>The denominator subquery omits "reset" hierarchies from GROUP BY,
 * effectively aggregating across all members of those hierarchies.
 */
public class NativeRatioCalc extends GenericCalc {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeRatioCalc.class);

    private final RolapCalculatedMember member;
    private final RolapEvaluatorRoot root;
    private final NativeRatioConfig.RatioMeasureDef def;
    private final Set<String> resetHierarchyNames;
    private final Calc fallbackCalc;

    // Lazy-resolved at first evaluate()
    private RolapStoredMeasure numeratorMeasure;
    private RolapStoredMeasure denominatorMeasure;
    private RolapCube baseCube;
    private boolean resolved;

    /** Cache: evaluator context key -> ratio value (Double or null). */
    private Map<String, Object> batchCache;

    private NativeRatioCalc(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root,
        NativeRatioConfig.RatioMeasureDef def,
        Calc fallbackCalc)
    {
        super(member.getExpression(), new Calc[0]);
        this.member = member;
        this.root = root;
        this.def = def;
        this.resetHierarchyNames =
            new HashSet<String>(def.getResetHierarchyNames());
        this.fallbackCalc = fallbackCalc;
    }

    /**
     * Lazy-resolves stored measures from the cube.
     * Must be called during evaluate() when RolapResult is available.
     */
    private boolean ensureResolved(Evaluator evaluator) {
        if (resolved) {
            return numeratorMeasure != null && denominatorMeasure != null;
        }
        resolved = true;
        final RolapCube cube = (RolapCube) evaluator.getCube();
        numeratorMeasure =
            findStoredMeasure(cube, def.getNumeratorMeasureName());
        denominatorMeasure =
            findStoredMeasure(cube, def.getDenominatorMeasureName());
        if (numeratorMeasure == null || denominatorMeasure == null) {
            LOGGER.warn(
                "NativeRatio: cannot resolve measures for '{}' (num={}, denom={})",
                member.getName(),
                numeratorMeasure == null ? "NOT FOUND" : "ok",
                denominatorMeasure == null ? "NOT FOUND" : "ok");
            return false;
        }
        baseCube = numeratorMeasure.getCube();
        LOGGER.info(
            "NativeRatio: resolved measures for '{}' (num={}, denom={}, baseCube={})",
            member.getName(),
            numeratorMeasure.getUniqueName(),
            denominatorMeasure.getUniqueName(),
            baseCube.getName());
        return true;
    }

    /**
     * Factory: resolves measures from the cube, validates, returns calc or null.
     */
    /**
     * Factory: creates a lazy NativeRatioCalc that defers measure resolution
     * and SQL generation to evaluate-time (when the RolapResult and axes
     * are available).  At compile time, only validates that the config exists.
     */
    static Calc tryCreate(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root,
        NativeRatioConfig.RatioMeasureDef def)
    {
        // Build fallback calc (standard MDX evaluation)
        final Calc fallback = root.getCompiled(
            member.getExpression(), true, null);

        System.out.println("NativeRatioCalc.tryCreate: creating lazy calc for " + member.getName());
        return new NativeRatioCalc(member, root, def, fallback);
    }

    @Override
    public Object evaluate(Evaluator evaluator) {
        System.out.println("NativeRatioCalc.evaluate for " + member.getName() + " resolved=" + resolved);
        // Lazy-resolve measures on first call
        if (!ensureResolved(evaluator)) {
            return fallbackCalc.evaluate(evaluator);
        }

        // Try batch cache first
        final String key = buildCacheKey(evaluator);
        if (batchCache != null) {
            if (batchCache.containsKey(key)) {
                return batchCache.get(key);
            }
            // Cache exists but key not found — member not in batch result
            return fallbackCalc.evaluate(evaluator);
        }

        // First call — try to build and execute batch query
        try {
            batchCache = executeBatchQuery(evaluator);
            if (batchCache.containsKey(key)) {
                return batchCache.get(key);
            }
        } catch (Exception e) {
            LOGGER.warn(
                "NativeRatio: batch query failed for '{}', "
                    + "falling back to MDX: {}",
                member.getName(), e.getMessage(), e);
            batchCache = Collections.emptyMap();
        }
        return fallbackCalc.evaluate(evaluator);
    }

    /**
     * Builds a cache key from the evaluator's non-measure, non-All members.
     * Must match the keys produced by the SQL result parsing.
     */
    private String buildCacheKey(Evaluator evaluator) {
        final StringBuilder key = new StringBuilder();
        for (Member m : evaluator.getMembers()) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }
            if (key.length() > 0) {
                key.append('|');
            }
            // Use the member key (database key) for matching with SQL results
            final Object memberKey = ((RolapMember) m).getKey();
            key.append(m.getHierarchy().getName())
               .append('=')
               .append(memberKey);
        }
        return key.toString();
    }

    /**
     * Generates and executes the batch SQL query, returns cache map.
     */
    private Map<String, Object> executeBatchQuery(Evaluator evaluator)
        throws java.sql.SQLException
    {
        final RolapStar star = baseCube.getStar();
        final RolapStar.Table factTable = star.getFactTable();
        final DataSource dataSource =
            evaluator.getSchemaReader().getDataSource();
        final Dialect dialect = root.currentDialect;

        // Collect axis dimension info (non-measure, non-All members)
        final List<DimBinding> axisDims =
            collectAxisDimBindings(evaluator, factTable, dialect);
        // Collect slicer predicates
        final List<String> slicerPredicates =
            collectSlicerPredicates(evaluator, factTable, dialect);

        final String factTableName = factTable.getTableName();
        final String numColumn = resolveColumnName(numeratorMeasure);
        final String denomColumn = resolveColumnName(denominatorMeasure);
        final double multiplier = def.getMultiplier();

        // Build batch SQL
        final String sql = buildBatchSql(
            dialect, factTableName,
            axisDims, slicerPredicates,
            numColumn, denomColumn, multiplier);

        LOGGER.info("NativeRatio: executing batch SQL for '{}': {}",
            member.getName(), sql);

        // Execute
        final Map<String, Object> results = new LinkedHashMap<String, Object>();
        final java.sql.Connection conn = dataSource.getConnection();
        try {
            final java.sql.Statement stmt = conn.createStatement();
            try {
                final java.sql.ResultSet rs = stmt.executeQuery(sql);
                try {
                    while (rs.next()) {
                        final StringBuilder key = new StringBuilder();
                        int colIdx = 1;
                        for (DimBinding dim : axisDims) {
                            if (dim.isReset) {
                                continue;
                            }
                            if (key.length() > 0) {
                                key.append('|');
                            }
                            key.append(dim.hierarchyName)
                               .append('=')
                               .append(rs.getString(colIdx++));
                        }
                        final double value = rs.getDouble(colIdx);
                        results.put(
                            key.toString(),
                            rs.wasNull() ? null : value);
                    }
                } finally {
                    rs.close();
                }
            } finally {
                stmt.close();
            }
        } finally {
            conn.close();
        }

        LOGGER.info("NativeRatio: batch query returned {} rows for '{}'",
            results.size(), member.getName());
        return results;
    }

    private String buildBatchSql(
        Dialect dialect,
        String factTableName,
        List<DimBinding> axisDims,
        List<String> slicerPredicates,
        String numColumn,
        String denomColumn,
        double multiplier)
    {
        final StringBuilder sql = new StringBuilder(1024);
        final String ft = quoteId(dialect, factTableName);
        final String where = buildWhereClause(slicerPredicates);

        // Numerator columns: only non-reset dims
        final StringBuilder numSelectCols = new StringBuilder();
        final StringBuilder numGroupBy = new StringBuilder();
        for (DimBinding dim : axisDims) {
            if (dim.isReset) {
                continue;
            }
            if (numSelectCols.length() > 0) {
                numSelectCols.append(", ");
                numGroupBy.append(", ");
            }
            numSelectCols.append(dim.qualifiedColumn);
            numGroupBy.append(dim.qualifiedColumn);
        }

        // Numerator subquery
        sql.append("SELECT ");
        sql.append(numSelectCols);
        sql.append(", ");

        // Denominator as a window/scalar subquery via CROSS JOIN
        // Numerator: SUM(num_col)
        sql.append("SUM(n.").append(quoteId(dialect, numColumn)).append(") AS nr_num");
        sql.append(", d.nr_denom");
        sql.append(" FROM ").append(ft).append(" AS n");

        // Join dimension tables if needed
        for (DimBinding dim : axisDims) {
            if (dim.joinClause != null) {
                sql.append(" ").append(dim.joinClause);
            }
        }

        // Denominator subquery (CROSS JOIN)
        sql.append(" CROSS JOIN (SELECT SUM(")
           .append(quoteId(dialect, denomColumn))
           .append(") AS nr_denom FROM ")
           .append(ft);
        if (!where.isEmpty()) {
            sql.append(" WHERE ").append(where);
        }
        sql.append(") AS d");

        // WHERE clause for numerator
        if (!where.isEmpty()) {
            sql.append(" WHERE ").append(where.replace(factTableName, "n"));
        }

        // GROUP BY
        sql.append(" GROUP BY ");
        sql.append(numGroupBy);
        sql.append(", d.nr_denom");

        return sql.toString();
    }

    private List<DimBinding> collectAxisDimBindings(
        Evaluator evaluator,
        RolapStar.Table factTable,
        Dialect dialect)
    {
        final List<DimBinding> dims = new ArrayList<DimBinding>();
        for (Member m : evaluator.getMembers()) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }
            final String hierName = m.getHierarchy().getName();
            final boolean isReset = resetHierarchyNames.contains(hierName);

            // Resolve the column from the level
            final RolapLevel level = (RolapLevel) m.getLevel();
            final MondrianDef.Expression keyExp = level.getKeyExp();
            if (!(keyExp instanceof MondrianDef.Column)) {
                LOGGER.warn(
                    "NativeRatio: non-column key expression for {}, skipping",
                    level.getUniqueName());
                continue;
            }
            final String columnName = ((MondrianDef.Column) keyExp).name;

            // Check if column is on fact table or needs a join
            String qualifiedColumn;
            String joinClause = null;
            if (factTable.lookupColumn(columnName) != null) {
                qualifiedColumn = "n." + quoteId(dialect, columnName);
            } else {
                // Column is on a dimension table — need a JOIN
                final RolapHierarchy hierarchy =
                    (RolapHierarchy) m.getHierarchy();
                final MondrianDef.RelationOrJoin relation =
                    hierarchy.getRelation();
                if (relation instanceof MondrianDef.Table) {
                    final MondrianDef.Table dimTable =
                        (MondrianDef.Table) relation;
                    final String dimAlias = dimTable.getAlias() != null
                        ? dimTable.getAlias()
                        : dimTable.name;
                    final String dimTableName = dimTable.name;

                    // Resolve foreign key
                    final Dimension dimension = hierarchy.getDimension();
                    String foreignKey = null;
                    if (dimension instanceof RolapCubeDimension) {
                        final RolapCubeDimension cubeDim =
                            (RolapCubeDimension) dimension;
                        if (cubeDim.xmlDimension != null) {
                            foreignKey = cubeDim.xmlDimension.foreignKey;
                        }
                    }

                    final MondrianDef.Hierarchy xmlHier =
                        hierarchy.getXmlHierarchy();
                    String primaryKey = xmlHier != null
                        ? xmlHier.primaryKey
                        : null;
                    if (primaryKey == null) {
                        primaryKey = columnName;
                    }

                    if (foreignKey != null) {
                        qualifiedColumn = quoteId(dialect, dimAlias)
                            + "." + quoteId(dialect, columnName);
                        joinClause = "INNER JOIN "
                            + quoteId(dialect, dimTableName)
                            + " AS " + quoteId(dialect, dimAlias)
                            + " ON n." + quoteId(dialect, foreignKey)
                            + " = " + quoteId(dialect, dimAlias)
                            + "." + quoteId(dialect, primaryKey);
                    } else {
                        LOGGER.warn(
                            "NativeRatio: no foreign key for dim {} in {}",
                            hierName, baseCube.getName());
                        qualifiedColumn = "n." + quoteId(dialect, columnName);
                    }
                } else {
                    qualifiedColumn = "n." + quoteId(dialect, columnName);
                }
            }

            dims.add(new DimBinding(
                hierName, columnName, qualifiedColumn, joinClause, isReset));
        }
        return dims;
    }

    private List<String> collectSlicerPredicates(
        Evaluator evaluator,
        RolapStar.Table factTable,
        Dialect dialect)
    {
        final List<String> predicates = new ArrayList<String>();
        for (Member m : evaluator.getMembers()) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }
            // Only slicer members (not axis members that are iterated)
            // In practice, we add predicates for all non-All members
            // The axis members will be in the GROUP BY
            final RolapLevel level = (RolapLevel) m.getLevel();
            final MondrianDef.Expression keyExp = level.getKeyExp();
            if (!(keyExp instanceof MondrianDef.Column)) {
                continue;
            }
            final String columnName = ((MondrianDef.Column) keyExp).name;

            // Only add predicate if this is a slicer member (single value)
            // not a GROUP BY member (axis member)
            // For now, detect slicer by checking if the member is non-All
            // and its hierarchy is NOT on the axes
            // We add ALL non-All members as predicates — axis GROUP BY
            // handles the axis members correctly
            if (factTable.lookupColumn(columnName) != null) {
                final StringBuilder pred = new StringBuilder();
                pred.append(quoteId(dialect, factTable.getTableName()));
                pred.append(".");
                pred.append(quoteId(dialect, columnName));
                pred.append(" = ");
                appendLiteral(pred, dialect, ((RolapMember) m).getKey());
                predicates.add(pred.toString());
            }
        }
        return predicates;
    }

    private String buildWhereClause(List<String> predicates) {
        if (predicates.isEmpty()) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < predicates.size(); i++) {
            if (i > 0) {
                sb.append(" AND ");
            }
            sb.append(predicates.get(i));
        }
        return sb.toString();
    }

    private static String resolveColumnName(RolapStoredMeasure measure) {
        final MondrianDef.Expression expr = measure.getMondrianDefExpression();
        if (expr instanceof MondrianDef.Column) {
            return ((MondrianDef.Column) expr).name;
        }
        return expr.getExpression(null);
    }

    private static String quoteId(Dialect dialect, String id) {
        final StringBuilder sb = new StringBuilder();
        dialect.quoteIdentifier(sb, id);
        return sb.toString();
    }

    private static void appendLiteral(
        StringBuilder sb, Dialect dialect, Object value)
    {
        if (value == null) {
            sb.append("NULL");
        } else if (value instanceof Number) {
            sb.append(value);
        } else {
            dialect.quoteStringLiteral(sb, String.valueOf(value));
        }
    }

    static RolapStoredMeasure findStoredMeasure(
        RolapCube cube, String measureName)
    {
        // Search in cube's own measures
        for (RolapMember m : cube.getMeasuresMembers()) {
            if (m instanceof RolapStoredMeasure
                && m.getName().equals(measureName))
            {
                return (RolapStoredMeasure) m;
            }
        }
        // For virtual cubes, search base cubes
        if (cube.isVirtual()) {
            for (RolapCube base : cube.getBaseCubes()) {
                for (RolapMember m : base.getMeasuresMembers()) {
                    if (m instanceof RolapStoredMeasure
                        && m.getName().equals(measureName))
                    {
                        return (RolapStoredMeasure) m;
                    }
                }
            }
        }
        return null;
    }

    static class DimBinding {
        final String hierarchyName;
        final String columnName;
        final String qualifiedColumn;
        final String joinClause;
        final boolean isReset;

        DimBinding(
            String hierarchyName,
            String columnName,
            String qualifiedColumn,
            String joinClause,
            boolean isReset)
        {
            this.hierarchyName = hierarchyName;
            this.columnName = columnName;
            this.qualifiedColumn = qualifiedColumn;
            this.joinClause = joinClause;
            this.isReset = isReset;
        }
    }
}
