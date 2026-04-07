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

import mondrian.olap.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * Phase D.1-D.2: Generates and executes SQL for each CoordinateClassPlan,
 * fills NativeQueryResultContext with results.
 *
 * <p>For each plan, this class:
 * <ol>
 *   <li>Generates a SQL SELECT with GROUP BY + aggregate expressions</li>
 *   <li>Executes it via JDBC</li>
 *   <li>Parses the result set into NativeQueryResultContext</li>
 * </ol>
 *
 * <p>Reuses {@link NativeSqlCalc} infrastructure for column resolution
 * and JOIN building.
 */
public class NativeQuerySqlGenerator {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeQuerySqlGenerator.class);

    private final RolapEvaluator evaluator;
    private final RolapCube baseCube;

    /**
     * Set by {@link #generateStoredSql} — the subset of plan requests
     * that actually produced aggregate expressions in the SQL.
     * Used by {@link #parseAndFill} to correctly map result columns
     * back to measure IDs (some requests may be skipped when their
     * measure belongs to a different base cube).
     */
    private List<PhysicalValueRequest> lastIncludedRequests;

    public NativeQuerySqlGenerator(
        RolapEvaluator evaluator,
        RolapCube baseCube)
    {
        this.evaluator = evaluator;
        this.baseCube = baseCube;
    }

    /**
     * Executes all coordinate class plans and fills the result context.
     * Returns false if any execution fails.
     */
    public boolean executeAll(
        List<CoordinateClassPlan> plans,
        NativeQueryResultContext context)
    {
        for (CoordinateClassPlan plan : plans) {
            if (!executePlan(plan, context)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Executes a single coordinate class plan.
     *
     * <p>Plans that contain NATIVE_TEMPLATE requests are skipped:
     * those templates contain unresolved {@code ${placeholders}} that
     * are designed for {@link NativeSqlCalc}, not for direct JDBC
     * execution.  Phase 1 of NativeQueryEngine only handles
     * STORED_COLUMN and STATE_AGGREGATE plans.
     */
    boolean executePlan(
        CoordinateClassPlan plan,
        NativeQueryResultContext context)
    {
        // Skip plans that contain NATIVE_TEMPLATE requests — they have
        // unresolved ${placeholders} and must be handled by NativeSqlCalc.
        if (containsNativeTemplate(plan)) {
            LOGGER.warn(
                "NativeQuerySqlGenerator: skipping NATIVE_TEMPLATE plan"
                + " for class={} (templates need NativeSqlCalc resolution)",
                plan.getClassId());
            return false;
        }

        try {
            String sql = generateSql(plan);
            if (sql == null) {
                LOGGER.warn(
                    "NativeQuerySqlGenerator: SQL generation failed"
                    + " for class={}",
                    plan.getClassId());
                return false;
            }

            LOGGER.info(
                "NativeQuerySqlGenerator: executing SQL for class={}: {}",
                plan.getClassId(), sql);

            executeAndFill(sql, plan, context);
            return true;
        } catch (Exception e) {
            LOGGER.warn(
                "NativeQuerySqlGenerator: execution failed for class={}",
                plan.getClassId(), e);
            return false;
        }
    }

    /**
     * Returns {@code true} if any request in the plan is a
     * NATIVE_TEMPLATE.
     */
    private boolean containsNativeTemplate(CoordinateClassPlan plan) {
        for (PhysicalValueRequest req : plan.getRequests()) {
            if (req.getProviderKind()
                == PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Generates SQL for a CoordinateClassPlan.
     *
     * <p>The generated SQL follows the pattern:
     * <pre>
     *   SELECT &lt;grouping_keys&gt;, &lt;agg_expressions&gt;
     *   FROM &lt;fact_table&gt; &lt;joins&gt;
     *   WHERE &lt;filters&gt;
     *   GROUP BY &lt;grouping_keys&gt;
     * </pre>
     *
     * <p>For NativeTemplate requests, the template SQL is used directly
     * (no generation needed).
     */
    String generateSql(CoordinateClassPlan plan) {
        List<PhysicalValueRequest> requests = plan.getRequests();
        if (requests.isEmpty()) {
            return null;
        }

        PhysicalValueRequest first = requests.get(0);

        // For native templates, return the template directly
        // (Phase 1 simplification: one template per class)
        if (first.getProviderKind()
            == PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE)
        {
            return first.getNativeTemplate();
        }

        // For stored column / state aggregate requests, build SQL
        return generateStoredSql(plan);
    }

    /**
     * Generates SQL for stored column / state aggregate requests.
     */
    private String generateStoredSql(CoordinateClassPlan plan) {
        final RolapStar star = baseCube.getStar();
        final RolapStar.Table factTable = star.getFactTable();
        final String factTableName = factTable.getTableName();
        final String factAlias = "f";

        final List<String> selectExprs = new ArrayList<String>();
        final List<String> selectAliases = new ArrayList<String>();
        final List<String> groupByExprs = new ArrayList<String>();
        final List<String> joinClauses = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();
        final List<String> wherePredicates = new ArrayList<String>();

        PhysicalValueRequest first = plan.getRequests().get(0);

        // 1. Build GROUP BY keys from projected hierarchies
        int keyIndex = 0;
        for (Hierarchy hierarchy : first.getProjectedHierarchies()) {
            // Skip reset hierarchies (they're removed from grouping)
            if (first.getResetHierarchies().contains(hierarchy)) {
                continue;
            }
            String qualifiedColumn = resolveHierarchyColumn(
                hierarchy, star, factTable, factAlias,
                joinClauses, seenJoins);
            if (qualifiedColumn != null) {
                String alias = "k" + keyIndex++;
                selectExprs.add(qualifiedColumn);
                selectAliases.add(alias);
                groupByExprs.add(qualifiedColumn);
            }
        }

        // 2. Build aggregate expressions for each request.
        //    Track which requests produced SQL (some may be skipped
        //    when the measure belongs to a different base cube).
        final List<PhysicalValueRequest> includedRequests =
            new ArrayList<PhysicalValueRequest>();
        int valueIndex = 0;
        for (PhysicalValueRequest req : plan.getRequests()) {
            String aggExpr = buildAggregateExpression(
                req, factTable, factAlias);
            if (aggExpr != null) {
                String alias = "v" + valueIndex++;
                selectExprs.add(aggExpr);
                selectAliases.add(alias);
                includedRequests.add(req);
            }
        }

        if (includedRequests.isEmpty()) {
            LOGGER.debug(
                "NativeQuerySqlGenerator: no includable measures"
                + " for class={}, skipping",
                plan.getClassId());
            return null;
        }
        lastIncludedRequests = includedRequests;

        // 3. Build WHERE from evaluator context (slicer + subselect)
        buildWhereFromContext(
            wherePredicates, first.getResetHierarchies(),
            first.getProjectedHierarchies(),
            star, factTable, factAlias, joinClauses, seenJoins);

        // 4. Assemble SQL
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        for (int i = 0; i < selectExprs.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(selectExprs.get(i));
            sql.append(" AS ").append(selectAliases.get(i));
        }
        sql.append(" FROM ").append(factTableName).append(" ").append(factAlias);
        for (String join : joinClauses) {
            sql.append(" ").append(join);
        }
        if (!wherePredicates.isEmpty()) {
            sql.append(" WHERE ");
            for (int i = 0; i < wherePredicates.size(); i++) {
                if (i > 0) {
                    sql.append(" AND ");
                }
                sql.append(wherePredicates.get(i));
            }
        }
        if (!groupByExprs.isEmpty()) {
            sql.append(" GROUP BY ");
            for (int i = 0; i < groupByExprs.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(groupByExprs.get(i));
            }
        }

        String result = sql.toString();
        LOGGER.debug(
            "NativeQuerySqlGenerator: generateStoredSql for class={}: {}",
            plan.getClassId(), result);
        return result;
    }

    /**
     * Resolves a hierarchy to its SQL column expression for GROUP BY.
     * Uses the leaf (lowest non-All) level's key expression.
     */
    private String resolveHierarchyColumn(
        Hierarchy hierarchy,
        RolapStar star,
        RolapStar.Table factTable,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        if (!(hierarchy instanceof RolapHierarchy)) {
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
            return null;
        }

        MondrianDef.Expression keyExp = leafLevel.getKeyExp();
        if (keyExp == null) {
            return null;
        }

        // Use NativeSqlCalc's level column resolution via star lookup
        if (keyExp instanceof MondrianDef.Column) {
            MondrianDef.Column keyColumn = (MondrianDef.Column) keyExp;
            NativeSqlCalc.ResolvedColumnSql resolved =
                NativeSqlCalc.resolveLevelColumnSql(
                    keyColumn, star, factAlias, joinClauses, seenJoins);
            if (resolved != null) {
                return resolved.qualifiedColumn;
            }

            // Fallback: try resolving via dimension table join
            return resolveDimensionColumn(
                keyColumn, rolapHier, factAlias,
                joinClauses, seenJoins);
        }

        return null;
    }

    /**
     * Resolves a dimension column by building the JOIN to the dimension
     * table. Used when the column is not directly in the star's fact table.
     */
    private String resolveDimensionColumn(
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

        // Find the foreign key from cube usages
        String foreignKey = null;
        final HierarchyUsage[] usages = baseCube.getUsages(hierarchy);
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
                "NativeQuerySqlGenerator: no foreign key for dim {} in {}"
                + " — skipping from GROUP BY",
                hierarchy.getName(), baseCube.getName());
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
     * Builds the aggregate SQL expression for a request.
     * e.g., {@code SUM(f.sales_amount)}, {@code count(distinct f.store_key)}
     *
     * <p>Phase 1 always queries the fact table, so even STATE_AGGREGATE
     * requests use the standard aggregator expression (e.g.
     * {@code count(distinct col)}) rather than a merge function.
     * Merge functions like {@code uniqCombinedMerge()} only work on
     * agg tables whose columns are AggregateFunction state types.
     */
    private String buildAggregateExpression(
        PhysicalValueRequest req,
        RolapStar.Table factTable,
        String factAlias)
    {
        if (req.getProviderKind()
            == PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN
            || req.getProviderKind()
            == PhysicalValueRequest.ExpressionProviderKind.STATE_AGGREGATE)
        {
            String measureId = req.getPhysicalMeasureId();

            // Find the star measure
            RolapStar.Measure starMeasure = findStarMeasure(
                factTable, measureId);
            if (starMeasure == null) {
                LOGGER.debug(
                    "NativeQuerySqlGenerator: cannot find star measure"
                    + " for: {} (may belong to a different base cube)",
                    measureId);
                return null;
            }

            String colExpr = factAlias + "." + getColumnName(starMeasure);

            // Phase 1: always queries fact table, so use standard aggregator.
            // (merge function only works on agg tables with state columns)
            RolapAggregator agg = starMeasure.getAggregator();
            return agg.getExpression(colExpr);
        }

        // For NATIVE_TEMPLATE, should not reach here (handled in generateSql)
        return null;
    }

    /**
     * Finds the RolapStar.Measure for a given measure unique name.
     */
    private RolapStar.Measure findStarMeasure(
        RolapStar.Table factTable,
        String measureUniqueName)
    {
        String cubeName = baseCube.getName();
        String simpleName = extractSimpleName(measureUniqueName);

        // Try lookupMeasureByName with cube name
        RolapStar.Measure m = factTable.lookupMeasureByName(
            cubeName, simpleName);
        if (m != null) {
            return m;
        }

        // Try matching by simple name only (across all cubes in this star)
        for (RolapStar.Column col : factTable.getColumns()) {
            if (col instanceof RolapStar.Measure) {
                if (col.getName().equals(simpleName)) {
                    return (RolapStar.Measure) col;
                }
            }
        }
        LOGGER.debug(
            "NativeQuerySqlGenerator.findStarMeasure: no match for"
            + " uniqueName={}, simpleName={}, cube={}",
            measureUniqueName, simpleName, cubeName);
        return null;
    }

    /**
     * Extracts simple name from unique name like
     * {@code [Measures].[Name]} to {@code Name}.
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
     * Returns the column name from a star measure's expression.
     */
    private String getColumnName(RolapStar.Measure measure) {
        MondrianDef.Expression expr = measure.getExpression();
        if (expr instanceof MondrianDef.Column) {
            return ((MondrianDef.Column) expr).name;
        }
        return measure.getName();
    }

    /**
     * Builds WHERE predicates from the evaluator's context (slicer members).
     * Skips reset hierarchies (forced to All) and projected hierarchies
     * (they become GROUP BY keys, not WHERE predicates).
     */
    private void buildWhereFromContext(
        List<String> wherePredicates,
        Set<Hierarchy> resetHierarchies,
        Set<Hierarchy> projectedHierarchies,
        RolapStar star,
        RolapStar.Table factTable,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        for (Member m : evaluator.getMembers()) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }
            Hierarchy h = m.getHierarchy();
            // Skip reset hierarchies (they're forced to All)
            if (resetHierarchies != null && resetHierarchies.contains(h)) {
                continue;
            }
            // Skip projected hierarchies (they become GROUP BY, not WHERE)
            if (projectedHierarchies != null
                && projectedHierarchies.contains(h))
            {
                continue;
            }

            // Build WHERE predicate for this slicer member
            String predicate = buildMemberPredicate(
                m, star, factAlias, joinClauses, seenJoins);
            if (predicate != null) {
                wherePredicates.add(predicate);
            }
        }

        // Subcube predicates (from MDX subselect).
        // These are NOT in evaluator.getMembers(); they come from the
        // StarPredicate tree built by Query.getSubcubePredicates().
        StarPredicate subcubePred =
            evaluator.getQuery().getSubcubePredicates(baseCube);
        if (subcubePred != null) {
            String subcubeSql = renderStarPredicate(
                subcubePred, star, factAlias, joinClauses, seenJoins);
            if (subcubeSql != null && !subcubeSql.isEmpty()) {
                wherePredicates.add(subcubeSql);
            }
        }
    }

    /**
     * Builds a SQL predicate for a single member, e.g.
     * {@code f.period_month = '2025-01'}.
     */
    private String buildMemberPredicate(
        Member member,
        RolapStar star,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        if (!(member instanceof RolapMember)) {
            return null;
        }
        RolapMember rm = (RolapMember) member;
        RolapLevel level = (RolapLevel) rm.getLevel();
        MondrianDef.Expression keyExp = level.getKeyExp();
        if (keyExp == null) {
            return null;
        }

        String qualifiedColumn = null;
        if (keyExp instanceof MondrianDef.Column) {
            MondrianDef.Column keyColumn = (MondrianDef.Column) keyExp;
            NativeSqlCalc.ResolvedColumnSql resolved =
                NativeSqlCalc.resolveLevelColumnSql(
                    keyColumn, star, factAlias, joinClauses, seenJoins);
            if (resolved != null) {
                qualifiedColumn = resolved.qualifiedColumn;
            } else {
                // Fallback: resolve via dimension table join
                qualifiedColumn = resolveDimensionColumn(
                    keyColumn,
                    (RolapHierarchy) member.getHierarchy(),
                    factAlias, joinClauses, seenJoins);
            }
        }

        if (qualifiedColumn == null) {
            return null;
        }

        Object key = rm.getKey();
        return qualifiedColumn + " = " + NativeSqlCalc.formatLiteral(key);
    }

    /**
     * Converts a {@link StarPredicate} tree (from subcube / MDX subselect)
     * into a SQL WHERE fragment.
     *
     * <p>Handles {@link mondrian.rolap.agg.AndPredicate},
     * {@link mondrian.rolap.agg.OrPredicate},
     * {@link mondrian.rolap.agg.MemberColumnPredicate}, and
     * {@link mondrian.rolap.agg.ValueColumnPredicate}.
     *
     * <p>Reuses {@link NativeSqlCalc#resolvePredicateColumnSql} for
     * column-to-SQL resolution (including dimension table JOINs).
     */
    private String renderStarPredicate(
        StarPredicate pred,
        RolapStar star,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        if (pred instanceof mondrian.rolap.agg.AndPredicate) {
            List<StarPredicate> children =
                ((mondrian.rolap.agg.AndPredicate) pred).getChildren();
            List<String> parts = new ArrayList<String>();
            for (StarPredicate child : children) {
                String s = renderStarPredicate(
                    child, star, factAlias, joinClauses, seenJoins);
                if (s != null && !s.isEmpty()) {
                    parts.add(s);
                }
            }
            if (parts.isEmpty()) {
                return null;
            }
            if (parts.size() == 1) {
                return parts.get(0);
            }
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < parts.size(); i++) {
                if (i > 0) {
                    sb.append(" AND ");
                }
                sb.append(parts.get(i));
            }
            return sb.append(")").toString();
        }

        if (pred instanceof mondrian.rolap.agg.OrPredicate) {
            List<StarPredicate> children =
                ((mondrian.rolap.agg.OrPredicate) pred).getChildren();
            List<String> parts = new ArrayList<String>();
            for (StarPredicate child : children) {
                String s = renderStarPredicate(
                    child, star, factAlias, joinClauses, seenJoins);
                if (s != null && !s.isEmpty()) {
                    parts.add(s);
                }
            }
            if (parts.isEmpty()) {
                return null;
            }
            if (parts.size() == 1) {
                return parts.get(0);
            }
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < parts.size(); i++) {
                if (i > 0) {
                    sb.append(" OR ");
                }
                sb.append(parts.get(i));
            }
            return sb.append(")").toString();
        }

        if (pred instanceof mondrian.rolap.agg.ValueColumnPredicate) {
            // MemberColumnPredicate extends ValueColumnPredicate, so
            // this branch handles both types.
            mondrian.rolap.agg.ValueColumnPredicate vcp =
                (mondrian.rolap.agg.ValueColumnPredicate) pred;
            NativeSqlCalc.ResolvedColumnSql resolved =
                NativeSqlCalc.resolvePredicateColumnSql(
                    vcp.getConstrainedColumn(),
                    star,
                    factAlias,
                    joinClauses,
                    seenJoins);
            if (resolved == null) {
                return null;
            }

            Object value = vcp.getValue();
            if (value == RolapUtil.sqlNullValue) {
                return resolved.qualifiedColumn + " IS NULL";
            }
            return resolved.qualifiedColumn + " = "
                + NativeSqlCalc.formatLiteral(value);
        }

        // For any other predicate type, log and skip.
        LOGGER.debug(
            "renderStarPredicate: unsupported type {}",
            pred.getClass().getSimpleName());
        return null;
    }

    /**
     * Executes SQL and fills the context with results.
     */
    private void executeAndFill(
        String sql,
        CoordinateClassPlan plan,
        NativeQueryResultContext context)
        throws SQLException
    {
        final DataSource dataSource =
            evaluator.getSchemaReader().getDataSource();
        final java.sql.Connection conn = dataSource.getConnection();
        try {
            final Statement stmt = conn.createStatement();
            try {
                final ResultSet rs = stmt.executeQuery(sql);
                try {
                    parseAndFill(rs, plan, context);
                } finally {
                    rs.close();
                }
            } finally {
                stmt.close();
            }
        } finally {
            conn.close();
        }
    }

    /**
     * Parses result set rows into the context.
     *
     * <p>Result set layout:
     * {@code k0, k1, ..., kN, v0, v1, ..., vM}
     * where k0..kN are grouping key columns and v0..vM are measure values.
     */
    private void parseAndFill(
        ResultSet rs,
        CoordinateClassPlan plan,
        NativeQueryResultContext context)
        throws SQLException
    {
        // Use the included requests (may be a subset of plan.getRequests()
        // if some measures were skipped during SQL generation).
        List<PhysicalValueRequest> requests =
            lastIncludedRequests != null
                ? lastIncludedRequests
                : plan.getRequests();
        int totalCols = rs.getMetaData().getColumnCount();
        int keyColCount = totalCols - requests.size();
        if (keyColCount < 0) {
            keyColCount = 0;
        }

        while (rs.next()) {
            // Build projected key from key columns
            String projectedKey = buildProjectedKey(rs, keyColCount);

            // Read each measure value using getObject() to preserve
            // the natural JDBC type (Integer, Long, BigDecimal, etc.)
            for (int i = 0; i < requests.size(); i++) {
                int colIndex = keyColCount + i + 1;
                Object raw = rs.getObject(colIndex);
                Object cellValue = (raw == null) ? null : raw;

                context.put(
                    plan.getClassId(),
                    projectedKey,
                    requests.get(i).getPhysicalMeasureId(),
                    cellValue);
            }
        }
    }

    /**
     * Builds a projected key string from result set key columns.
     * Parts are separated by {@code '\0'} (null character) to avoid
     * collisions with data values that may contain pipe or other
     * printable characters.
     */
    static String buildProjectedKey(ResultSet rs, int keyColCount)
        throws SQLException
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= keyColCount; i++) {
            if (i > 1) {
                sb.append('\0');
            }
            sb.append(String.valueOf(rs.getObject(i)));
        }
        return sb.toString();
    }

    /**
     * Encodes axis tuple values into a projected key string.
     * Used by PostProcessEvaluator to look up values.
     * Parts are separated by {@code '\0'} (null character).
     */
    public static String encodeProjectedKey(List<?> parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append('\0');
            }
            sb.append(String.valueOf(parts.get(i)));
        }
        return sb.toString();
    }
}
