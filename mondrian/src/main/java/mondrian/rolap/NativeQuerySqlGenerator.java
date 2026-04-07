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
     */
    boolean executePlan(
        CoordinateClassPlan plan,
        NativeQueryResultContext context)
    {
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
     * Executes a plan with a restricted projection (subset of GROUP BY
     * hierarchies), storing results under a custom granularity-specific
     * classId.
     *
     * <p>This is used by multi-granularity execution: DrilldownMember
     * axes have positions at different granularity levels (some
     * hierarchies are All, others are leaf-level). Each granularity
     * needs its own SQL with the matching GROUP BY columns.
     *
     * @param plan                 the original coordinate class plan
     * @param effectiveProjection  the reduced set of hierarchies to
     *                             GROUP BY (non-All hierarchies at
     *                             this granularity level)
     * @param granClassId          the classId to store results under,
     *                             typically {@code baseClassId + "#" + suffix}
     * @param context              the result context to fill
     * @return true if successful, false on failure
     */
    boolean executePlanWithProjection(
        CoordinateClassPlan plan,
        Set<Hierarchy> effectiveProjection,
        String granClassId,
        NativeQueryResultContext context)
    {
        try {
            PhysicalValueRequest first = plan.getRequests().get(0);

            // For native templates, use the standard path (template SQL
            // handles its own GROUP BY via placeholders)
            if (first.getProviderKind()
                == PhysicalValueRequest.ExpressionProviderKind
                       .NATIVE_TEMPLATE)
            {
                String sql = generateNativeTemplateSql(
                    plan, effectiveProjection);
                if (sql == null) {
                    LOGGER.warn(
                        "NativeQuerySqlGenerator: template SQL generation"
                        + " failed for granId={}",
                        granClassId);
                    return false;
                }
                LOGGER.info(
                    "NativeQuerySqlGenerator: executing template SQL"
                    + " for granId={}: {}",
                    granClassId, sql);
                executeAndFillWithClassId(
                    sql, plan, granClassId, context);
                return true;
            }

            // For stored/state requests, generate SQL with reduced
            // GROUP BY
            String sql = generateStoredSqlWithProjection(
                plan, effectiveProjection);
            if (sql == null) {
                LOGGER.warn(
                    "NativeQuerySqlGenerator: SQL generation failed"
                    + " for granId={}",
                    granClassId);
                return false;
            }

            LOGGER.info(
                "NativeQuerySqlGenerator: executing SQL for"
                + " granId={}: {}",
                granClassId, sql);

            executeAndFillWithClassId(
                sql, plan, granClassId, context);
            return true;
        } catch (Exception e) {
            LOGGER.warn(
                "NativeQuerySqlGenerator: execution failed for"
                + " granId={}",
                granClassId, e);
            return false;
        }
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

        // For native templates, resolve placeholders and return
        if (first.getProviderKind()
            == PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE)
        {
            return generateNativeTemplateSql(plan, null);
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
     * Generates SQL for stored column / state aggregate requests with
     * a restricted set of GROUP BY hierarchies.
     *
     * <p>This is the multi-granularity variant of
     * {@link #generateStoredSql(CoordinateClassPlan)}. Instead of
     * using the plan's full projected hierarchies for GROUP BY, it
     * uses only the {@code effectiveProjection} — the subset of
     * hierarchies where the axis position has non-All members.
     */
    private String generateStoredSqlWithProjection(
        CoordinateClassPlan plan,
        Set<Hierarchy> effectiveProjection)
    {
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

        // 1. Build GROUP BY keys from effective projection only
        int keyIndex = 0;
        for (Hierarchy hierarchy : effectiveProjection) {
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
                + " for reduced-projection plan, skipping");
            return null;
        }
        lastIncludedRequests = includedRequests;

        // 3. Build WHERE from evaluator context.
        //    Use the full projected hierarchies for WHERE exclusion:
        //    hierarchies in the full plan projection that are NOT in
        //    the effective projection should become WHERE predicates
        //    (if the evaluator has a specific member for them) or be
        //    left out (aggregated over).
        //    Actually, slicer members are not projected hierarchies,
        //    so use the effective projection for the WHERE skip logic.
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
        sql.append(" FROM ").append(factTableName)
           .append(" ").append(factAlias);
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
            "NativeQuerySqlGenerator: generateStoredSqlWithProjection:"
            + " {}",
            result);
        return result;
    }

    /**
     * Generates SQL for a NATIVE_TEMPLATE plan by resolving placeholders
     * in the template SQL using
     * {@link NativeSqlCalc#substitutePlaceholders(String, Map, List)}.
     *
     * <p>Resolves built-in placeholders ({@code factTable}, {@code factAlias},
     * {@code joinClauses}, {@code whereClause}, axis bindings) plus
     * user-defined variables from the measure's {@code nativeSql.variables}
     * annotation. The {@code whereClauseExcept} dynamic placeholder is
     * handled by {@code substitutePlaceholders} using the predicate list.
     */
    private String generateNativeTemplateSql(
        CoordinateClassPlan plan,
        Set<Hierarchy> effectiveProjection)
    {
        PhysicalValueRequest first = plan.getRequests().get(0);
        String template = first.getNativeTemplate();
        if (template == null || template.isEmpty()) {
            return null;
        }

        if (baseCube == null) {
            LOGGER.warn(
                "NativeQuerySqlGenerator: baseCube is null for"
                + " NATIVE_TEMPLATE plan");
            return null;
        }
        final RolapStar star = baseCube.getStar();
        if (star == null) {
            LOGGER.warn(
                "NativeQuerySqlGenerator: no star on cube {} for"
                + " NATIVE_TEMPLATE plan",
                baseCube.getName());
            return null;
        }
        final RolapStar.Table factTable = star.getFactTable();
        final String factAlias = "f";

        // Placeholder map
        final Map<String, String> placeholders =
            new LinkedHashMap<String, String>();
        placeholders.put("factTable", factTable.getTableName());
        placeholders.put("factAlias", factAlias);

        // Add user-defined variables from nativeSql.variables annotation
        placeholders.putAll(first.getNativeVariables());

        // Build axis bindings from projected hierarchies
        final List<String> joinClauses = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        // Use effectiveProjection (reduced for multi-granularity)
        // instead of the full projectedHierarchies
        Set<Hierarchy> projection = (effectiveProjection != null)
            ? effectiveProjection
            : first.getProjectedHierarchies();
        List<NativeSqlCalc.AxisBinding> axisBindings =
            buildAxisBindings(
                projection,
                first.getResetHierarchies(),
                star, factTable, factAlias,
                joinClauses, seenJoins);

        // Resolve axis expressions: axisExpr1, axisExpr2, ...
        int axisCount = axisBindings.size();
        for (int i = 0; i < axisCount; i++) {
            placeholders.put(
                "axisExpr" + (i + 1),
                axisBindings.get(i).qualifiedColumn);
        }
        placeholders.put("axisCount", String.valueOf(axisCount));

        // Build axis list placeholders
        placeholders.put(
            "axisPresenceSelectList",
            NativeSqlCalc.renderAxisPresenceSelectList(axisBindings));
        placeholders.put(
            "axisResultSelectList",
            NativeSqlCalc.renderAxisResultSelectList(axisBindings, "pr"));
        placeholders.put(
            "axisGroupByList",
            NativeSqlCalc.renderAxisGroupByList(axisBindings, "pr"));

        // Build WHERE predicates (PredicateInfo objects for
        // whereClauseExcept support)
        List<NativeSqlCalc.PredicateInfo> predicates =
            buildPredicateInfoList(
                first.getResetHierarchies(),
                first.getProjectedHierarchies(),
                star, factTable, factAlias,
                joinClauses, seenJoins);

        // Build full WHERE clause string
        placeholders.put(
            "whereClause",
            NativeSqlCalc.buildWhereFromPredicates(predicates, null));

        // Build join clauses string
        StringBuilder joinBuf = new StringBuilder();
        for (int i = 0; i < joinClauses.size(); i++) {
            if (i > 0) {
                joinBuf.append("\n");
            }
            joinBuf.append(joinClauses.get(i));
        }
        placeholders.put("joinClauses", joinBuf.toString());

        // Resolve template (substitutePlaceholders handles
        // ${whereClauseExcept:...} via the predicates list)
        String sql = NativeSqlCalc.substitutePlaceholders(
            template, placeholders, predicates);

        LOGGER.info(
            "NativeQuerySqlGenerator: generateNativeTemplateSql"
            + " for class={}: {}",
            plan.getClassId(), sql);

        // NATIVE_TEMPLATE plans have exactly one request (one measure),
        // so the result set maps 1:1 with that request.
        lastIncludedRequests = plan.getRequests();

        return sql;
    }

    /**
     * Builds {@link NativeSqlCalc.AxisBinding} objects from projected
     * hierarchies. Each hierarchy that resolves to a column in the star
     * gets a binding with key alias {@code k0, k1, ...}.
     */
    private List<NativeSqlCalc.AxisBinding> buildAxisBindings(
        Set<Hierarchy> projectedHierarchies,
        Set<Hierarchy> resetHierarchies,
        RolapStar star,
        RolapStar.Table factTable,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        List<NativeSqlCalc.AxisBinding> bindings =
            new ArrayList<NativeSqlCalc.AxisBinding>();
        int index = 0;
        for (Hierarchy hierarchy : projectedHierarchies) {
            if (resetHierarchies != null
                && resetHierarchies.contains(hierarchy))
            {
                continue;
            }
            String qualifiedColumn = resolveHierarchyColumn(
                hierarchy, star, factTable, factAlias,
                joinClauses, seenJoins);
            if (qualifiedColumn != null) {
                bindings.add(new NativeSqlCalc.AxisBinding(
                    hierarchy,
                    hierarchy.getName(),
                    qualifiedColumn,
                    "k" + index++));
            }
        }
        return bindings;
    }

    /**
     * Builds a list of {@link NativeSqlCalc.PredicateInfo} objects from
     * the evaluator's context (slicer members + subcube predicates),
     * preserving dimension/hierarchy metadata needed for
     * {@code ${whereClauseExcept:...}} resolution.
     *
     * <p>Skips reset hierarchies and projected hierarchies (projected
     * hierarchies are GROUP BY keys, not WHERE predicates).
     */
    private List<NativeSqlCalc.PredicateInfo> buildPredicateInfoList(
        Set<Hierarchy> resetHierarchies,
        Set<Hierarchy> projectedHierarchies,
        RolapStar star,
        RolapStar.Table factTable,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        List<NativeSqlCalc.PredicateInfo> predicates =
            new ArrayList<NativeSqlCalc.PredicateInfo>();

        // 1. Slicer members from evaluator context
        for (Member m : evaluator.getMembers()) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }
            Hierarchy h = m.getHierarchy();
            if (resetHierarchies != null && resetHierarchies.contains(h)) {
                continue;
            }
            if (projectedHierarchies != null
                && projectedHierarchies.contains(h))
            {
                continue;
            }

            // Resolve column and build predicate
            if (!(m instanceof RolapMember)) {
                continue;
            }
            RolapMember rm = (RolapMember) m;
            RolapLevel level = (RolapLevel) rm.getLevel();
            MondrianDef.Expression keyExp = level.getKeyExp();
            if (!(keyExp instanceof MondrianDef.Column)) {
                continue;
            }

            String qualifiedColumn = null;
            MondrianDef.Column keyColumn = (MondrianDef.Column) keyExp;
            NativeSqlCalc.ResolvedColumnSql resolved =
                NativeSqlCalc.resolveLevelColumnSql(
                    keyColumn, star, factAlias, joinClauses, seenJoins);
            if (resolved != null) {
                qualifiedColumn = resolved.qualifiedColumn;
            } else {
                qualifiedColumn = resolveDimensionColumn(
                    keyColumn,
                    (RolapHierarchy) m.getHierarchy(),
                    factAlias, joinClauses, seenJoins);
            }

            if (qualifiedColumn == null) {
                continue;
            }

            Object key = rm.getKey();
            String sql = qualifiedColumn + " = "
                + NativeSqlCalc.formatLiteral(key);
            String dimName = h.getDimension().getName();
            String hierName = h.getName();
            predicates.add(
                new NativeSqlCalc.AtomicPredicateInfo(
                    dimName, hierName, sql));
        }

        // 2. Subcube predicates (from MDX subselect)
        StarPredicate subcubePred =
            evaluator.getQuery().getSubcubePredicates(baseCube);
        if (subcubePred != null) {
            NativeSqlCalc.PredicateInfo subcubeInfo =
                buildStarPredicateInfo(
                    subcubePred, star, factAlias,
                    joinClauses, seenJoins);
            if (subcubeInfo != null) {
                predicates.add(subcubeInfo);
            }
        }

        return predicates;
    }

    /**
     * Converts a {@link StarPredicate} tree into a
     * {@link NativeSqlCalc.PredicateInfo} tree with dimension/hierarchy
     * metadata, for use with {@code ${whereClauseExcept:...}}.
     */
    private NativeSqlCalc.PredicateInfo buildStarPredicateInfo(
        StarPredicate pred,
        RolapStar star,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        if (pred instanceof mondrian.rolap.agg.MemberColumnPredicate) {
            mondrian.rolap.agg.MemberColumnPredicate mcp =
                (mondrian.rolap.agg.MemberColumnPredicate) pred;
            return buildAtomicStarPredicateInfo(
                mcp, mcp.getMember(), star, factAlias,
                joinClauses, seenJoins);
        }

        if (pred instanceof mondrian.rolap.agg.ValueColumnPredicate) {
            mondrian.rolap.agg.ValueColumnPredicate vcp =
                (mondrian.rolap.agg.ValueColumnPredicate) pred;
            return buildAtomicStarPredicateInfo(
                vcp, null, star, factAlias,
                joinClauses, seenJoins);
        }

        if (pred instanceof mondrian.rolap.agg.AndPredicate) {
            List<NativeSqlCalc.PredicateInfo> children =
                new ArrayList<NativeSqlCalc.PredicateInfo>();
            for (StarPredicate child
                : ((mondrian.rolap.agg.AndPredicate) pred).getChildren())
            {
                NativeSqlCalc.PredicateInfo childInfo =
                    buildStarPredicateInfo(
                        child, star, factAlias, joinClauses, seenJoins);
                if (childInfo != null) {
                    children.add(childInfo);
                }
            }
            return new NativeSqlCalc.CompositePredicateInfo(
                "AND", children);
        }

        if (pred instanceof mondrian.rolap.agg.OrPredicate) {
            List<NativeSqlCalc.PredicateInfo> children =
                new ArrayList<NativeSqlCalc.PredicateInfo>();
            for (StarPredicate child
                : ((mondrian.rolap.agg.OrPredicate) pred).getChildren())
            {
                NativeSqlCalc.PredicateInfo childInfo =
                    buildStarPredicateInfo(
                        child, star, factAlias, joinClauses, seenJoins);
                if (childInfo != null) {
                    children.add(childInfo);
                }
            }
            return new NativeSqlCalc.CompositePredicateInfo(
                "OR", children);
        }

        LOGGER.debug(
            "buildStarPredicateInfo: unsupported type {}",
            pred.getClass().getSimpleName());
        return null;
    }

    /**
     * Builds an {@link NativeSqlCalc.AtomicPredicateInfo} from a
     * {@link mondrian.rolap.agg.ValueColumnPredicate}, resolving the
     * constrained column to SQL and extracting dimension/hierarchy names.
     */
    private NativeSqlCalc.AtomicPredicateInfo buildAtomicStarPredicateInfo(
        mondrian.rolap.agg.ValueColumnPredicate pred,
        RolapMember member,
        RolapStar star,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        NativeSqlCalc.ResolvedColumnSql resolved =
            NativeSqlCalc.resolvePredicateColumnSql(
                pred.getConstrainedColumn(),
                star, factAlias, joinClauses, seenJoins);
        if (resolved == null) {
            return null;
        }

        Object value = pred.getValue();
        String sql = value == RolapUtil.sqlNullValue
            ? resolved.qualifiedColumn + " IS NULL"
            : resolved.qualifiedColumn + " = "
                + NativeSqlCalc.formatLiteral(value);

        // Resolve dimension/hierarchy metadata
        String dimName;
        String hierName;
        if (member != null) {
            dimName = member.getHierarchy().getDimension().getName();
            hierName = member.getHierarchy().getName();
        } else {
            NativeSqlCalc.PredicateMetadata metadata =
                NativeSqlCalc.resolvePredicateMetadata(
                    null, pred.getConstrainedColumn(), baseCube);
            dimName = metadata.dimensionName;
            hierName = metadata.hierarchyName;
        }

        return new NativeSqlCalc.AtomicPredicateInfo(
            dimName, hierName, sql);
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
     * Executes SQL and fills the context under a custom classId.
     * Used by multi-granularity execution where the classId includes
     * a granularity suffix.
     */
    private void executeAndFillWithClassId(
        String sql,
        CoordinateClassPlan plan,
        String classId,
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
                    parseAndFillWithClassId(
                        rs, plan, classId, context);
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
     * Parses result set rows into the context under a custom classId.
     * Used by multi-granularity execution.
     */
    private void parseAndFillWithClassId(
        ResultSet rs,
        CoordinateClassPlan plan,
        String classId,
        NativeQueryResultContext context)
        throws SQLException
    {
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
            String projectedKey = buildProjectedKey(rs, keyColCount);

            for (int i = 0; i < requests.size(); i++) {
                int colIndex = keyColCount + i + 1;
                Object raw = rs.getObject(colIndex);
                Object cellValue = (raw == null) ? null : raw;

                context.put(
                    classId,
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
