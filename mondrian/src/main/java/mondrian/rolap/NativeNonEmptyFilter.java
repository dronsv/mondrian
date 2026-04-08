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

import mondrian.calc.TupleList;
import mondrian.mdx.MemberExpr;
import mondrian.olap.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * SQL pre-pruning for NON EMPTY axis evaluation.
 *
 * <p>Generates SQL to find non-empty dimension key combinations,
 * then filters candidate tuples via HashSet lookup. This is a
 * PRUNE_ONLY optimization — legacy nonEmptyList evaluation
 * remains authoritative on the reduced list.
 */
public class NativeNonEmptyFilter {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeNonEmptyFilter.class);

    /** Reason codes for eligibility fallback. */
    public enum FallbackReason {
        DISABLED_BY_CONFIG,
        EMPTY_CANDIDATES,
        DIMENSION_CALC_MEMBER,
        CROSS_CUBE_MEASURES,
        UNSUPPORTED_MEASURE_SEMANTICS,
        UNRESOLVABLE_HIERARCHY,
        NO_STAR,
        RESULT_SHAPE_TOO_LARGE,
        SQL_EXECUTION_FAILED
    }

    /** Aggregation kind for HAVING clause generation. */
    enum AggKind {
        SUM,
        COUNT_DISTINCT
    }

    private NativeNonEmptyFilter() {}

    /**
     * Attempts to prune obviously-empty tuples from the candidate list
     * via SQL pre-filter. Returns a reduced list if successful, or
     * null to signal fallback.
     *
     * <p>The caller MUST continue with legacy nonEmptyList evaluation
     * on the returned list — this method is PRUNE_ONLY.
     */
    public static TupleList tryPrune(
        RolapEvaluator evaluator,
        TupleList candidates,
        Set<Member> measures)
    {
        if (!MondrianProperties.instance()
                .NativeNonEmptyFilterEnable.get())
        {
            return null;
        }
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }

        FallbackReason reason = assessEligibility(
            evaluator, candidates, measures);
        if (reason != null) {
            return null;
        }

        // TODO: SQL execution, filtering (Task 5)
        // For now, resolve base cube and generate SQL to validate the
        // generation path, but don't execute it yet.
        RolapCube baseCube = resolveBaseCube(evaluator, measures);
        if (baseCube == null) {
            return null;
        }

        Map<String, AggKind> leafColumns =
            resolveLeafMeasures(measures, baseCube);
        if (leafColumns == null) {
            return null;
        }

        Set<Set<Hierarchy>> signatures = collectSignatures(candidates);
        if (signatures.isEmpty()) {
            return null;
        }

        // Generate SQL for each signature (validation only for Task 4;
        // Task 5 will execute and filter)
        for (Set<Hierarchy> sig : signatures) {
            String sql = buildNonEmptySql(
                sig, leafColumns, baseCube, evaluator);
            if (sql != null) {
                LOGGER.info(
                    "NativeNonEmptyFilter: generated SQL for signature"
                    + " ({}): {}",
                    sig.size(), sql);
            }
        }

        return null;
    }

    /**
     * Checks all preconditions for SQL pre-pruning.
     * Returns a FallbackReason if ineligible, or null if eligible.
     */
    static FallbackReason assessEligibility(
        RolapEvaluator evaluator,
        TupleList candidates,
        Set<Member> measures)
    {
        // 1. Check for dimension calculated members in any tuple
        for (List<Member> tuple : candidates) {
            for (Member m : tuple) {
                if (m.isMeasure()) {
                    continue;
                }
                if (m.isCalculated()) {
                    LOGGER.info(
                        "NativeNonEmptyFilter: fallback reason={}, member={}",
                        FallbackReason.DIMENSION_CALC_MEMBER,
                        m.getUniqueName());
                    return FallbackReason.DIMENSION_CALC_MEMBER;
                }
            }
        }

        // 2. Resolve base cube (single-cube check for VirtualCube)
        RolapCube baseCube = resolveBaseCube(evaluator, measures);
        if (baseCube == null) {
            return FallbackReason.CROSS_CUBE_MEASURES;
        }

        // 3. Get star — virtual cubes don't have one
        RolapStar star = baseCube.getStar();
        if (star == null) {
            LOGGER.info(
                "NativeNonEmptyFilter: fallback reason={}, cube={}",
                FallbackReason.NO_STAR, baseCube.getName());
            return FallbackReason.NO_STAR;
        }

        // All checks passed
        return null;
    }

    /**
     * Resolves the single base cube for the given measures.
     * Returns null if no non-virtual cube could be determined
     * (e.g. VirtualCube cross-cube scenario).
     */
    static RolapCube resolveBaseCube(
        RolapEvaluator evaluator, Set<Member> measures)
    {
        // Try to find a stored measure to get the base cube
        if (measures != null) {
            for (Member m : measures) {
                Member unwrapped = m;
                while (unwrapped instanceof DelegatingRolapMember) {
                    unwrapped =
                        ((DelegatingRolapMember) unwrapped).member;
                }
                if (unwrapped instanceof RolapStoredMeasure) {
                    return ((RolapStoredMeasure) unwrapped).getCube();
                }
            }
        }

        // Fallback to evaluator cube (works for non-virtual cubes)
        RolapCube evalCube = evaluator.getCube();
        if (evalCube != null && !evalCube.isVirtual()) {
            return evalCube;
        }

        LOGGER.info(
            "NativeNonEmptyFilter: fallback reason={}, "
            + "could not resolve non-virtual base cube",
            FallbackReason.CROSS_CUBE_MEASURES);
        return null;
    }

    // ------------------------------------------------------------------
    // Task 4: SQL generation helpers
    // ------------------------------------------------------------------

    /**
     * Resolves query measures to leaf stored column names with their
     * aggregation kind. Returns null on any unresolvable measure
     * (signals fallback to legacy evaluation).
     *
     * <p>For stored measures, uses {@link RolapStoredMeasure#getStarMeasure()}
     * directly. For calculated measures, walks the formula expression tree
     * to find underlying stored measures. Returns null if any measure
     * cannot be resolved.
     */
    static Map<String, AggKind> resolveLeafMeasures(
        Set<Member> measures, RolapCube baseCube)
    {
        Map<String, AggKind> result = new LinkedHashMap<String, AggKind>();

        for (Member m : measures) {
            Member unwrapped = m;
            while (unwrapped instanceof DelegatingRolapMember) {
                unwrapped =
                    ((DelegatingRolapMember) unwrapped).member;
            }

            if (unwrapped instanceof RolapStoredMeasure) {
                if (!addStoredMeasure(
                        (RolapStoredMeasure) unwrapped, result))
                {
                    return null;
                }
            } else if (unwrapped.isCalculated()) {
                // Walk the formula expression tree to find stored
                // measures referenced by the calculated measure
                Exp formula = unwrapped.getExpression();
                if (formula == null) {
                    LOGGER.info(
                        "NativeNonEmptyFilter: fallback reason={},"
                        + " measure={} (null expression)",
                        FallbackReason.UNSUPPORTED_MEASURE_SEMANTICS,
                        unwrapped.getUniqueName());
                    return null;
                }
                Set<RolapStoredMeasure> storedMeasures =
                    new LinkedHashSet<RolapStoredMeasure>();
                collectStoredMeasures(formula, storedMeasures);
                if (storedMeasures.isEmpty()) {
                    LOGGER.info(
                        "NativeNonEmptyFilter: fallback reason={},"
                        + " measure={} (no stored measures in formula)",
                        FallbackReason.UNSUPPORTED_MEASURE_SEMANTICS,
                        unwrapped.getUniqueName());
                    return null;
                }
                for (RolapStoredMeasure stored : storedMeasures) {
                    if (!addStoredMeasure(stored, result)) {
                        return null;
                    }
                }
            } else {
                LOGGER.info(
                    "NativeNonEmptyFilter: fallback reason={},"
                    + " measure={} (not stored, not calculated)",
                    FallbackReason.UNSUPPORTED_MEASURE_SEMANTICS,
                    unwrapped.getUniqueName());
                return null;
            }
        }

        if (result.isEmpty()) {
            return null;
        }
        return result;
    }

    /**
     * Adds a stored measure's column name and aggregation kind to the
     * result map. Returns false if the star measure cannot be resolved.
     */
    private static boolean addStoredMeasure(
        RolapStoredMeasure stored, Map<String, AggKind> result)
    {
        Object starObj = stored.getStarMeasure();
        if (!(starObj instanceof RolapStar.Measure)) {
            LOGGER.info(
                "NativeNonEmptyFilter: cannot resolve star measure"
                + " for {}", stored.getUniqueName());
            return false;
        }
        RolapStar.Measure starMeasure = (RolapStar.Measure) starObj;
        String colName = getColumnName(starMeasure);
        AggKind kind = starMeasure.getAggregator().isDistinct()
            ? AggKind.COUNT_DISTINCT : AggKind.SUM;
        result.put(colName, kind);
        return true;
    }

    /**
     * Recursively walks an MDX expression tree to collect all
     * {@link RolapStoredMeasure} references. Handles
     * {@link MemberExpr} (leaf member references) and
     * {@link FunCall} (function applications with arguments).
     * For nested calculated measures, recurses into their formulas.
     */
    private static void collectStoredMeasures(
        Exp exp, Set<RolapStoredMeasure> result)
    {
        if (exp instanceof MemberExpr) {
            Member m = ((MemberExpr) exp).getMember();
            Member unwrapped = m;
            while (unwrapped instanceof DelegatingRolapMember) {
                unwrapped =
                    ((DelegatingRolapMember) unwrapped).member;
            }
            if (unwrapped instanceof RolapStoredMeasure) {
                result.add((RolapStoredMeasure) unwrapped);
            } else if (unwrapped.isCalculated()
                && unwrapped.getExpression() != null)
            {
                collectStoredMeasures(
                    unwrapped.getExpression(), result);
            }
        } else if (exp instanceof FunCall) {
            FunCall fc = (FunCall) exp;
            for (Exp arg : fc.getArgs()) {
                collectStoredMeasures(arg, result);
            }
        }
    }

    /**
     * Collects unique granularity signatures from candidate tuples.
     * Each signature is the set of hierarchies that have non-All,
     * non-measure members in a given tuple.
     *
     * <p>For example, a DrilldownMember axis may have tuples at
     * different granularity levels — some hierarchies are All (aggregated)
     * while others have specific members. Each unique combination of
     * active hierarchies forms a signature that needs its own SQL query.
     */
    static Set<Set<Hierarchy>> collectSignatures(TupleList candidates) {
        Set<Set<Hierarchy>> signatures =
            new LinkedHashSet<Set<Hierarchy>>();
        for (List<Member> tuple : candidates) {
            Set<Hierarchy> sig = new LinkedHashSet<Hierarchy>();
            for (Member m : tuple) {
                if (!m.isMeasure() && !m.isAll()) {
                    sig.add(m.getHierarchy());
                }
            }
            signatures.add(sig);
        }
        return signatures;
    }

    /**
     * Builds a SQL query to find non-empty dimension key combinations
     * for a given granularity signature.
     *
     * <p>The generated SQL follows the pattern:
     * <pre>
     *   SELECT &lt;dim_keys&gt;
     *   FROM &lt;fact_table&gt; f [JOINs]
     *   WHERE &lt;slicer + subcube predicates&gt;
     *   GROUP BY &lt;dim_keys&gt;
     *   HAVING &lt;non-empty check per leaf measure&gt;
     * </pre>
     *
     * <p>The HAVING clause uses {@code SUM(col) IS NOT NULL} for
     * additive measures and {@code COUNT(DISTINCT col) > 0} for
     * distinct-count measures.
     *
     * @param signature    set of hierarchies to include in GROUP BY
     * @param leafColumns  map of column name to aggregation kind
     * @param baseCube     the resolved base cube
     * @param evaluator    the current evaluator (for WHERE context)
     * @return SQL string, or null if any hierarchy cannot be resolved
     */
    static String buildNonEmptySql(
        Set<Hierarchy> signature,
        Map<String, AggKind> leafColumns,
        RolapCube baseCube,
        RolapEvaluator evaluator)
    {
        RolapStar star = baseCube.getStar();
        RolapStar.Table factTable = star.getFactTable();
        String factTableName = factTable.getTableName();
        String factAlias = "f";

        List<String> selectExprs = new ArrayList<String>();
        List<String> groupByExprs = new ArrayList<String>();
        List<String> joinClauses = new ArrayList<String>();
        Set<String> seenJoins = new LinkedHashSet<String>();
        List<String> wherePredicates = new ArrayList<String>();

        // 1. SELECT + GROUP BY: one column per hierarchy in signature.
        //    Reuse NativeQuerySqlGenerator for column resolution —
        //    same package, so package-private methods are accessible.
        NativeQuerySqlGenerator sqlGen =
            new NativeQuerySqlGenerator(evaluator, baseCube);

        for (Hierarchy h : signature) {
            String col = sqlGen.resolveHierarchyColumn(
                h, star, factTable, factAlias,
                joinClauses, seenJoins);
            if (col == null) {
                LOGGER.info(
                    "NativeNonEmptyFilter: fallback reason={},"
                    + " hierarchy={}",
                    FallbackReason.UNRESOLVABLE_HIERARCHY,
                    h.getUniqueName());
                return null;
            }
            selectExprs.add(col);
            groupByExprs.add(col);
        }

        // 2. WHERE: slicer + subcube predicates from evaluator context.
        //    Skip signature hierarchies (they are GROUP BY keys).
        sqlGen.buildWhereFromContext(
            wherePredicates,
            null,        // no reset hierarchies
            signature,   // projected = signature hierarchies (skip)
            star, factTable, factAlias, joinClauses, seenJoins);

        // 3. HAVING: OR-combined non-empty checks per leaf column.
        //    A tuple is non-empty if ANY measure has a non-null value.
        List<String> havingParts = new ArrayList<String>();
        for (Map.Entry<String, AggKind> entry : leafColumns.entrySet()) {
            String colName = entry.getKey();
            AggKind kind = entry.getValue();
            String colExpr = factAlias + "." + colName;
            if (kind == AggKind.COUNT_DISTINCT) {
                havingParts.add("COUNT(DISTINCT " + colExpr + ") > 0");
            } else {
                havingParts.add("SUM(" + colExpr + ") IS NOT NULL");
            }
        }

        // 4. Assemble SQL
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append(join(selectExprs, ", "));
        sql.append(" FROM ").append(factTableName)
           .append(" ").append(factAlias);
        for (String j : joinClauses) {
            sql.append(" ").append(j);
        }
        if (!wherePredicates.isEmpty()) {
            sql.append(" WHERE ").append(join(wherePredicates, " AND "));
        }
        if (!groupByExprs.isEmpty()) {
            sql.append(" GROUP BY ").append(join(groupByExprs, ", "));
        }
        if (!havingParts.isEmpty()) {
            sql.append(" HAVING ").append(join(havingParts, " OR "));
        }

        return sql.toString();
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    /**
     * Returns the column name from a star measure's expression.
     */
    private static String getColumnName(RolapStar.Measure measure) {
        MondrianDef.Expression expr = measure.getExpression();
        if (expr instanceof MondrianDef.Column) {
            return ((MondrianDef.Column) expr).name;
        }
        return measure.getName();
    }

    /**
     * Joins a list of strings with the given separator.
     * (Java 7 compatible — no String.join)
     */
    private static String join(List<String> parts, String separator) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(parts.get(i));
        }
        return sb.toString();
    }
}
