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
import mondrian.calc.impl.ArrayTupleList;
import mondrian.olap.*;
import mondrian.rolap.nativesql.NativeSqlError;
import mondrian.rolap.nativesql.NativeSqlExecutor;
import mondrian.rolap.nativesql.NativeSqlFingerprint;
import mondrian.rolap.nativesql.NativeSqlTelemetry;
import mondrian.rolap.nativesql.StatementLocalCache;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;

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

    /**
     * Per-thread SQL result cache — avoids re-executing identical SQL
     * within the same {@code tryPrune} invocation.  Cleared at end of
     * tryPrune.
     *
     * <p>Phase 6 substrate migration (Contract 7): the cache primitive
     * is now {@link StatementLocalCache}, which is the same primitive
     * used by {@link mondrian.rolap.nativesql.CellPhaseNativeRegistry}
     * for its in-statement state — but this NNEF instance is
     * deliberately separate (per-domain ownership).  Sharing across
     * domains is not allowed by Contract 7 because the result shapes
     * differ: NNEF stores tuple sets, the registry stores cell-phase
     * scalar/batch payloads.
     */
    private static final ThreadLocal<StatementLocalCache<NativeSqlFingerprint, Set<List<Object>>>>
        SQL_CACHE = new ThreadLocal<>();

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
        // Init per-call SQL cache (substrate primitive — Contract 7
        // says NNEF gets its own instance, not shared with the
        // cell-phase registry).
        SQL_CACHE.set(new StatementLocalCache<NativeSqlFingerprint, Set<List<Object>>>());
        try {
            return tryPruneImpl(evaluator, candidates, measures);
        } finally {
            SQL_CACHE.remove();
        }
    }

    private static TupleList tryPruneImpl(
        RolapEvaluator evaluator,
        TupleList candidates,
        Set<Member> measures)
    {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }

        // Resolve base cube once (used by eligibility + SQL generation)
        RolapCube baseCube = resolveBaseCube(evaluator, measures);
        if (baseCube == null) {
            return null;
        }

        FallbackReason reason = assessEligibility(
            evaluator, candidates, baseCube);
        if (reason != null) {
            return null;
        }

        Map<String, AggKind> leafColumns = resolveLeafMeasures(measures);
        if (leafColumns == null) {
            return null;
        }

        Set<Set<Hierarchy>> signatures = collectSignatures(candidates);
        if (signatures.isEmpty()) {
            return null;
        }

        // Single SQL at the finest granularity. Coarser signatures
        // are derived by projecting (dropping columns). One round-trip
        // regardless of signature count — critical for high-latency CH.
        long startTime = System.currentTimeMillis();

        // Find the finest signature (most hierarchies)
        Set<Hierarchy> finestSig = null;
        for (Set<Hierarchy> sig : signatures) {
            if (finestSig == null || sig.size() > finestSig.size()) {
                finestSig = sig;
            }
        }

        String sql = buildNonEmptySql(
            finestSig, leafColumns, baseCube, evaluator);
        if (sql == null) {
            return null;
        }

        Set<List<Object>> finestKeys = executeNonEmptyQuery(
            sql, finestSig.size(), evaluator);
        if (finestKeys == null) {
            return null;
        }

        // Build key sets for all signatures by projecting from finest
        List<Hierarchy> finestOrder =
            new ArrayList<Hierarchy>(finestSig);
        Map<Set<Hierarchy>, Set<List<Object>>> keysBySignature =
            new HashMap<Set<Hierarchy>, Set<List<Object>>>();
        keysBySignature.put(finestSig, finestKeys);

        for (Set<Hierarchy> sig : signatures) {
            if (sig.equals(finestSig)) {
                continue;
            }
            // Project: for each finest key, extract only the columns
            // corresponding to this signature's hierarchies
            List<Integer> indices = new ArrayList<Integer>();
            for (Hierarchy h : sig) {
                int idx = finestOrder.indexOf(h);
                if (idx < 0) {
                    // Hierarchy not in finest sig — can't project,
                    // fall back to separate SQL for this sig
                    Set<List<Object>> keys = executeNonEmptyQuery(
                        buildNonEmptySql(
                            sig, leafColumns, baseCube, evaluator),
                        sig.size(), evaluator);
                    if (keys == null) {
                        return null;
                    }
                    keysBySignature.put(sig, keys);
                    indices = null;
                    break;
                }
                indices.add(idx);
            }
            if (indices != null) {
                Set<List<Object>> projected =
                    new HashSet<List<Object>>();
                for (List<Object> key : finestKeys) {
                    List<Object> proj =
                        new ArrayList<Object>(indices.size());
                    for (int idx : indices) {
                        proj.add(key.get(idx));
                    }
                    projected.add(proj);
                }
                keysBySignature.put(sig, projected);
            }
        }

        // Filter candidates
        TupleList pruned = filterCandidates(candidates, keysBySignature);

        long elapsedMs = System.currentTimeMillis() - startTime;
        LOGGER.info(
            "NativeNonEmptyFilter: pruned {} -> {} tuples"
            + " ({} signatures, 1 SQL, {}ms)",
            candidates.size(), pruned.size(),
            signatures.size(), elapsedMs);

        return pruned;
    }

    /**
     * Checks all preconditions for SQL pre-pruning.
     * Returns a FallbackReason if ineligible, or null if eligible.
     */
    static FallbackReason assessEligibility(
        RolapEvaluator evaluator,
        TupleList candidates,
        RolapCube baseCube)
    {
        // 1. Check for dimension calculated members in any tuple
        for (List<Member> tuple : candidates) {
            for (Member m : tuple) {
                if (m.isMeasure()) {
                    continue;
                }
                if (m.isCalculated()) {
                    return FallbackReason.DIMENSION_CALC_MEMBER;
                }
            }
        }

        // 2. Get star — virtual cubes don't have one
        RolapStar star = baseCube.getStar();
        if (star == null) {
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
     * aggregation kind.
     *
     * <p>Phase 1 (PRUNE_ONLY): only resolves direct stored measures.
     * Calculated measures are skipped — we only need at least one
     * stored measure to prove a tuple has data. Walking calc formula
     * trees can find virtual/template columns that don't exist in
     * the fact table.
     *
     * <p>Returns null if no stored measures could be resolved.
     */
    static Map<String, AggKind> resolveLeafMeasures(
        Set<Member> measures)
    {
        Map<String, AggKind> result = new LinkedHashMap<String, AggKind>();

        for (Member m : measures) {
            Member unwrapped = m;
            while (unwrapped instanceof DelegatingRolapMember) {
                unwrapped =
                    ((DelegatingRolapMember) unwrapped).member;
            }

            if (unwrapped instanceof RolapStoredMeasure) {
                // Direct stored measure — safe to include
                addStoredMeasure(
                    (RolapStoredMeasure) unwrapped, result);
            }
            // Phase 1: skip calculated measures — don't walk formulas.
            // We only need SOME columns to check non-emptiness.
        }

        if (result.isEmpty()) {
            LOGGER.info(
                "NativeNonEmptyFilter: fallback reason={},"
                + " no direct stored measures found",
                FallbackReason.UNSUPPORTED_MEASURE_SEMANTICS);
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
            // Skip empty signatures (tuples where all members are All)
            if (!sig.isEmpty()) {
                signatures.add(sig);
            }
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
            // Unwrap RolapCubeHierarchy to RolapHierarchy —
            // candidate tuples contain RolapCubeHierarchy wrappers
            // which don't match HierarchyUsage names in baseCube
            Hierarchy resolved = h;
            if (h instanceof RolapCubeHierarchy) {
                resolved = ((RolapCubeHierarchy) h).getRolapHierarchy();
            }
            String col = sqlGen.resolveHierarchyColumn(
                resolved, star, factTable, factAlias,
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
    // Task 5: SQL execution, key matching, tuple filtering
    // ------------------------------------------------------------------

    /**
     * Executes a non-empty SQL query and collects result keys as
     * typed value lists. Returns null on SQL error (fallback).
     *
     * <p>Phase 6 substrate migration: routed through
     * {@link NativeSqlExecutor} for the JDBC envelope,
     * {@link NativeSqlFingerprint} for the cache key, and
     * {@link NativeSqlTelemetry} for execution telemetry.  All
     * errors classify via {@link NativeSqlError#classify} but NNEF's
     * fallback semantic is unchanged: any failure → return null →
     * caller falls back to legacy {@code nonEmptyList} evaluation.
     */
    private static Set<List<Object>> executeNonEmptyQuery(
        String sql, final int keyColCount, RolapEvaluator evaluator)
    {
        final DataSource dataSource =
            evaluator.getSchemaReader().getDataSource();

        final NativeSqlFingerprint fp = NativeSqlFingerprint.of(
            sql, Collections.<Object>emptyList(), dataSource,
            /*session*/ null);

        // Check cache first
        final StatementLocalCache<NativeSqlFingerprint, Set<List<Object>>>
            cache = SQL_CACHE.get();
        if (cache != null && cache.contains(fp)) {
            return cache.get(fp);
        }

        final long startNanos = System.nanoTime();
        final String fpId = fp.toString();
        NativeSqlTelemetry.executionStart(fpId);

        Set<List<Object>> keys;
        try {
            keys = NativeSqlExecutor.run(
                sql,
                dataSource,
                /*timeoutSeconds*/ 0,
                new NativeSqlExecutor.ResultSetHandler<Set<List<Object>>>() {
                    @Override
                    public Set<List<Object>> handle(ResultSet rs)
                        throws SQLException
                    {
                        final Set<List<Object>> out =
                            new HashSet<List<Object>>();
                        while (rs.next()) {
                            final List<Object> key =
                                new ArrayList<Object>(keyColCount);
                            for (int i = 0; i < keyColCount; i++) {
                                key.add(rs.getObject(i + 1));
                            }
                            out.add(key);
                        }
                        return out;
                    }
                });
        } catch (Throwable t) {
            // NNEF semantic: ANY failure → fallback to legacy evaluator.
            // Classify for telemetry visibility but route uniformly to
            // null-return regardless of FALLBACK vs PROPAGATE.
            final NativeSqlError.Classification cls =
                NativeSqlError.classify(t);
            final long durationMs =
                (System.nanoTime() - startNanos) / 1_000_000L;
            NativeSqlTelemetry.executionFailed(fpId, t, cls, durationMs);
            LOGGER.info(
                "NativeNonEmptyFilter: fallback reason={}, classification={}, sql error: {}",
                FallbackReason.SQL_EXECUTION_FAILED,
                cls,
                t.getMessage());
            return null;
        }

        final long durationMs =
            (System.nanoTime() - startNanos) / 1_000_000L;
        NativeSqlTelemetry.executionSuccess(fpId, durationMs);

        // Store in cache
        if (cache != null) {
            cache.put(fp, keys);
        }
        return keys;
    }

    /**
     * Extracts the typed key from a candidate tuple for a given signature.
     * Key parts are the member keys for non-measure, non-All members,
     * in the same order as the signature's hierarchy iteration order.
     */
    static List<Object> buildKeyFromTuple(
        List<Member> tuple, Set<Hierarchy> signature)
    {
        List<Object> key = new ArrayList<Object>(signature.size());
        for (Hierarchy h : signature) {
            for (Member m : tuple) {
                if (!m.isMeasure() && m.getHierarchy().equals(h)) {
                    if (m.isAll()) {
                        // This hierarchy is All in this tuple — shouldn't
                        // match this signature (signature only has non-All)
                        break;
                    }
                    key.add(((RolapMember) m).getKey());
                    break;
                }
            }
        }
        return key;
    }

    /**
     * Computes the granularity signature for a tuple — the set of
     * hierarchies that have non-All, non-measure members.
     */
    static Set<Hierarchy> signatureFromTuple(List<Member> tuple) {
        Set<Hierarchy> sig = new LinkedHashSet<Hierarchy>();
        for (Member m : tuple) {
            if (!m.isMeasure() && !m.isAll()) {
                sig.add(m.getHierarchy());
            }
        }
        return sig;
    }

    /**
     * Filters candidate tuples, keeping only those whose key exists
     * in the non-empty key set for their signature.
     */
    static TupleList filterCandidates(
        TupleList candidates,
        Map<Set<Hierarchy>, Set<List<Object>>> keysBySignature)
    {
        int arity = candidates.getArity();
        ArrayTupleList result = new ArrayTupleList(arity);

        for (List<Member> tuple : candidates) {
            Set<Hierarchy> sig = signatureFromTuple(tuple);
            Set<List<Object>> validKeys = keysBySignature.get(sig);
            if (validKeys == null) {
                // No SQL was run for this signature — keep the tuple
                // (conservative: let legacy evaluation handle it)
                result.addTuple(
                    tuple.toArray(new Member[arity]));
                continue;
            }
            List<Object> key = buildKeyFromTuple(tuple, sig);
            if (validKeys.contains(key)) {
                result.addTuple(
                    tuple.toArray(new Member[arity]));
            }
        }
        return result;
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
