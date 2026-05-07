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
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.nativesql.BatchNativeSqlWork;
import mondrian.rolap.nativesql.NativeSqlLookupResult;
import mondrian.rolap.nativesql.NativeSqlWorkKind;
import mondrian.rolap.nativesql.NativeSqlError;
import mondrian.rolap.nativesql.NativeSqlFingerprint;
import mondrian.spi.Dialect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Evaluates a native SQL calculated measure via a batch SQL query.
 *
 * <p>Resolves context-specific placeholders ({@code ${factTable}},
 * {@code ${axisExprN}}, {@code ${whereClause}}, etc.) in a user-provided
 * SQL template, executes the resulting query once per axis sweep, and
 * caches the results keyed by axis member keys.
 *
 * <p>On any error, falls back to the standard MDX formula evaluation.
 */
public class NativeSqlCalc extends GenericCalc {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeSqlCalc.class);

    /** Pattern matching {@code ${identifier}} and {@code ${fn:args}} placeholders. */
    private static final Pattern PLACEHOLDER_PATTERN =
        Pattern.compile("\\$\\{([a-zA-Z_][a-zA-Z0-9_]*(?::[^}]*)?)\\}");

    /** Sentinel for grand-total / All-member axis cells in rowKey. */
    public static final String ALL_MEMBER_MARKER = "(all)";

    /** Sentinel for real-data NULL in axis-key columns. NUL char prefix
     *  guarantees no collision with user-provided strings. Visible in
     *  logs as "&lt;NUL&gt;NULL". */
    public static final String NULL_KEY_MARKER = "\0NULL";

    private final RolapCalculatedMember member;
    private final RolapEvaluatorRoot root;
    private final NativeSqlConfig.NativeSqlDef def;
    // Lazy-resolved at first evaluate()
    private RolapCube baseCube;
    private boolean resolved;

    /** Lazily compiled fallback — NOT created in create() to avoid
     *  recursive compilation, but compiled on first unsupported query. */
    private Calc lazyFallback;
    private boolean fallbackAttempted;

    /**
     * Bundle of placeholder values, predicates, and axis bindings
     * produced by {@link #buildPlaceholders}. Returned as an immutable
     * local value instead of stored in instance fields — avoids race
     * conditions when concurrent XMLA requests share a NativeSqlCalc
     * instance.
     */
    private record PlaceholderBundle(
        Map<String, String> placeholders,
        List<PredicateInfo> predicates,
        List<AxisBinding> axisBindings) {}

    private NativeSqlCalc(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root,
        NativeSqlConfig.NativeSqlDef def)
    {
        super(member.getExpression(), new Calc[0]);
        this.member = member;
        this.root = root;
        this.def = def;
    }

    /**
     * Factory method called by {@link NativeSqlRegistry}.
     * Does NOT compile the fallback MDX formula — that would trigger
     * recursive compilation of referenced calculated members.
     */
    static Calc create(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root,
        NativeSqlConfig.NativeSqlDef def)
    {
        LOGGER.debug(
            "NativeSqlCalc.create: creating lazy calc for [{}]",
            member.getName());
        return new NativeSqlCalc(member, root, def);
    }

    /**
     * Lazy-resolves the base cube from the evaluator's current cube.
     */
    private boolean ensureResolved(Evaluator evaluator) {
        if (resolved) {
            return baseCube != null;
        }
        resolved = true;
        final RolapCube cube = (RolapCube) evaluator.getCube();
        if (cube.isVirtual()) {
            // For virtual cubes, use the member's own base cube
            baseCube = member.getBaseCube();
        } else {
            baseCube = cube;
        }
        if (baseCube == null) {
            LOGGER.warn(
                "NativeSqlCalc: cannot resolve baseCube for [{}]",
                member.getName());
            return false;
        }
        return true;
    }

    @Override
    public Object evaluate(Evaluator evaluator) {
        if (!ensureResolved(evaluator)) {
            return null;
        }
        return evaluateViaRegistry(evaluator);
    }

    /**
     * Returns true when a {@code rollupAxes}-enabled measure has more axes
     * than v1 supports (3-axis cap; 4 axes = 16 grouping sets is too much
     * for the rollupAxes path to absorb in a single round trip).
     *
     * <p>Above the cap, {@link #evaluateViaRegistry} must NOT render the
     * CUBE template — it should delegate to {@link #fallbackOrNull} (MDX)
     * instead.
     */
    static boolean shouldFallbackForAxisCap(
        NativeSqlConfig.NativeSqlDef def, int axisCount)
    {
        return def.isRollupAxes() && axisCount > 3;
    }

    /**
     * Phase 4 path: walk the template fallback chain via the per-statement
     * {@link mondrian.rolap.nativesql.NativeSqlRegistry}.
     *
     * <p>For each template index in order:
     * <ul>
     *   <li>Substitute the template against current evaluator context.
     *       Substitution failure (unresolved placeholder) → skip to next.
     *   <li>Look up the registry under the resulting SQL fingerprint.
     *   <li>MISS → register a {@link NscBatchWork} unit, return
     *       value-not-ready sentinel. {@code RolapResult.phase()} drains
     *       and the next iteration sees terminal state at this template.
     *   <li>SUCCESS → materialize the per-row value from the batch payload.
     *   <li>ERROR (fallback or propagate) → try the next template.
     * </ul>
     *
     * <p>Only after all templates terminate in error does this method
     * route to {@code fallbackOrNull} (MDX).
     */
    private Object evaluateViaRegistry(Evaluator evaluator) {
        final PlaceholderBundle bundle;
        final String rowKey;
        try {
            bundle = buildPlaceholders(evaluator);
            // Rollup path needs the encoder that emits ALL_MEMBER_MARKER
            // for All-member axis hierarchies — symmetric with
            // parseResultSetWithGroupingFlags. Non-rollup keeps the
            // legacy String.valueOf-based buildRowKey symmetric with the
            // unchanged parseResultSet.
            rowKey = def.isRollupAxes()
                ? encodeRowKey(evaluator, bundle.axisBindings())
                : buildRowKey(evaluator, bundle.axisBindings());
        } catch (Exception e) {
            LOGGER.warn(
                "NativeSqlCalc: native path unavailable for [{}], exceptionType={}, message={}",
                member.getName(),
                e.getClass().getName(),
                e.getMessage(),
                e);
            return fallbackOrNull(evaluator);
        }

        // v1 rollupAxes axis cap: 4 axes = 16 grouping sets, too much for a
        // single CUBE round trip. Above the cap delegate to MDX before any
        // template renders, so we never emit a CUBE we can't service.
        if (shouldFallbackForAxisCap(def, bundle.axisBindings().size())) {
            LOGGER.info(
                "NativeSqlCalc: rollupAxes axis count {} > 3 cap, "
                    + "falling back to MDX for [{}]",
                bundle.axisBindings().size(), member.getName());
            return fallbackOrNull(evaluator);
        }

        final DataSource dataSource =
            evaluator.getSchemaReader().getDataSource();
        final List<String> templates = def.getTemplates();

        // Walk the template fallback chain. Each template's SQL is
        // looked up / executed synchronously via executeOrLookup, which
        // hits the process-wide GLOBAL_SUCCESS cache for cross-statement
        // reuse and only falls back to JDBC on a true cache miss.
        // Per-statement errors short-circuit re-execution within the
        // same statement; subsequent templates are tried on each error.
        for (int ti = 0; ti < templates.size(); ti++) {
            final String sql;
            try {
                sql = substitutePlaceholders(
                    templates.get(ti),
                    bundle.placeholders(),
                    bundle.predicates(),
                    bundle.axisBindings());
            } catch (Exception e) {
                LOGGER.info(
                    "NativeSqlCalc: template[{}] unresolvable for [{}] ({}), trying next",
                    ti, member.getName(), e.getMessage());
                continue;
            }

            final NativeSqlFingerprint fp = NativeSqlFingerprint.of(
                sql, Collections.<Object>emptyList(), dataSource, /*session*/ null);

            final NativeSqlLookupResult r = root.nativeSqlRegistry.executeOrLookup(
                new NscBatchWork(
                    fp, dataSource, sql, this, bundle.axisBindings(),
                    def.isRollupAxes()));

            if (r.isSuccess()) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> batch =
                    (Map<String, Object>) r.successPayload();
                if (batch.containsKey(rowKey)) {
                    final Object value = batch.get(rowKey);
                    logReturnedValue("registry hit", rowKey, sql, value);
                    return value;
                }
                return null;
            }
            // ERROR (fallback or propagate) — try the next template.
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "NativeSqlCalc: registry cached error for [{}] template[{}], trying next",
                    member.getName(), ti, r.errorThrowable());
            }
        }

        // All templates terminated in error. Route to the legacy MDX fallback.
        return fallbackOrNull(evaluator);
    }

    /**
     * Returns MDX fallback result if enabled by config; otherwise null.
     */
    private Object fallbackOrNull(Evaluator evaluator) {
        if (!def.isFallbackMdx()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "NativeSqlCalc: fallback disabled for [{}]",
                    member.getName());
            }
            return null;
        }
        final Object value = evaluateFallback(evaluator);
        logReturnedValue("fallback", null, null, value);
        return value;
    }

    /**
     * Lazily compiles and evaluates the MDX formula fallback.
     * NOT done in create() to avoid recursive compilation of
     * calculated members that reference each other.
     */
    private Object evaluateFallback(Evaluator evaluator) {
        if (!fallbackAttempted) {
            fallbackAttempted = true;
            try {
                lazyFallback = root.getCompiled(
                    member.getExpression(), true, null);
            } catch (Exception e) {
                LOGGER.warn(
                    "NativeSqlCalc: fallback compilation failed for [{}]",
                    member.getName(), e);
            }
        }
        if (lazyFallback != null) {
            return lazyFallback.evaluate(evaluator);
        }
        return null;
    }

    /**
     * Clears the {@code NativeSqlRegistry.GLOBAL_SUCCESS} cache used
     * by {@link #evaluateViaRegistry}.  Call on schema flush.
     */
    public static void clearCache() {
        mondrian.rolap.nativesql.NativeSqlRegistry.clearGlobalCache();
    }

    /**
     * {@link BatchNativeSqlWork} adapter for {@link NativeSqlCalc}.
     *
     * <p>Shape: one templated SQL executes once per phase sweep and
     * returns a {@code Map<rowKey, scalar>} populated by
     * {@link NativeSqlCalc#parseResultSet}. Per-cell materialization
     * looks up the scalar at the consumer's {@code rowKey}.
     *
     * <p>Error policy: overrides {@link #policyAdjust} and
     * {@link #allowsPropagateDowngrade} to force FALLBACK on every
     * error, preserving NSC's existing "on error, try MDX fallback"
     * semantic. PROPAGATE is never observed at the consumer site.
     */
    private static final class NscBatchWork extends BatchNativeSqlWork {
        private final NativeSqlCalc owner;
        private final List<AxisBinding> axisBindings;
        private final boolean rollupAxes;

        NscBatchWork(
            NativeSqlFingerprint fp,
            DataSource dataSource,
            String sql,
            NativeSqlCalc owner,
            List<AxisBinding> axisBindings,
            boolean rollupAxes)
        {
            super(fp, dataSource, sql);
            this.owner = owner;
            this.axisBindings = axisBindings;
            this.rollupAxes = rollupAxes;
        }

        @Override
        public Object consume(ResultSet rs) throws SQLException {
            return rollupAxes
                ? parseResultSetWithGroupingFlags(rs, axisBindings)
                : owner.parseResultSet(rs, axisBindings);
        }

        @Override
        public Object materialize(Object cachedPayload, Object coordKey) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> batch =
                (Map<String, Object>) cachedPayload;
            return batch.get(coordKey);
        }

        @Override
        public NativeSqlError.Classification policyAdjust(
            Throwable t, NativeSqlError.Classification base)
        {
            // NSC's existing semantic: ALL errors route to MDX fallback.
            return NativeSqlError.Classification.FALLBACK;
        }

        @Override
        public boolean allowsPropagateDowngrade() {
            // Required opt-in for the PROPAGATE → FALLBACK override above.
            return true;
        }

        @Override
        public void onError(Throwable t) {
            LOGGER.warn(
                "NativeSqlCalc: batch query failed, fingerprint={}, exceptionType={}, message={}",
                fingerprint(),
                t.getClass().getName(),
                t.getMessage(),
                t);
        }
    }

    private void logReturnedValue(
        String source,
        String rowKey,
        String batchKey,
        Object value)
    {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }
        LOGGER.debug(
            "NativeSqlCalc: {} returned for [{}], rowKey={}, batchKeyHash={}, value={}, valueType={}",
            source,
            member.getName(),
            rowKey,
            batchKey == null ? null : batchKey.hashCode(),
            value,
            value == null ? null : value.getClass().getName());
    }

    /**
     * Collects all placeholder values for template substitution.
     *
     * <p>Built-in placeholders:
     * <ul>
     *   <li>{@code factTable} — physical fact table name
     *   <li>{@code factAlias} — always "f"
     *   <li>{@code axisExpr1}, {@code axisExpr2}, ... — qualified column
     *       expressions for non-All, non-measure evaluator members
     *   <li>{@code axisCount} — number of axis expressions
     *   <li>{@code joinClauses} — newline-joined JOIN clauses for
     *       dimension tables
     *   <li>{@code whereClause} — AND-joined predicates, or "1 = 1"
     * </ul>
     *
     * <p>All static variables from {@code nativeSql.variables} are added last.
     */
    private PlaceholderBundle buildPlaceholders(Evaluator evaluator) {
        final Map<String, String> ph = new LinkedHashMap<String, String>();
        final RolapStar star = baseCube.getStar();
        final RolapStar.Table factTable = star.getFactTable();
        final Dialect dialect = root.currentDialect;
        final String factTableName = factTable.getTableName();
        final String factAlias = "f";

        ph.put("factTable", factTableName);
        ph.put("factAlias", factAlias);

        // Determine which hierarchies are on query axes (vs slicer).
        // Axis members get GROUP BY (via axisExprN), slicer members
        // get WHERE predicates. This ensures one SQL returns all axis
        // values in a single batch.
        final Set<Hierarchy> axisHierarchies =
            resolveAxisHierarchies(evaluator.getQuery());

        final Map<Hierarchy, AxisBinding> axisBindingByHierarchy =
            new LinkedHashMap<Hierarchy, AxisBinding>();
        final List<AxisBinding> axisBindings = new ArrayList<AxisBinding>();
        final List<PredicateInfo> wherePredicates = new ArrayList<PredicateInfo>();

        // Collect all context members: evaluator members + subcube members.
        // Subcube members (from MDX subselect) are NOT in evaluator.getMembers()
        // — they live in query.getSubcube().getAxes(). We need them as WHERE
        // predicates for correct filtering (e.g., Category from subselect).
        final List<Member> allContextMembers = new ArrayList<Member>();
        for (Member m : evaluator.getMembers()) {
            if (m != null && !m.isMeasure() && !m.isAll()) {
                allContextMembers.add(m);
            }
        }
        // Subcube predicates are collected separately below,
        // directly into wherePredicates (after member loop).

        if (LOGGER.isDebugEnabled()) {
            StringBuilder dbg = new StringBuilder("NativeSqlCalc context: axes=[");
            for (Hierarchy h : axisHierarchies) {
                dbg.append(h.getUniqueName()).append(", ");
            }
            dbg.append("], members=[");
            for (Member m : evaluator.getMembers()) {
                if (m != null && !m.isMeasure()) {
                    dbg.append(m.getUniqueName()).append(", ");
                }
            }
            dbg.append("], subcube=");
            dbg.append(evaluator.getQuery().getSubcubePredicates(baseCube));
            LOGGER.debug(dbg.toString());
        }

        for (Member m : allContextMembers) {
            // Resolve the physical column from the level
            final RolapLevel level = (RolapLevel) m.getLevel();
            final MondrianDef.Expression keyExp = level.getKeyExp();
            if (!(keyExp instanceof MondrianDef.Column)) {
                LOGGER.warn(
                    "NativeSqlCalc: non-column key expression for {}, "
                        + "skipping",
                    level.getUniqueName());
                continue;
            }
            final ResolvedColumnSql resolved =
                resolveMemberColumnSql(
                    (MondrianDef.Column) keyExp,
                    factAlias);
            final String qualifiedColumn = resolved.qualifiedColumn;

            final String dimName =
                m.getHierarchy().getDimension().getName();
            final String hierName = m.getHierarchy().getName();
            // Compare by unique name — axisHierarchies may contain
            // different object instances than evaluator members
            // (e.g. query-compiled vs cube-level hierarchy wrappers)
            final boolean isAxisHierarchy =
                containsHierarchy(axisHierarchies, m.getHierarchy());

            if (isAxisHierarchy) {
                // Axis member → GROUP BY via axisExprN, NOT in WHERE.
                // SQL returns all axis values in one batch query.
                if (!axisBindingByHierarchy.containsKey(m.getHierarchy())) {
                    final String colName = qualifiedColumn.contains(".")
                        ? qualifiedColumn.substring(
                            qualifiedColumn.lastIndexOf('.') + 1)
                        : qualifiedColumn;
                    axisBindingByHierarchy.put(
                        m.getHierarchy(),
                        new AxisBinding(
                            m.getHierarchy(),
                            hierName,
                            qualifiedColumn,
                            colName,
                            null));
                }
            } else {
                // Slicer/subselect member → WHERE predicate only.
                final Object memberKey = ((RolapMember) m).getKey();
                wherePredicates.add(new AtomicPredicateInfo(
                    dimName, hierName,
                    qualifiedColumn + " = "
                        + formatLiteral(memberKey)));
            }
        }

        // Collect subcube predicates (from MDX subselect) into WHERE.
        // These are NOT in evaluator.getMembers(). Extract column=value
        // pairs from the StarPredicate tree.
        final StarPredicate subcubePred =
            evaluator.getQuery().getSubcubePredicates(baseCube);
        if (subcubePred != null) {
            wherePredicates.add(buildStarPredicate(
                subcubePred, star, baseCube, factAlias));
        }

        // joinClauses + seenJoins for synthetic bindings under rollupAxes.
        // Currently no other code path inside buildPlaceholders registers
        // JOINs (the existing resolver path is fact-first only), so these
        // start fresh. Task 12 will surface them through the joinClauses
        // placeholder when wiring cube macros end-to-end.
        final List<String> joinClauses = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();

        // Task 44: build the candidate-agg set ONCE per measure evaluation.
        // The synthetic resolver consults it to skip column bindings that
        // no template in the fallback chain could possibly satisfy — saving
        // a CK round-trip per mismatched template.
        final Set<AggStar> candidateAggs = def.isRollupAxes()
            ? resolveCandidateAggs(
                extractAggTableNamesFromTemplates(def.getTemplates()), star)
            : Collections.<AggStar>emptySet();

        for (Hierarchy axisHierarchy : axisHierarchies) {
            // Measures hierarchy is never a real dim axis: it has only the
            // [Measures] All level (no non-All levels), and members are
            // synthesized rather than resolved from a fact column. Skip it
            // for both the bound-by-evaluator path and the synthetic
            // resolveSyntheticBinding rollupAxes path.
            if (axisHierarchy.getDimension() != null
                && axisHierarchy.getDimension().isMeasures())
            {
                continue;
            }
            final AxisBinding binding = axisBindingByHierarchy.get(axisHierarchy);
            if (binding != null) {
                axisBindings.add(new AxisBinding(
                    axisHierarchy,
                    binding.hierarchyName,
                    binding.qualifiedColumn,
                    binding.columnName,
                    "k" + axisBindings.size()));
            } else if (def.isRollupAxes()) {
                final AxisBinding synthetic = resolveSyntheticBinding(
                    axisHierarchy, star, factAlias,
                    joinClauses, seenJoins, axisBindings.size(),
                    candidateAggs);
                if (synthetic != null) {
                    axisBindings.add(synthetic);
                } else {
                    // Task 44: column not present on ANY candidate agg —
                    // every template would fail at execution. Abort the
                    // native path so the bundle-level catch in
                    // evaluateViaRegistry routes to the MDX fallback
                    // without firing SQL.
                    throw new MondrianException(
                        "NativeSqlCalc: synthetic axis '"
                        + axisHierarchy.getUniqueName()
                        + "' cannot be bound — column not on any candidate"
                        + " agg in the template fallback chain");
                }
            }
        }

        // Scalar mode: execute SQL once, replicate value for all axis members
        if (def.isScalar()) {
            axisBindings.clear();
            ph.put("axisPresenceSelectList", "");
            ph.put("axisResultSelectList", "");
            ph.put("axisSelectList", "");
            ph.put("axisGroupByList", "");
            ph.put("axisCount", "0");
            for (int i = 1; i <= def.getMaxAxes(); i++) {
                ph.put("axisExpr" + i, "NULL");
            }
        } else {
            // Validate axis count
            final int axisCount = axisBindings.size();
            if (axisCount > def.getMaxAxes()) {
                throw new MondrianException(
                    "NativeSqlCalc: axis count " + axisCount
                        + " exceeds maxAxes " + def.getMaxAxes()
                        + " for [" + member.getName() + "]");
            }
            // Set axis expressions: axisExpr1, axisExpr2, ...
            for (int i = 0; i < axisCount; i++) {
                ph.put("axisExpr" + (i + 1), axisBindings.get(i).qualifiedColumn);
            }
            for (int i = axisCount; i < def.getMaxAxes(); i++) {
                ph.put("axisExpr" + (i + 1), "NULL");
            }
            final String alias = def.getRelationAlias();
            ph.put("axisPresenceSelectList", renderAxisPresenceSelectList(axisBindings));
            ph.put("axisResultSelectList", renderAxisResultSelectList(axisBindings, alias));
            ph.put("axisSelectList", renderAxisSelectListNoPrefix(axisBindings));
            ph.put("axisGroupByList", renderAxisGroupByList(axisBindings, alias));
            // Cube macros for rollupAxes templates. Populated unconditionally
            // — Contract A in NativeSqlConfig.validateCubeMacroOptIn ensures
            // a template that references these macros has rollupAxes=true,
            // and a non-rollup template won't reference them, so emitting
            // the rendered values here is safe regardless of opt-in state.
            ph.put("axisCubeSelectFlags",
                renderAxisCubeSelectFlags(axisBindings, alias));
            ph.put("axisGroupByListCube",
                renderAxisGroupByListCube(axisBindings, alias));
            ph.put("axisCount", String.valueOf(axisCount));
        }

        // joinClauses placeholder: under rollupAxes, synthetic bindings may
        // resolve axis keys to dim columns and register LEFT JOIN clauses
        // via the dim-fallback resolver. Surface them here so the template's
        // ${joinClauses} expands to the required JOINs. Empty for fact-only
        // resolution (the common case) and for non-rollupAxes templates.
        if (joinClauses.isEmpty()) {
            ph.put("joinClauses", "");
        } else {
            final StringBuilder sb = new StringBuilder();
            for (String clause : joinClauses) {
                if (sb.length() > 0) {
                    sb.append('\n');
                }
                sb.append(clause);
            }
            ph.put("joinClauses", sb.toString());
        }

        // WHERE clause (full)
        ph.put("whereClause", buildWhereFromPredicates(wherePredicates, null));

        // Add all static variables from the definition
        for (Map.Entry<String, String> entry
            : def.getVariables().entrySet())
        {
            ph.put(entry.getKey(), entry.getValue());
        }

        return new PlaceholderBundle(
            ph, wherePredicates, new ArrayList<AxisBinding>(axisBindings));
    }

    /**
     * Builds AND-joined WHERE clause from predicates, optionally
     * excluding predicates matching given dimension/hierarchy names.
     */
    static String buildWhereFromPredicates(
        List<PredicateInfo> predicates,
        Set<String> exceptNames)
    {
        final StringBuilder buf = new StringBuilder();
        for (PredicateInfo p : predicates) {
            final String rendered = p.render(exceptNames);
            if (rendered == null || rendered.isEmpty()) {
                continue;
            }
            if (buf.length() > 0) {
                buf.append(" AND ");
            }
            buf.append(rendered);
        }
        return buf.length() == 0 ? "1 = 1" : buf.toString();
    }

    /**
     * Returns true if predicate matches any of the except names.
     * If name contains a dot (e.g. "Продукт.Бренд"), matches hierarchy.
     * Otherwise matches dimension (all hierarchies of that dimension).
     */
    private static boolean shouldExclude(
        Set<String> predicateNames,
        Set<String> exceptNames)
    {
        if (predicateNames == null || predicateNames.isEmpty()) {
            return false;
        }
        for (String name : exceptNames) {
            if (predicateNames.contains(name)) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> defaultExclusionNames(
        String dimensionName,
        String hierarchyName)
    {
        final Set<String> names = new LinkedHashSet<String>();
        if (dimensionName != null) {
            names.add(dimensionName);
            if (hierarchyName != null) {
                names.add(hierarchyName);
                if (!hierarchyName.startsWith(dimensionName + ".")) {
                    names.add(dimensionName + "." + hierarchyName);
                }
            }
        }
        return names;
    }

    /**
     * Resolves which hierarchies are on query axes.
     */
    private static Set<Hierarchy> resolveAxisHierarchies(
        Evaluator evaluator)
    {
        return resolveAxisHierarchies(evaluator.getQuery());
    }

    /**
     * Resolves which hierarchies are on query axes.
     */
    static Set<Hierarchy> resolveAxisHierarchies(Query query) {
        final Set<Hierarchy> result = new LinkedHashSet<Hierarchy>();
        if (query != null) {
            for (QueryAxis axis : query.getAxes()) {
                if (axis == null || axis.getSet() == null) {
                    continue;
                }
                final mondrian.olap.type.Type setType =
                    axis.getSet().getType();
                if (setType instanceof mondrian.olap.type.SetType) {
                    collectAxisHierarchies(
                        ((mondrian.olap.type.SetType) setType)
                            .getElementType(),
                        result);
                }
            }
        }
        return result;
    }

    /**
     * Checks if a hierarchy set contains a hierarchy by unique name.
     * Avoids object identity issues between different hierarchy
     * wrapper types (RolapCubeHierarchy vs compiled query hierarchy).
     */
    private static boolean containsHierarchy(
        Set<Hierarchy> set, Hierarchy target)
    {
        if (set.contains(target)) {
            return true;
        }
        final String targetName = target.getUniqueName();
        for (Hierarchy h : set) {
            if (h.getUniqueName().equals(targetName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extracts hierarchies from a set element type without calling
     * {@code getHierarchy()} on {@code TupleType}, which throws
     * {@link UnsupportedOperationException}. Crossjoin axes produce
     * tuple element types and should yield all member hierarchies.
     */
    static void collectAxisHierarchies(
        mondrian.olap.type.Type elementType,
        Set<Hierarchy> target)
    {
        if (elementType instanceof mondrian.olap.type.TupleType) {
            for (mondrian.olap.type.Type tupleElement
                : ((mondrian.olap.type.TupleType) elementType).elementTypes)
            {
                final Hierarchy hierarchy = tupleElement.getHierarchy();
                if (hierarchy != null) {
                    target.add(hierarchy);
                }
            }
            return;
        }
        final Hierarchy hierarchy = elementType.getHierarchy();
        if (hierarchy != null) {
            target.add(hierarchy);
        }
    }

    /**
     * Walks a StarPredicate tree (from subcube/subselect) and extracts
     * column = value predicates into wherePredicates with proper
     * dimension/hierarchy metadata.
     */
    private static PredicateInfo buildStarPredicate(
        StarPredicate pred,
        RolapStar star,
        RolapCube baseCube,
        String factAlias)
    {
        if (pred instanceof mondrian.rolap.agg.MemberColumnPredicate) {
            final mondrian.rolap.agg.MemberColumnPredicate mcp =
                (mondrian.rolap.agg.MemberColumnPredicate) pred;
            return buildAtomicPredicateInfo(
                mcp, mcp.getMember(), star, baseCube, factAlias);
        } else if (pred instanceof mondrian.rolap.agg.ValueColumnPredicate) {
            return buildAtomicPredicateInfo(
                (mondrian.rolap.agg.ValueColumnPredicate) pred,
                null, star, baseCube, factAlias);
        } else if (pred instanceof mondrian.rolap.agg.AndPredicate) {
            final List<PredicateInfo> children = new ArrayList<PredicateInfo>();
            for (StarPredicate child
                : ((mondrian.rolap.agg.AndPredicate) pred).getChildren())
            {
                children.add(buildStarPredicate(
                    child, star, baseCube, factAlias));
            }
            return new CompositePredicateInfo("AND", children);
        } else if (pred instanceof mondrian.rolap.agg.OrPredicate) {
            final List<PredicateInfo> children = new ArrayList<PredicateInfo>();
            for (StarPredicate child
                : ((mondrian.rolap.agg.OrPredicate) pred).getChildren())
            {
                children.add(buildStarPredicate(
                    child, star, baseCube, factAlias));
            }
            return new CompositePredicateInfo("OR", children);
        } else if (pred instanceof StarColumnPredicate) {
            throw new MondrianException(
                "NativeSqlCalc: unsupported subcube predicate type "
                    + pred.getClass().getSimpleName());
        }
        throw new MondrianException(
            "NativeSqlCalc: unsupported StarPredicate "
                + pred.getClass().getSimpleName());
    }

    private static PredicateInfo buildAtomicPredicateInfo(
        mondrian.rolap.agg.ValueColumnPredicate pred,
        RolapMember member,
        RolapStar star,
        RolapCube baseCube,
        String factAlias)
    {
        // NativeSqlCalc templates control their own FROM/JOIN scope.
        // Always resolve to factAlias.columnName — the template's agg
        // table has dimension columns denormalized.
        final RolapStar.Column starCol = pred.getConstrainedColumn();
        final String colName = starCol.getExpression() instanceof MondrianDef.Column
            ? ((MondrianDef.Column) starCol.getExpression()).name
            : starCol.getName();
        final ResolvedColumnSql resolved =
            new ResolvedColumnSql(factAlias + "." + colName);
        final PredicateMetadata metadata =
            mergePredicateMetadata(
                resolvePredicateMetadata(
                    member,
                    pred.getConstrainedColumn(),
                    baseCube),
                resolvePredicateMetadata(
                    null,
                    pred.getConstrainedColumn(),
                    baseCube));
        final Set<String> exclusionNames = new LinkedHashSet<String>();
        exclusionNames.addAll(metadata.exclusionNames);
        exclusionNames.addAll(
            collectSiblingHierarchyExclusionNames(
                member,
                pred.getConstrainedColumn(),
                baseCube));
        final Object value = pred.getValue();
        final String sql = value == RolapUtil.sqlNullValue
            ? resolved.qualifiedColumn + " IS NULL"
            : resolved.qualifiedColumn + " = " + formatLiteral(value);
        return new AtomicPredicateInfo(
            metadata.dimensionName,
            metadata.hierarchyName,
            sql,
            exclusionNames);
    }

    static PredicateMetadata resolvePredicateMetadata(
        RolapMember member,
        RolapStar.Column column,
        RolapCube baseCube)
    {
        if (member != null) {
            return new PredicateMetadata(
                member.getHierarchy().getDimension().getName(),
                member.getHierarchy().getName());
        }
        if (column == null || baseCube == null) {
            return PredicateMetadata.UNKNOWN;
        }
        final MondrianDef.Expression expression = column.getExpression();
        if (!(expression instanceof MondrianDef.Column)) {
            return PredicateMetadata.UNKNOWN;
        }
        final MondrianDef.Column targetColumn = (MondrianDef.Column) expression;
        final List<PredicateMetadataCandidate> exactMatches =
            new ArrayList<PredicateMetadataCandidate>();
        final List<PredicateMetadataCandidate> sameTableMatches =
            new ArrayList<PredicateMetadataCandidate>();
        for (RolapHierarchy hierarchy : baseCube.getHierarchies()) {
            for (Level level : hierarchy.getLevels()) {
                if (!(level instanceof RolapLevel)) {
                    continue;
                }
                final MondrianDef.Expression keyExp =
                    ((RolapLevel) level).getKeyExp();
                if (!(keyExp instanceof MondrianDef.Column)) {
                    continue;
                }
                final MondrianDef.Column levelColumn =
                    (MondrianDef.Column) keyExp;
                if (matchesColumn(
                    levelColumn,
                    targetColumn))
                {
                    exactMatches.add(new PredicateMetadataCandidate(
                        new PredicateMetadata(
                            hierarchy.getDimension().getName(),
                            hierarchy.getName()),
                        isPreferredExactMatch(hierarchy, level)));
                    continue;
                }
                if (matchesColumnByTableName(
                    levelColumn,
                    targetColumn,
                    hierarchy,
                    column))
                {
                    sameTableMatches.add(new PredicateMetadataCandidate(
                        new PredicateMetadata(
                            hierarchy.getDimension().getName(),
                            hierarchy.getName()),
                        isPreferredExactMatch(hierarchy, level)));
                    continue;
                }
            }
        }
        if (!exactMatches.isEmpty()) {
            return selectPredicateMetadata(exactMatches);
        }
        if (!sameTableMatches.isEmpty()) {
            return selectPredicateMetadata(sameTableMatches);
        }
        return PredicateMetadata.UNKNOWN;
    }

    static PredicateMetadata mergePredicateMetadata(
        PredicateMetadata primary,
        PredicateMetadata secondary)
    {
        if (primary == null || primary == PredicateMetadata.UNKNOWN) {
            return secondary == null ? PredicateMetadata.UNKNOWN : secondary;
        }
        if (secondary == null || secondary == PredicateMetadata.UNKNOWN) {
            return primary;
        }
        final Set<String> exclusionNames = new LinkedHashSet<String>();
        exclusionNames.addAll(primary.exclusionNames);
        exclusionNames.addAll(secondary.exclusionNames);
        return new PredicateMetadata(
            primary.dimensionName,
            primary.hierarchyName,
            exclusionNames);
    }

    static Set<String> collectSiblingHierarchyExclusionNames(
        RolapMember member,
        RolapStar.Column column,
        RolapCube baseCube)
    {
        final Set<String> names = new LinkedHashSet<String>();
        if (member == null
            || column == null
            || baseCube == null
            || member.getHierarchy() == null
            || member.getHierarchy().getDimension() == null)
        {
            return names;
        }
        final String dimensionName =
            member.getHierarchy().getDimension().getName();
        final String targetColumnName = resolveConstrainedColumnName(column);
        if (targetColumnName == null) {
            return names;
        }
        for (RolapHierarchy hierarchy : baseCube.getHierarchies()) {
            if (hierarchy.getDimension() == null
                || !dimensionName.equals(
                    hierarchy.getDimension().getName()))
            {
                continue;
            }
            for (Level level : hierarchy.getLevels()) {
                if (!(level instanceof RolapLevel)) {
                    continue;
                }
                final MondrianDef.Expression keyExp =
                    ((RolapLevel) level).getKeyExp();
                if (!(keyExp instanceof MondrianDef.Column)) {
                    continue;
                }
                if (targetColumnName.equals(
                    ((MondrianDef.Column) keyExp).name))
                {
                    names.addAll(defaultExclusionNames(
                        hierarchy.getDimension().getName(),
                        hierarchy.getName()));
                    break;
                }
            }
        }
        return names;
    }

    private static String resolveConstrainedColumnName(RolapStar.Column column) {
        if (column == null) {
            return null;
        }
        final MondrianDef.Expression expression = column.getExpression();
        if (expression instanceof MondrianDef.Column) {
            return ((MondrianDef.Column) expression).name;
        }
        return column.getName();
    }

    private static boolean isPreferredExactMatch(
        RolapHierarchy hierarchy,
        Level level)
    {
        final Level[] levels = hierarchy.getLevels();
        return levels != null
            && levels.length == 1
            && hierarchy.getName().equals(level.getName());
    }

    private static PredicateMetadata selectPredicateMetadata(
        List<PredicateMetadataCandidate> candidates)
    {
        final Set<String> exclusionNames = new LinkedHashSet<String>();
        PredicateMetadata primary = null;
        for (PredicateMetadataCandidate candidate : candidates) {
            exclusionNames.addAll(candidate.metadata.exclusionNames);
            if (primary == null || candidate.preferred) {
                primary = candidate.metadata;
                if (candidate.preferred) {
                    break;
                }
            }
        }
        if (primary == null) {
            return PredicateMetadata.UNKNOWN;
        }
        return new PredicateMetadata(
            primary.dimensionName,
            primary.hierarchyName,
            exclusionNames);
    }

    private static boolean matchesColumn(
        MondrianDef.Column left,
        MondrianDef.Column right)
    {
        return left.name.equals(right.name)
            && Objects.equals(left.getTableAlias(), right.getTableAlias());
    }

    private static boolean matchesColumnByTableName(
        MondrianDef.Column levelColumn,
        MondrianDef.Column targetColumn,
        RolapHierarchy hierarchy,
        RolapStar.Column targetStarColumn)
    {
        if (!levelColumn.name.equals(targetColumn.name)) {
            return false;
        }
        if (!(hierarchy.getRelation() instanceof MondrianDef.Table)) {
            return false;
        }
        if (targetStarColumn == null || targetStarColumn.getTable() == null) {
            return false;
        }
        return ((MondrianDef.Table) hierarchy.getRelation()).name.equals(
            targetStarColumn.getTable().getTableName());
    }

    /**
     * Resolves a member's key column to a SQL expression for NativeSqlCalc.
     *
     * <p>Always returns {@code factAlias.columnName}. NativeSqlCalc templates
     * are hand-written SQL for a specific database — they control their own
     * FROM/JOIN scope. The template's fact alias ({@code f}) points to a
     * denormalized agg table that has dimension columns inline. Star schema
     * dim table resolution is not used here.
     *
     * <p>If the template's agg table doesn't have the column, the SQL fails
     * at execution and the fallback chain tries the next template.
     */
    private static ResolvedColumnSql resolveMemberColumnSql(
        MondrianDef.Column keyColumn, String factAlias)
    {
        return new ResolvedColumnSql(factAlias + "." + keyColumn.name);
    }

    static ResolvedColumnSql resolveLevelColumnSql(
        MondrianDef.Column keyColumn,
        RolapStar star,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        if (keyColumn == null || keyColumn.getTableAlias() == null) {
            return null;
        }
        final RolapStar.Column starColumn =
            star.lookupColumn(keyColumn.getTableAlias(), keyColumn.name);
        if (starColumn == null) {
            return null;
        }
        return resolvePredicateColumnSql(
            starColumn, star, factAlias, joinClauses, seenJoins);
    }

    /**
     * Resolves a synthetic {@link AxisBinding} for an axis hierarchy whose
     * evaluator {@code CurrentMember} is the All-level member.
     *
     * <p>Returns {@code factAlias.columnName} for the level's keyExp.
     * NativeSqlCalc templates are hand-written SQL controlling their own
     * FROM/JOIN scope, with the fact alias pointing at a denormalized agg
     * table that has dimension columns inline. The agg table name is baked
     * into the template SQL.
     *
     * <p><b>Task 44 — pre-validation against candidate aggs:</b> When
     * {@code candidateAggs} is non-null and non-empty, this method confirms
     * that at least one of the supplied {@link AggStar}s carries the synthetic
     * column inline before binding it. If <em>none</em> of the candidate aggs
     * have the column, returns {@code null} — the caller treats this as a
     * non-resolution and the entire native path is short-circuited (no SQL
     * fired against any agg, no noisy CK "Code: 47 column not found" errors).
     *
     * <p>When {@code candidateAggs} is null or empty, the legacy contract
     * applies: always return the binding and let the SQL execution + template
     * fallback chain catch the mismatch the slow way (one CK round-trip per
     * mismatched template).
     *
     * <p>The {@code joinClauses}/{@code seenJoins} parameters are kept for
     * API compatibility (and may be repurposed by future template macros)
     * but no JOINs are registered here.
     *
     * @return the resolved {@link AxisBinding}, or {@code null} if
     *         {@code candidateAggs} is non-empty and none carry the column
     * @throws MondrianException if the hierarchy lacks a non-All level,
     *         the first non-All level is not a {@link RolapLevel}, or
     *         the level's keyExp is not a column.
     */
    @SuppressWarnings("unused")
    static AxisBinding resolveSyntheticBinding(
        Hierarchy h,
        RolapStar star,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins,
        int kIndex,
        Set<AggStar> candidateAggs)
    {
        Level[] levels = h.getLevels();
        if (levels.length < 2) {
            throw new MondrianException(
                "NativeSqlCalc: rollupAxes hierarchy " + h.getUniqueName()
                + " has no non-All level");
        }
        if (!(levels[1] instanceof RolapLevel)) {
            throw new MondrianException(
                "NativeSqlCalc: rollupAxes hierarchy " + h.getUniqueName()
                + " first non-All level is not a RolapLevel");
        }
        RolapLevel dataLevel = (RolapLevel) levels[1];

        MondrianDef.Expression keyExp = dataLevel.getKeyExp();
        if (!(keyExp instanceof MondrianDef.Column)) {
            throw new MondrianException(
                "NativeSqlCalc: rollupAxes hierarchy " + h.getUniqueName()
                + " — level " + dataLevel.getUniqueName()
                + " key expression is not a column");
        }

        String columnName = ((MondrianDef.Column) keyExp).name;

        // Task 44: pre-validate column existence on the candidate aggs.
        // When the caller has identified a concrete set of aggs (extracted
        // from FROM clauses of the template fallback chain), confirm at
        // least one carries this column inline. Otherwise the synthetic
        // binding would emit f.<col> against an agg that lacks the column,
        // wasting a ClickHouse round-trip per template before fallback.
        if (candidateAggs != null && !candidateAggs.isEmpty()
            && !anyAggHasColumn(candidateAggs, columnName))
        {
            LOGGER.info(
                "NativeSqlCalc: synthetic axis '{}' column '{}' not present"
                + " on any candidate agg {} — returning null binding",
                h.getUniqueName(), columnName,
                candidateAggTableNames(candidateAggs));
            return null;
        }

        String qualifiedColumn = factAlias + "." + columnName;

        return new AxisBinding(
            h,
            h.getUniqueName(),
            qualifiedColumn,
            columnName,
            "k" + kIndex);
    }

    /**
     * Pattern matching the table name following a {@code FROM} keyword in
     * a NativeSqlCalc template. Captures bare identifiers and back-quoted
     * identifiers; ignores subqueries ({@code FROM (SELECT ...)}).
     */
    private static final Pattern FROM_TABLE_PATTERN =
        Pattern.compile(
            "(?i)\\bFROM\\s+(`?[A-Za-z_][A-Za-z0-9_]*`?)");

    /**
     * Extracts the set of physical table names referenced in {@code FROM}
     * clauses across all templates in the fallback chain. Skips placeholders
     * ({@code ${factTable}}), subqueries, and CTE-aliased sources.
     *
     * <p>The returned set is the universe of physical aggregate-table
     * candidates that synthetic bindings must validate against.
     */
    static Set<String> extractAggTableNamesFromTemplates(
        List<String> templates)
    {
        Set<String> names = new LinkedHashSet<String>();
        if (templates == null) {
            return names;
        }
        for (String tmpl : templates) {
            if (tmpl == null) {
                continue;
            }
            Matcher m = FROM_TABLE_PATTERN.matcher(tmpl);
            while (m.find()) {
                String name = m.group(1);
                // Strip optional back-quotes
                if (name.startsWith("`") && name.endsWith("`")) {
                    name = name.substring(1, name.length() - 1);
                }
                names.add(name);
            }
        }
        return names;
    }

    /**
     * Resolves the supplied physical table names to {@link AggStar}s on the
     * given {@link RolapStar}. Names that do not correspond to a known agg
     * (e.g. CTE aliases like {@code presence}, the fact table itself, or
     * intermediate denormalized tables outside the agg-matcher's view) are
     * silently dropped — they cannot be pre-validated and must rely on the
     * downstream SQL execution to fail cleanly.
     */
    static Set<AggStar> resolveCandidateAggs(
        Set<String> tableNames, RolapStar star)
    {
        Set<AggStar> aggs = new LinkedHashSet<AggStar>();
        if (tableNames == null || tableNames.isEmpty() || star == null) {
            return aggs;
        }
        for (AggStar agg : star.getAggStars()) {
            String aggName = agg.getFactTable().getName();
            if (tableNames.contains(aggName)) {
                aggs.add(agg);
            }
        }
        return aggs;
    }

    /**
     * Returns true iff at least one of the supplied {@link AggStar}s has a
     * column matching {@code columnName} (case-sensitive). Searches across
     * fact-table columns and child dim-table columns of each agg.
     */
    private static boolean anyAggHasColumn(
        Set<AggStar> aggs, String columnName)
    {
        if (columnName == null || aggs == null) {
            return false;
        }
        for (AggStar agg : aggs) {
            for (AggStar.Table.Column col : agg.getFactTable().getColumns()) {
                if (columnName.equals(col.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Returns the agg table names (for diagnostic logging only). */
    private static List<String> candidateAggTableNames(Set<AggStar> aggs) {
        List<String> names = new ArrayList<String>(aggs.size());
        for (AggStar agg : aggs) {
            names.add(agg.getFactTable().getName());
        }
        return names;
    }

    @SuppressWarnings("ReferenceEquality")
    static ResolvedColumnSql resolvePredicateColumnSql(
        RolapStar.Column col,
        RolapStar star,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        final String colName = col.getExpression() instanceof MondrianDef.Column
            ? ((MondrianDef.Column) col.getExpression()).name
            : col.getName();
        final RolapStar.Table table = col.getTable();
        // Quote identifiers for non-Latin column/table names (Issue #53)
        final mondrian.rolap.sql.SqlQuery sqlQuery = star.getSqlQuery();
        final Dialect dialect = sqlQuery != null ? sqlQuery.getDialect() : null;
        final String qColName = dialect != null
            ? dialect.quoteIdentifier(colName) : colName;
        if (table == star.getFactTable()) {
            return new ResolvedColumnSql(factAlias + "." + qColName);
        }
        final String tableAlias = table.getAlias();
        final String qualifiedCol = tableAlias + "." + qColName;
        final RolapStar.Condition joinCond = table.getJoinCondition();
        if (joinCond != null) {
            final String leftCol =
                ((MondrianDef.Column) joinCond.getLeft()).name;
            final String rightCol =
                ((MondrianDef.Column) joinCond.getRight()).name;
            final String qTableName = dialect != null
                ? dialect.quoteIdentifier(table.getTableName())
                : table.getTableName();
            final String qLeftCol = dialect != null
                ? dialect.quoteIdentifier(leftCol) : leftCol;
            final String qRightCol = dialect != null
                ? dialect.quoteIdentifier(rightCol) : rightCol;
            final String join = "JOIN "
                + qTableName
                + " " + tableAlias
                + " ON " + factAlias + "."
                + qLeftCol
                + " = " + tableAlias + "."
                + qRightCol;
            if (seenJoins.add(join)) {
                joinClauses.add(join);
            }
        }
        return new ResolvedColumnSql(qualifiedCol);
    }

    /** Predicate expression with hierarchy metadata-aware rendering. */
    static abstract class PredicateInfo {
        abstract String render(Set<String> exceptNames);
    }

    /** Atomic predicate with dimension/hierarchy metadata. */
    static final class AtomicPredicateInfo extends PredicateInfo {
        final String dimensionName;
        final String hierarchyName;
        final String sql;
        final Set<String> exclusionNames;

        AtomicPredicateInfo(
            String dimensionName,
            String hierarchyName,
            String sql)
        {
            this(
                dimensionName,
                hierarchyName,
                sql,
                defaultExclusionNames(dimensionName, hierarchyName));
        }

        AtomicPredicateInfo(
            String dimensionName,
            String hierarchyName,
            String sql,
            Set<String> exclusionNames)
        {
            this.dimensionName = dimensionName;
            this.hierarchyName = hierarchyName;
            this.sql = sql;
            this.exclusionNames =
                exclusionNames == null
                    ? Collections.<String>emptySet()
                    : new LinkedHashSet<String>(exclusionNames);
        }

        @Override
        String render(Set<String> exceptNames) {
            return exceptNames != null
                && shouldExclude(exclusionNames, exceptNames)
                ? null
                : sql;
        }
    }

    /** Composite predicate preserving AND/OR tree shape. */
    static final class CompositePredicateInfo extends PredicateInfo {
        final String op;
        final List<PredicateInfo> children;

        CompositePredicateInfo(String op, List<PredicateInfo> children) {
            this.op = op;
            this.children = children;
        }

        @Override
        String render(Set<String> exceptNames) {
            final List<String> renderedChildren = new ArrayList<String>();
            for (PredicateInfo child : children) {
                final String rendered = child.render(exceptNames);
                if (rendered != null && !rendered.isEmpty()) {
                    renderedChildren.add(rendered);
                }
            }
            if (renderedChildren.isEmpty()) {
                return null;
            }
            if (renderedChildren.size() == 1) {
                return renderedChildren.get(0);
            }
            final StringBuilder buf = new StringBuilder("(");
            for (int i = 0; i < renderedChildren.size(); i++) {
                if (i > 0) {
                    buf.append(" ").append(op).append(" ");
                }
                buf.append(renderedChildren.get(i));
            }
            buf.append(")");
            return buf.toString();
        }
    }

    static final class ResolvedColumnSql {
        final String qualifiedColumn;

        ResolvedColumnSql(String qualifiedColumn) {
            this.qualifiedColumn = qualifiedColumn;
        }
    }

    static final class PredicateMetadata {
        static final PredicateMetadata UNKNOWN =
            new PredicateMetadata(
                "unknown",
                "unknown",
                Collections.<String>emptySet());

        final String dimensionName;
        final String hierarchyName;
        final Set<String> exclusionNames;

        PredicateMetadata(String dimensionName, String hierarchyName) {
            this(
                dimensionName,
                hierarchyName,
                defaultExclusionNames(dimensionName, hierarchyName));
        }

        PredicateMetadata(
            String dimensionName,
            String hierarchyName,
            Set<String> exclusionNames)
        {
            this.dimensionName = dimensionName;
            this.hierarchyName = hierarchyName;
            this.exclusionNames =
                exclusionNames == null
                    ? Collections.<String>emptySet()
                    : new LinkedHashSet<String>(exclusionNames);
        }
    }

    static final class PredicateMetadataCandidate {
        final PredicateMetadata metadata;
        final boolean preferred;

        PredicateMetadataCandidate(
            PredicateMetadata metadata,
            boolean preferred)
        {
            this.metadata = metadata;
            this.preferred = preferred;
        }
    }

    /**
     * Parses comma-separated hierarchy names into a normalized set.
     * Shared by whereClauseExcept and denominator macros.
     */
    static Set<String> parseExceptNames(String csv) {
        Set<String> names = new LinkedHashSet<String>();
        if (csv == null || csv.isEmpty()) {
            return names;
        }
        for (String s : csv.split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) {
                names.add(t);
            }
        }
        return names;
    }

    /**
     * Substitutes placeholders. Handles both simple {@code ${name}}
     * and scoped {@code ${whereClauseExcept:Dim1,Dim2}} placeholders.
     *
     * <p>Delegates to the 4-arg overload with an empty axis bindings list.
     *
     * @param template SQL template
     * @param placeholders simple name→value map
     * @param predicates predicate list for whereClauseExcept resolution
     *                   (may be null if no Except placeholders used)
     */
    static String substitutePlaceholders(
        String template,
        Map<String, String> placeholders,
        List<PredicateInfo> predicates)
    {
        return substitutePlaceholders(
            template, placeholders, predicates, Collections.<AxisBinding>emptyList());
    }

    /**
     * Substitutes placeholders in a SQL template. Handles:
     * <ul>
     *   <li>Simple {@code ${name}} lookups from the placeholders map
     *   <li>{@code ${whereClauseExcept:Dim1,Dim2}} — predicate filtering
     *   <li>{@code ${denominatorSelect:except1,except2}} — denominator SELECT via qualifiedColumn
     *   <li>{@code ${denominatorGroupBy:except1,except2}} — bare-alias GROUP BY (ClickHouse)
     *   <li>{@code ${denominatorGroupBy:srcAlias:except1,except2}} — prefixed GROUP BY
     *   <li>{@code ${denominatorJoin:leftAlias:rightAlias:except1,except2}} — denominator JOIN
     * </ul>
     *
     * @param template SQL template
     * @param placeholders simple name→value map
     * @param predicates predicate list for whereClauseExcept resolution
     *                   (may be null if no Except placeholders used)
     * @param axisBindings axis bindings for denominator macro resolution
     *                     (may be empty if no denominator macros used)
     */
    static String substitutePlaceholders(
        String template,
        Map<String, String> placeholders,
        List<PredicateInfo> predicates,
        List<AxisBinding> axisBindings)
    {
        final Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
        final StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String token = matcher.group(1);
            String value;
            if (token.startsWith("whereClauseExcept:")) {
                // Dynamic: filter predicates by dimension/hierarchy
                final String args =
                    token.substring("whereClauseExcept:".length());
                final Set<String> exceptNames = parseExceptNames(args);
                if (predicates == null) {
                    value = "1 = 1";
                } else {
                    value = buildWhereFromPredicates(
                        predicates, exceptNames);
                }
            } else if (token.startsWith("denominatorSelect:")) {
                value = dispatchDenominatorMacro(
                    "denominatorSelect", token, axisBindings);
            } else if (token.startsWith("denominatorGroupBy:")) {
                value = dispatchDenominatorMacro(
                    "denominatorGroupBy", token, axisBindings);
            } else if (token.startsWith("denominatorJoin:")) {
                value = dispatchDenominatorMacro(
                    "denominatorJoin", token, axisBindings);
            } else {
                value = placeholders.get(token);
                if (value == null) {
                    throw new MondrianException(
                        "NativeSqlCalc: unresolved placeholder ${"
                            + token + "} in template");
                }
            }
            matcher.appendReplacement(
                sb, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /** Backward-compatible overload for tests without predicates. */
    static String substitutePlaceholders(
        String template,
        Map<String, String> placeholders)
    {
        return substitutePlaceholders(template, placeholders, null);
    }

    /**
     * Dispatches a denominator macro. Parses colon-separated args,
     * builds {@link DenominatorProjection}, and calls the appropriate
     * render method.
     *
     * <p>Formats:
     * <ul>
     *   <li>{@code denominatorSelect:except1,except2} — uses qualifiedColumn
     *   <li>{@code denominatorGroupBy:except1,except2} — bare aliases (ClickHouse GROUP BY)
     *   <li>{@code denominatorGroupBy:srcAlias:except1,except2} — prefixed aliases
     *   <li>{@code denominatorJoin:leftAlias:rightAlias:except1,except2} — unchanged
     * </ul>
     */
    private static String dispatchDenominatorMacro(
        String macroName, String fullToken, List<AxisBinding> axisBindings)
    {
        String argsStr = fullToken.substring(macroName.length() + 1);
        String[] parts = argsStr.split(":", -1);

        switch (macroName) {
        case "denominatorSelect": {
            Set<String> except = parseExceptNames(parts[0]);
            DenominatorProjection dp =
                DenominatorProjection.build(axisBindings, except);
            return renderDenominatorSelect(dp);
        }
        case "denominatorGroupBy": {
            if (parts.length >= 2) {
                String srcAlias = parts[0].trim();
                Set<String> except = parseExceptNames(parts[1]);
                DenominatorProjection dp =
                    DenominatorProjection.build(axisBindings, except);
                return renderDenominatorGroupBy(dp, srcAlias);
            } else {
                Set<String> except = parseExceptNames(parts[0]);
                DenominatorProjection dp =
                    DenominatorProjection.build(axisBindings, except);
                return renderDenominatorGroupBy(dp, null);
            }
        }
        case "denominatorJoin": {
            if (parts.length < 3) {
                throw new MondrianException(
                    "denominatorJoin requires leftAlias:rightAlias:exceptList"
                        + ", got: " + fullToken);
            }
            String leftAlias = parts[0].trim();
            String rightAlias = parts[1].trim();
            Set<String> except = parseExceptNames(parts[2]);
            DenominatorProjection dp =
                DenominatorProjection.build(axisBindings, except);
            return renderDenominatorJoin(dp, leftAlias, rightAlias);
        }
        default:
            throw new MondrianException(
                "Unknown denominator macro: " + macroName);
        }
    }

    /**
     * Tries each template in order, returning the first that resolves
     * all placeholders successfully. Returns null if all templates fail.
     */
    static String resolveFirstViableTemplate(
        List<String> templates,
        Map<String, String> placeholders,
        List<PredicateInfo> predicates)
    {
        for (int i = 0; i < templates.size(); i++) {
            try {
                return substitutePlaceholders(
                    templates.get(i), placeholders, predicates);
            } catch (Exception e) {
                LOGGER.info(
                    "NativeSqlCalc: template[{}] failed ({}), "
                    + "trying next template",
                    i, e.getMessage());
            }
        }
        return null;
    }

    /**
     * Parses the result set with output contract: the last column is
     * the value ({@code val}), preceding columns are axis keys
     * ({@code k1..kN}). Builds a map keyed by
     * {@code "hierName1=val1|hierName2=val2|..."}.
     */
    private Map<String, Object> parseResultSet(
        java.sql.ResultSet rs,
        List<AxisBinding> axisBindings)
        throws java.sql.SQLException
    {
        final Map<String, Object> results =
            new LinkedHashMap<String, Object>();
        final java.sql.ResultSetMetaData meta = rs.getMetaData();
        final int colCount = meta.getColumnCount();
        // Output contract: last column is val, preceding columns are axis keys.
        // Prefer the resolved axis binding count over raw column count so old
        // fixed-width templates with trailing NULL keys do not leak into row keys.
        final int keyColCount = axisBindings == null
            ? colCount - 1
            : Math.min(axisBindings.size(), colCount - 1);

        while (rs.next()) {
            final List<String> parts = new ArrayList<String>(keyColCount);
            for (int i = 1; i <= keyColCount; i++) {
                parts.add(String.valueOf(rs.getObject(i)));
            }
            final String rowKey = encodeRowKey(parts);
            final double value = rs.getDouble(colCount);
            results.put(
                rowKey,
                rs.wasNull() ? null : value);
        }

        return results;
    }

    /**
     * Parses a ResultSet whose schema is:
     * <pre>
     *   k0, k1, ..., kN, k0_isAll, k1_isAll, ..., kN_isAll, val
     * </pre>
     * The {@code kN_isAll} columns are produced by GROUPING(kN) per
     * {@code ${axisCubeSelectFlags}}. When the flag is 1, the
     * corresponding rowKey component is {@link #ALL_MEMBER_MARKER};
     * otherwise the value is normalized + escaped via
     * {@link #normalizeAxisKey} and {@link #escapeAxisKeyPart}, with raw
     * NULL mapped to {@link #NULL_KEY_MARKER}.
     *
     * <p>Real-data NULL (flag=0) and GROUPING subtotal (flag=1) produce
     * DIFFERENT rowKey components, by design.
     */
    static Map<String, Object> parseResultSetWithGroupingFlags(
        java.sql.ResultSet rs,
        List<AxisBinding> axisBindings)
        throws java.sql.SQLException
    {
        final int n = axisBindings.size();
        final int valueCol = 1 + 2 * n;          // 1-based JDBC index
        final Map<String, Object> result =
            new LinkedHashMap<String, Object>();

        while (rs.next()) {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < n; i++) {
                final int kCol = 1 + i;            // k0, k1, ...
                final int flagCol = 1 + n + i;     // k0_isAll, k1_isAll, ...
                final int isAll = rs.getInt(flagCol);
                final String part;
                if (isAll == 1) {
                    part = ALL_MEMBER_MARKER;
                } else {
                    final Object v = rs.getObject(kCol);
                    part = escapeAxisKeyPart(normalizeAxisKey(v, null));
                }
                if (i > 0) {
                    sb.append('|');
                }
                sb.append(part);
            }
            final Object val = rs.getObject(valueCol);
            result.put(sb.toString(), val);
        }
        return result;
    }

    /**
     * Builds row key from AXIS members only, using the same encoding
     * as {@link #parseResultSet}. Both sides use {@link #encodeRowKey}
     * with {@code String.valueOf()} to guarantee matching keys.
     */
    private String buildRowKey(
        Evaluator evaluator,
        List<AxisBinding> axisBindings)
    {
        final List<String> parts = collectAxisKeyParts(
            evaluator.getMembers(),
            axisBindings);
        return encodeRowKey(parts);
    }

    /**
     * Rollup-aware static rowKey encoder. Used ONLY by the rollupAxes
     * flow — symmetric with parseResultSetWithGroupingFlags. NOT a
     * drop-in replacement for the instance buildRowKey method, which
     * preserves legacy non-normalized String.valueOf semantics for
     * compatibility with parseResultSet's existing key shape.
     */
    static String encodeRowKey(
        Evaluator evaluator,
        List<AxisBinding> axisBindings)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < axisBindings.size(); i++) {
            final AxisBinding b = axisBindings.get(i);
            final Member m = evaluator.getContext(b.hierarchy);
            final String part;
            if (m == null || m.isAll()) {
                part = ALL_MEMBER_MARKER;
            } else {
                final Object key = (m instanceof RolapMember)
                    ? ((RolapMember) m).getKey()
                    : m.getName();
                part = escapeAxisKeyPart(normalizeAxisKey(key, null));
            }
            if (i > 0) {
                sb.append('|');
            }
            sb.append(part);
        }
        return sb.toString();
    }

    static List<String> collectAxisKeyParts(
        Member[] members,
        List<AxisBinding> axisBindings)
    {
        final Map<Hierarchy, Member> memberByHierarchy =
            new LinkedHashMap<Hierarchy, Member>();
        for (Member m : members) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }
            memberByHierarchy.put(m.getHierarchy(), m);
        }

        final List<String> parts = new ArrayList<String>();
        if (axisBindings == null || axisBindings.isEmpty()) {
            for (Member m : memberByHierarchy.values()) {
                parts.add(String.valueOf(((RolapMember) m).getKey()));
            }
        } else {
            for (AxisBinding binding : axisBindings) {
                final Member member = memberByHierarchy.get(binding.hierarchy);
                if (member != null) {
                    parts.add(String.valueOf(((RolapMember) member).getKey()));
                }
            }
        }
        return parts;
    }

    /**
     * Single shared encoding for row keys. Both parseResultSet and
     * buildRowKey use this, guaranteeing key match.
     */
    static String encodeRowKey(List<?> parts) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append('|');
            }
            sb.append(String.valueOf(parts.get(i)));
        }
        return sb.toString();
    }

    static String renderAxisPresenceSelectList(List<AxisBinding> axisBindings) {
        final StringBuilder sb = new StringBuilder();
        for (AxisBinding binding : axisBindings) {
            sb.append(",\n    ")
                .append(binding.qualifiedColumn)
                .append(" AS ")
                .append(binding.keyAlias);
        }
        return sb.toString();
    }

    static String renderAxisResultSelectList(
        List<AxisBinding> axisBindings,
        String relationAlias)
    {
        final StringBuilder sb = new StringBuilder();
        for (AxisBinding binding : axisBindings) {
            sb.append("  ")
                .append(relationAlias)
                .append(".")
                .append(binding.keyAlias)
                .append(" AS ")
                .append(binding.keyAlias)
                .append(",\n");
        }
        return sb.toString();
    }

    static String renderAxisGroupByList(
        List<AxisBinding> axisBindings,
        String relationAlias)
    {
        final StringBuilder sb = new StringBuilder();
        for (AxisBinding binding : axisBindings) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(relationAlias)
                .append(".")
                .append(binding.keyAlias);
        }
        if (sb.length() > 0) {
            sb.append(", ");
        }
        return sb.toString();
    }

    /**
     * Emits SELECT-list grouping flag projections for {@code rollupAxes}
     * templates. Format (trailing comma + newline, mirroring
     * {@link #renderAxisResultSelectList} so it can sit between
     * {@code axisResultSelectList} and the next SELECT-list expression
     * without producing double-comma or missing-comma adjacency):
     * <pre>
     *   "  GROUPING(pr.k0) AS k0_isAll,
     *     GROUPING(pr.k1) AS k1_isAll,
     *   "
     * </pre>
     * Empty bindings &rarr; empty string.
     *
     * <p>The keyAlias on each binding is the single source of truth — the
     * alias used here in {@code GROUPING(pr.kN)} matches the one used in
     * {@link #renderAxisGroupByListCube} so the flag column corresponds to
     * the same expression that participates in {@code CUBE(...)}.
     */
    static String renderAxisCubeSelectFlags(
        List<AxisBinding> bindings, String alias)
    {
        if (bindings.isEmpty()) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();
        for (AxisBinding b : bindings) {
            sb.append("  GROUPING(")
                .append(alias).append('.').append(b.keyAlias)
                .append(") AS ").append(b.keyAlias).append("_isAll,\n");
        }
        return sb.toString();
    }

    /**
     * Emits the GROUP BY clause body for {@code rollupAxes} templates.
     *
     * <p>Form C — bare {@code CUBE(...)} without trailing space and without
     * a {@code tuple()} anchor (validated against ClickHouse 24.1, Task 0.5).
     * {@code CUBE} inherently produces the empty grouping set.
     *
     * <p>Schema authors must NOT add other expressions in GROUP BY after
     * this macro — extra group keys belong in SELECT as {@code any(...)}
     * projections, not in the grouping list.
     *
     * <p>Empty bindings &rarr; empty string.
     */
    static String renderAxisGroupByListCube(
        List<AxisBinding> bindings, String alias)
    {
        if (bindings.isEmpty()) {
            return "";
        }
        final StringBuilder sb = new StringBuilder("CUBE(");
        for (int i = 0; i < bindings.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(alias).append('.').append(bindings.get(i).keyAlias);
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Renders axis key aliases without any table prefix — for use in
     * flat queries where columns are not qualified by a relation alias.
     */
    static String renderAxisSelectListNoPrefix(List<AxisBinding> axisBindings) {
        final StringBuilder sb = new StringBuilder();
        for (AxisBinding binding : axisBindings) {
            sb.append("  ")
                .append(binding.keyAlias)
                .append(",\n");
        }
        return sb.toString();
    }

    /**
     * Canonical string encoding for axis-key values. Used symmetrically by
     * {@code buildRowKey} and {@code parseResultSetWithGroupingFlags} so that
     * the same logical value produces the same rowKey component in both encoders.
     *
     * <ul>
     *   <li>{@code null} &rarr; {@link #NULL_KEY_MARKER}.</li>
     *   <li>{@code Date}/{@code LocalDate} &rarr; ISO yyyy-MM-dd via toString().</li>
     *   <li>{@code BigDecimal} &rarr; integer form if scale &le; 0, otherwise
     *       {@code stripTrailingZeros().toPlainString()} so {@code 1}, {@code 1.0},
     *       {@code 1.00} all collapse to {@code "1"}.</li>
     *   <li>Other &rarr; {@code toString()}.</li>
     * </ul>
     *
     * <p>{@code declaredType} is reserved for future use; pass {@code null} for now.
     */
    static String normalizeAxisKey(Object key, Class<?> declaredType) {
        if (key == null) {
            return NULL_KEY_MARKER;
        }
        if (key instanceof java.sql.Date) {
            return key.toString();
        }
        if (key instanceof java.time.LocalDate) {
            return key.toString();
        }
        if (key instanceof java.math.BigDecimal) {
            java.math.BigDecimal d = (java.math.BigDecimal) key;
            return d.scale() <= 0
                ? d.toBigInteger().toString()
                : d.stripTrailingZeros().toPlainString();
        }
        return key.toString();
    }

    /**
     * Escapes the rowKey separator and backslash so that
     * {@code String.join("|", parts)} produces unambiguous rowKeys.
     * {@code "\\"} &rarr; {@code "\\\\"}; {@code "|"} &rarr; {@code "\\|"}.
     */
    static String escapeAxisKeyPart(String part) {
        if (part.indexOf('\\') < 0 && part.indexOf('|') < 0) {
            return part;
        }
        StringBuilder sb = new StringBuilder(part.length() + 4);
        for (int i = 0; i < part.length(); i++) {
            char c = part.charAt(i);
            if (c == '\\' || c == '|') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Formats a literal value for SQL: numbers as-is, strings with
     * single-quote escaping, null as NULL.
     */
    static String formatLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        // String — escape single quotes
        final String s = String.valueOf(value);
        return "'" + s.replace("'", "''") + "'";
    }

    /**
     * Quotes a SQL identifier with backticks, escaping any embedded
     * backticks. ClickHouse, MySQL, and most SQL dialects accept this.
     * Used for schema-defined table/column names in generated JOINs.
     */
    static String quoteId(String id) {
        if (id == null) {
            return "NULL";
        }
        return "`" + id.replace("`", "``") + "`";
    }

    /**
     * Immutable projection of axis bindings for denominator partitioning.
     * Filters axis bindings by excluding subject-related hierarchies.
     * Kept bindings preserve original axis order.
     *
     * <p>Contract: hierarchyName matching uses the canonical unique name
     * set at AxisBinding construction time (hierarchy.getUniqueName()).
     * This is not display-name matching.
     */
    static final class DenominatorProjection {
        private final List<AxisBinding> keptBindings;
        private final boolean scalar;

        private DenominatorProjection(List<AxisBinding> kept) {
            this.keptBindings = Collections.unmodifiableList(kept);
            this.scalar = kept.isEmpty();
        }

        /**
         * Builds projection by excluding bindings whose hierarchy matches
         * the except-list. Matching checks both short name ("Бренд") and
         * dimension-qualified name ("Продукт.Бренд") to be consistent
         * with whereClauseExcept template syntax.
         */
        static DenominatorProjection build(
            List<AxisBinding> bindings, Set<String> exceptHierarchyNames)
        {
            List<AxisBinding> kept = new ArrayList<AxisBinding>();
            for (AxisBinding b : bindings) {
                if (isExcluded(b, exceptHierarchyNames)) {
                    continue;
                }
                kept.add(b);
            }
            return new DenominatorProjection(kept);
        }

        private static boolean isExcluded(
            AxisBinding b, Set<String> exceptNames)
        {
            // Match by short hierarchy name
            if (exceptNames.contains(b.hierarchyName)) {
                return true;
            }
            // Match by dimension.hierarchy qualified name
            if (b.hierarchy != null
                && b.hierarchy.getDimension() != null)
            {
                String dimName = b.hierarchy.getDimension().getName();
                String qualified = dimName + "." + b.hierarchyName;
                if (exceptNames.contains(qualified)) {
                    return true;
                }
            }
            return false;
        }

        boolean isScalar() { return scalar; }
        List<AxisBinding> getKeptBindings() { return keptBindings; }
    }

    /**
     * Renders denominator SELECT: {@code qualifiedColumn AS keyAlias}
     * for each kept binding.
     *
     * <p>Uses {@link AxisBinding#qualifiedColumn} — always
     * {@code f.columnName} since the resolver returns fact-alias-qualified
     * column references for all dimensions.
     *
     * @return empty string when the projection is scalar
     */
    static String renderDenominatorSelect(DenominatorProjection dp) {
        if (dp.isScalar()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (AxisBinding b : dp.getKeptBindings()) {
            sb.append("  ").append(b.qualifiedColumn)
                .append(" AS ").append(b.keyAlias).append(",\n");
        }
        return sb.toString();
    }

    /**
     * Renders denominator GROUP BY fragment.
     *
     * <p>Two modes based on {@code srcAlias}:
     * <ul>
     *   <li><b>Bare-alias</b> ({@code srcAlias} is null or empty):
     *       {@code k0, k1, } — for ClickHouse alias-based GROUP BY
     *       in inner denominator subquery (contract C3).
     *   <li><b>Prefixed</b> ({@code srcAlias} is non-empty):
     *       {@code srcAlias.k0, srcAlias.k1, } — for outer
     *       denominator CTE SELECT and GROUP BY.
     * </ul>
     *
     * <p>Uses {@link AxisBinding#keyAlias} only — never raw column names.
     *
     * @return empty string when the projection is scalar
     */
    static String renderDenominatorGroupBy(
        DenominatorProjection dp, String srcAlias)
    {
        if (dp.isScalar()) {
            return "";
        }
        final boolean bare = (srcAlias == null || srcAlias.isEmpty());
        StringBuilder sb = new StringBuilder();
        for (AxisBinding b : dp.getKeptBindings()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            if (!bare) {
                sb.append(srcAlias).append(".");
            }
            sb.append(b.keyAlias);
        }
        sb.append(", ");
        return sb.toString();
    }

    /**
     * Renders a denominator JOIN clause.
     *
     * <p>Non-scalar: {@code JOIN rightAlias ON leftAlias.k0 = rightAlias.k0
     * AND ...}. Scalar: {@code CROSS JOIN rightAlias}.
     *
     * <p>Uses {@link AxisBinding#keyAlias} only — never raw column names.
     */
    static String renderDenominatorJoin(
        DenominatorProjection dp, String leftAlias, String rightAlias)
    {
        if (dp.isScalar()) {
            return "CROSS JOIN " + rightAlias;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("JOIN ").append(rightAlias).append(" ON ");
        boolean first = true;
        for (AxisBinding b : dp.getKeptBindings()) {
            if (!first) {
                sb.append(" AND ");
            }
            sb.append(leftAlias).append(".").append(b.keyAlias)
                .append(" = ")
                .append(rightAlias).append(".").append(b.keyAlias);
            first = false;
        }
        return sb.toString();
    }

    /**
     * Holds the qualified column expression for an axis dimension.
     */
    static final class AxisBinding {
        final Hierarchy hierarchy;
        final String hierarchyName;
        final String qualifiedColumn;
        final String columnName;
        final String keyAlias;

        AxisBinding(
            Hierarchy hierarchy,
            String hierarchyName,
            String qualifiedColumn,
            String columnName,
            String keyAlias)
        {
            this.hierarchy = hierarchy;
            this.hierarchyName = hierarchyName;
            this.qualifiedColumn = qualifiedColumn;
            this.columnName = columnName;
            this.keyAlias = keyAlias;
        }
    }
}
