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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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

    /** Diagnostic for the engine's only JDBC column-metadata probe (#95). */
    private static final Logger JDBC_METADATA_LOGGER =
        LogManager.getLogger("mondrian.rolap.JdbcMetadata");

    private static final java.util.concurrent.atomic.AtomicInteger
        JDBC_COLUMN_PROBES =
            new java.util.concurrent.atomic.AtomicInteger();

    private record TableColumnKey(String schema, String table) {
    }

    private static final Map<DataSource, Map<TableColumnKey, Set<String>>>
        TABLE_COLUMN_CACHE =
            Collections.synchronizedMap(
                new IdentityHashMap<DataSource, Map<TableColumnKey, Set<String>>>());

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
     * Per-query resolution cache. The first {@code evaluate()} call in a
     * given context walks the template fallback chain and pays the
     * buildPlaceholders / substitute / fingerprint / executeOrLookup
     * cost. Subsequent cells in the same context reuse the resolved
     * bundle and batch payload via a single {@code Map.get(rowKey)} —
     * eliminating ~0.2 ms of redundant work per cell. On wide axes
     * (e.g. ~2800 visible tuples for a Brand × Region pivot) this turns
     * 650 ms of Java-side overhead into a few ms of rowKey lookups.
     *
     * <p>Entries are keyed by {@link #contextSignature}: a resolution is
     * only valid for cells whose non-axis context members match it and
     * whose axis hierarchies are bound the same way. Axis-bound members
     * vary per cell by design (the rowKey covers them); everything else
     * — slicer months pinned by calc formulas, grand-total (All) cells
     * that resolve a scalar batch with no axis bindings — produces a
     * different SQL shape and must not share an entry. (A root-identity
     * check alone let one grand-total scalar batch serve every cell of
     * a Manufacturer × Format query.)
     *
     * <p>Identity-compared on {@code RolapEvaluatorRoot}: each query has
     * its own root, so different queries (or schema reloads) invalidate
     * the cache automatically. Volatile holder + concurrent map —
     * concurrent evaluators within the same query at worst race and
     * rebuild; entries are immutable so there is no torn-state risk.
     */
    private volatile QueryScopedCache queryCache;

    /**
     * Result of a successful template resolution shared across all cells
     * of one query context. Holds only what the per-cell rowKey lookup
     * needs.
     */
    private record ResolvedQueryCache(
        List<AxisBinding> axisBindings,
        Map<String, Object> batchPayload,
        boolean fallback) {}

    /** Per-statement cache holder: root identity, the query's axis
     *  hierarchy names (stable per statement), and resolutions keyed by
     *  {@link #contextSignature}. */
    private static final class QueryScopedCache {
        final Object rootRef;
        final Set<String> axisHierarchyNames;
        final ConcurrentHashMap<String, ResolvedQueryCache> bySignature =
            new ConcurrentHashMap<String, ResolvedQueryCache>();
        /** Bounds the #89 mis-keyed batch WARN to one line per measure
         *  per query. */
        final AtomicBoolean miskeyWarned = new AtomicBoolean();

        QueryScopedCache(Object rootRef, Set<String> axisHierarchyNames) {
            this.rootRef = rootRef;
            this.axisHierarchyNames = axisHierarchyNames;
        }
    }

    /**
     * Canonical signature of the evaluation context for cache keying.
     * Non-measure, non-All members contribute: axis-bound hierarchies as
     * a bound marker ({@code A:<hierarchy>} — any member of them maps to
     * the same resolution via the rowKey), all others as the member's
     * unique name (a different pinned member means different SQL).
     */
    static String contextSignature(
        Member[] members, Set<String> axisHierarchyNames)
    {
        final StringBuilder sb = new StringBuilder();
        for (Member m : members) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }
            final String hierarchyName =
                m.getHierarchy().getUniqueName();
            sb.append('|');
            if (axisHierarchyNames.contains(hierarchyName)) {
                sb.append("A:").append(hierarchyName);
            } else {
                sb.append(m.getUniqueName());
            }
        }
        return sb.toString();
    }

    /** Returns the statement-scoped cache holder, creating it when the
     *  statement changes. */
    private QueryScopedCache queryCacheFor(Evaluator evaluator) {
        QueryScopedCache qc = queryCache;
        if (qc == null || qc.rootRef != root) {
            final Set<String> names = new LinkedHashSet<String>();
            for (Hierarchy h
                : resolveAxisHierarchies(evaluator.getQuery()))
            {
                names.add(h.getUniqueName());
            }
            qc = new QueryScopedCache(root, names);
            queryCache = qc;
        }
        return qc;
    }

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
     * Resolves a SUCCESS batch that lacks the cell's rowKey (#89).
     *
     * <p>A missing row is the normal empty-cell signal, including a
     * scalar or grand-total template that legitimately returns no row,
     * so the cell stays null unless the measure opts in to
     * {@code nativeSql.fallbackOnMissingRowKey} (which evaluates the MDX
     * formula for every missing cell).
     *
     * <p>A non-empty batch with zero axis bindings cannot legitimately
     * miss: its rows carry no key columns, so the only key is the empty
     * one. That is a row-key construction defect (the #89 grand-total
     * NULL was one, fixed by keying resolved scalar contexts without
     * slicer members); it is logged once per measure per query, with
     * counts and a key hash only, and otherwise handled like any other
     * miss.
     */
    private Object missingRowKey(
        Evaluator evaluator,
        QueryScopedCache qc,
        int axisCount,
        Map<String, Object> batch,
        String rowKey)
    {
        if (isMiskeyedScalarBatch(axisCount, batch.size())
            && qc.miskeyWarned.compareAndSet(false, true))
        {
            LOGGER.warn(
                "NativeSqlCalc: [{}] zero-binding batch has {} row(s) but"
                + " none under the cell key (keyLength={}, keyHash={});"
                + " returning {} — row-key construction mismatch (#89)",
                member.getName(), batch.size(), rowKey.length(),
                Integer.toHexString(rowKey.hashCode()),
                def.isFallbackOnMissingRowKey() ? "MDX fallback" : "null");
        }
        return def.isFallbackOnMissingRowKey()
            ? fallbackOrNull(evaluator)
            : null;
    }

    /** True when a zero-binding batch has rows yet misses the cell key. */
    static boolean isMiskeyedScalarBatch(int axisCount, int batchRows) {
        return axisCount == 0 && batchRows > 0;
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
        // Per-query fast path: if a previous cell in the same query
        // AND the same evaluation context already resolved the template
        // chain, reuse its axisBindings and batch payload — only the
        // rowKey is per-cell. This skips buildPlaceholders,
        // substitutePlaceholders, fingerprinting, and executeOrLookup
        // on every cell after the first.
        final QueryScopedCache qc = queryCacheFor(evaluator);
        final String signature = contextSignature(
            evaluator.getMembers(), qc.axisHierarchyNames);
        final ResolvedQueryCache cache = qc.bySignature.get(signature);
        if (cache != null) {
            if (cache.fallback) {
                return fallbackOrNull(evaluator);
            }
            String fastRowKey = null;
            try {
                fastRowKey = def.isRollupAxes()
                    ? encodeRowKey(evaluator, cache.axisBindings)
                    : buildRowKey(evaluator, cache.axisBindings);
            } catch (Exception e) {
                // Fall through to full resolution path on any rowKey
                // build failure — defensive, should not happen if first
                // cell succeeded.
                LOGGER.debug(
                    "NativeSqlCalc: per-query fast-path rowKey build failed for [{}], reverting to full resolution",
                    member.getName(), e);
            }
            if (fastRowKey != null) {
                if (cache.batchPayload.containsKey(fastRowKey)) {
                    return cache.batchPayload.get(fastRowKey);
                }
                // MDX failures must propagate, not be retried as if
                // row-key construction had failed.
                return missingRowKey(
                    evaluator, qc, cache.axisBindings.size(),
                    cache.batchPayload, fastRowKey);
            }
        }

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

        // #74a fail-fast: every hierarchy name referenced inside a
        // dynamic macro (whereClauseExcept / denominatorSelect /
        // denominatorGroupBy / denominatorJoin) must resolve against
        // the cube. The historical silent fallback kept unresolved
        // references rather than excluding them, producing
        // syntactically valid but semantically wrong SQL.
        // Validation runs OUTSIDE the per-template try/catch below
        // because a typo is a schema-design bug, not a recoverable
        // template-fallback condition.
        final Set<String> knownHierarchyNames =
            collectKnownHierarchyNames(baseCube);
        for (int ti = 0; ti < templates.size(); ti++) {
            validateMacroHierarchyRefs(
                extractMacroHierarchyRefs(templates.get(ti)),
                knownHierarchyNames,
                member.getName(),
                ti);
            NativeSqlFactJoins.validateTemplate(
                templates.get(ti), member.getName(), ti);
        }

        // Walk the template fallback chain. Each template's SQL is
        // looked up / executed synchronously via executeOrLookup, which
        // hits the process-wide GLOBAL_SUCCESS cache for cross-statement
        // reuse and only falls back to JDBC on a true cache miss.
        // Per-statement errors short-circuit re-execution within the
        // same statement; subsequent templates are tried on each error.
        final List<TemplateColumnSkip> columnSkips =
            new ArrayList<TemplateColumnSkip>();
        for (int ti = 0; ti < templates.size(); ti++) {
            final String rawTemplate = templates.get(ti);
            final boolean hasFactJoins = rawTemplate.contains(
                NativeSqlFactJoins.PLACEHOLDER_TOKEN);
            // ${factJoins} opt-in: bind axis/predicate columns missing
            // from this template's f-bound source through the star join
            // path. Identity pass-through for legacy templates.
            final NativeSqlFactJoins.Rebase rebase =
                NativeSqlFactJoins.resolveTemplate(
                    rawTemplate, ti, member.getName(),
                    bundle.placeholders(),
                    bundle.axisBindings(),
                    bundle.predicates(),
                    root.currentDialect,
                    dataSource);
            if (rebase.skip != null) {
                columnSkips.add(rebase.skip);
                LOGGER.info(
                    "NativeSqlCalc: template[{}] for [{}] skipped — {}"
                    + " (table {}, column(s) {}), trying next",
                    ti, member.getName(),
                    rebase.skip.reason(),
                    rebase.skip.tableName(),
                    rebase.skip.missingColumns());
                continue;
            }

            final String sql;
            try {
                sql = substitutePlaceholders(
                    rawTemplate,
                    rebase.placeholders,
                    rebase.predicates,
                    rebase.axisBindings);
            } catch (Exception e) {
                LOGGER.info(
                    "NativeSqlCalc: template[{}] unresolvable for [{}] ({}), trying next",
                    ti, member.getName(), e.getMessage());
                continue;
            }

            TemplateColumnSkip columnSkip = findMissingDbColumns(
                ti,
                sql,
                rebase.axisBindings,
                rebase.predicates,
                dataSource);
            if (columnSkip != null) {
                // Without the placeholder the actionable remedy is to
                // add ${factJoins} (or a wider fallback template).
                if (!hasFactJoins) {
                    columnSkip = columnSkip.withReason(
                        TemplateSkipReason.NO_FACT_JOINS_PLACEHOLDER);
                }
                columnSkips.add(columnSkip);
                LOGGER.info(
                    "NativeSqlCalc: template[{}] for [{}] skipped — table"
                    + " {} lacks required column(s) {}, trying next",
                    ti, member.getName(),
                    columnSkip.tableName(),
                    columnSkip.missingColumns());
                continue;
            }

            final NativeSqlFingerprint fp = NativeSqlFingerprint.of(
                sql, Collections.<Object>emptyList(), dataSource, /*session*/ null);

            final NativeSqlLookupResult r = root.nativeSqlRegistry.executeOrLookup(
                new NscBatchWork(
                    fp, dataSource, sql, this, rebase.axisBindings,
                    def.isRollupAxes()));

            if (r.isSuccess()) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> batch =
                    (Map<String, Object>) r.successPayload();
                // Cache for subsequent cells of this query context.
                qc.bySignature.put(
                    signature,
                    new ResolvedQueryCache(
                        rebase.axisBindings, batch, false));
                if (batch.containsKey(rowKey)) {
                    final Object value = batch.get(rowKey);
                    logReturnedValue("registry hit", rowKey, sql, value);
                    return value;
                }
                return missingRowKey(
                    evaluator, qc, bundle.axisBindings().size(),
                    batch, rowKey);
            }
            // ERROR (fallback or propagate) — try the next template.
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "NativeSqlCalc: registry cached error for [{}] template[{}], trying next",
                    member.getName(), ti, r.errorThrowable());
            }
        }

        // All templates terminated in error or were skipped. When at
        // least one template was skipped for missing source columns,
        // WARN with the full per-template detail (issue #81): without
        // it the measure silently returns fallback NULL and the schema
        // author has no signal that no template in the chain carries
        // the axis column (e.g. a join-dimension level not
        // denormalized into any aggregate).
        if (!columnSkips.isEmpty()) {
            LOGGER.warn(describeExhaustedTemplateChain(
                member.getName(),
                templates.size(),
                columnSkips,
                bundle.axisBindings()));
        }
        // Route to the legacy MDX fallback, and remember the decision
        // so subsequent cells of this query context skip the full
        // template walk too.
        qc.bySignature.put(
            signature,
            new ResolvedQueryCache(
                bundle.axisBindings(),
                Collections.<String, Object>emptyMap(),
                true));
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
        TABLE_COLUMN_CACHE.clear();
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
            dbg.append(
                evaluator.getQuery().getSubcubePredicates(
                    baseCube,
                    Collections.<Hierarchy>emptySet(),
                    evaluator));
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
            final MondrianDef.Column keyColumn =
                (MondrianDef.Column) keyExp;
            final ResolvedColumnSql resolved =
                resolveMemberColumnSql(keyColumn, factAlias);
            final String qualifiedColumn = resolved.qualifiedColumn;
            // Star provenance for ${factJoins}: lets a template rebase
            // resolve the dim table + join condition when its source
            // lacks the column. Null (e.g. degenerate levels without a
            // table alias) simply means "not star-joinable".
            final RolapStar.Column memberStarColumn =
                keyColumn.getTableAlias() == null
                    ? null
                    : star.lookupColumn(
                        keyColumn.getTableAlias(), keyColumn.name);

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
                            null,
                            memberStarColumn));
                }
            } else {
                // Slicer/subselect member → WHERE predicate only.
                final Object memberKey = ((RolapMember) m).getKey();
                wherePredicates.add(new AtomicPredicateInfo(
                    dimName, hierName,
                    keyColumn.name,
                    "= " + formatLiteral(memberKey),
                    memberStarColumn,
                    null));
            }
        }

        // Collect subcube predicates (from MDX subselect) into WHERE.
        // These are NOT in evaluator.getMembers(). Extract column=value
        // pairs from the StarPredicate tree.
        final StarPredicate subcubePred =
            evaluator.getQuery().getSubcubePredicates(
                baseCube,
                Collections.<Hierarchy>emptySet(),
                evaluator);
        if (subcubePred != null) {
            wherePredicates.add(subcubePredicateInfo(subcubePred, baseCube));
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
        // M2: a ${factJoins}-opted chain lets the synthetic resolver keep
        // bindings whose column is missing from every candidate agg but
        // reachable through the star join path.
        final boolean chainHasFactJoins =
            NativeSqlFactJoins.chainContainsPlaceholder(def.getTemplates());

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
                    "k" + axisBindings.size(),
                    binding.starColumn));
            } else if (def.isRollupAxes()) {
                final AxisBinding synthetic = resolveSyntheticBinding(
                    axisHierarchy, star, factAlias,
                    joinClauses, seenJoins, axisBindings.size(),
                    candidateAggs, chainHasFactJoins);
                if (synthetic != null) {
                    axisBindings.add(synthetic);
                } else {
                    // Task 44: column not present on ANY candidate agg and
                    // not rescuable via ${factJoins} — every template
                    // would fail at execution. Abort the native path so
                    // the bundle-level catch in evaluateViaRegistry routes
                    // to the MDX fallback without firing SQL.
                    throw new MondrianException(
                        "NativeSqlCalc: synthetic axis '"
                        + axisHierarchy.getUniqueName()
                        + "' cannot be bound — column not on any candidate"
                        + " agg in the template fallback chain and no"
                        + " ${factJoins} star path");
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
        final RenderedPredicate where =
            BooleanOp.AND.combine(predicates, exceptNames);
        return where == RenderedPredicate.TRUE ? "1 = 1" : where.sql;
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
     * Converts a subcube {@link StarPredicate} tree into a
     * {@link PredicateInfo} tree for {@code whereClause} and
     * {@code whereClauseExcept} rendering. Shared by NativeSqlCalc and
     * NativeQuerySqlGenerator so both SQL paths agree on Boolean structure:
     * AND/OR keep their shape (an empty AND is TRUE, an empty OR is FALSE)
     * and literals become constants. {@code atom} builds the column atoms
     * ({@link mondrian.rolap.agg.MemberColumnPredicate} included). NOT,
     * list and SQL-subquery predicates have no exclusion semantics here;
     * {@code unsupported} supplies the exception to throw for them.
     */
    static PredicateInfo toPredicateInfo(
        StarPredicate pred,
        Function<mondrian.rolap.agg.ValueColumnPredicate, PredicateInfo> atom,
        Function<StarPredicate, RuntimeException> unsupported)
    {
        if (pred instanceof mondrian.rolap.agg.ValueColumnPredicate value) {
            return atom.apply(value);
        }
        if (pred instanceof mondrian.rolap.agg.LiteralStarPredicate literal) {
            return ConstantPredicateInfo.of(literal.getValue());
        }
        final BooleanOp op;
        final List<StarPredicate> operands;
        if (pred instanceof mondrian.rolap.agg.AndPredicate and) {
            op = BooleanOp.AND;
            operands = and.getChildren();
        } else if (pred instanceof mondrian.rolap.agg.OrPredicate or) {
            op = BooleanOp.OR;
            operands = or.getChildren();
        } else {
            throw unsupported.apply(pred);
        }
        final List<PredicateInfo> children =
            new ArrayList<PredicateInfo>(operands.size());
        for (StarPredicate operand : operands) {
            children.add(toPredicateInfo(operand, atom, unsupported));
        }
        return new CompositePredicateInfo(op, children);
    }

    /**
     * Subcube predicate of {@code baseCube} as NativeSqlCalc renders it:
     * atoms bind {@code f.<column>} directly on the template's source.
     */
    static PredicateInfo subcubePredicateInfo(
        StarPredicate pred,
        RolapCube baseCube)
    {
        return toPredicateInfo(
            pred,
            atom -> buildAtomicPredicateInfo(atom, baseCube),
            unsupported -> new MondrianException(
                "NativeSqlCalc: unsupported subcube predicate type "
                    + unsupported.getClass().getSimpleName()));
    }

    private static PredicateInfo buildAtomicPredicateInfo(
        mondrian.rolap.agg.ValueColumnPredicate pred,
        RolapCube baseCube)
    {
        // NativeSqlCalc templates control their own FROM/JOIN scope.
        // Default rendering is factAlias.columnName — the template's
        // agg table has dimension columns denormalized. The structural
        // form keeps the star column so a ${factJoins} rebase can
        // requalify it when the source lacks the column.
        final RolapStar.Column starCol = pred.getConstrainedColumn();
        final String colName = starCol.getExpression() instanceof MondrianDef.Column
            ? ((MondrianDef.Column) starCol.getExpression()).name
            : starCol.getName();
        final PredicateMetadata metadata =
            subcubeAtomMetadata(pred, baseCube);
        return new AtomicPredicateInfo(
            metadata.dimensionName,
            metadata.hierarchyName,
            colName,
            valueSqlTail(pred.getValue()),
            starCol,
            metadata.exclusionNames);
    }

    /**
     * Hierarchy metadata of a subcube atom, shared by both template paths
     * (NativeSqlCalc and NQE) so {@code whereClauseExcept} releases the
     * same atoms in each: the member's hierarchy merged with the
     * hierarchies resolved from the constrained column, plus every
     * same-dimension hierarchy keyed on that column. Excluding any
     * hierarchy over the column releases the column.
     */
    static PredicateMetadata subcubeAtomMetadata(
        mondrian.rolap.agg.ValueColumnPredicate pred,
        RolapCube baseCube)
    {
        final RolapMember member =
            pred instanceof mondrian.rolap.agg.MemberColumnPredicate mcp
                ? mcp.getMember()
                : null;
        final RolapStar.Column column = pred.getConstrainedColumn();
        final PredicateMetadata metadata =
            mergePredicateMetadata(
                resolvePredicateMetadata(member, column, baseCube),
                resolvePredicateMetadata(null, column, baseCube));
        final Set<String> exclusionNames =
            new LinkedHashSet<String>(metadata.exclusionNames);
        exclusionNames.addAll(
            collectSiblingHierarchyExclusionNames(member, column, baseCube));
        return new PredicateMetadata(
            metadata.dimensionName,
            metadata.hierarchyName,
            exclusionNames);
    }

    /** SQL comparison tail for a column value: {@code = v} or IS NULL. */
    static String valueSqlTail(Object value) {
        return value == RolapUtil.sqlNullValue
            ? "IS NULL"
            : "= " + formatLiteral(value);
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
     * <p><b>M2 — {@code ${factJoins}} rescue:</b> when
     * {@code chainHasFactJoins} is true, a column missing from every
     * candidate agg no longer voids the binding if the star join path is
     * intact and the fact-side join key IS carried by a candidate agg:
     * the binding stays {@code f.<col>} and the per-template rebase in
     * the template walk requalifies it to {@code nscdN.<col>} for
     * templates that opt in (templates without the placeholder are
     * skipped by the rendered-SQL missing-column check as usual).
     *
     * <p>The {@code joinClauses}/{@code seenJoins} parameters are kept for
     * API compatibility (and may be repurposed by future template macros)
     * but no JOINs are registered here.
     *
     * @return the resolved {@link AxisBinding}, or {@code null} if
     *         {@code candidateAggs} is non-empty, none carry the column,
     *         and no {@code ${factJoins}} star path can rescue it
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
        Set<AggStar> candidateAggs,
        boolean chainHasFactJoins)
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

        final MondrianDef.Column keyCol = (MondrianDef.Column) keyExp;
        final RolapStar.Column syntheticStarColumn =
            star == null || keyCol.getTableAlias() == null
                ? null
                : star.lookupColumn(keyCol.getTableAlias(), keyCol.name);

        // Task 44: pre-validate column existence on the candidate aggs.
        // When the caller has identified a concrete set of aggs (extracted
        // from FROM clauses of the template fallback chain), confirm at
        // least one carries this column inline. Otherwise the synthetic
        // binding would emit f.<col> against an agg that lacks the column,
        // wasting a ClickHouse round-trip per template before fallback.
        //
        // M2 relaxation: when the chain opts into ${factJoins}, a column
        // missing from every candidate agg is still bindable if the star
        // join path exists AND the join FK is carried by a candidate agg —
        // the per-template rebase requalifies it to nscdN.<col> later.
        if (candidateAggs != null && !candidateAggs.isEmpty()
            && !anyAggHasColumn(candidateAggs, columnName))
        {
            final String rescueFk = chainHasFactJoins
                ? NativeSqlFactJoins.factForeignKey(syntheticStarColumn)
                : null;
            if (rescueFk == null
                || !anyAggHasColumn(candidateAggs, rescueFk))
            {
                LOGGER.info(
                    "NativeSqlCalc: synthetic axis '{}' column '{}' not"
                    + " present on any candidate agg {} and no"
                    + " ${{factJoins}} star path — returning null binding"
                    + " (chainHasFactJoins={}, keyExpTableAlias={},"
                    + " starColumn={}, rescueFk={}, fkOnAgg={})",
                    h.getUniqueName(), columnName,
                    candidateAggTableNames(candidateAggs),
                    chainHasFactJoins,
                    keyCol.getTableAlias(),
                    syntheticStarColumn,
                    rescueFk,
                    rescueFk != null
                        && anyAggHasColumn(candidateAggs, rescueFk));
                return null;
            }
            LOGGER.info(
                "NativeSqlCalc: synthetic axis '{}' column '{}' not on any"
                + " candidate agg {} but star-join rescuable via"
                + " ${{factJoins}} (FK {})",
                h.getUniqueName(), columnName,
                candidateAggTableNames(candidateAggs), rescueFk);
        }

        String qualifiedColumn = factAlias + "." + columnName;

        return new AxisBinding(
            h,
            h.getUniqueName(),
            qualifiedColumn,
            columnName,
            "k" + kIndex,
            syntheticStarColumn);
    }

    /** Legacy 7-arg form: no {@code ${factJoins}} rescue (M1 contract). */
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
        return resolveSyntheticBinding(
            h, star, factAlias, joinClauses, seenJoins, kIndex,
            candidateAggs, false);
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
     * Pattern matching a simple table reference bound to a concrete alias.
     * Used for pre-validating templates that bind a denormalized agg to the
     * canonical fact alias {@code f}. Subqueries are intentionally ignored.
     */
    private static final Pattern TABLE_ALIAS_PATTERN =
        Pattern.compile(
            "(?i)\\b(?:FROM|JOIN)\\s+"
            + "(`?[A-Za-z_][A-Za-z0-9_]*`?"
            + "(?:\\.(`?[A-Za-z_][A-Za-z0-9_]*`?))?)"
            + "\\s+(?:AS\\s+)?([A-Za-z_][A-Za-z0-9_]*)\\b");

    /**
     * Pattern matching column references on a SQL alias. Captures bare and
     * back-quoted identifiers, including non-Latin quoted names.
     */
    private static final Pattern ALIAS_COLUMN_PATTERN =
        Pattern.compile(
            "\\b([A-Za-z_][A-Za-z0-9_]*)\\."
            + "(?:`([^`]+)`|([A-Za-z_][A-Za-z0-9_]*))");

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
     * Extracts physical table names bound to {@code alias} in a rendered SQL
     * template. Qualified names are reduced to their last segment to match
     * {@link AggStar} fact-table names.
     */
    static Set<String> extractTableNamesForAlias(String sql, String alias) {
        Set<String> names = new LinkedHashSet<String>();
        if (sql == null || alias == null) {
            return names;
        }
        Matcher m = TABLE_ALIAS_PATTERN.matcher(sql);
        while (m.find()) {
            if (!alias.equalsIgnoreCase(m.group(3))) {
                continue;
            }
            String name = m.group(2) != null ? m.group(2) : m.group(1);
            names.add(unquoteIdentifier(name));
        }
        return names;
    }

    /**
     * A physical relation a template binds to a SQL alias, with the
     * qualifier the template itself wrote.
     */
    record QualifiedTable(String schema, String name) {
        QualifiedTable {
            schema = normalizeSchema(schema);
        }

        @Override public String toString() {
            return schema == null ? name : schema + "." + name;
        }
    }

    /**
     * Like {@link #extractTableNamesForAlias}, but keeps the qualifier
     * a {@code FROM analytics.fact f} wrote instead of reducing the
     * reference to {@code fact}. The unqualified form is what
     * {@link AggStar} matching wants; column probing wants this one,
     * because on a driver whose single metadata filter is the database
     * (ClickHouse) a bare name reads every database on the server.
     */
    static Set<QualifiedTable> extractQualifiedTableNamesForAlias(
        String sql, String alias)
    {
        Set<QualifiedTable> tables = new LinkedHashSet<QualifiedTable>();
        if (sql == null || alias == null) {
            return tables;
        }
        Matcher m = TABLE_ALIAS_PATTERN.matcher(sql);
        while (m.find()) {
            if (!alias.equalsIgnoreCase(m.group(3))) {
                continue;
            }
            final String qualified = m.group(1);
            final String name = m.group(2);
            if (name == null) {
                tables.add(new QualifiedTable(null, unquoteIdentifier(qualified)));
            } else {
                tables.add(new QualifiedTable(
                    unquoteIdentifier(
                        qualified.substring(
                            0, qualified.length() - name.length() - 1)),
                    unquoteIdentifier(name)));
            }
        }
        return tables;
    }

    /**
     * Extracts column names referenced as {@code alias.column} from rendered
     * SQL. Back-quoted identifiers are unquoted in the returned set.
     */
    static Set<String> extractColumnNamesForAlias(String sql, String alias) {
        Set<String> names = new LinkedHashSet<String>();
        if (sql == null || alias == null) {
            return names;
        }
        Matcher m = ALIAS_COLUMN_PATTERN.matcher(sql);
        while (m.find()) {
            if (!alias.equalsIgnoreCase(m.group(1))) {
                continue;
            }
            names.add(m.group(2) != null ? m.group(2) : m.group(3));
        }
        return names;
    }

    private static String unquoteIdentifier(String identifier) {
        if (identifier != null
            && identifier.length() >= 2
            && identifier.startsWith("`")
            && identifier.endsWith("`"))
        {
            return identifier.substring(1, identifier.length() - 1);
        }
        return identifier;
    }

    /**
     * Why a template in the fallback chain was skipped.
     *
     * <ul>
     *   <li>{@link #COLUMN_MISSING} — the rendered SQL references a
     *       column the {@code f}-bound source lacks (historical #81
     *       case for templates that opted into {@code ${factJoins}}
     *       but still reference an authored column that is missing).
     *   <li>{@link #NO_FACT_JOINS_PLACEHOLDER} — same missing-column
     *       condition, but the template has no {@code ${factJoins}}
     *       placeholder to receive a star join; adding one (or a
     *       fallback template) is the remedy.
     *   <li>{@link #NO_STAR_PATH} — the missing column has no star
     *       join path (no star column, fact-table column, or no join
     *       condition).
     *   <li>{@link #FK_MISSING_ON_SOURCE} — the star join FK is not
     *       physically present on the {@code f}-bound source.
     *   <li>{@link #DIM_COLUMN_MISSING} — the dim table's metadata is
     *       readable but lacks the target (or join key) column, or the
     *       dim metadata cannot be read at all (fail-closed).
     * </ul>
     */
    enum TemplateSkipReason {
        COLUMN_MISSING,
        NO_FACT_JOINS_PLACEHOLDER,
        NO_STAR_PATH,
        FK_MISSING_ON_SOURCE,
        DIM_COLUMN_MISSING
    }

    /**
     * Missing-column detail for one rendered template in the fallback
     * chain: the {@code f}-bound table that lacks required columns and
     * the columns it lacks. Accumulated across the template walk to
     * emit the exhaustion diagnostic
     * (dronsv/emondrian-clickhouse#81).
     */
    record TemplateColumnSkip(
        int templateIndex,
        String tableName,
        Set<String> missingColumns,
        TemplateSkipReason reason)
    {
        TemplateColumnSkip(
            int templateIndex,
            String tableName,
            Set<String> missingColumns)
        {
            this(
                templateIndex, tableName, missingColumns,
                TemplateSkipReason.COLUMN_MISSING);
        }

        TemplateColumnSkip withReason(TemplateSkipReason newReason) {
            return new TemplateColumnSkip(
                templateIndex, tableName, missingColumns, newReason);
        }
    }

    /**
     * Returns true when a rendered template references axis/predicate columns
     * on alias {@code f}, but the concrete table/view bound to {@code f} does
     * not carry at least one of those columns according to JDBC metadata.
     *
     * <p>Fail-open by design: if metadata cannot be read or reports no
     * columns for a particular table, that table is ignored. The template is
     * skipped only when at least one concrete table with available metadata
     * proves that a required column is missing.
     */
    static boolean shouldSkipTemplateForMissingDbColumns(
        String sql,
        List<AxisBinding> axisBindings,
        List<PredicateInfo> predicates,
        DataSource dataSource)
    {
        return findMissingDbColumns(
            -1, sql, axisBindings, predicates, dataSource) != null;
    }

    /**
     * Detail-returning form of
     * {@link #shouldSkipTemplateForMissingDbColumns}: resolves WHICH
     * {@code f}-bound table lacks WHICH required columns, so the
     * template walk can name them in diagnostics instead of silently
     * falling back (dronsv/emondrian-clickhouse#81).
     *
     * @return skip detail for the first offending table, or {@code null}
     *         when the template is viable (same fail-open contract as
     *         the boolean form)
     */
    static TemplateColumnSkip findMissingDbColumns(
        int templateIndex,
        String sql,
        List<AxisBinding> axisBindings,
        List<PredicateInfo> predicates,
        DataSource dataSource)
    {
        final Set<QualifiedTable> tableNames =
            extractQualifiedTableNamesForAlias(sql, "f");
        if (tableNames.isEmpty()) {
            return null;
        }

        final Set<String> requiredColumns =
            collectRequiredTemplateColumns(sql, axisBindings, predicates);
        if (requiredColumns.isEmpty()) {
            return null;
        }

        for (QualifiedTable table : tableNames) {
            final String tableName = table.toString();
            final Set<String> availableColumns =
                loadTableColumns(dataSource, table.schema(), table.name());
            if (availableColumns.isEmpty()) {
                continue;
            }
            final Set<String> missingColumns =
                new LinkedHashSet<String>();
            for (String required : requiredColumns) {
                if (!availableColumns.contains(required)) {
                    missingColumns.add(required);
                }
            }
            if (!missingColumns.isEmpty()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "NativeSqlCalc: rendered template uses columns {}"
                        + " not present on table {} columns {}",
                        requiredColumns,
                        tableName,
                        availableColumns);
                }
                return new TemplateColumnSkip(
                    templateIndex, tableName, missingColumns);
            }
        }
        return null;
    }

    /**
     * Formats the actionable WARN emitted when the whole template
     * fallback chain is exhausted and at least one template was skipped
     * because its source table lacks required columns
     * (dronsv/emondrian-clickhouse#81).
     *
     * <p>Missing columns are mapped back to the axis hierarchies that
     * required them, so the schema author can see which query axis has
     * no viable template — typically a join-dimension level whose
     * column is not denormalized into any aggregate in the chain.
     *
     * @return the formatted message, or {@code null} when there were no
     *         column skips (nothing actionable to report)
     */
    static String describeExhaustedTemplateChain(
        String memberName,
        int templateCount,
        List<TemplateColumnSkip> columnSkips,
        List<AxisBinding> axisBindings)
    {
        if (columnSkips == null || columnSkips.isEmpty()) {
            return null;
        }
        final StringBuilder buf = new StringBuilder();
        buf.append("NativeSqlCalc [").append(memberName)
            .append("]: all ").append(templateCount)
            .append(" template(s) unusable, ").append(columnSkips.size())
            .append(" skipped for missing source columns: ");
        for (int i = 0; i < columnSkips.size(); i++) {
            if (i > 0) {
                buf.append("; ");
            }
            final TemplateColumnSkip skip = columnSkips.get(i);
            buf.append("template[").append(skip.templateIndex())
                .append("] table=").append(skip.tableName())
                .append(" missing=[");
            int j = 0;
            for (String column : skip.missingColumns()) {
                if (j++ > 0) {
                    buf.append(", ");
                }
                buf.append(column);
                final String axisName =
                    axisHierarchyForColumn(axisBindings, column);
                if (axisName != null) {
                    buf.append(" (axis '").append(axisName).append("')");
                }
            }
            buf.append("] reason=").append(skip.reason());
        }
        buf.append(". Result falls back to MDX (typically NULL).")
            .append(" Add a fallback template over a source carrying the")
            .append(" missing columns, or take the hierarchy off the")
            .append(" native measure's axes.");
        return buf.toString();
    }

    /** Maps a source column back to the axis hierarchy that bound it. */
    private static String axisHierarchyForColumn(
        List<AxisBinding> axisBindings, String columnName)
    {
        if (axisBindings == null || columnName == null) {
            return null;
        }
        for (AxisBinding binding : axisBindings) {
            if (binding != null
                && columnName.equals(binding.columnName))
            {
                return binding.hierarchyName;
            }
        }
        return null;
    }

    static Set<String> collectRequiredTemplateColumns(
        String sql,
        List<AxisBinding> axisBindings,
        List<PredicateInfo> predicates)
    {
        final Set<String> usedFactColumns =
            extractColumnNamesForAlias(sql, "f");
        final Set<String> requestedColumns = new LinkedHashSet<String>();

        if (axisBindings != null) {
            for (AxisBinding binding : axisBindings) {
                if (binding != null && binding.columnName != null) {
                    requestedColumns.add(binding.columnName);
                }
            }
        }

        if (predicates != null && !predicates.isEmpty()) {
            requestedColumns.addAll(
                extractColumnNamesForAlias(
                    buildWhereFromPredicates(predicates, null),
                    "f"));
        }

        requestedColumns.retainAll(usedFactColumns);
        return requestedColumns;
    }

    static Set<String> loadTableColumns(
        DataSource dataSource,
        String tableName)
    {
        return loadTableColumns(dataSource, null, tableName);
    }

    /** Loads the declared physical relation, keeping schema in its cache key. */
    static Set<String> loadTableColumns(
        DataSource dataSource,
        String schemaName,
        String tableName)
    {
        if (dataSource == null || tableName == null || tableName.isEmpty()) {
            return Collections.<String>emptySet();
        }
        // An absent schema attribute and schema="" are the same relation:
        // XOM hands back "" for the latter, which JDBC reads as "the table
        // has no schema" and which renders as ".table" in diagnostics.
        final String schema = normalizeSchema(schemaName);
        final TableColumnKey key = new TableColumnKey(schema, tableName);
        final Map<TableColumnKey, Set<String>> tableCache =
            tableColumnCacheFor(dataSource);
        Set<String> cached = tableCache.get(key);
        if (cached != null) {
            return cached;
        }
        final String relationName = schema == null
            ? tableName : schema + "." + tableName;

        // #95 observation 3: this is the engine's only JDBC column-metadata
        // call site, so the probe count seen in the database's query log is
        // the miss count here. Report every miss with the running total, so a
        // cache that is not holding shows up as a climbing number for one
        // table instead of a single line per table.
        final int probe = JDBC_COLUMN_PROBES.incrementAndGet();
        JDBC_METADATA_LOGGER.info(
            "JDBC COLUMN PROBE table={} cacheMiss totalProbes={}",
            relationName, probe);

        final Set<String> columns = new LinkedHashSet<String>();
        try (Connection connection = dataSource.getConnection()) {
            final DatabaseMetaData metadata = connection.getMetaData();
            final String escape = metadata.getSearchStringEscape();
            // A schema-qualified relation is only isolated if the driver
            // reads the qualifier from the argument we pass it; the two
            // arguments are not interchangeable. The ClickHouse driver
            // binds its single `database` filter from the CATALOG under
            // its default databaseTerm=catalog and from the SCHEMA under
            // databaseTerm=schema, and an unbound filter degrades to
            // `database LIKE '%'` — every database on the server. Both
            // dispositions are declared by supportsSchemas/CatalogsIn-
            // TableDefinitions, so ask rather than guess.
            final boolean qualifierIsCatalog =
                schema != null && schemaIsCatalog(metadata);
            try (ResultSet rs = metadata.getColumns(
                // The catalog argument is a literal name, the schema
                // argument a LIKE pattern: only the latter is escaped.
                qualifierIsCatalog ? schema : null,
                qualifierIsCatalog ? null : metadataPattern(schema, escape),
                metadataPattern(tableName, escape), null))
            {
                while (rs.next()) {
                    if (schema != null && !rowQualifiedBy(rs, schema)) {
                        // The driver did not honour the filter. Keeping
                        // the row would let one schema's columns vouch
                        // for another's relation of the same name.
                        continue;
                    }
                    final String column = rs.getString("COLUMN_NAME");
                    if (column != null && !column.isEmpty()) {
                        columns.add(column);
                    }
                }
            }
        } catch (SQLException e) {
            LOGGER.debug(
                "NativeSqlCalc: cannot read JDBC metadata columns for table {}",
                relationName,
                e);
            return Collections.<String>emptySet();
        }

        final Set<String> immutableColumns =
            Collections.unmodifiableSet(columns);
        if (tableCache instanceof java.util.concurrent.ConcurrentMap) {
            @SuppressWarnings("unchecked")
            final java.util.concurrent.ConcurrentMap<TableColumnKey, Set<String>>
                concurrentTableCache =
                    (java.util.concurrent.ConcurrentMap<TableColumnKey, Set<String>>)
                        tableCache;
            final Set<String> previous =
                concurrentTableCache.putIfAbsent(key, immutableColumns);
            return previous == null ? immutableColumns : previous;
        }
        tableCache.put(key, immutableColumns);
        return immutableColumns;
    }

    private static String metadataPattern(String identifier, String escape) {
        if (identifier == null || escape == null || escape.isEmpty()) {
            return identifier;
        }
        // JDBC takes patterns, while schema/table names are literal identifiers.
        return identifier.replace(escape, escape + escape)
            .replace("_", escape + "_").replace("%", escape + "%");
    }

    /**
     * An absent schema and an empty one denote the same relation. XOM
     * yields {@code ""} for {@code schema=""}, which JDBC reads as "the
     * table has no schema" and which renders as {@code ".table"}.
     *
     * @return the schema, or null when it names nothing
     */
    static String normalizeSchema(String schema) {
        return schema == null || schema.isBlank() ? null : schema;
    }

    /**
     * Whether this driver exposes what the schema author wrote as a
     * {@code schema=} qualifier through the JDBC <em>catalog</em>
     * argument rather than the schema argument.
     *
     * <p>Fails to the schema argument, which is what a driver that
     * cannot answer (or that supports both) is expected to honour.
     */
    private static boolean schemaIsCatalog(DatabaseMetaData metadata) {
        try {
            return !metadata.supportsSchemasInTableDefinitions()
                && metadata.supportsCatalogsInTableDefinitions();
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Whether a {@code getColumns} row belongs to the probed schema.
     * Rows that do not name an owner at all are kept: the probe cannot
     * disprove them, and the driver's own filter has already run.
     */
    private static boolean rowQualifiedBy(ResultSet rs, String schema) {
        String owner = metadataValue(rs, "TABLE_SCHEM");
        if (owner == null) {
            owner = metadataValue(rs, "TABLE_CAT");
        }
        return owner == null || owner.equalsIgnoreCase(schema);
    }

    /** Reads an optional {@code getColumns} column, null when absent. */
    private static String metadataValue(ResultSet rs, String label) {
        try {
            final String value = rs.getString(label);
            return value == null || value.isEmpty() ? null : value;
        } catch (SQLException e) {
            return null;
        }
    }

    private static Map<TableColumnKey, Set<String>> tableColumnCacheFor(
        DataSource dataSource)
    {
        synchronized (TABLE_COLUMN_CACHE) {
            Map<TableColumnKey, Set<String>> tableCache =
                TABLE_COLUMN_CACHE.get(dataSource);
            if (tableCache == null) {
                tableCache = new ConcurrentHashMap<TableColumnKey, Set<String>>();
                TABLE_COLUMN_CACHE.put(dataSource, tableCache);
            }
            return tableCache;
        }
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
     * fact-table column matching {@code columnName} (case-sensitive).
     *
     * <p>{@code columnName} is a PHYSICAL name (level keyExp / join FK),
     * but AggStar level columns are symbolically named (the level name,
     * e.g. {@code Адрес} for {@code store_key}) with the physical column
     * only in the expression — so both are compared.
     */
    private static boolean anyAggHasColumn(
        Set<AggStar> aggs, String columnName)
    {
        if (columnName == null || aggs == null) {
            return false;
        }
        for (AggStar agg : aggs) {
            for (AggStar.Table.Column col : agg.getFactTable().getColumns()) {
                if (columnName.equals(col.getName())
                    || columnName.equals(physicalColumnName(col)))
                {
                    return true;
                }
            }
        }
        return false;
    }

    /** Physical column name of an agg column, or null when the
     *  expression is not a plain column. */
    private static String physicalColumnName(AggStar.Table.Column col) {
        final MondrianDef.Expression expression = col.getExpression();
        return expression instanceof MondrianDef.Column
            ? ((MondrianDef.Column) expression).name
            : null;
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

    /**
     * Rendering of a predicate: SQL text, or one of the constants
     * {@link #TRUE} / {@link #FALSE}. Composites fold constants instead of
     * emitting them, so "no restriction" and "matches nothing" never share
     * a representation with each other or with SQL text.
     */
    static final class RenderedPredicate {
        static final RenderedPredicate TRUE = new RenderedPredicate("true");
        static final RenderedPredicate FALSE = new RenderedPredicate("false");

        final String sql;

        private RenderedPredicate(String sql) {
            this.sql = sql;
        }

        static RenderedPredicate sql(String sql) {
            return new RenderedPredicate(sql);
        }
    }

    /** Boolean connective of a {@link CompositePredicateInfo}. */
    enum BooleanOp {
        AND(" AND "),
        OR(" OR ");

        private final String separator;

        BooleanOp(String separator) {
            this.separator = separator;
        }

        /** Value over no operands; an operand with this value drops out. */
        RenderedPredicate identity() {
            return this == AND ? RenderedPredicate.TRUE : RenderedPredicate.FALSE;
        }

        /** An operand with this value decides the connective. */
        RenderedPredicate absorbing() {
            return this == AND ? RenderedPredicate.FALSE : RenderedPredicate.TRUE;
        }

        RenderedPredicate combine(
            List<PredicateInfo> operands,
            Set<String> exceptNames)
        {
            final List<String> parts = new ArrayList<String>();
            for (PredicateInfo operand : operands) {
                final RenderedPredicate rendered =
                    operand.renderPredicate(exceptNames);
                if (rendered == absorbing()) {
                    return rendered;
                }
                if (rendered != identity()) {
                    parts.add(rendered.sql);
                }
            }
            return parts.isEmpty()
                ? identity()
                : RenderedPredicate.sql(join(parts));
        }

        /** Joins operand SQL; no operands yield the identity literal. */
        String join(List<String> parts) {
            if (parts.isEmpty()) {
                return identity().sql;
            }
            return parts.size() == 1
                ? parts.get(0)
                : "(" + String.join(separator, parts) + ")";
        }
    }

    /** Predicate expression with hierarchy metadata-aware rendering. */
    static abstract class PredicateInfo {
        /**
         * Renders the predicate with every atom matching
         * {@code exceptNames} replaced by TRUE (no restriction).
         */
        abstract RenderedPredicate renderPredicate(Set<String> exceptNames);

        /**
         * Renders the predicate as a WHERE conjunct: SQL text, or
         * {@code null} when it imposes no restriction.
         */
        final String render(Set<String> exceptNames) {
            final RenderedPredicate rendered = renderPredicate(exceptNames);
            return rendered == RenderedPredicate.TRUE ? null : rendered.sql;
        }
    }

    /** Boolean literal: a subcube axis that matches everything or nothing. */
    static final class ConstantPredicateInfo extends PredicateInfo {
        private static final ConstantPredicateInfo TRUE =
            new ConstantPredicateInfo(RenderedPredicate.TRUE);
        private static final ConstantPredicateInfo FALSE =
            new ConstantPredicateInfo(RenderedPredicate.FALSE);

        private final RenderedPredicate value;

        private ConstantPredicateInfo(RenderedPredicate value) {
            this.value = value;
        }

        static ConstantPredicateInfo of(boolean value) {
            return value ? TRUE : FALSE;
        }

        @Override
        RenderedPredicate renderPredicate(Set<String> exceptNames) {
            return value;
        }
    }

    /**
     * Atomic predicate with dimension/hierarchy metadata.
     *
     * <p>Two forms. The <b>structural</b> form stores the column name and
     * the SQL tail ({@code "= 'X'"} / {@code "IS NULL"}) separately, so
     * rendering is late-bound: the default qualifier is the fact alias
     * ({@code f.col tail}, byte-identical to the historical pre-rendered
     * string), and a {@code ${factJoins}} rebase can requalify the column
     * via {@link #withQualifiedExpr} without string surgery. The
     * <b>pre-rendered</b> form ({@code columnName == null}) carries the
     * full SQL in {@code sqlTail} and is never rebased — used by literal
     * (true/false) predicates and by NativeQuerySqlGenerator.
     */
    static final class AtomicPredicateInfo extends PredicateInfo {
        final String dimensionName;
        final String hierarchyName;
        final String columnName;
        final String sqlTail;
        final String qualifiedExpr;
        final RolapStar.Column starColumn;
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
            this(
                dimensionName, hierarchyName,
                null, sql, null, null,
                exclusionNames == null
                    ? Collections.<String>emptySet()
                    : exclusionNames);
        }

        AtomicPredicateInfo(
            String dimensionName,
            String hierarchyName,
            String columnName,
            String sqlTail,
            RolapStar.Column starColumn,
            Set<String> exclusionNames)
        {
            this(
                dimensionName, hierarchyName,
                columnName, sqlTail,
                columnName == null ? null : "f." + columnName,
                starColumn,
                exclusionNames == null
                    ? defaultExclusionNames(dimensionName, hierarchyName)
                    : exclusionNames);
        }

        private AtomicPredicateInfo(
            String dimensionName,
            String hierarchyName,
            String columnName,
            String sqlTail,
            String qualifiedExpr,
            RolapStar.Column starColumn,
            Set<String> exclusionNames)
        {
            this.dimensionName = dimensionName;
            this.hierarchyName = hierarchyName;
            this.columnName = columnName;
            this.sqlTail = sqlTail;
            this.qualifiedExpr = qualifiedExpr;
            this.starColumn = starColumn;
            this.exclusionNames =
                exclusionNames == null
                    ? Collections.<String>emptySet()
                    : new LinkedHashSet<String>(exclusionNames);
        }

        /** Copy with the column rendered through a different qualified
         *  expression (e.g. {@code nscd0.`region`}); structural only. */
        AtomicPredicateInfo withQualifiedExpr(String expr) {
            return new AtomicPredicateInfo(
                dimensionName, hierarchyName,
                columnName, sqlTail, expr, starColumn, exclusionNames);
        }

        @Override
        RenderedPredicate renderPredicate(Set<String> exceptNames) {
            if (exceptNames != null
                && shouldExclude(exclusionNames, exceptNames))
            {
                return RenderedPredicate.TRUE;
            }
            return RenderedPredicate.sql(
                columnName == null
                    ? sqlTail
                    : qualifiedExpr + " " + sqlTail);
        }
    }

    /**
     * Composite predicate preserving AND/OR tree shape. An atom excluded
     * by {@code whereClauseExcept} renders TRUE: it drops out of an AND and
     * makes an enclosing OR unrestricted.
     */
    static final class CompositePredicateInfo extends PredicateInfo {
        final BooleanOp op;
        final List<PredicateInfo> children;

        CompositePredicateInfo(BooleanOp op, List<PredicateInfo> children) {
            this.op = op;
            this.children = children;
        }

        @Override
        RenderedPredicate renderPredicate(Set<String> exceptNames) {
            return op.combine(children, exceptNames);
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
     * A single reference to a hierarchy name inside a dynamic macro
     * argument list (whereClauseExcept / denominatorSelect /
     * denominatorGroupBy / denominatorJoin). Used by the typo
     * validator (issue dronsv/emondrian-clickhouse#74a).
     */
    public record MacroHierarchyRef(String macroName, String hierarchyName) {
    }

    /**
     * Extracts every hierarchy-name reference appearing in dynamic
     * macros within {@code template}.
     *
     * <p>The macros covered are:
     * <ul>
     *   <li>{@code ${whereClauseExcept:H1,H2,…}}
     *   <li>{@code ${denominatorSelect:H1,H2,…}}
     *   <li>{@code ${denominatorGroupBy:H1,H2,…}} (bare-alias mode)
     *   <li>{@code ${denominatorGroupBy:srcAlias:H1,H2,…}}
     *       (prefixed mode — only the names after the alias)
     *   <li>{@code ${denominatorJoin:lhs:rhs:H1,H2,…}} (only the
     *       names after the two aliases; malformed forms with fewer
     *       than 3 colon parts are silently skipped — the dispatch
     *       will throw at render)
     * </ul>
     *
     * <p>Static placeholders ({@code ${factTable}},
     * {@code ${whereClause}}, {@code ${axisExprN}}, etc.) produce no
     * refs.
     */
    static List<MacroHierarchyRef> extractMacroHierarchyRefs(String template) {
        final List<MacroHierarchyRef> refs =
            new ArrayList<MacroHierarchyRef>();
        if (template == null) {
            return refs;
        }
        final Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
        while (matcher.find()) {
            final String token = matcher.group(1);
            String macroName = null;
            String csv = null;
            if (token.startsWith("whereClauseExcept:")) {
                macroName = "whereClauseExcept";
                csv = token.substring(macroName.length() + 1);
            } else if (token.startsWith("denominatorSelect:")) {
                macroName = "denominatorSelect";
                csv = token.substring(macroName.length() + 1);
            } else if (token.startsWith("denominatorGroupBy:")) {
                macroName = "denominatorGroupBy";
                final String args =
                    token.substring(macroName.length() + 1);
                final String[] parts = args.split(":", -1);
                csv = parts.length >= 2 ? parts[1] : parts[0];
            } else if (token.startsWith("denominatorJoin:")) {
                macroName = "denominatorJoin";
                final String args =
                    token.substring(macroName.length() + 1);
                final String[] parts = args.split(":", -1);
                if (parts.length < 3) {
                    continue;
                }
                csv = parts[2];
            }
            if (macroName == null || csv == null) {
                continue;
            }
            for (String name : parseExceptNames(csv)) {
                refs.add(new MacroHierarchyRef(macroName, name));
            }
        }
        return refs;
    }

    /**
     * Throws {@link MondrianException} on the first
     * {@link MacroHierarchyRef} whose {@code hierarchyName} is not a
     * member of {@code knownHierarchyNames}.
     *
     * <p>Fail-fast guard against typos and renames in schema templates
     * (issue dronsv/emondrian-clickhouse#74a). The historical silent
     * fallback meant an unresolved name was kept rather than excluded,
     * producing a syntactically valid but semantically wrong
     * predicate / projection.
     *
     * <p>The error message embeds the measure name, template index,
     * macro name, and the offending hierarchy reference so the schema
     * author can locate the typo immediately.
     */
    static void validateMacroHierarchyRefs(
        List<MacroHierarchyRef> refs,
        Set<String> knownHierarchyNames,
        String measureName,
        int templateIndex)
    {
        if (refs == null || refs.isEmpty()) {
            return;
        }
        for (MacroHierarchyRef ref : refs) {
            if (!knownHierarchyNames.contains(ref.hierarchyName())) {
                throw new MondrianException(
                    "NativeSqlCalc [" + measureName + "] template["
                    + templateIndex + "] macro ${" + ref.macroName()
                    + "} references unknown hierarchy '"
                    + ref.hierarchyName() + "'");
            }
        }
    }

    /**
     * Builds the set of hierarchy-name forms that a dynamic-macro
     * argument list is allowed to reference in {@code cube}. The
     * canonical forms come from
     * {@link #defaultExclusionNames(String, String)} — dimension
     * short name, hierarchy short name, and the
     * {@code Dim.Hier} qualified form. A macro argument that matches
     * any of these is treated as resolved.
     */
    static Set<String> collectKnownHierarchyNames(RolapCube cube) {
        final Set<String> known = new LinkedHashSet<String>();
        if (cube == null) {
            return known;
        }
        for (Dimension dim : cube.getDimensions()) {
            if (dim == null) {
                continue;
            }
            for (Hierarchy h : dim.getHierarchies()) {
                if (h == null) {
                    continue;
                }
                known.addAll(
                    defaultExclusionNames(dim.getName(), h.getName()));
            }
        }
        return known;
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
            // Same reader as the non-rollup parseResultSet: a template's
            // val column is a numeric scalar, and the cell it becomes is
            // typed by its Java class all the way out to XMLA's xsi:type.
            // getObject would hand out whatever the driver returns for the
            // template's result type (BigDecimal for a ClickHouse Decimal,
            // say), so the same measure would change type with the
            // rollup/non-rollup shape of the query.
            final double raw = rs.getDouble(valueCol);
            final Object val = rs.wasNull() ? null : raw;
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
        // An explicitly empty binding list is a resolved scalar query. Its
        // result has no key columns, even if a tuple (e.g. ClosingPeriod) pins
        // non-All slicer members. Keep legacy inference only for unknown axes.
        if (axisBindings == null) {
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
     *
     * <p>{@code starColumn} is the star-schema provenance of the key
     * column (nullable): it lets a {@code ${factJoins}} rebase resolve
     * the dim table and join condition when the column is missing from
     * a template's {@code f}-bound source.
     */
    static final class AxisBinding {
        final Hierarchy hierarchy;
        final String hierarchyName;
        final String qualifiedColumn;
        final String columnName;
        final String keyAlias;
        final RolapStar.Column starColumn;

        AxisBinding(
            Hierarchy hierarchy,
            String hierarchyName,
            String qualifiedColumn,
            String columnName,
            String keyAlias)
        {
            this(
                hierarchy, hierarchyName, qualifiedColumn, columnName,
                keyAlias, null);
        }

        AxisBinding(
            Hierarchy hierarchy,
            String hierarchyName,
            String qualifiedColumn,
            String columnName,
            String keyAlias,
            RolapStar.Column starColumn)
        {
            this.hierarchy = hierarchy;
            this.hierarchyName = hierarchyName;
            this.qualifiedColumn = qualifiedColumn;
            this.columnName = columnName;
            this.keyAlias = keyAlias;
            this.starColumn = starColumn;
        }
    }
}
