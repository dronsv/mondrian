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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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

    /**
     * Shared cache: survives calc recreation across getCompiledExpression
     * calls. Keyed by SQL fingerprint (hash of expanded SQL), value is
     * the batch result map. Different query contexts (different slicer,
     * subselect, axis layout) produce different SQL → different cache key.
     * Cleared on schema flush.
     */
    private static final ConcurrentHashMap<String, Map<String, Object>>
        SHARED_CACHE = new ConcurrentHashMap<String, Map<String, Object>>();
    private static final ConcurrentHashMap<String, AtomicLong>
        OBSERVABILITY_STATS =
        new ConcurrentHashMap<String, AtomicLong>();

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

    /** Last collected predicates — used for whereClauseExcept resolution. */
    private List<PredicateInfo> lastPredicates;

    /** Hierarchies on query axes — cached for buildCacheKey. */
    private Set<Hierarchy> resolvedAxisHierarchies;

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
        LOGGER.info(
            "NativeSqlCalc: resolved baseCube='{}' for [{}]",
            baseCube.getName(), member.getName());
        return true;
    }

    @Override
    public Object evaluate(Evaluator evaluator) {
        recordStat("evaluate.calls");
        if (!ensureResolved(evaluator)) {
            recordStat("resolve.baseCube.unresolved");
            return null;
        }

        // Resolve axis hierarchies (needed for row key)
        if (resolvedAxisHierarchies == null) {
            resolvedAxisHierarchies = resolveAxisHierarchies(evaluator);
        }

        // Build row key from axis members (same encoding as SQL result)
        final String rowKey = buildRowKey(evaluator);

        // Build batch SQL and its fingerprint (cache key).
        // We must build placeholders to know the SQL fingerprint even
        // for cache lookup. This is cheap — no JDBC, just string ops.
        final Map<String, String> placeholders;
        final String sql;
        final String batchKey;
        try {
            placeholders = buildPlaceholders(evaluator);
            sql = substitutePlaceholders(
                def.getTemplate(), placeholders, lastPredicates);
            batchKey = sql;
        } catch (Exception e) {
            final String reason = classifyNativeUnavailable(e);
            recordStat("native.unavailable");
            recordStat("native.unavailable.reason." + reason);
            LOGGER.debug(
                "NativeSqlCalc: native path unavailable for [{}], reason={}: {}",
                member.getName(),
                reason,
                e.getMessage());
            return fallbackOrNull(evaluator, reason);
        }

        // Check shared cache: keyed by SQL fingerprint
        Map<String, Object> cached = SHARED_CACHE.get(batchKey);
        if (cached != null) {
            recordStat("cache.batch.hit");
            if (cached.containsKey(rowKey)) {
                recordStat("cache.row.hit");
                final Object value = cached.get(rowKey);
                logReturnedValue("cache hit", rowKey, batchKey, value);
                return value;
            }
            // Batch executed but this row key absent → member has no data
            recordStat("cache.row.miss");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "NativeSqlCalc: cache hit but row missing for [{}], rowKey={}, batchKeyHash={}",
                    member.getName(),
                    rowKey,
                    batchKey.hashCode());
            }
            return null;
        }
        recordStat("cache.batch.miss");

        // First call for this batch context — execute SQL
        try {
            recordStat("batch.execute.attempt");
            LOGGER.info(
                "NativeSqlCalc: executing batch SQL for [{}]: {}",
                member.getName(), sql);
            Map<String, Object> results = executeSql(evaluator, sql);
            recordStat("batch.execute.success");
            addStat("batch.rows.total", results.size());
            SHARED_CACHE.put(batchKey, results);
            if (results.containsKey(rowKey)) {
                recordStat("result.row.hit.afterExecute");
                final Object value = results.get(rowKey);
                logReturnedValue("post-execute", rowKey, batchKey, value);
                return value;
            }
            recordStat("result.row.miss.afterExecute");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "NativeSqlCalc: batch executed but row missing for [{}], rowKey={}, batchKeyHash={}",
                    member.getName(),
                    rowKey,
                    batchKey.hashCode());
            }
        } catch (Exception e) {
            recordStat("batch.execute.error");
            LOGGER.warn(
                "NativeSqlCalc: batch query failed for [{}]: {}",
                member.getName(), e.getMessage());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("NativeSqlCalc error detail", e);
            }
            // Do NOT cache emptyMap on error — allow retry on next call.
            // Transient failures (connection timeout, ClickHouse restart)
            // should not permanently suppress native evaluation.
            return fallbackOrNull(evaluator, "sql_error");
        }
        return null;
    }

    /**
     * Returns MDX fallback result if enabled by config; otherwise null.
     */
    private Object fallbackOrNull(Evaluator evaluator, String reason) {
        if (!def.isFallbackMdx()) {
            recordRoute("null", reason);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "NativeSqlCalc: fallback disabled for [{}], reason={}",
                    member.getName(),
                    reason);
            }
            return null;
        }
        recordRoute("fallback", reason);
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
            recordStat("fallback.compile.attempt");
            try {
                lazyFallback = root.getCompiled(
                    member.getExpression(), true, null);
                recordStat("fallback.compile.success");
            } catch (Exception e) {
                recordStat("fallback.compile.error");
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
     * Clears the shared cache. Call on schema flush.
     */
    public static void clearCache() {
        SHARED_CACHE.clear();
    }

    static void clearStats() {
        OBSERVABILITY_STATS.clear();
    }

    static Map<String, Long> getStatsSnapshot() {
        final Map<String, Long> snapshot = new TreeMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry
            : OBSERVABILITY_STATS.entrySet())
        {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    static void recordStat(String key) {
        addStat(key, 1L);
    }

    static void addStat(String key, long delta) {
        OBSERVABILITY_STATS.computeIfAbsent(
            key,
            new java.util.function.Function<String, AtomicLong>() {
                public AtomicLong apply(String ignored) {
                    return new AtomicLong();
                }
            }).addAndGet(delta);
    }

    private static void recordRoute(String route, String reason) {
        recordStat("route." + route);
        if (reason != null && !reason.isEmpty()) {
            recordStat(
                "route." + route + ".reason."
                    + sanitizeStatToken(reason));
        }
    }

    static String classifyNativeUnavailable(Throwable throwable) {
        if (throwable == null) {
            return "unknown";
        }
        final String message = throwable.getMessage();
        if (message != null) {
            if (message.contains("axis count")) {
                return "axis_count_exceeded";
            }
            if (message.contains("unsupported subcube predicate type")
                || message.contains("unsupported StarPredicate"))
            {
                return "unsupported_predicate";
            }
            if (message.contains("unresolved placeholder")) {
                return "unresolved_placeholder";
            }
            if (message.contains("non-column key expression")) {
                return "non_column_key_expression";
            }
        }
        return sanitizeStatToken(throwable.getClass().getSimpleName());
    }

    static String sanitizeStatToken(String value) {
        final StringBuilder buf = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            final char ch = value.charAt(i);
            if ((ch >= 'a' && ch <= 'z')
                || (ch >= 'A' && ch <= 'Z')
                || (ch >= '0' && ch <= '9'))
            {
                buf.append(Character.toLowerCase(ch));
            } else {
                buf.append('_');
            }
        }
        return buf.toString();
    }

    /**
     * Executes the given SQL and parses the result set into a cache map.
     */
    private Map<String, Object> executeSql(
        Evaluator evaluator, String sql)
        throws java.sql.SQLException
    {
        final DataSource dataSource =
            evaluator.getSchemaReader().getDataSource();
        final java.sql.Connection conn = dataSource.getConnection();
        try {
            final java.sql.Statement stmt = conn.createStatement();
            try {
                final java.sql.ResultSet rs = stmt.executeQuery(sql);
                try {
                    return parseResultSet(rs);
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
    private Map<String, String> buildPlaceholders(Evaluator evaluator) {
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
        final List<String> joinClauses = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();
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
                    (RolapMember) m,
                    (MondrianDef.Column) keyExp,
                    star,
                    factTable,
                    factAlias,
                    joinClauses,
                    seenJoins);
            final String qualifiedColumn = resolved.qualifiedColumn;

            final String dimName =
                m.getHierarchy().getDimension().getName();
            final String hierName = m.getHierarchy().getName();
            final boolean isAxisHierarchy =
                axisHierarchies.contains(m.getHierarchy());

            if (isAxisHierarchy) {
                // Axis member → GROUP BY via axisExprN, NOT in WHERE.
                // SQL returns all axis values in one batch query.
                if (!axisBindingByHierarchy.containsKey(m.getHierarchy())) {
                    axisBindingByHierarchy.put(
                        m.getHierarchy(),
                        new AxisBinding(hierName, qualifiedColumn));
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
                subcubePred,
                star,
                baseCube,
                factAlias,
                joinClauses,
                seenJoins));
        }

        for (Hierarchy axisHierarchy : axisHierarchies) {
            final AxisBinding binding = axisBindingByHierarchy.get(axisHierarchy);
            if (binding != null) {
                axisBindings.add(binding);
            }
        }

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
        ph.put("axisCount", String.valueOf(axisCount));

        // Join clauses
        final StringBuilder joinBuf = new StringBuilder();
        for (int i = 0; i < joinClauses.size(); i++) {
            if (i > 0) {
                joinBuf.append("\n");
            }
            joinBuf.append(joinClauses.get(i));
        }
        ph.put("joinClauses", joinBuf.toString());

        // WHERE clause (full)
        ph.put("whereClause", buildWhereFromPredicates(wherePredicates, null));

        // Store for use by substitution and cache key
        this.lastPredicates = wherePredicates;
        this.resolvedAxisHierarchies = axisHierarchies;

        // Add all static variables from the definition
        for (Map.Entry<String, String> entry
            : def.getVariables().entrySet())
        {
            ph.put(entry.getKey(), entry.getValue());
        }

        return ph;
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
        String dimensionName,
        String hierarchyName,
        Set<String> exceptNames)
    {
        for (String name : exceptNames) {
            if (name.contains(".")) {
                // Match by hierarchy: "Dimension.Hierarchy"
                String fullHier = dimensionName + "." + hierarchyName;
                if (fullHier.equals(name)) {
                    return true;
                }
                // Also try just hierarchy name for flat hierarchies
                // where dim and hierarchy share same prefix
                if (hierarchyName.equals(name.substring(
                    name.indexOf('.') + 1)))
                {
                    String dimPart = name.substring(0, name.indexOf('.'));
                    if (dimPart.equals(dimensionName)) {
                        return true;
                    }
                }
            } else {
                // Match by dimension: all hierarchies
                if (dimensionName.equals(name)) {
                    return true;
                }
            }
        }
        return false;
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
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        if (pred instanceof mondrian.rolap.agg.MemberColumnPredicate) {
            final mondrian.rolap.agg.MemberColumnPredicate mcp =
                (mondrian.rolap.agg.MemberColumnPredicate) pred;
            return buildAtomicPredicateInfo(
                mcp,
                mcp.getMember(),
                star,
                baseCube,
                factAlias,
                joinClauses,
                seenJoins);
        } else if (pred instanceof mondrian.rolap.agg.ValueColumnPredicate) {
            return buildAtomicPredicateInfo(
                (mondrian.rolap.agg.ValueColumnPredicate) pred,
                null,
                star,
                baseCube,
                factAlias,
                joinClauses,
                seenJoins);
        } else if (pred instanceof mondrian.rolap.agg.AndPredicate) {
            final List<PredicateInfo> children = new ArrayList<PredicateInfo>();
            for (StarPredicate child
                : ((mondrian.rolap.agg.AndPredicate) pred).getChildren())
            {
                children.add(buildStarPredicate(
                    child,
                    star,
                    baseCube,
                    factAlias,
                    joinClauses,
                    seenJoins));
            }
            return new CompositePredicateInfo("AND", children);
        } else if (pred instanceof mondrian.rolap.agg.OrPredicate) {
            final List<PredicateInfo> children = new ArrayList<PredicateInfo>();
            for (StarPredicate child
                : ((mondrian.rolap.agg.OrPredicate) pred).getChildren())
            {
                children.add(buildStarPredicate(
                    child,
                    star,
                    baseCube,
                    factAlias,
                    joinClauses,
                    seenJoins));
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
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        final ResolvedColumnSql resolved = resolvePredicateColumnSql(
            pred.getConstrainedColumn(),
            star,
            factAlias,
            joinClauses,
            seenJoins);
        final PredicateMetadata metadata =
            resolvePredicateMetadata(member, pred.getConstrainedColumn(), baseCube);
        final Object value = pred.getValue();
        final String sql = value == RolapUtil.sqlNullValue
            ? resolved.qualifiedColumn + " IS NULL"
            : resolved.qualifiedColumn + " = " + formatLiteral(value);
        return new AtomicPredicateInfo(
            metadata.dimensionName,
            metadata.hierarchyName,
            sql);
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
        PredicateMetadata nameOnlyMatch = null;
        boolean ambiguousNameOnlyMatch = false;
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
                    return new PredicateMetadata(
                        hierarchy.getDimension().getName(),
                        hierarchy.getName());
                }
                if (matchesColumnNameOnly(levelColumn, targetColumn)) {
                    final PredicateMetadata candidate =
                        new PredicateMetadata(
                            hierarchy.getDimension().getName(),
                            hierarchy.getName());
                    if (nameOnlyMatch == null) {
                        nameOnlyMatch = candidate;
                    } else if (!sameMetadata(nameOnlyMatch, candidate)) {
                        ambiguousNameOnlyMatch = true;
                    }
                }
            }
        }
        return !ambiguousNameOnlyMatch && nameOnlyMatch != null
            ? nameOnlyMatch
            : PredicateMetadata.UNKNOWN;
    }

    private static boolean sameMetadata(
        PredicateMetadata left,
        PredicateMetadata right)
    {
        return left.dimensionName.equals(right.dimensionName)
            && left.hierarchyName.equals(right.hierarchyName);
    }

    private static boolean matchesColumnNameOnly(
        MondrianDef.Column left,
        MondrianDef.Column right)
    {
        return left.name.equals(right.name);
    }

    private static boolean matchesColumn(
        MondrianDef.Column left,
        MondrianDef.Column right)
    {
        return left.name.equals(right.name)
            && Objects.equals(left.getTableAlias(), right.getTableAlias());
    }

    private ResolvedColumnSql resolveMemberColumnSql(
        RolapMember member,
        MondrianDef.Column keyColumn,
        RolapStar star,
        RolapStar.Table factTable,
        String factAlias,
        List<String> joinClauses,
        Set<String> seenJoins)
    {
        final ResolvedColumnSql starResolved =
            resolveLevelColumnSql(
                keyColumn,
                star,
                factAlias,
                joinClauses,
                seenJoins);
        if (starResolved != null) {
            return starResolved;
        }

        final String columnName = keyColumn.name;
        if (factTable.lookupColumn(columnName) != null) {
            return new ResolvedColumnSql(factAlias + "." + columnName);
        }

        final RolapHierarchy hierarchy = (RolapHierarchy) member.getHierarchy();
        final MondrianDef.RelationOrJoin relation = hierarchy.getRelation();
        if (!(relation instanceof MondrianDef.Table)) {
            return new ResolvedColumnSql(factAlias + "." + columnName);
        }

        final MondrianDef.Table dimTable = (MondrianDef.Table) relation;
        final String dimAlias = dimTable.getAlias() != null
            ? dimTable.getAlias()
            : dimTable.name;
        final String dimTableName = dimTable.name;

        String foreignKey = null;
        RolapCube fkCube = baseCube;
        if (fkCube.isVirtual()) {
            final Dimension dim = hierarchy.getDimension();
            if (dim instanceof RolapCubeDimension) {
                final RolapCubeDimension cubeDim =
                    (RolapCubeDimension) dim;
                if (cubeDim.xmlDimension
                    instanceof MondrianDef.VirtualCubeDimension)
                {
                    final String factCubeName =
                        ((MondrianDef.VirtualCubeDimension)
                            cubeDim.xmlDimension).cubeName;
                    if (factCubeName != null) {
                        final RolapCube resolved =
                            (RolapCube) baseCube.getSchema()
                                .lookupCube(factCubeName);
                        if (resolved != null) {
                            fkCube = resolved;
                        }
                    }
                }
            }
        }
        final HierarchyUsage[] usages = fkCube.getUsages(hierarchy);
        if (usages != null && usages.length > 0) {
            foreignKey = usages[0].getForeignKey();
        }

        final MondrianDef.Hierarchy xmlHier = hierarchy.getXmlHierarchy();
        String primaryKey = xmlHier != null ? xmlHier.primaryKey : null;
        if (primaryKey == null) {
            primaryKey = columnName;
        }

        if (foreignKey == null) {
            LOGGER.warn(
                "NativeSqlCalc: no foreign key for dim {} in {}",
                member.getHierarchy().getName(),
                baseCube.getName());
            return new ResolvedColumnSql(factAlias + "." + columnName);
        }

        final String join = "JOIN " + dimTableName
            + " " + dimAlias
            + " ON " + factAlias + "." + foreignKey
            + " = " + dimAlias + "." + primaryKey;
        if (seenJoins.add(join)) {
            joinClauses.add(join);
        }
        return new ResolvedColumnSql(dimAlias + "." + columnName);
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
        if (table == star.getFactTable()) {
            return new ResolvedColumnSql(factAlias + "." + colName);
        }
        final String tableAlias = table.getAlias();
        final String qualifiedCol = tableAlias + "." + colName;
        final RolapStar.Condition joinCond = table.getJoinCondition();
        if (joinCond != null) {
            final String join = "JOIN " + table.getTableName()
                + " " + tableAlias
                + " ON " + factAlias + "."
                + ((MondrianDef.Column) joinCond.getLeft()).name
                + " = " + tableAlias + "."
                + ((MondrianDef.Column) joinCond.getRight()).name;
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

        AtomicPredicateInfo(
            String dimensionName,
            String hierarchyName,
            String sql)
        {
            this.dimensionName = dimensionName;
            this.hierarchyName = hierarchyName;
            this.sql = sql;
        }

        String render(Set<String> exceptNames) {
            return exceptNames != null
                && shouldExclude(dimensionName, hierarchyName, exceptNames)
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
            new PredicateMetadata("unknown", "unknown");

        final String dimensionName;
        final String hierarchyName;

        PredicateMetadata(String dimensionName, String hierarchyName) {
            this.dimensionName = dimensionName;
            this.hierarchyName = hierarchyName;
        }
    }

    /**
     * Substitutes {@code ${name}} placeholders in the template with values
     * from the map. Throws {@link MondrianException} if any placeholder
     * in the template is not present in the map.
     *
     * @param template     SQL template with ${name} placeholders
     * @param placeholders map of placeholder name to value
     * @return the substituted SQL string
     */
    /**
     * Substitutes placeholders. Handles both simple {@code ${name}}
     * and scoped {@code ${whereClauseExcept:Dim1,Dim2}} placeholders.
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
        final Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
        final StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String token = matcher.group(1);
            String value;
            if (token.startsWith("whereClauseExcept:")) {
                // Dynamic: filter predicates by dimension/hierarchy
                final String args =
                    token.substring("whereClauseExcept:".length());
                final Set<String> exceptNames =
                    new LinkedHashSet<String>();
                for (String s : args.split(",")) {
                    final String trimmed = s.trim();
                    if (!trimmed.isEmpty()) {
                        exceptNames.add(trimmed);
                    }
                }
                if (predicates == null) {
                    value = "1 = 1";
                } else {
                    value = buildWhereFromPredicates(
                        predicates, exceptNames);
                }
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
     * Parses the result set with output contract: the last column is
     * the value ({@code val}), preceding columns are axis keys
     * ({@code k1..kN}). Builds a map keyed by
     * {@code "hierName1=val1|hierName2=val2|..."}.
     */
    private Map<String, Object> parseResultSet(java.sql.ResultSet rs)
        throws java.sql.SQLException
    {
        final Map<String, Object> results =
            new LinkedHashMap<String, Object>();
        final java.sql.ResultSetMetaData meta = rs.getMetaData();
        final int colCount = meta.getColumnCount();
        // Output contract: last column is val, preceding are k1..kN
        final int keyColCount = colCount - 1;

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

        LOGGER.info("NativeSqlCalc: batch query returned {} rows for [{}]",
            results.size(), member.getName());
        return results;
    }

    /**
     * Builds row key from AXIS members only, using the same encoding
     * as {@link #parseResultSet}. Both sides use {@link #encodeRowKey}
     * with {@code String.valueOf()} to guarantee matching keys.
     */
    private String buildRowKey(Evaluator evaluator) {
        final List<String> parts = collectAxisKeyParts(
            evaluator.getMembers(),
            resolvedAxisHierarchies,
            def.getMaxAxes());
        return encodeRowKey(parts);
    }

    static List<String> collectAxisKeyParts(
        Member[] members,
        Set<Hierarchy> axisHierarchies,
        int paddedAxisCount)
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
        if (axisHierarchies == null || axisHierarchies.isEmpty()) {
            for (Member m : memberByHierarchy.values()) {
                parts.add(String.valueOf(((RolapMember) m).getKey()));
            }
        } else {
            for (Hierarchy hierarchy : axisHierarchies) {
                final Member member = memberByHierarchy.get(hierarchy);
                if (member != null) {
                    parts.add(String.valueOf(((RolapMember) member).getKey()));
                }
            }
        }
        while (parts.size() < paddedAxisCount) {
            parts.add(String.valueOf((Object) null));
        }
        return parts;
    }

    /**
     * Single shared encoding for row keys. Both parseResultSet and
     * buildRowKey use this, guaranteeing key match.
     */
    static String encodeRowKey(List<String> parts) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append('|');
            }
            sb.append(parts.get(i));
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
     * Holds the qualified column expression for an axis dimension.
     */
    private static class AxisBinding {
        final String hierarchyName;
        final String qualifiedColumn;

        AxisBinding(String hierarchyName, String qualifiedColumn) {
            this.hierarchyName = hierarchyName;
            this.qualifiedColumn = qualifiedColumn;
        }
    }
}
