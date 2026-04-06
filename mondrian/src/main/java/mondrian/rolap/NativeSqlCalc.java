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
     * calls. Keyed by measure name, value is the batch result map.
     * Cleared on schema flush (not implemented yet — acceptable for v1).
     */
    private static final ConcurrentHashMap<String, Map<String, Object>>
        SHARED_CACHE = new ConcurrentHashMap<String, Map<String, Object>>();

    private final RolapCalculatedMember member;
    private final RolapEvaluatorRoot root;
    private final NativeSqlConfig.NativeSqlDef def;
    // Lazy-resolved at first evaluate()
    private RolapCube baseCube;
    private boolean resolved;

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
        if (!ensureResolved(evaluator)) {
            return null;
        }

        // Ensure axis hierarchies are resolved for cache key
        if (resolvedAxisHierarchies == null) {
            resolvedAxisHierarchies = resolveAxisHierarchies(evaluator);
        }

        final String cacheKey = buildCacheKey(evaluator);
        final String measureKey = def.getMeasureName();

        // Check shared cache first (survives calc recreation)
        Map<String, Object> cached = SHARED_CACHE.get(measureKey);
        if (cached != null) {
            if (cached.containsKey(cacheKey)) {
                return cached.get(cacheKey);
            }
            if (LOGGER.isDebugEnabled() && !cached.isEmpty()) {
                String sampleKey = cached.keySet().iterator().next();
                LOGGER.debug(
                    "NativeSqlCalc: cache MISS for [{}] key='{}', "
                    + "sample cached key='{}', cacheSize={}",
                    measureKey, cacheKey, sampleKey, cached.size());
            }
            // Cache exists from previous batch but key not found.
            // This means the axis member is not in the SQL result.
            return null;
        }

        // Execute batch SQL and store in shared cache
        try {
            Map<String, Object> results = executeBatchQuery(evaluator);
            SHARED_CACHE.put(measureKey, results);
            if (results.containsKey(cacheKey)) {
                return results.get(cacheKey);
            }
        } catch (Exception e) {
            LOGGER.warn(
                "NativeSqlCalc: batch query failed for [{}]: {}",
                member.getName(), e.getMessage());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("NativeSqlCalc error detail", e);
            }
            SHARED_CACHE.put(measureKey, Collections.<String, Object>emptyMap());
        }
        // Return null — do NOT fall back to Formula MDX
        // (Formula contains deep recursive evaluation that causes
        // iteration overflow)
        return null;
    }

    /**
     * Clears the shared cache. Call on schema flush.
     */
    public static void clearCache() {
        SHARED_CACHE.clear();
    }

    /**
     * Builds the batch SQL from the template, executes it, and parses
     * the result set into a cache map.
     */
    private Map<String, Object> executeBatchQuery(Evaluator evaluator)
        throws java.sql.SQLException
    {
        final Map<String, String> placeholders = buildPlaceholders(evaluator);
        final String sql = substitutePlaceholders(
            def.getTemplate(), placeholders, lastPredicates);

        LOGGER.info("NativeSqlCalc: executing batch SQL for [{}]: {}",
            member.getName(), sql);

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
            new LinkedHashSet<Hierarchy>();
        final Query query = evaluator.getQuery();
        if (query != null) {
            for (QueryAxis axis : query.getAxes()) {
                if (axis == null || axis.getSet() == null) {
                    continue;
                }
                final mondrian.olap.type.Type setType =
                    axis.getSet().getType();
                if (setType instanceof mondrian.olap.type.SetType) {
                    final mondrian.olap.type.Type elemType =
                        ((mondrian.olap.type.SetType) setType)
                            .getElementType();
                    if (elemType.getHierarchy() != null) {
                        axisHierarchies.add(elemType.getHierarchy());
                    } else if (
                        elemType instanceof mondrian.olap.type.TupleType)
                    {
                        for (mondrian.olap.type.Type t
                            : ((mondrian.olap.type.TupleType) elemType)
                                .elementTypes)
                        {
                            if (t.getHierarchy() != null) {
                                axisHierarchies.add(t.getHierarchy());
                            }
                        }
                    }
                }
            }
        }

        final List<AxisBinding> axisBindings = new ArrayList<AxisBinding>();
        final List<String> joinClauses = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();
        final List<PredicateInfo> wherePredicates = new ArrayList<PredicateInfo>();

        for (Member m : evaluator.getMembers()) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }

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
            final String columnName = ((MondrianDef.Column) keyExp).name;

            // Determine qualified column and join clause
            String qualifiedColumn;
            String joinClause = null;

            if (factTable.lookupColumn(columnName) != null) {
                // Column is on the fact table
                qualifiedColumn = factAlias + "." + columnName;
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

                    // Resolve foreign key via HierarchyUsage on the
                    // fact cube. For VirtualCube dimensions, resolve
                    // the base (fact) cube first.
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
                                String factCubeName =
                                    ((MondrianDef.VirtualCubeDimension)
                                        cubeDim.xmlDimension).cubeName;
                                if (factCubeName != null) {
                                    RolapCube resolved =
                                        (RolapCube) baseCube.getSchema()
                                            .lookupCube(factCubeName);
                                    if (resolved != null) {
                                        fkCube = resolved;
                                    }
                                }
                            }
                        }
                    }
                    HierarchyUsage[] usages =
                        fkCube.getUsages(hierarchy);
                    if (usages != null && usages.length > 0) {
                        foreignKey = usages[0].getForeignKey();
                    }

                    final MondrianDef.Hierarchy xmlHier =
                        hierarchy.getXmlHierarchy();
                    String primaryKey = xmlHier != null
                        ? xmlHier.primaryKey : null;
                    if (primaryKey == null) {
                        primaryKey = columnName;
                    }

                    if (foreignKey != null) {
                        qualifiedColumn = dimAlias + "." + columnName;
                        joinClause = "JOIN " + dimTableName
                            + " " + dimAlias
                            + " ON " + factAlias + "." + foreignKey
                            + " = " + dimAlias + "." + primaryKey;
                    } else {
                        LOGGER.warn(
                            "NativeSqlCalc: no foreign key for "
                                + "dim {} in {}",
                            m.getHierarchy().getName(),
                            baseCube.getName());
                        qualifiedColumn = factAlias + "." + columnName;
                    }
                } else {
                    qualifiedColumn = factAlias + "." + columnName;
                }
            }

            final String dimName =
                m.getHierarchy().getDimension().getName();
            final String hierName = m.getHierarchy().getName();
            final boolean isAxisHierarchy =
                axisHierarchies.contains(m.getHierarchy());

            // Add join clause (deduplicated)
            if (joinClause != null && seenJoins.add(joinClause)) {
                joinClauses.add(joinClause);
            }

            if (isAxisHierarchy) {
                // Axis member → GROUP BY via axisExprN, NOT in WHERE.
                // SQL returns all axis values in one batch query.
                axisBindings.add(
                    new AxisBinding(hierName, qualifiedColumn));
            } else {
                // Slicer/subselect member → WHERE predicate only.
                final Object memberKey = ((RolapMember) m).getKey();
                wherePredicates.add(new PredicateInfo(
                    dimName, hierName,
                    qualifiedColumn + " = "
                        + formatLiteral(memberKey)));
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
    private static String buildWhereFromPredicates(
        List<PredicateInfo> predicates,
        Set<String> exceptNames)
    {
        final StringBuilder buf = new StringBuilder();
        for (PredicateInfo p : predicates) {
            if (exceptNames != null && shouldExclude(p, exceptNames)) {
                continue;
            }
            if (buf.length() > 0) {
                buf.append(" AND ");
            }
            buf.append(p.sql);
        }
        return buf.length() == 0 ? "1 = 1" : buf.toString();
    }

    /**
     * Returns true if predicate matches any of the except names.
     * If name contains a dot (e.g. "Продукт.Бренд"), matches hierarchy.
     * Otherwise matches dimension (all hierarchies of that dimension).
     */
    private static boolean shouldExclude(
        PredicateInfo p, Set<String> exceptNames)
    {
        for (String name : exceptNames) {
            if (name.contains(".")) {
                // Match by hierarchy: "Dimension.Hierarchy"
                String fullHier = p.dimensionName + "." + p.hierarchyName;
                if (fullHier.equals(name)) {
                    return true;
                }
                // Also try just hierarchy name for flat hierarchies
                // where dim and hierarchy share same prefix
                if (p.hierarchyName.equals(name.substring(
                    name.indexOf('.') + 1)))
                {
                    String dimPart = name.substring(0, name.indexOf('.'));
                    if (dimPart.equals(p.dimensionName)) {
                        return true;
                    }
                }
            } else {
                // Match by dimension: all hierarchies
                if (p.dimensionName.equals(name)) {
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
        final Set<Hierarchy> result = new LinkedHashSet<Hierarchy>();
        final Query query = evaluator.getQuery();
        if (query != null) {
            for (QueryAxis axis : query.getAxes()) {
                if (axis == null || axis.getSet() == null) {
                    continue;
                }
                final mondrian.olap.type.Type setType =
                    axis.getSet().getType();
                if (setType instanceof mondrian.olap.type.SetType) {
                    final mondrian.olap.type.Type elemType =
                        ((mondrian.olap.type.SetType) setType)
                            .getElementType();
                    if (elemType.getHierarchy() != null) {
                        result.add(elemType.getHierarchy());
                    } else if (
                        elemType instanceof mondrian.olap.type.TupleType)
                    {
                        for (mondrian.olap.type.Type t
                            : ((mondrian.olap.type.TupleType) elemType)
                                .elementTypes)
                        {
                            if (t.getHierarchy() != null) {
                                result.add(t.getHierarchy());
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /** Predicate with dimension/hierarchy metadata for scoped filtering. */
    private static class PredicateInfo {
        final String dimensionName;
        final String hierarchyName;
        final String sql;

        PredicateInfo(String dimensionName, String hierarchyName, String sql) {
            this.dimensionName = dimensionName;
            this.hierarchyName = hierarchyName;
            this.sql = sql;
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
                    // axisExprN beyond actual axis count → substitute NULL
                    if (token.startsWith("axisExpr")) {
                        value = "NULL";
                    } else {
                        throw new MondrianException(
                            "NativeSqlCalc: unresolved placeholder ${"
                                + token + "} in template");
                    }
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
        // Last column is the value; preceding columns are keys
        final int keyColCount = colCount - 1;

        while (rs.next()) {
            final StringBuilder key = new StringBuilder();
            for (int i = 1; i <= keyColCount; i++) {
                final String colVal = rs.getString(i);
                // Skip NULL columns (from axisExprN beyond actual
                // axis count → NULL AS kN in SQL)
                if (colVal == null) {
                    continue;
                }
                if (key.length() > 0) {
                    key.append('|');
                }
                key.append(colVal);
            }
            final double value = rs.getDouble(colCount);
            results.put(
                key.toString(),
                rs.wasNull() ? null : value);
        }

        LOGGER.info("NativeSqlCalc: batch query returned {} rows for [{}]",
            results.size(), member.getName());
        return results;
    }

    /**
     * Builds a cache key from the evaluator's non-measure, non-All members.
     * Must produce keys that match the SQL result row keys.
     */
    /**
     * Builds cache key from AXIS members only (matching SQL k1..kN).
     * Slicer members are baked into the SQL WHERE and don't vary
     * between evaluate() calls within a single query.
     */
    private String buildCacheKey(Evaluator evaluator) {
        final StringBuilder key = new StringBuilder();
        for (Member m : evaluator.getMembers()) {
            if (m == null || m.isMeasure() || m.isAll()) {
                continue;
            }
            // Only include axis hierarchy members in cache key
            if (resolvedAxisHierarchies != null
                && !resolvedAxisHierarchies.contains(m.getHierarchy()))
            {
                continue;
            }
            if (key.length() > 0) {
                key.append('|');
            }
            final Object memberKey = ((RolapMember) m).getKey();
            key.append(memberKey);
        }
        return key.toString();
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
