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

    /** Pattern matching {@code ${identifier}} placeholders. */
    private static final Pattern PLACEHOLDER_PATTERN =
        Pattern.compile("\\$\\{([a-zA-Z_][a-zA-Z0-9_]*)\\}");

    private final RolapCalculatedMember member;
    private final RolapEvaluatorRoot root;
    private final NativeSqlConfig.NativeSqlDef def;
    private final Calc fallbackCalc;

    // Lazy-resolved at first evaluate()
    private RolapCube baseCube;
    private boolean resolved;

    /** Cache: evaluator context key -> value (Double or null). */
    private Map<String, Object> batchCache;

    private NativeSqlCalc(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root,
        NativeSqlConfig.NativeSqlDef def,
        Calc fallbackCalc)
    {
        super(member.getExpression(), new Calc[0]);
        this.member = member;
        this.root = root;
        this.def = def;
        this.fallbackCalc = fallbackCalc;
    }

    /**
     * Factory method called by {@link NativeSqlRegistry}.
     * Creates a fallback calc from the member's MDX formula, then
     * wraps it in a NativeSqlCalc that defers SQL generation to
     * evaluate-time.
     */
    static Calc create(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root,
        NativeSqlConfig.NativeSqlDef def)
    {
        final Calc fallback = root.getCompiled(
            member.getExpression(), true, null);
        LOGGER.debug(
            "NativeSqlCalc.create: creating lazy calc for [{}]",
            member.getName());
        return new NativeSqlCalc(member, root, def, fallback);
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
        // Lazy-resolve on first call
        if (!ensureResolved(evaluator)) {
            return fallbackCalc.evaluate(evaluator);
        }

        // Try batch cache first
        final String key = buildCacheKey(evaluator);
        if (batchCache != null) {
            if (batchCache.containsKey(key)) {
                return batchCache.get(key);
            }
            // Cache exists but key not found
            return fallbackCalc.evaluate(evaluator);
        }

        // First call — build placeholders, substitute, execute
        try {
            batchCache = executeBatchQuery(evaluator);
            if (batchCache.containsKey(key)) {
                return batchCache.get(key);
            }
        } catch (Exception e) {
            LOGGER.warn(
                "NativeSqlCalc: batch query failed for [{}], "
                    + "falling back to MDX: {}",
                member.getName(), e.getMessage(), e);
            batchCache = Collections.emptyMap();
        }
        return fallbackCalc.evaluate(evaluator);
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
            def.getTemplate(), placeholders);

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

        // Collect axis bindings and slicer predicates from evaluator members
        final List<AxisBinding> axisBindings = new ArrayList<AxisBinding>();
        final List<String> joinClauses = new ArrayList<String>();
        final Set<String> seenJoins = new LinkedHashSet<String>();
        final List<String> wherePredicates = new ArrayList<String>();

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

                    // Resolve foreign key from the cube dimension
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

            // Add axis binding
            axisBindings.add(new AxisBinding(
                m.getHierarchy().getName(), qualifiedColumn));

            // Add join clause (deduplicated)
            if (joinClause != null && seenJoins.add(joinClause)) {
                joinClauses.add(joinClause);
            }

            // Add WHERE predicate for this member's value
            final Object memberKey = ((RolapMember) m).getKey();
            wherePredicates.add(
                qualifiedColumn + " = " + formatLiteral(memberKey));
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

        // WHERE clause
        if (wherePredicates.isEmpty()) {
            ph.put("whereClause", "1 = 1");
        } else {
            final StringBuilder whereBuf = new StringBuilder();
            for (int i = 0; i < wherePredicates.size(); i++) {
                if (i > 0) {
                    whereBuf.append(" AND ");
                }
                whereBuf.append(wherePredicates.get(i));
            }
            ph.put("whereClause", whereBuf.toString());
        }

        // Add all static variables from the definition
        for (Map.Entry<String, String> entry
            : def.getVariables().entrySet())
        {
            ph.put(entry.getKey(), entry.getValue());
        }

        return ph;
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
    static String substitutePlaceholders(
        String template,
        Map<String, String> placeholders)
    {
        final Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
        final StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String name = matcher.group(1);
            final String value = placeholders.get(name);
            if (value == null) {
                throw new MondrianException(
                    "NativeSqlCalc: unresolved placeholder ${"
                        + name + "} in template");
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(sb);
        return sb.toString();
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
                if (key.length() > 0) {
                    key.append('|');
                }
                key.append(rs.getString(i));
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
    private String buildCacheKey(Evaluator evaluator) {
        final StringBuilder key = new StringBuilder();
        for (Member m : evaluator.getMembers()) {
            if (m == null || m.isMeasure() || m.isAll()) {
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
