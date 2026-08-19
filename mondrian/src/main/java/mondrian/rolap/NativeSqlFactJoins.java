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

import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianException;
import mondrian.spi.Dialect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

/**
 * Star-join binding for NSC templates via the {@code ${factJoins}}
 * placeholder (dronsv/emondrian-clickhouse#81 option 1, design spec
 * {@code 2026-08-18-nsc-join-dimension-axes-design.md}).
 *
 * <p>A template that contains {@code ${factJoins}} after each
 * {@code FROM <source> f} site declares that the engine may bind
 * axis/predicate columns missing from the source through the RolapStar
 * join path, rendering the JOINs into the placeholder. Templates
 * without the placeholder keep behaviour bit-for-bit identical to the
 * pre-placeholder engine.
 */
final class NativeSqlFactJoins {

    private static final Logger LOGGER =
        LogManager.getLogger(NativeSqlFactJoins.class);

    /** The literal opt-in placeholder. */
    static final String PLACEHOLDER_TOKEN = "${factJoins}";

    /** Engine-owned dim-table alias prefix: {@code nscd0}, {@code nscd1}, … */
    static final String ALIAS_PREFIX = "nscd";

    /**
     * A {@code FROM <source> f} site in a raw template. The source is a
     * literal (optionally back-quoted / schema-qualified) table name or
     * a {@code ${placeholder}}; subqueries never match ({@code FROM (}
     * has no identifier at the source position).
     */
    private static final Pattern FACT_FROM_SITE_PATTERN =
        Pattern.compile(
            "(?i)\\bFROM\\s+"
            + "(?:\\$\\{[A-Za-z_][A-Za-z0-9_]*\\}"
            + "|`?[A-Za-z_][A-Za-z0-9_]*`?"
            + "(?:\\.`?[A-Za-z_][A-Za-z0-9_]*`?)?)"
            + "\\s+(?:AS\\s+)?f\\b");

    /** The reserved engine alias: {@code nscd} or {@code nscd<N>}. */
    private static final Pattern RESERVED_ALIAS_PATTERN =
        Pattern.compile("(?i)\\bnscd[0-9]*\\b");

    private NativeSqlFactJoins() {}

    /**
     * Schema-error validation of one raw template. No-op for templates
     * that do not contain {@code ${factJoins}} (bit-for-bit legacy).
     *
     * <p>For opted-in templates, throws {@link MondrianException} naming
     * the measure and template index when:
     * <ul>
     *   <li>the raw template uses the reserved {@code nscd} alias, or
     *   <li>the {@code ${factJoins}} occurrence count differs from the
     *       {@code FROM … f} site count (the engine enforces the count;
     *       placing each placeholder directly after its FROM site is the
     *       author's responsibility).
     * </ul>
     */
    static void validateTemplate(
        String rawTemplate, String measureName, int templateIndex)
    {
        if (rawTemplate == null
            || !rawTemplate.contains(PLACEHOLDER_TOKEN))
        {
            return;
        }
        if (RESERVED_ALIAS_PATTERN.matcher(rawTemplate).find()) {
            throw new MondrianException(
                "NativeSqlCalc [" + measureName + "] template["
                + templateIndex + "]: alias prefix '" + ALIAS_PREFIX
                + "' is reserved for ${factJoins} star-join bindings");
        }
        final int placeholderCount =
            countOccurrences(rawTemplate, PLACEHOLDER_TOKEN);
        final int fromSiteCount = countFactAliasFromSites(rawTemplate);
        if (placeholderCount != fromSiteCount) {
            throw new MondrianException(
                "NativeSqlCalc [" + measureName + "] template["
                + templateIndex + "]: found " + placeholderCount
                + " ${factJoins} placeholder(s) but " + fromSiteCount
                + " 'FROM <source> f' site(s) — each FROM site must"
                + " carry exactly one ${factJoins}");
        }
    }

    /** Counts {@code FROM <source> f} sites in a raw template. */
    static int countFactAliasFromSites(String rawTemplate) {
        if (rawTemplate == null) {
            return 0;
        }
        final Matcher m = FACT_FROM_SITE_PATTERN.matcher(rawTemplate);
        int count = 0;
        while (m.find()) {
            count++;
        }
        return count;
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    /**
     * Result of a per-template rebase. Either {@code skip} is non-null
     * (the template cannot serve this query — try the next one) or the
     * three data members carry the immutable per-template view: the
     * placeholder map (base copy + requalified axis exprs, presence
     * list, WHERE clause, and the rendered {@code factJoins} value),
     * the axis bindings and the predicates. The base bundle is never
     * mutated.
     */
    static final class Rebase {
        final Map<String, String> placeholders;
        final List<NativeSqlCalc.AxisBinding> axisBindings;
        final List<NativeSqlCalc.PredicateInfo> predicates;
        final NativeSqlCalc.TemplateColumnSkip skip;

        private Rebase(
            Map<String, String> placeholders,
            List<NativeSqlCalc.AxisBinding> axisBindings,
            List<NativeSqlCalc.PredicateInfo> predicates,
            NativeSqlCalc.TemplateColumnSkip skip)
        {
            this.placeholders = placeholders;
            this.axisBindings = axisBindings;
            this.predicates = predicates;
            this.skip = skip;
        }
    }

    /**
     * Single entry point for the template walk. Templates without
     * {@code ${factJoins}} pass the base objects through by identity —
     * the structural guarantee that legacy templates render bit-for-bit
     * identically (no copy, no re-render, no metadata reads). Opted-in
     * templates go through {@link #rebase}.
     */
    static Rebase resolveTemplate(
        String rawTemplate,
        int templateIndex,
        String measureName,
        Map<String, String> basePlaceholders,
        List<NativeSqlCalc.AxisBinding> baseBindings,
        List<NativeSqlCalc.PredicateInfo> basePredicates,
        Dialect dialect,
        DataSource dataSource)
    {
        if (rawTemplate == null
            || !rawTemplate.contains(PLACEHOLDER_TOKEN))
        {
            return new Rebase(
                basePlaceholders, baseBindings, basePredicates, null);
        }
        return rebase(
            rawTemplate, templateIndex, measureName,
            basePlaceholders, baseBindings, basePredicates,
            dialect, dataSource);
    }

    /**
     * Resolves every axis-binding and predicate column of the bundle
     * against the template's {@code f}-bound source per the
     * {@code ${factJoins}} contract:
     *
     * <ul>
     *   <li>column present on the source (JDBC metadata) — bound as
     *       {@code f.col}, unchanged;
     *   <li>column provably missing and the star join path is intact —
     *       requalified to an engine-owned dim alias
     *       ({@code nscd0}, {@code nscd1}, … in order of first need)
     *       and the JOIN rendered into {@code factJoins};
     *   <li>anything else — the whole template is skipped with a
     *       {@link NativeSqlCalc.TemplateSkipReason}.
     * </ul>
     *
     * <p>Unreadable <em>source</em> metadata proves nothing and keeps
     * {@code f.col} (same fail-open contract as the missing-column skip
     * check); unreadable <em>dim</em> metadata fails closed to a skip.
     */
    static Rebase rebase(
        String rawTemplate,
        int templateIndex,
        String measureName,
        Map<String, String> basePlaceholders,
        List<NativeSqlCalc.AxisBinding> baseBindings,
        List<NativeSqlCalc.PredicateInfo> basePredicates,
        Dialect dialect,
        DataSource dataSource)
    {
        final JoinContext ctx = new JoinContext(
            NativeSqlCalc.extractTableNamesForAlias(rawTemplate, "f"),
            dialect, dataSource, measureName, templateIndex);
        try {
            final List<NativeSqlCalc.AxisBinding> bindings =
                new ArrayList<NativeSqlCalc.AxisBinding>(
                    baseBindings.size());
            for (NativeSqlCalc.AxisBinding b : baseBindings) {
                final String requalified =
                    ctx.requalify(b.columnName, b.starColumn);
                bindings.add(
                    requalified == null
                        ? b
                        : new NativeSqlCalc.AxisBinding(
                            b.hierarchy, b.hierarchyName, requalified,
                            b.columnName, b.keyAlias, b.starColumn));
            }
            final List<NativeSqlCalc.PredicateInfo> predicates =
                rebasePredicates(basePredicates, ctx);
            final Map<String, String> ph =
                new LinkedHashMap<String, String>(basePlaceholders);
            for (int i = 0; i < bindings.size(); i++) {
                ph.put(
                    "axisExpr" + (i + 1),
                    bindings.get(i).qualifiedColumn);
            }
            ph.put(
                "axisPresenceSelectList",
                NativeSqlCalc.renderAxisPresenceSelectList(bindings));
            ph.put(
                "whereClause",
                NativeSqlCalc.buildWhereFromPredicates(predicates, null));
            ph.put("factJoins", ctx.renderJoins());
            return new Rebase(ph, bindings, predicates, null);
        } catch (SkipTemplate e) {
            return new Rebase(null, null, null, e.skip);
        }
    }

    private static List<NativeSqlCalc.PredicateInfo> rebasePredicates(
        List<NativeSqlCalc.PredicateInfo> predicates, JoinContext ctx)
    {
        final List<NativeSqlCalc.PredicateInfo> out =
            new ArrayList<NativeSqlCalc.PredicateInfo>(predicates.size());
        for (NativeSqlCalc.PredicateInfo p : predicates) {
            out.add(rebasePredicate(p, ctx));
        }
        return out;
    }

    private static NativeSqlCalc.PredicateInfo rebasePredicate(
        NativeSqlCalc.PredicateInfo p, JoinContext ctx)
    {
        if (p instanceof NativeSqlCalc.AtomicPredicateInfo) {
            final NativeSqlCalc.AtomicPredicateInfo atomic =
                (NativeSqlCalc.AtomicPredicateInfo) p;
            final String requalified =
                ctx.requalify(atomic.columnName, atomic.starColumn);
            return requalified == null
                ? atomic
                : atomic.withQualifiedExpr(requalified);
        }
        if (p instanceof NativeSqlCalc.CompositePredicateInfo) {
            final NativeSqlCalc.CompositePredicateInfo composite =
                (NativeSqlCalc.CompositePredicateInfo) p;
            return new NativeSqlCalc.CompositePredicateInfo(
                composite.op,
                rebasePredicates(composite.children, ctx));
        }
        return p;
    }

    /** Control-flow escape for the fail-closed skip decisions. */
    private static final class SkipTemplate extends RuntimeException {
        final NativeSqlCalc.TemplateColumnSkip skip;

        SkipTemplate(NativeSqlCalc.TemplateColumnSkip skip) {
            super(null, null, false, false);
            this.skip = skip;
        }
    }

    /**
     * Per-template resolution state: the {@code f}-bound source tables,
     * metadata access, and the ordered dim-join registry
     * ({@code nscd0}, {@code nscd1}, … — one alias per distinct
     * (dim table, FK, PK) triple, in order of first need).
     */
    private static final class JoinContext {
        private final Set<String> sourceTables;
        private final Dialect dialect;
        private final DataSource dataSource;
        private final String measureName;
        private final int templateIndex;
        private final Map<String, String> aliasByJoinKey =
            new LinkedHashMap<String, String>();
        private final List<String> joinClauses = new ArrayList<String>();

        JoinContext(
            Set<String> sourceTables,
            Dialect dialect,
            DataSource dataSource,
            String measureName,
            int templateIndex)
        {
            this.sourceTables = sourceTables;
            this.dialect = dialect;
            this.dataSource = dataSource;
            this.measureName = measureName;
            this.templateIndex = templateIndex;
        }

        /**
         * Returns the requalified column expression
         * ({@code nscdN.`col`}) when the column must be star-joined,
         * {@code null} when the {@code f}-binding stays, and throws
         * {@link SkipTemplate} when the template cannot serve the
         * query.
         */
        String requalify(String columnName, RolapStar.Column starColumn) {
            if (columnName == null) {
                return null;
            }
            final String offender = sourceTableLacking(columnName);
            if (offender == null) {
                return null;
            }
            if (starColumn == null) {
                throw skip(
                    NativeSqlCalc.TemplateSkipReason.NO_STAR_PATH,
                    offender, columnName);
            }
            final RolapStar.Table dimTable = starColumn.getTable();
            final RolapStar.Condition condition =
                dimTable == null ? null : dimTable.getJoinCondition();
            if (condition == null
                || !(condition.getLeft() instanceof MondrianDef.Column)
                || !(condition.getRight() instanceof MondrianDef.Column))
            {
                throw skip(
                    NativeSqlCalc.TemplateSkipReason.NO_STAR_PATH,
                    offender, columnName);
            }
            final String fk =
                ((MondrianDef.Column) condition.getLeft()).name;
            final String pk =
                ((MondrianDef.Column) condition.getRight()).name;
            final String fkOffender = sourceTableLacking(fk);
            if (fkOffender != null) {
                throw skip(
                    NativeSqlCalc.TemplateSkipReason.FK_MISSING_ON_SOURCE,
                    fkOffender, fk);
            }
            final String dimTableName = dimTable.getTableName();
            final Set<String> dimColumns =
                NativeSqlCalc.loadTableColumns(dataSource, dimTableName);
            if (dimColumns.isEmpty()
                || !dimColumns.contains(columnName)
                || !dimColumns.contains(pk))
            {
                throw skip(
                    NativeSqlCalc.TemplateSkipReason.DIM_COLUMN_MISSING,
                    dimTableName, columnName);
            }
            final String alias = aliasFor(dimTableName, fk, pk);
            LOGGER.info(
                "NativeSqlCalc [{}] template[{}]: ${{factJoins}} binds"
                + " '{}' via {} (FK {})",
                measureName, templateIndex, columnName, dimTableName, fk);
            return alias + "." + quote(columnName);
        }

        /**
         * Returns the first {@code f}-bound table whose readable
         * metadata lacks the column, or null when nothing is provably
         * missing (present everywhere, or no metadata — fail-open).
         */
        private String sourceTableLacking(String columnName) {
            for (String table : sourceTables) {
                final Set<String> columns =
                    NativeSqlCalc.loadTableColumns(dataSource, table);
                if (!columns.isEmpty() && !columns.contains(columnName)) {
                    return table;
                }
            }
            return null;
        }

        private String aliasFor(String dimTable, String fk, String pk) {
            final String key = dimTable + ' ' + fk + ' ' + pk;
            String alias = aliasByJoinKey.get(key);
            if (alias == null) {
                alias = ALIAS_PREFIX + aliasByJoinKey.size();
                aliasByJoinKey.put(key, alias);
                joinClauses.add(
                    joinKeyword() + " " + quote(dimTable) + " " + alias
                    + " ON f." + quote(fk)
                    + " = " + alias + "." + quote(pk));
            }
            return alias;
        }

        String renderJoins() {
            return String.join("\n", joinClauses);
        }

        private String joinKeyword() {
            // LEFT ANY JOIN makes fan-out physically impossible on
            // ClickHouse even with duplicate PKs in the dim table;
            // elsewhere dim-PK uniqueness is the documented contract.
            return dialect != null
                && dialect.getDatabaseProduct()
                    == Dialect.DatabaseProduct.CLICKHOUSE
                ? "LEFT ANY JOIN"
                : "LEFT JOIN";
        }

        private String quote(String identifier) {
            return dialect == null
                ? identifier
                : dialect.quoteIdentifier(identifier);
        }

        private SkipTemplate skip(
            NativeSqlCalc.TemplateSkipReason reason,
            String tableName,
            String columnName)
        {
            return new SkipTemplate(
                new NativeSqlCalc.TemplateColumnSkip(
                    templateIndex,
                    tableName,
                    new LinkedHashSet<String>(
                        Collections.singletonList(columnName)),
                    reason));
        }
    }
}
