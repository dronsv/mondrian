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
import mondrian.olap.MondrianProperties;
import mondrian.spi.Dialect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
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
 *
 * <p>A predicate bound this way filters on the dim table's column, and no
 * database derives a fact-key range from that: the fact is scanned whole.
 * So each star-joined dim table also gets the fact-side condition its
 * predicates imply, {@code f.fk IN (SELECT pk FROM dim WHERE …)} — see
 * {@link FkPushdown}. It reads the dim table a second time, which is only
 * the same table for a plain stored one that is not rewritten while
 * queries run; {@link JoinContext#fkPushdowns} declines what it can tell.
 */
final class NativeSqlFactJoins {

    private static final Logger LOGGER =
        LogManager.getLogger(NativeSqlFactJoins.class);

    /** The literal opt-in placeholder. */
    static final String PLACEHOLDER_TOKEN = "${factJoins}";

    /** Engine-owned dim-table alias prefix: {@code nscd0}, {@code nscd1}, … */
    static final String ALIAS_PREFIX = "nscd";

    /** Alias prefix of the same dim table inside its FK-pushdown subquery. */
    static final String PUSHDOWN_ALIAS_PREFIX = "nscs";

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

    /** Per data source: whether a limit on IN sets is in effect. */
    private static final Map<DataSource, Boolean> SET_LIMITS =
        Collections.synchronizedMap(
            new IdentityHashMap<DataSource, Boolean>());

    static void clearCache() {
        SET_LIMITS.clear();
    }

    /**
     * ClickHouse bounds the set behind IN by {@code max_rows_in_set} /
     * {@code max_bytes_in_set}, which a JOIN never sees: with
     * {@code set_overflow_mode = 'break'} an overflowing key set is cut
     * short without an error and the pushdown would drop rows. Unknown
     * counts as limited.
     */
    private static boolean setLimited(Dialect dialect, DataSource dataSource) {
        if (dialect == null
            || dialect.getDatabaseProduct()
                != Dialect.DatabaseProduct.CLICKHOUSE)
        {
            return false;
        }
        final Boolean known = SET_LIMITS.get(dataSource);
        if (known != null) {
            return known;
        }
        boolean limited = true;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                 "SELECT count() FROM system.settings WHERE name IN"
                 + " ('max_rows_in_set', 'max_bytes_in_set')"
                 + " AND value != '0'"))
        {
            limited = !rs.next() || rs.getLong(1) != 0;
        } catch (SQLException | RuntimeException e) {
            LOGGER.debug("cannot read the IN set limits", e);
        }
        SET_LIMITS.put(dataSource, limited);
        return limited;
    }

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

    /** True when any template of the fallback chain opts into
     *  {@code ${factJoins}} — the gate for relaxing the rollupAxes
     *  synthetic-binding pre-validation (M2). */
    static boolean chainContainsPlaceholder(
        java.util.List<String> templates)
    {
        if (templates == null) {
            return false;
        }
        for (String t : templates) {
            if (t != null && t.contains(PLACEHOLDER_TOKEN)) {
                return true;
            }
        }
        return false;
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
            if (MondrianProperties.instance()
                .NativeSqlFactJoinsFkPushdown.get())
            {
                predicates.addAll(ctx.fkPushdowns(predicates));
            }
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
            final DimJoin join =
                ctx.resolveJoin(atomic.columnName, atomic.starColumn);
            if (join == null) {
                return atomic;
            }
            final NativeSqlCalc.AtomicPredicateInfo rebased =
                atomic.withQualifiedExpr(
                    ctx.qualify(join.alias(), atomic.columnName));
            ctx.starJoined(rebased, join);
            return rebased;
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
        private final Map<String, DimJoin> joinsByKey =
            new LinkedHashMap<String, DimJoin>();
        private final List<String> joinClauses = new ArrayList<String>();
        /** Rebased predicate atom to the join its column came through. */
        private final Map<NativeSqlCalc.AtomicPredicateInfo, DimJoin>
            starJoinedAtoms =
                new IdentityHashMap<
                    NativeSqlCalc.AtomicPredicateInfo, DimJoin>();

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
            final DimJoin join = resolveJoin(columnName, starColumn);
            return join == null ? null : qualify(join.alias(), columnName);
        }

        /** The star join the column must go through; null and
         *  {@link SkipTemplate} as for {@link #requalify}. */
        DimJoin resolveJoin(String columnName, RolapStar.Column starColumn) {
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
            final DimJoin join = joinFor(dimTableName, fk, pk);
            LOGGER.info(
                "NativeSqlCalc [{}] template[{}]: ${{factJoins}} binds"
                + " '{}' via {} (FK {})",
                measureName, templateIndex, columnName, dimTableName, fk);
            return join;
        }

        String qualify(String alias, String columnName) {
            return alias + "." + quote(columnName);
        }

        void starJoined(
            NativeSqlCalc.AtomicPredicateInfo rebased, DimJoin join)
        {
            starJoinedAtoms.put(rebased, join);
        }

        /**
         * One {@link FkPushdown} per dim join that has a predicate atom
         * worth pushing, in join order.
         */
        List<FkPushdown> fkPushdowns(
            List<NativeSqlCalc.PredicateInfo> rebasedPredicates)
        {
            final List<NativeSqlCalc.PredicateInfo> source =
                Collections.unmodifiableList(
                    new ArrayList<NativeSqlCalc.PredicateInfo>(
                        rebasedPredicates));
            final List<FkPushdown> pushdowns = new ArrayList<FkPushdown>();
            if (joinsByKey.isEmpty()) {
                return pushdowns;
            }
            // A Distributed source resolves the subquery's table on the
            // shards, where an unqualified name may not exist.
            for (String table : sourceTables) {
                if (!NativeSqlCalc.isPlainTable(dataSource, table)) {
                    return declined("source " + table + " is no plain table");
                }
            }
            if (setLimited(dialect, dataSource)) {
                return declined("a limit on IN sets is in effect");
            }
            for (DimJoin join : joinsByKey.values()) {
                // A view may give other rows on its second read, or cost
                // as much again. IN casts f.fk to an Enum pk and throws on
                // a value outside it; the JOIN compares them as strings.
                if (!NativeSqlCalc.isPlainTable(dataSource, join.dimTable())
                    || !hasPlainType(join.dimTable(), join.pk()))
                {
                    declined(
                        join.dimTable() + " is no plain table with a"
                        + " plainly typed key " + join.pk());
                    continue;
                }
                final String inner = PUSHDOWN_ALIAS_PREFIX + join.index();
                final Map<NativeSqlCalc.AtomicPredicateInfo, String> atoms =
                    new IdentityHashMap<
                        NativeSqlCalc.AtomicPredicateInfo, String>();
                for (Map.Entry<NativeSqlCalc.AtomicPredicateInfo, DimJoin> e
                    : starJoinedAtoms.entrySet())
                {
                    final NativeSqlCalc.AtomicPredicateInfo atom = e.getKey();
                    if (e.getValue().equals(join)
                        && FkPushdown.falseOnUnmatchedRow(atom.sqlTail)
                        && hasPlainType(join.dimTable(), atom.columnName))
                    {
                        atoms.put(
                            atom,
                            qualify(inner, atom.columnName)
                            + " " + atom.sqlTail);
                    }
                }
                if (atoms.isEmpty()) {
                    continue;
                }
                // kept even when ${whereClause} renders nothing of it: a
                // ${whereClauseExcept:…} site may
                final FkPushdown pushdown = new FkPushdown(
                    source,
                    atoms,
                    "f." + quote(join.fk()) + " IN (SELECT "
                    + qualify(inner, join.pk()) + " FROM "
                    + quote(join.dimTable()) + " " + inner
                    + " WHERE ");
                pushdowns.add(pushdown);
                final String rendered = pushdown.render(null);
                if (rendered != null) {
                    LOGGER.info(
                        "NativeSqlCalc [{}] template[{}]: ${{factJoins}}"
                        + " pushes down {}",
                        measureName, templateIndex, rendered);
                }
            }
            return pushdowns;
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

        /**
         * True when the driver named the column's type and it is no Enum:
         * an unmatched outer-join row carries the first Enum value, which
         * — unlike the zero-like defaults of other types — no literal
         * reveals.
         */
        private boolean hasPlainType(String table, String column) {
            final String type =
                NativeSqlCalc.columnTypeName(dataSource, table, column);
            return type != null
                && !type.toLowerCase(Locale.ROOT).contains("enum");
        }

        /** Says why, at the level of the "binds" line it answers. */
        private List<FkPushdown> declined(String reason) {
            LOGGER.info(
                "NativeSqlCalc [{}] template[{}]: no FK pushdown, {}",
                measureName, templateIndex, reason);
            return Collections.<FkPushdown>emptyList();
        }

        private DimJoin joinFor(String dimTable, String fk, String pk) {
            final String key = dimTable + '\0' + fk + '\0' + pk;
            DimJoin join = joinsByKey.get(key);
            if (join == null) {
                join = new DimJoin(joinsByKey.size(), dimTable, fk, pk);
                joinsByKey.put(key, join);
                joinClauses.add(
                    joinKeyword() + " " + quote(dimTable) + " "
                    + join.alias() + " ON f." + quote(fk)
                    + " = " + qualify(join.alias(), pk));
            }
            return join;
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

    /** One rendered star join: {@code f.fk = nscd<index>.pk}. */
    private record DimJoin(int index, String dimTable, String fk, String pk) {
        String alias() {
            return ALIAS_PREFIX + index;
        }
    }

    /**
     * The fact-side condition implied by the predicates of one star-joined
     * dim table: {@code f.fk IN (SELECT pk FROM dim WHERE …)}.
     *
     * <p>It must never reject a row the predicates accept, so the inner
     * condition is a weakening of them. An atom of another table counts
     * as true; an AND keeps what is left, an OR with a true branch is
     * true and ends the pushdown. An atom is only taken when it is false
     * on a fact row without a dim row ({@link #falseOnUnmatchedRow}) —
     * such a row passes the outer join but can never be in the key set.
     *
     * <p>Rendering follows the exclusion names of
     * {@code ${whereClauseExcept:…}}: the weakening is taken of what the
     * predicates render to, not of what they were built from.
     */
    static final class FkPushdown extends NativeSqlCalc.PredicateInfo {
        /**
         * A longer condition is a wide member list: its key set prunes
         * little, and repeating it at every WHERE site walks the statement
         * into the server's query size limit (256 KiB on ClickHouse).
         */
        static final int MAX_INNER_LENGTH = 4096;

        private final List<NativeSqlCalc.PredicateInfo> source;
        private final Map<NativeSqlCalc.AtomicPredicateInfo, String> atoms;
        private final String head;

        FkPushdown(
            List<NativeSqlCalc.PredicateInfo> source,
            Map<NativeSqlCalc.AtomicPredicateInfo, String> atoms,
            String head)
        {
            this.source = source;
            this.atoms = atoms;
            this.head = head;
        }

        @Override
        String render(Set<String> exceptNames) {
            final Weakened inner = weakenAll(source, "AND", exceptNames);
            return inner.kind() == Weakened.Kind.SQL
                && inner.sql().length() <= MAX_INNER_LENGTH
                ? head + inner.sql() + ")"
                : null;
        }

        private Weakened weaken(
            NativeSqlCalc.PredicateInfo p, Set<String> exceptNames)
        {
            if (p instanceof NativeSqlCalc.AtomicPredicateInfo atom) {
                if (atom.render(exceptNames) == null) {
                    return Weakened.ABSENT;
                }
                final String sql = atoms.get(atom);
                return sql == null ? Weakened.TRUE : Weakened.of(sql);
            }
            if (p instanceof NativeSqlCalc.CompositePredicateInfo composite
                && ("AND".equals(composite.op) || "OR".equals(composite.op)))
            {
                final Weakened w =
                    weakenAll(composite.children, composite.op, exceptNames);
                return w.kind() == Weakened.Kind.SQL && w.parts() > 1
                    ? Weakened.of("(" + w.sql() + ")")
                    : w;
            }
            return Weakened.TRUE;
        }

        private Weakened weakenAll(
            List<NativeSqlCalc.PredicateInfo> children,
            String op,
            Set<String> exceptNames)
        {
            final Set<String> parts = new LinkedHashSet<String>();
            boolean present = false;
            for (NativeSqlCalc.PredicateInfo child : children) {
                final Weakened w = weaken(child, exceptNames);
                if (w.kind() == Weakened.Kind.ABSENT) {
                    continue;
                }
                present = true;
                if (w.kind() == Weakened.Kind.SQL) {
                    parts.add(w.sql());
                } else if ("OR".equals(op)) {
                    return Weakened.TRUE;
                }
            }
            if (!present) {
                return Weakened.ABSENT;
            }
            return parts.isEmpty()
                ? Weakened.TRUE
                : new Weakened(
                    Weakened.Kind.SQL,
                    String.join(" " + op + " ", parts),
                    parts.size());
        }

        /**
         * True when {@code column <tail>} cannot hold on the right side of
         * an outer join that found no match. Such a row carries NULLs or,
         * on ClickHouse, the type's default — so only an equality with a
         * literal that is no default of any type qualifies.
         */
        static boolean falseOnUnmatchedRow(String sqlTail) {
            if (sqlTail == null || !sqlTail.startsWith("= ")) {
                return false;
            }
            String literal = sqlTail.substring(2).trim();
            if (literal.equalsIgnoreCase("NULL")) {
                return false;
            }
            if (literal.length() >= 2
                && literal.startsWith("'") && literal.endsWith("'"))
            {
                literal = literal.substring(1, literal.length() - 1);
            }
            return !isTypeDefault(literal);
        }

        private static boolean isTypeDefault(String literal) {
            if (literal.equalsIgnoreCase("false")
                // the epoch, as a date or a datetime in any time zone
                || literal.startsWith("1970-01-01")
                || literal.startsWith("1969-12-31"))
            {
                return true;
            }
            try {
                return new BigDecimal(literal).signum() == 0;
            } catch (NumberFormatException e) {
                // '', '00:00:00', '0.0.0.0', '::', the zero UUID, NULs
                return literal.chars().allMatch(
                    c -> c == '0' || c == 0 || ":.-/ ".indexOf(c) >= 0);
            }
        }
    }

    /** What is left of a predicate after weakening it to one dim table. */
    private record Weakened(Kind kind, String sql, int parts) {
        enum Kind {
            /** Not rendered at all (excluded): as if it was never there. */
            ABSENT,
            /** Says nothing about the dim table. */
            TRUE,
            SQL
        }

        static final Weakened ABSENT = new Weakened(Kind.ABSENT, null, 0);
        static final Weakened TRUE = new Weakened(Kind.TRUE, null, 0);

        static Weakened of(String sql) {
            return new Weakened(Kind.SQL, sql, 1);
        }
    }
}
