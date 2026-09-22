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

import mondrian.olap.Annotation;
import mondrian.olap.MondrianException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;

/**
 * Parses native SQL measure configuration from {@code nativeSql.*}
 * schema annotations on a {@link RolapCalculatedMember}.
 *
 * <p>A native SQL measure replaces per-tuple MDX evaluation with a
 * single batch SQL query defined by a user-provided template. Mondrian
 * substitutes context-specific placeholders ({@code ${factTable}},
 * {@code ${whereClause}}, {@code ${axisExprN}}, etc.) and executes
 * the resulting SQL, caching results keyed by {@code k1..kN, val}.
 *
 * <p>Configuration example:
 * <pre>{@code
 * <CalculatedMember name="WD %" dimension="Measures">
 *   <Annotation name="nativeSql.enabled">true</Annotation>
 *   <Annotation name="nativeSql.template"><![CDATA[
 *     SELECT ${axisExpr1} AS k1, ... AS val FROM agg_table ...
 *   ]]></Annotation>
 *   <Annotation name="nativeSql.template.1"><![CDATA[
 *     SELECT ${axisExpr1} AS k1, ... AS val FROM fact_table ...
 *   ]]></Annotation>
 *   <Annotation name="nativeSql.variables">
 *     weightMeasure=sales_rub;multiplier=100
 *   </Annotation>
 *   <Annotation name="nativeSql.maxAxes">2</Annotation>
 *   <Annotation name="nativeSql.fallbackMdx">true</Annotation>
 *   <Formula>... (last-resort MDX fallback) ...</Formula>
 * </CalculatedMember>
 * }</pre>
 *
 * <p>Templates are tried in order: {@code template} &rarr;
 * {@code template.1} &rarr; {@code template.2} &rarr; ... &rarr;
 * {@code <Formula>}. Collection stops at the first gap or blank
 * value in the numbering sequence.
 */
public class NativeSqlConfig {

    private static final Logger LOGGER =
        LogManager.getLogger(NativeSqlConfig.class);

    private static final String PREFIX = "nativeSql.";
    private enum ValueKind { TEXT, BOOLEAN, INTEGER }

    /** Shared definitions for parsing and schema-load diagnostics. */
    private enum Setting {
        ENABLED("enabled", ValueKind.BOOLEAN),
        TEMPLATE("template", ValueKind.TEXT),
        VARIABLES("variables", ValueKind.TEXT),
        MAX_AXES("maxAxes", ValueKind.INTEGER),
        FALLBACK_MDX("fallbackMdx", ValueKind.BOOLEAN),
        RELATION_ALIAS("relationAlias", ValueKind.TEXT),
        SCALAR("scalar", ValueKind.BOOLEAN),
        ROLLUP_AXES("rollupAxes", ValueKind.BOOLEAN),
        FALLBACK_ON_MISSING_ROW_KEY("fallbackOnMissingRowKey", ValueKind.BOOLEAN);

        final String key;
        final ValueKind kind;

        Setting(String suffix, ValueKind kind) {
            this.key = PREFIX + suffix;
            this.kind = kind;
        }

        static Setting forKey(String key) {
            for (Setting setting : values()) {
                if (setting.key.equals(key)) {
                    return setting;
                }
            }
            return null;
        }
    }

    static final String ANN_ENABLED = Setting.ENABLED.key;
    static final String ANN_TEMPLATE = Setting.TEMPLATE.key;
    static final String ANN_VARIABLES = Setting.VARIABLES.key;
    static final String ANN_MAX_AXES = Setting.MAX_AXES.key;
    static final String ANN_FALLBACK_MDX = Setting.FALLBACK_MDX.key;
    static final String ANN_RELATION_ALIAS = Setting.RELATION_ALIAS.key;
    static final String ANN_SCALAR = Setting.SCALAR.key;
    static final String ANN_TEMPLATE_PREFIX = ANN_TEMPLATE + ".";
    static final String ANN_ROLLUP_AXES = Setting.ROLLUP_AXES.key;
    static final String ANN_FALLBACK_ON_MISSING_ROW_KEY =
        Setting.FALLBACK_ON_MISSING_ROW_KEY.key;

    private static final String DEFAULT_RELATION_ALIAS = "pr";
    private static final String AXIS_RESULT_SELECT_LIST =
        "${axisResultSelectList}";
    private static final String AXIS_GROUP_BY_LIST = "${axisGroupByList}";

    private NativeSqlConfig() {}

    /**
     * Warns about ignored names, unreachable templates and malformed values at
     * schema load, for a calculated member: the only element whose nativeSql
     * annotations are read. {@code annotations} is the parsed view, where a
     * repeated name already kept its last value.
     * Keep this separate from runtime parsing, which can be called for every
     * cell. Values may contain SQL or other private configuration, so only
     * the member and annotation names belong in these diagnostics.
     */
    static void validateAnnotations(
        String measureName,
        Map<String, Annotation> annotations)
    {
        final List<String> templates = readTemplateChain(annotations);
        boolean configured = false;
        boolean numberedTemplates = false;
        for (String name : annotations.keySet()) {
            if (!isNativeSqlName(name)) {
                continue;
            }
            final Setting setting = Setting.forKey(name);
            final boolean numberedTemplate = isNumberedTemplateAnnotation(name);
            configured |= setting != null || numberedTemplate;
            numberedTemplates |= numberedTemplate;
            if (setting == null && !numberedTemplate)
            {
                warnUnknown(measureName, name);
            } else if (numberedTemplate) {
                int index = Integer.parseInt(name.substring(ANN_TEMPLATE_PREFIX.length()));
                if (index >= templates.size()) {
                    LOGGER.warn(
                        "NativeSqlConfig [{}]: annotation '{}' is ignored;"
                        + " template collection stops at the first missing"
                        + " or blank template",
                        measureName, name);
                }
            } else if (setting.kind == ValueKind.INTEGER) {
                String value = getAnnString(annotations, name);
                try {
                    Integer.parseInt(value == null ? "" : value.trim());
                } catch (NumberFormatException e) {
                    LOGGER.warn(
                        "NativeSqlConfig [{}]: annotation '{}' requires an"
                        + " integer value; the default is used",
                        measureName, name);
                }
            } else if (setting.kind == ValueKind.BOOLEAN) {
                String value = getAnnString(annotations, name);
                if (value == null
                    || !("true".equalsIgnoreCase(value.trim())
                        || "false".equalsIgnoreCase(value.trim())))
                {
                    LOGGER.warn(
                        "NativeSqlConfig [{}]: annotation '{}' requires a"
                        + " boolean value (true or false)",
                        measureName, name);
                }
            }
        }
        if (configured) {
            validateEnabledDefinition(
                measureName, annotations, templates, numberedTemplates);
        }
    }

    /** Settings that parse to nothing although their names are known. */
    private static void validateEnabledDefinition(
        String measureName,
        Map<String, Annotation> annotations,
        List<String> templates,
        boolean numberedTemplates)
    {
        if (getAnnString(annotations, ANN_ENABLED) == null) {
            LOGGER.warn(
                "NativeSqlConfig [{}]: nativeSql annotations are ignored"
                + " because '{}' is missing",
                measureName, ANN_ENABLED);
            return;
        }
        if (!isEnabled(annotations)) {
            return;
        }
        if (templates.isEmpty() && !numberedTemplates) {
            LOGGER.warn(
                "NativeSqlConfig [{}]: '{}' is true but '{}' is missing or"
                + " blank; the formula is evaluated instead",
                measureName, ANN_ENABLED, ANN_TEMPLATE);
        }
        final Variables variables =
            readVariables(getAnnString(annotations, ANN_VARIABLES));
        if (variables.unnamed() > 0) {
            LOGGER.warn(
                "NativeSqlConfig [{}]: annotation '{}' has {} entries that are"
                + " not 'name=value'; they are ignored",
                measureName, ANN_VARIABLES, variables.unnamed());
        }
        if (variables.repeated() > 0) {
            LOGGER.warn(
                "NativeSqlConfig [{}]: annotation '{}' repeats a variable name"
                + " {} time(s); the last value is used",
                measureName, ANN_VARIABLES, variables.repeated());
        }
        final String relationAlias = relationAlias(annotations);
        for (int i = 0; i < templates.size(); i++) {
            final String template = templates.get(i);
            if ((template.contains(AXIS_RESULT_SELECT_LIST)
                    || template.contains(AXIS_GROUP_BY_LIST))
                && !template.contains(relationAlias))
            {
                LOGGER.warn(
                    "NativeSqlConfig [{}]: template[{}] uses {} or {} without"
                    + " the relation alias set by '{}' (default '{}');"
                    + " axis queries will fail at runtime",
                    measureName, i, AXIS_RESULT_SELECT_LIST,
                    AXIS_GROUP_BY_LIST, ANN_RELATION_ALIAS,
                    DEFAULT_RELATION_ALIAS);
            }
        }
    }

    /**
     * Warns, at schema load, that nativeSql annotations on an element other
     * than a calculated member are never read.
     */
    static void validateIgnoredAnnotations(
        String element,
        Collection<String> names)
    {
        for (String name : names) {
            if (isNativeSqlName(name)) {
                LOGGER.warn(
                    "NativeSqlConfig: annotation '{}' on {} is ignored;"
                    + " nativeSql annotations are read only on calculated"
                    + " members",
                    name, element);
            }
        }
    }

    /** Names the parser reads are case-sensitive; this match is not. */
    private static boolean isNativeSqlName(String name) {
        return name != null
            && name.regionMatches(true, 0, PREFIX, 0, PREFIX.length());
    }

    private static void warnUnknown(String measureName, String name) {
        final String supported = supportedSpelling(name);
        if (supported == null) {
            LOGGER.warn(
                "NativeSqlConfig [{}]: unknown annotation '{}' is ignored",
                measureName, name);
        } else {
            LOGGER.warn(
                "NativeSqlConfig [{}]: unknown annotation '{}' is ignored;"
                + " names are case-sensitive, the supported name is '{}'",
                measureName, name, supported);
        }
    }

    /** The name the parser reads that matches {@code name} ignoring case. */
    private static String supportedSpelling(String name) {
        for (Setting setting : Setting.values()) {
            if (setting.key.equalsIgnoreCase(name)) {
                return setting.key;
            }
        }
        final int prefix = ANN_TEMPLATE_PREFIX.length();
        if (name.regionMatches(true, 0, ANN_TEMPLATE_PREFIX, 0, prefix)) {
            final String numbered =
                ANN_TEMPLATE_PREFIX + name.substring(prefix);
            if (isNumberedTemplateAnnotation(numbered)) {
                return numbered;
            }
        }
        return null;
    }

    private static boolean isNumberedTemplateAnnotation(String name) {
        if (!name.startsWith(ANN_TEMPLATE_PREFIX)) {
            return false;
        }
        String suffix = name.substring(ANN_TEMPLATE_PREFIX.length());
        try {
            int index = Integer.parseInt(suffix);
            // Runtime lookup constructs canonical positive decimal names.
            return index > 0 && Integer.toString(index).equals(suffix);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Returns true if native SQL evaluation is globally enabled
     * via {@code mondrian.native.sql.enable=true}.
     */
    public static boolean isGloballyEnabled() {
        return mondrian.olap.MondrianProperties.instance()
            .NativeSqlEnable.get();
    }

    /**
     * Parses annotations into NativeSqlDef. Returns null if
     * {@code nativeSql.enabled} is missing/false or template is absent.
     */
    public static NativeSqlDef fromAnnotations(
        String measureName,
        Map<String, Annotation> annotations)
    {
        if (!isEnabled(annotations)) {
            return null;
        }
        List<String> templates = readTemplateChain(annotations);
        if (templates.isEmpty()) {
            return null;
        }
        Map<String, String> variables = parseVariables(
            getAnnString(annotations, ANN_VARIABLES));
        int maxAxes = parseInt(
            getAnnString(annotations, ANN_MAX_AXES), 10);
        boolean fallbackMdx = parseBoolean(
            getAnnString(annotations, ANN_FALLBACK_MDX), true);

        String relationAlias = relationAlias(annotations);

        boolean scalar = parseBoolean(
            getAnnString(annotations, ANN_SCALAR), false);

        boolean rollupAxes = parseBoolean(
            getAnnString(annotations, ANN_ROLLUP_AXES), false);

        // #89: opt-in — when a SUCCESS batch does not contain the
        // cell's rowKey, route to the MDX fallback instead of returning
        // a bare null. Default false in every context, grand total
        // included: a rowKey miss is the normal empty-cell signal NON
        // EMPTY relies on, and an unconditional fallback would evaluate
        // MDX for every empty cell.
        boolean fallbackOnMissingRowKey = parseBoolean(
            getAnnString(annotations, ANN_FALLBACK_ON_MISSING_ROW_KEY),
            false);

        validateCubeMacroOptIn(measureName, templates, rollupAxes);

        return new NativeSqlDef(
            measureName, templates, variables, maxAxes, fallbackMdx,
            relationAlias, scalar, rollupAxes, fallbackOnMissingRowKey);
    }

    private static boolean isEnabled(Map<String, Annotation> annotations) {
        final String enabled = getAnnString(annotations, ANN_ENABLED);
        return enabled != null && "true".equalsIgnoreCase(enabled.trim());
    }

    private static String relationAlias(Map<String, Annotation> annotations) {
        final String alias = getAnnString(annotations, ANN_RELATION_ALIAS);
        return alias == null || alias.trim().isEmpty()
            ? DEFAULT_RELATION_ALIAS
            : alias.trim();
    }

    private static List<String> readTemplateChain(
        Map<String, Annotation> annotations)
    {
        String primary = getAnnString(annotations, ANN_TEMPLATE);
        if (primary == null || primary.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> templates = new ArrayList<>();
        templates.add(primary.trim());
        for (int i = 1; ; i++) {
            String alternative = getAnnString(
                annotations, ANN_TEMPLATE_PREFIX + i);
            if (alternative == null || alternative.trim().isEmpty()) {
                return templates;
            }
            templates.add(alternative.trim());
        }
    }

    /**
     * Searches for nativeSql annotations on the member itself, then
     * on the base cube's source member (for VirtualCube references).
     */
    public static NativeSqlDef fromMember(RolapCalculatedMember member) {
        NativeSqlDef def = fromAnnotations(
            member.getName(), member.getAnnotationMap());
        if (def != null) {
            return def;
        }
        Map<String, Annotation> baseAnns =
            findBaseCubeAnnotations(member);
        if (baseAnns != null) {
            return fromAnnotations(member.getName(), baseAnns);
        }
        return null;
    }

    /**
     * Resolves a member to the underlying calculated member that has
     * nativeSql configuration, unwrapping cube/delegating wrappers.
     *
     * <p>Returns null if the member is not a calculated member or does not
     * have nativeSql annotations.
     */
    public static RolapCalculatedMember findNativeSqlMember(
        RolapMember member)
    {
        RolapMember current = member;
        while (current instanceof DelegatingRolapMember) {
            current = ((DelegatingRolapMember) current).member;
        }
        if (!(current instanceof RolapCalculatedMember)) {
            return null;
        }
        final RolapCalculatedMember calcMember =
            (RolapCalculatedMember) current;
        return fromMember(calcMember) == null ? null : calcMember;
    }

    private static Map<String, Annotation> findBaseCubeAnnotations(
        RolapCalculatedMember member)
    {
        RolapCube baseCube = member.getBaseCube();
        if (baseCube == null) {
            return null;
        }
        // Look up the XML definition of this calculated member in the
        // base cube's schema, which preserves the annotations.
        final mondrian.olap.MondrianDef.CalculatedMember xmlCalcMember =
            baseCube.getSchema().lookupXmlCalculatedMember(
                member.getUniqueName(), baseCube.getName());
        if (xmlCalcMember == null || xmlCalcMember.annotations == null) {
            return null;
        }
        return RolapHierarchy.createAnnotationMap(xmlCalcMember.annotations);
    }

    static Map<String, String> parseVariables(String raw) {
        return readVariables(raw).values();
    }

    /**
     * Parses {@code name=value} entries separated by {@code ;}. Empty
     * entries are skipped, an entry without a name is dropped and a
     * repeated name keeps its last value; the counts of the last two let
     * schema load report them.
     */
    private static Variables readVariables(String raw) {
        final Map<String, String> vars = new LinkedHashMap<String, String>();
        int unnamed = 0;
        int repeated = 0;
        if (raw != null) {
            for (String pair : raw.split(";")) {
                String trimmed = pair.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                int eq = trimmed.indexOf('=');
                if (eq <= 0) {
                    unnamed++;
                } else if (vars.put(
                        trimmed.substring(0, eq).trim(),
                        trimmed.substring(eq + 1).trim()) != null)
                {
                    repeated++;
                }
            }
        }
        return new Variables(vars, unnamed, repeated);
    }

    private record Variables(
        Map<String, String> values, int unnamed, int repeated) {}

    private static String getAnnString(
        Map<String, Annotation> anns, String key)
    {
        Annotation a = anns.get(key);
        return a != null && a.getValue() != null
            ? a.getValue().toString() : null;
    }

    private static int parseInt(String s, int defaultVal) {
        if (s == null) return defaultVal;
        try { return Integer.parseInt(s.trim()); }
        catch (NumberFormatException e) { return defaultVal; }
    }

    private static boolean parseBoolean(String s, boolean defaultVal) {
        if (s == null) return defaultVal;
        return Boolean.parseBoolean(s.trim());
    }

    /**
     * Enforces Contract A from the rollup-axes design spec:
     * {@code nativeSql.rollupAxes=true} must be paired with BOTH
     * {@code ${axisGroupByListCube}} AND {@code ${axisCubeSelectFlags}}
     * in every template; either macro alone (without the flag, or vice
     * versa, or only one of the pair) is rejected.
     *
     * @throws MondrianException if any template violates the contract
     */
    private static void validateCubeMacroOptIn(
        String measureName,
        List<String> templates,
        boolean rollupAxes)
    {
        final String GBL_CUBE = "${axisGroupByListCube}";
        final String SELFLAGS = "${axisCubeSelectFlags}";

        for (int i = 0; i < templates.size(); i++) {
            String t = templates.get(i);
            boolean hasGbl = t.contains(GBL_CUBE);
            boolean hasFlags = t.contains(SELFLAGS);

            // Pair-check: both or neither, regardless of rollupAxes flag.
            if (hasGbl != hasFlags) {
                throw new MondrianException(
                    "NativeSqlConfig: template[" + i + "] for [" + measureName
                    + "] must contain BOTH ${axisGroupByListCube} AND "
                    + "${axisCubeSelectFlags}, or neither. Found: "
                    + "axisGroupByListCube=" + hasGbl
                    + ", axisCubeSelectFlags=" + hasFlags);
            }

            if (rollupAxes && !hasGbl) {
                throw new MondrianException(
                    "NativeSqlConfig: nativeSql.rollupAxes=true requires every "
                    + "template to contain ${axisGroupByListCube} and "
                    + "${axisCubeSelectFlags}; template[" + i
                    + "] for [" + measureName + "] is missing them.");
            }

            if (!rollupAxes && hasGbl) {
                throw new MondrianException(
                    "NativeSqlConfig: template[" + i + "] for [" + measureName
                    + "] uses cube macros (${axisGroupByListCube} + "
                    + "${axisCubeSelectFlags}) but nativeSql.rollupAxes is not "
                    + "true. Either set nativeSql.rollupAxes=true or remove the "
                    + "cube macros.");
            }
        }
    }

    /**
     * Parsed configuration for a native SQL measure.
     */
    public static class NativeSqlDef {
        private final String measureName;
        private final List<String> templates;
        private final Map<String, String> variables;
        private final int maxAxes;
        private final boolean fallbackMdx;
        private final String relationAlias;
        private final boolean scalar;
        private final boolean rollupAxes;
        private final boolean fallbackOnMissingRowKey;

        NativeSqlDef(
            String measureName,
            List<String> templates,
            Map<String, String> variables,
            int maxAxes,
            boolean fallbackMdx,
            String relationAlias,
            boolean scalar,
            boolean rollupAxes,
            boolean fallbackOnMissingRowKey)
        {
            this.measureName = measureName;
            this.templates = Collections.unmodifiableList(templates);
            this.variables = Collections.unmodifiableMap(variables);
            this.maxAxes = maxAxes;
            this.fallbackMdx = fallbackMdx;
            this.relationAlias = relationAlias;
            this.scalar = scalar;
            this.rollupAxes = rollupAxes;
            this.fallbackOnMissingRowKey = fallbackOnMissingRowKey;
        }

        public String getMeasureName() { return measureName; }
        /** Returns the primary template (first in list). */
        public String getTemplate() { return templates.get(0); }
        /** Returns all templates in fallback order. */
        public List<String> getTemplates() { return templates; }
        public Map<String, String> getVariables() { return variables; }
        public String getVariable(String key) { return variables.get(key); }
        public int getMaxAxes() { return maxAxes; }
        public boolean isFallbackMdx() { return fallbackMdx; }
        /** Returns the relation alias used by axis macros (default: "pr"). */
        public String getRelationAlias() { return relationAlias; }
        /**
         * Returns true if this measure is scalar: its SQL executes once
         * per query context and the single value is replicated for every
         * axis member. Contract note (#89): {@code nativeSql.scalar} is
         * NOT a per-context grand-total template selector. A grand-total
         * cell whose template returns no row stays null unless the
         * measure sets {@code nativeSql.fallbackOnMissingRowKey}.
         */
        public boolean isScalar() { return scalar; }
        /** Returns true if this measure rolls up axis cells via WITH CUBE / GROUPING SETS. */
        public boolean isRollupAxes() { return rollupAxes; }
        /**
         * Returns true when a SUCCESS batch that lacks the cell's rowKey
         * should route to the MDX fallback instead of returning a bare
         * null (#89). Off by default — see
         * {@link NativeSqlConfig#ANN_FALLBACK_ON_MISSING_ROW_KEY}.
         */
        public boolean isFallbackOnMissingRowKey() {
            return fallbackOnMissingRowKey;
        }
    }
}
