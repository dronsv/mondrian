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
 *     SELECT ${axisExpr1} AS k1, ... AS val FROM ...
 *   ]]></Annotation>
 *   <Annotation name="nativeSql.variables">
 *     weightMeasure=sales_rub;multiplier=100
 *   </Annotation>
 *   <Annotation name="nativeSql.maxAxes">2</Annotation>
 *   <Annotation name="nativeSql.fallbackMdx">true</Annotation>
 *   <Formula>... (fallback MDX) ...</Formula>
 * </CalculatedMember>
 * }</pre>
 */
public class NativeSqlConfig {

    private static final String PREFIX = "nativeSql.";
    static final String ANN_ENABLED = PREFIX + "enabled";
    static final String ANN_TEMPLATE = PREFIX + "template";
    static final String ANN_VARIABLES = PREFIX + "variables";
    static final String ANN_MAX_AXES = PREFIX + "maxAxes";
    static final String ANN_FALLBACK_MDX = PREFIX + "fallbackMdx";

    private NativeSqlConfig() {}

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
        String enabled = getAnnString(annotations, ANN_ENABLED);
        if (enabled == null || !"true".equalsIgnoreCase(enabled.trim())) {
            return null;
        }
        String template = getAnnString(annotations, ANN_TEMPLATE);
        if (template == null || template.trim().isEmpty()) {
            return null;
        }
        Map<String, String> variables = parseVariables(
            getAnnString(annotations, ANN_VARIABLES));
        int maxAxes = parseInt(
            getAnnString(annotations, ANN_MAX_AXES), 10);
        boolean fallbackMdx = parseBoolean(
            getAnnString(annotations, ANN_FALLBACK_MDX), true);

        return new NativeSqlDef(
            measureName, template.trim(), variables, maxAxes, fallbackMdx);
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
        Map<String, String> vars = new LinkedHashMap<String, String>();
        if (raw == null || raw.trim().isEmpty()) {
            return vars;
        }
        for (String pair : raw.split(";")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int eq = trimmed.indexOf('=');
            if (eq > 0) {
                vars.put(
                    trimmed.substring(0, eq).trim(),
                    trimmed.substring(eq + 1).trim());
            }
        }
        return vars;
    }

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
     * Parsed configuration for a native SQL measure.
     */
    public static class NativeSqlDef {
        private final String measureName;
        private final String template;
        private final Map<String, String> variables;
        private final int maxAxes;
        private final boolean fallbackMdx;

        NativeSqlDef(
            String measureName,
            String template,
            Map<String, String> variables,
            int maxAxes,
            boolean fallbackMdx)
        {
            this.measureName = measureName;
            this.template = template;
            this.variables = Collections.unmodifiableMap(variables);
            this.maxAxes = maxAxes;
            this.fallbackMdx = fallbackMdx;
        }

        public String getMeasureName() { return measureName; }
        public String getTemplate() { return template; }
        public Map<String, String> getVariables() { return variables; }
        public String getVariable(String key) { return variables.get(key); }
        public int getMaxAxes() { return maxAxes; }
        public boolean isFallbackMdx() { return fallbackMdx; }
    }
}
