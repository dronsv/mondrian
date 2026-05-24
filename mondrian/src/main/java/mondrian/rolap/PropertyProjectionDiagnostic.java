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

import mondrian.olap.Property;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Structured observability for level-property projection decisions in
 * tuple/member SQL readers (see issue #21 — P1 instrumentation).
 *
 * <p>This is <strong>observability only</strong>: emits a single-line
 * structured log entry per decision, does not affect SQL output or query
 * semantics. Output is gated on the {@code mondrian.rolap.PropertyProjection}
 * log4j category; when disabled the emission methods short-circuit
 * before doing any work, so the runtime cost is one volatile-read.
 *
 * <p>The log format is space-separated {@code key=value} pairs on a
 * single line for easy grep/parse. Reason codes for V1 are intentionally
 * small — see {@link Reason}.
 *
 * <p>See follow-up issue #22 for the optimisation that consumes this
 * observability infrastructure to drive a feature-flagged pruning
 * decision; this class deliberately does not make any pruning decision
 * itself.
 */
public final class PropertyProjectionDiagnostic {

    public static final String CATEGORY =
        "mondrian.rolap.PropertyProjection";

    private static final Logger LOGGER = LogManager.getLogger(CATEGORY);

    /**
     * SQL site that decided to include a level property in projection.
     * Naming mirrors the methods in {@link SqlTupleReader} and
     * {@link SqlMemberSource} so log entries point straight at the
     * source.
     */
    public enum ReaderSite {
        /** {@code SqlTupleReader.addLevelMemberSql} — main tuple-reader. */
        TUPLE_READER,
        /** {@code SqlMemberSource.makeKeysSql} — per-level keys SQL. */
        MEMBER_SOURCE_KEYS_SQL,
        /** {@code SqlMemberSource.makeChildMemberSql} — children of a member. */
        MEMBER_SOURCE_CHILD_MEMBER_SQL,
        /** {@code SqlMemberSource.addLevel} (from {@code makeChildMemberSql_PCRoot}). */
        MEMBER_SOURCE_ADD_LEVEL,
        /** {@code SqlMemberSource.makeChildMemberSqlPC} — parent-child children. */
        MEMBER_SOURCE_CHILD_MEMBER_SQL_PC,
        /** Catch-all if a new site is added without being mapped here. */
        UNKNOWN_READER_SITE,
    }

    /**
     * Reason a property or expression was included in the SQL projection.
     * The V1 set is intentionally narrow — only what the SQL site itself
     * can determine without the dependency analyser planned for #22.
     */
    public enum Reason {
        /** Engine needs this expression for member identity / structure. */
        ENGINE_REQUIRED,
        /** Level key expression. Subcase of {@link #ENGINE_REQUIRED}. */
        KEY_EXPRESSION,
        /** Level caption or name expression. */
        CAPTION_OR_NAME_EXPRESSION,
        /** Level ordinal expression. */
        ORDINAL_EXPRESSION,
        /** Level parent expression (parent-child hierarchies). */
        PARENT_EXPRESSION,
        /** Client requested via MDX {@code DIMENSION PROPERTIES} clause. */
        DIMENSION_PROPERTIES_REQUESTED,
        /**
         * Property is declared in the schema and projected by current
         * eager-loading default. This is the dominant V1 reason and the
         * primary signal for the #22 optimisation to target.
         */
        LEVEL_PROPERTY_EAGER_DEFAULT,
    }

    private PropertyProjectionDiagnostic() {
        // no instances
    }

    /**
     * Returns true when projection diagnostics should be emitted.
     * Callers gate their structured payload construction on this to
     * keep the off-path cost to a volatile read.
     */
    public static boolean isEnabled() {
        return LOGGER.isInfoEnabled();
    }

    /**
     * Records that all schema-declared properties for {@code level}
     * were projected by {@code site} under the current eager-loading
     * default. Each property is emitted with reason
     * {@link Reason#LEVEL_PROPERTY_EAGER_DEFAULT}.
     *
     * <p>No-op when {@link #isEnabled()} returns false.
     */
    public static void recordEagerLevelProperties(
        ReaderSite site, RolapLevel level, RolapProperty[] properties)
    {
        if (!isEnabled() || level == null) {
            return;
        }
        int count = properties == null ? 0 : properties.length;
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add(safeName(properties[i]));
        }
        LOGGER.info(
            "PropertyProjection site={} level={} schemaProperties={}"
            + " selectedProperties={} reason={} props={}",
            site,
            level.getUniqueName(),
            count,
            count,
            Reason.LEVEL_PROPERTY_EAGER_DEFAULT,
            joinCsv(names));
    }

    /**
     * Records the {@code DIMENSION PROPERTIES} list the client requested
     * for a single axis. Emitted once per axis at query-compile time so
     * subscribers can compare the requested set against the projected
     * set emitted by the SQL sites.
     *
     * <p>No-op when {@link #isEnabled()} returns false.
     */
    public static void recordDimensionPropertiesRequested(
        String axisLabel, List<String> propertyNames)
    {
        if (!isEnabled()) {
            return;
        }
        LOGGER.info(
            "PropertyProjection site=QUERY_AXIS_COMPILE axis={}"
            + " dimensionProperties={} reason={}",
            axisLabel == null ? "?" : axisLabel,
            joinCsv(propertyNames),
            Reason.DIMENSION_PROPERTIES_REQUESTED);
    }

    private static String safeName(Property p) {
        if (p == null) {
            return "?";
        }
        String n = p.getName();
        return n == null ? "?" : n;
    }

    private static String joinCsv(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder(values.size() * 16);
        sb.append('[');
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            String v = values.get(i);
            sb.append(v == null ? "?" : v);
        }
        sb.append(']');
        return sb.toString();
    }
}

// End PropertyProjectionDiagnostic.java
