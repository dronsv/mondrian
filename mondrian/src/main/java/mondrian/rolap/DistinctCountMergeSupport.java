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

import mondrian.olap.MondrianProperties;
import mondrian.spi.Dialect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper for distinct-count merge-state policy.
 *
 * <p>Two distinct semantics share this helper:
 * <ul>
 *   <li><b>Measure routing</b> (allow-list, fail-closed). When
 *       {@link #PROP_DISTINCT_MERGE_FUNCTION_MAP} is configured, only
 *       listed measures are eligible for aggregate-state merge routing.
 *       Unlisted measures and malformed map entries return {@code null}
 *       from {@link #getMergeFunctionForDialect(Dialect, String)} and
 *       therefore fall back to fact-table {@code count(distinct)}. Used
 *       by {@code AggregationManager}, {@code AggStar},
 *       {@code RolapAggregator}, {@code NqeTableStrategy}.</li>
 *   <li><b>Batch availability</b> (any-config). Whether distinct-count
 *       merge support is potentially applicable to at least one measure
 *       on the dialect, given the current configuration. Returns
 *       {@code true} from {@link #isAnyMergeConfigured(Dialect)} if
 *       either the global function or any valid parsed map entry is
 *       dialect-supported. Used by {@code FastBatchingCellReader} as
 *       the default for {@code splitMixedDistinctMeasureBatches}.</li>
 * </ul>
 *
 * <p>The two semantics intentionally differ on malformed-map handling:
 * routing fails closed (returns {@code null} for the queried measure),
 * while availability tolerates a malformed map provided a valid global
 * function is configured.
 */
public final class DistinctCountMergeSupport {
    private static final Logger LOGGER =
        LogManager.getLogger(DistinctCountMergeSupport.class);

    public static final String PROP_DISTINCT_MERGE_FUNCTION =
        "mondrian.rolap.aggregates.DistinctCountMergeFunction";
    public static final String PROP_DISTINCT_MERGE_MODE =
        "mondrian.rolap.aggregates.DistinctCountMergeMode";
    public static final String PROP_DISTINCT_MERGE_FUNCTION_MAP =
        "mondrian.rolap.aggregates.DistinctCountMergeFunctionMap";

    private static final AtomicBoolean INVALID_MODE_WARNED =
        new AtomicBoolean(false);
    private static final AtomicBoolean INVALID_MAP_WARNED =
        new AtomicBoolean(false);

    private DistinctCountMergeSupport() {
    }

    public enum Mode {
        OFF,
        AUTO,
        ON
    }

    public static String getConfiguredMergeFunction() {
        final String value = MondrianProperties.instance()
            .getProperty(PROP_DISTINCT_MERGE_FUNCTION);
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public static boolean isMergeFunctionConfigured() {
        return getConfiguredMergeFunction() != null;
    }

    public static String getConfiguredMergeFunctionForMeasure(
        String measureName)
    {
        if (measureName == null) {
            return null;
        }
        final String trimmedMeasureName = measureName.trim();
        if (trimmedMeasureName.isEmpty()) {
            return null;
        }
        final String mapValue = MondrianProperties.instance()
            .getProperty(PROP_DISTINCT_MERGE_FUNCTION_MAP);
        final Map<String, String> functionMap =
            parseMergeFunctionMap(mapValue);
        if (functionMap.isEmpty()) {
            return null;
        }
        final String exact = functionMap.get(trimmedMeasureName);
        if (exact != null) {
            return exact;
        }
        for (Map.Entry<String, String> entry : functionMap.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(trimmedMeasureName)) {
                return entry.getValue();
            }
        }
        return null;
    }

    public static Mode getConfiguredMode() {
        final String value = MondrianProperties.instance()
            .getProperty(PROP_DISTINCT_MERGE_MODE);
        if (value == null) {
            return Mode.AUTO;
        }
        final String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return Mode.AUTO;
        }
        if ("off".equalsIgnoreCase(trimmed)) {
            return Mode.OFF;
        }
        if ("on".equalsIgnoreCase(trimmed)) {
            return Mode.ON;
        }
        if ("auto".equalsIgnoreCase(trimmed)) {
            return Mode.AUTO;
        }
        if (INVALID_MODE_WARNED.compareAndSet(false, true)
            && LOGGER.isWarnEnabled())
        {
            LOGGER.warn(
                "Invalid value for " + PROP_DISTINCT_MERGE_MODE
                + ": '" + value + "'. Falling back to AUTO.");
        }
        return Mode.AUTO;
    }

    /**
     * No-measure overload. Returns the global merge function if (and
     * only if) global is configured AND the per-measure map is
     * unset/empty AND the dialect supports it. Allow-list-strict.
     *
     * <p><b>Not appropriate for batch-strategy decisions</b> that need
     * to know whether any measure-specific merge function is
     * configured. Use {@link #isAnyMergeConfigured(Dialect)} for that.
     *
     * @param dialect the dialect, may be null
     * @return the global merge function name or {@code null}
     */
    public static String getMergeFunctionForDialect(Dialect dialect) {
        return getMergeFunctionForDialect(dialect, null);
    }

    public static String getMergeFunctionForDialect(
        Dialect dialect,
        String measureName)
    {
        String mergeFunction =
            getConfiguredMergeFunctionForMeasure(measureName);
        if (mergeFunction == null) {
            // Only fall back to global function when no per-measure map
            // is configured. If the map IS configured, unlisted measures
            // must NOT use the merge function — they are regular
            // count(distinct) measures, not HLL state columns.
            final String mapValue = MondrianProperties.instance()
                .getProperty(PROP_DISTINCT_MERGE_FUNCTION_MAP);
            if (mapValue == null || mapValue.trim().isEmpty()) {
                mergeFunction = getConfiguredMergeFunction();
            }
        }
        if (mergeFunction == null) {
            return null;
        }
        final Mode mode = getConfiguredMode();
        if (mode == Mode.OFF) {
            return null;
        }
        if (mode == Mode.ON) {
            return mergeFunction;
        }
        if (dialect == null) {
            return null;
        }
        return dialect.supportsDistinctCountMergeFunction(mergeFunction)
            ? mergeFunction
            : null;
    }

    /**
     * Returns {@code true} only when a global merge function is
     * configured AND no per-measure map is configured AND the dialect
     * supports it.
     *
     * <p><b>Not appropriate for batch-strategy decisions</b> that need
     * to know whether any measure-specific merge function is
     * configured. Use {@link #isAnyMergeConfigured(Dialect)} for that.
     *
     * @param dialect the dialect, may be null
     * @return {@code true} iff the no-measure global function applies
     */
    public static boolean isEnabledForDialect(Dialect dialect) {
        return getMergeFunctionForDialect(dialect) != null;
    }

    public static boolean isEnabledForDialect(
        Dialect dialect,
        String measureName)
    {
        return getMergeFunctionForDialect(dialect, measureName) != null;
    }

    /**
     * Wraps {@link #isEnabledForDialect(Dialect)} for callers that have
     * a {@code RolapStar} but no specific measure name. Inherits the
     * no-measure semantic of the underlying overload.
     *
     * <p><b>Not appropriate for batch-strategy decisions</b> that need
     * to know whether any measure-specific merge function is
     * configured. Use {@link #isAnyMergeConfigured(Dialect)} for that.
     *
     * @param star the star, may be null
     * @return {@code true} iff the no-measure global function applies
     *         to the star's dialect
     */
    public static boolean isEnabledForStar(RolapStar star) {
        return star != null && isEnabledForDialect(star.getSqlQueryDialect());
    }

    /**
     * Returns true iff distinct-count merge support is potentially
     * applicable to at least one measure on {@code dialect} given the
     * current configuration. Used by batch-strategy callers that need to
     * know whether any measure in a mixed-distinct batch could be
     * merge-routed; not appropriate for SQL-routing decisions, which
     * require a specific measure name and use
     * {@link #isEnabledForDialect(Dialect, String)} or
     * {@link #getMergeFunctionForDialect(Dialect, String)}.
     *
     * <p>Returns true if all of the following hold:
     * <ul>
     *   <li>{@code dialect} is non-null;</li>
     *   <li>The configured {@link Mode} is not {@link Mode#OFF};</li>
     *   <li>At least one configured merge function (the global function
     *       or any valid parsed map entry) is dialect-supported per
     *       {@link Dialect#supportsDistinctCountMergeFunction(String)}
     *       when Mode is AUTO. Mode ON skips the dialect predicate;
     *       Mode OFF returns {@code false} unconditionally.</li>
     * </ul>
     *
     * <p>Returns {@code false} otherwise. In particular: a map containing
     * only malformed entries (no successfully parsed entry) produces an
     * empty candidate set, so {@code isAnyMergeConfigured} returns
     * {@code false} unless a valid global function is also configured.
     *
     * <p>This is the deliberate contrast against measure routing: a
     * malformed map fails routing closed for the queried measure, but
     * leaves availability alone if a valid global function is present.
     */
    public static boolean isAnyMergeConfigured(Dialect dialect) {
        if (dialect == null) {
            return false;
        }
        final Mode mode = getConfiguredMode();
        if (mode == Mode.OFF) {
            return false;
        }

        final LinkedHashSet<String> candidates = new LinkedHashSet<>();
        final String global = getConfiguredMergeFunction();
        if (global != null) {
            candidates.add(global);
        }
        final String mapValue = MondrianProperties.instance()
            .getProperty(PROP_DISTINCT_MERGE_FUNCTION_MAP);
        candidates.addAll(parseMergeFunctionMap(mapValue).values());

        if (candidates.isEmpty()) {
            return false;
        }
        if (mode == Mode.ON) {
            return true;
        }
        // Mode.AUTO: at least one candidate must be dialect-supported.
        for (String fn : candidates) {
            if (dialect.supportsDistinctCountMergeFunction(fn)) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, String> parseMergeFunctionMap(
        String mapValue)
    {
        final Map<String, String> map = new LinkedHashMap<>();
        if (mapValue == null) {
            return map;
        }
        final String trimmedValue = mapValue.trim();
        if (trimmedValue.isEmpty()) {
            return map;
        }
        final String[] tokens = trimmedValue.split("[,;]");
        for (String token : tokens) {
            final String trimmedToken = token == null ? "" : token.trim();
            if (trimmedToken.isEmpty()) {
                continue;
            }
            final int eq = trimmedToken.indexOf('=');
            if (eq <= 0 || eq >= trimmedToken.length() - 1) {
                warnInvalidMapValue(mapValue);
                continue;
            }
            final String measureName = trimmedToken.substring(0, eq).trim();
            final String functionName = trimmedToken.substring(eq + 1).trim();
            if (measureName.isEmpty() || functionName.isEmpty()) {
                warnInvalidMapValue(mapValue);
                continue;
            }
            map.put(measureName, functionName);
        }
        return map;
    }

    private static void warnInvalidMapValue(String mapValue) {
        if (INVALID_MAP_WARNED.compareAndSet(false, true)
            && LOGGER.isWarnEnabled())
        {
            LOGGER.warn(
                "Invalid value for " + PROP_DISTINCT_MERGE_FUNCTION_MAP
                + ": '" + mapValue + "'. Entries must be"
                + " 'measure=function' separated by ',' or ';'.");
        }
    }
}
