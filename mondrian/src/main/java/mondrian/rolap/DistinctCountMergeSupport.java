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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper for distinct-count merge-state routing policy.
 */
public final class DistinctCountMergeSupport {
    private static final Logger LOGGER =
        LogManager.getLogger(DistinctCountMergeSupport.class);

    public static final String PROP_DISTINCT_MERGE_FUNCTION =
        "mondrian.rolap.aggregates.DistinctCountMergeFunction";
    public static final String PROP_DISTINCT_MERGE_MODE =
        "mondrian.rolap.aggregates.DistinctCountMergeMode";

    private static final AtomicBoolean INVALID_MODE_WARNED =
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

    public static String getMergeFunctionForDialect(Dialect dialect) {
        final String mergeFunction = getConfiguredMergeFunction();
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

    public static boolean isEnabledForDialect(Dialect dialect) {
        return getMergeFunctionForDialect(dialect) != null;
    }

    public static boolean isEnabledForStar(RolapStar star) {
        return star != null && isEnabledForDialect(star.getSqlQueryDialect());
    }
}

