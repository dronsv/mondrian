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

import java.util.*;

/**
 * Parses and holds configuration for native ratio measure evaluation.
 * Configured via mondrian.properties {@code mondrian.native.ratio.*}.
 *
 * <p>A ratio measure is computed as:
 * {@code numerator_stored_measure / denominator_stored_measure * multiplier}
 * where the denominator is aggregated with certain hierarchies "reset"
 * (removed from GROUP BY), collapsing those dimensions to All level.
 */
public class NativeRatioConfig {
    private final boolean enabled;
    private final Map<String, RatioMeasureDef> definitions;

    private NativeRatioConfig(
        boolean enabled,
        Map<String, RatioMeasureDef> definitions)
    {
        this.enabled = enabled;
        this.definitions = Collections.unmodifiableMap(definitions);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public RatioMeasureDef getDefinition(String measureName) {
        if (!enabled) {
            return null;
        }
        return definitions.get(measureName);
    }

    /**
     * Creates config from explicit parameter values.
     * For the initial version, supports a single measure (all measures
     * share numerator/denominator/reset/multiplier settings).
     */
    public static NativeRatioConfig fromProperties(
        boolean enabled,
        String measureNames,
        String numerator,
        String denominator,
        String resetHierarchies,
        String multiplier,
        String nullIfDenominatorZero)
    {
        Map<String, RatioMeasureDef> defs =
            new HashMap<String, RatioMeasureDef>();
        if (enabled && measureNames != null) {
            for (String name : measureNames.split(",")) {
                name = name.trim();
                if (!name.isEmpty()) {
                    defs.put(name, new RatioMeasureDef(
                        name,
                        numerator != null ? numerator.trim() : "",
                        denominator != null ? denominator.trim() : "",
                        parseList(resetHierarchies),
                        parseDouble(multiplier, 1.0),
                        Boolean.parseBoolean(nullIfDenominatorZero)));
                }
            }
        }
        return new NativeRatioConfig(enabled, defs);
    }

    /**
     * Creates config from MondrianProperties singleton.
     */
    public static NativeRatioConfig fromMondrianProperties() {
        final mondrian.olap.MondrianProperties props =
            mondrian.olap.MondrianProperties.instance();
        return fromProperties(
            props.NativeRatioEnable.get(),
            props.NativeRatioMeasures.get(),
            props.NativeRatioNumerator.get(),
            props.NativeRatioDenominator.get(),
            props.NativeRatioDenominatorReset.get(),
            props.NativeRatioMultiplier.get(),
            String.valueOf(props.NativeRatioNullIfDenominatorZero.get()));
    }

    private static List<String> parseList(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>();
        for (String item : value.split(",")) {
            item = item.trim();
            if (!item.isEmpty()) {
                result.add(item);
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static double parseDouble(String value, double defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Definition of one ratio measure: numerator / denominator * multiplier.
     */
    public static class RatioMeasureDef {
        private final String measureName;
        private final String numeratorMeasureName;
        private final String denominatorMeasureName;
        private final List<String> resetHierarchyNames;
        private final double multiplier;
        private final boolean nullIfDenominatorZero;

        RatioMeasureDef(
            String measureName,
            String numeratorMeasureName,
            String denominatorMeasureName,
            List<String> resetHierarchyNames,
            double multiplier,
            boolean nullIfDenominatorZero)
        {
            this.measureName = measureName;
            this.numeratorMeasureName = numeratorMeasureName;
            this.denominatorMeasureName = denominatorMeasureName;
            this.resetHierarchyNames = resetHierarchyNames;
            this.multiplier = multiplier;
            this.nullIfDenominatorZero = nullIfDenominatorZero;
        }

        public String getMeasureName() { return measureName; }
        public String getNumeratorMeasureName() { return numeratorMeasureName; }
        public String getDenominatorMeasureName() { return denominatorMeasureName; }
        public List<String> getResetHierarchyNames() { return resetHierarchyNames; }
        public double getMultiplier() { return multiplier; }
        public boolean isNullIfDenominatorZero() { return nullIfDenominatorZero; }
    }
}
