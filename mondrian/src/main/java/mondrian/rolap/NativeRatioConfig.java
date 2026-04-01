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
 * Parses ratio measure configuration from schema annotations on
 * {@link RolapCalculatedMember}.
 *
 * <p>A ratio measure is computed as:
 * {@code numerator_stored_measure / denominator_stored_measure * multiplier}
 * where the denominator is aggregated with certain hierarchies "reset"
 * (removed from GROUP BY), collapsing those dimensions to All level.
 *
 * <p>Configuration is declared via {@code <Annotation>} elements on the
 * calculated member in the schema XML:
 * <pre>{@code
 * <CalculatedMember name="WD %" dimension="Measures">
 *   <Annotation name="nativeRatio.numerator">WD numerator cat</Annotation>
 *   <Annotation name="nativeRatio.denominator">Продажи руб</Annotation>
 *   <Annotation name="nativeRatio.denominator.reset">
 *     Продукт.Бренд,ТТ.Адрес
 *   </Annotation>
 *   <Annotation name="nativeRatio.multiplier">100</Annotation>
 *   <Formula>...</Formula>
 * </CalculatedMember>
 * }</pre>
 *
 * <p>Global enable flag: {@code mondrian.native.ratio.enable=true} in
 * mondrian.properties.
 */
public class NativeRatioConfig {

    private static final String PREFIX = "nativeRatio.";
    private static final String ANN_NUMERATOR = PREFIX + "numerator";
    private static final String ANN_DENOMINATOR = PREFIX + "denominator";
    private static final String ANN_DENOM_RESET = PREFIX + "denominator.reset";
    private static final String ANN_MULTIPLIER = PREFIX + "multiplier";
    private static final String ANN_NULL_IF_ZERO = PREFIX + "nullIfDenominatorZero";

    private NativeRatioConfig() {}

    /**
     * Returns true if native ratio evaluation is globally enabled.
     */
    public static boolean isEnabled() {
        return mondrian.olap.MondrianProperties.instance()
            .NativeRatioEnable.get();
    }

    /**
     * Attempts to extract a ratio definition from the member's annotations.
     * Returns null if the member has no {@code nativeRatio.numerator}
     * annotation (i.e. is not configured for native ratio evaluation).
     */
    public static RatioMeasureDef fromAnnotations(
        RolapCalculatedMember member)
    {
        final Map<String, Annotation> annotations = member.getAnnotationMap();
        if (annotations == null || annotations.isEmpty()) {
            return null;
        }
        final String numerator = getAnnotationString(annotations, ANN_NUMERATOR);
        if (numerator == null) {
            return null;  // not a native ratio measure
        }
        final String denominator = getAnnotationString(annotations, ANN_DENOMINATOR);
        if (denominator == null) {
            return null;
        }
        return new RatioMeasureDef(
            member.getName(),
            numerator,
            denominator,
            parseList(getAnnotationString(annotations, ANN_DENOM_RESET)),
            parseDouble(getAnnotationString(annotations, ANN_MULTIPLIER), 1.0),
            parseBoolean(getAnnotationString(annotations, ANN_NULL_IF_ZERO), true));
    }

    private static String getAnnotationString(
        Map<String, Annotation> annotations, String name)
    {
        final Annotation ann = annotations.get(name);
        if (ann == null || ann.getValue() == null) {
            return null;
        }
        final String value = String.valueOf(ann.getValue()).trim();
        return value.isEmpty() ? null : value;
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
        if (value == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }

    /**
     * Definition of one ratio measure.
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
