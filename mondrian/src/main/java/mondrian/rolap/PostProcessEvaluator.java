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

import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Literal;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Phase D.3: Evaluates PostProcess formulas over the
 * NativeQueryResultContext for each axis tuple.
 *
 * <p>After Phase D.1-D.2 fills NativeQueryResultContext with physical values
 * from SQL, PostProcessEvaluator takes each PostProcess measure's formula plan,
 * looks up the leaf values from the context for a given projected key, applies
 * the formula (ratio, scaled ratio, additive, etc.), and returns the computed
 * value.
 *
 * <p>Supported patterns: RATIO, SCALED_RATIO, SCALED_VALUE, ADDITIVE,
 * SINGLE_REF.
 */
public class PostProcessEvaluator {

    private static final Logger LOGGER =
        LogManager.getLogger(PostProcessEvaluator.class);

    private PostProcessEvaluator() {}

    /**
     * Evaluates a PostProcess formula for a specific projected key.
     *
     * @param plan                  the PostProcess plan (formula + leaf bindings)
     * @param context               the query result context filled by SQL execution
     * @param projectedKeyByClassId per-class projected keys; each key includes
     *                              only the hierarchies that appear in that
     *                              class's GROUP BY
     * @param classPlans            map from classId to CoordinateClassPlan, used to
     *                              find which class each leaf belongs to
     * @return computed value as a {@link Double}, or {@code null} if any
     *         required leaf value is missing or the denominator is zero
     */
    public static Object evaluate(
        DependencyResolver.PostProcessPlan plan,
        NativeQueryResultContext context,
        Map<String, String> projectedKeyByClassId,
        Map<String, CoordinateClassPlan> classPlans)
    {
        FormulaNormalizer.Result nf = plan.normalizedFormula;
        Map<Integer, PhysicalValueRequest> bindings = plan.leafBindings;

        // Look up each leaf value from the context
        Map<Integer, Double> leafValues =
            new LinkedHashMap<Integer, Double>();

        for (Map.Entry<Integer, PhysicalValueRequest> entry
                : bindings.entrySet())
        {
            int leafIndex = entry.getKey();
            PhysicalValueRequest req = entry.getValue();

            // Find which coordinate class this request belongs to
            String classId = findClassId(req, classPlans);
            if (classId == null) {
                LOGGER.warn(
                    "PostProcessEvaluator: no class for measure={}",
                    req.getPhysicalMeasureId());
                return null;
            }

            // Use the class-specific projected key: each class's SQL
            // only groups by hierarchies that exist in its cube, so
            // the lookup key must match.
            String leafProjectedKey = projectedKeyByClassId.get(classId);

            Object value = context.get(
                classId, leafProjectedKey, req.getPhysicalMeasureId());

            if (value == null) {
                // Distinguish: key present but null vs key missing entirely
                if (!context.containsKey(
                        classId, leafProjectedKey,
                        req.getPhysicalMeasureId()))
                {
                    // No data for this tuple — no result
                    return null;
                }
                // Explicitly null value — also no result
                return null;
            }

            if (value instanceof Number) {
                leafValues.put(leafIndex, ((Number) value).doubleValue());
            } else {
                // Non-numeric value — cannot compute formula
                return null;
            }
        }

        return applyFormula(nf, leafValues);
    }

    // -----------------------------------------------------------------------
    // Formula application
    // -----------------------------------------------------------------------

    /**
     * Dispatches to the appropriate formula applicator based on the
     * normalized pattern.
     */
    static Object applyFormula(
        FormulaNormalizer.Result nf,
        Map<Integer, Double> leafValues)
    {
        switch (nf.pattern) {
        case RATIO:
            return applyRatio(leafValues);
        case SCALED_RATIO:
            return applyScaledRatio(nf, leafValues);
        case SCALED_VALUE:
            return applyScaledValue(nf, leafValues);
        case ADDITIVE:
            return applyAdditive(nf, leafValues);
        case SINGLE_REF:
            return applySingleRef(leafValues);
        default:
            LOGGER.warn(
                "PostProcessEvaluator: unsupported formula pattern {}",
                nf.pattern);
            return null;
        }
    }

    /**
     * RATIO: numerator (leaf 0) / denominator (leaf 1).
     * Returns {@code null} when denominator is zero or either leaf is absent.
     */
    private static Object applyRatio(Map<Integer, Double> leafValues) {
        if (leafValues.size() < 2) {
            return null;
        }
        Double numerator = leafValues.get(0);
        Double denominator = leafValues.get(1);
        if (numerator == null || denominator == null) {
            return null;
        }
        if (denominator == 0.0) {
            return null;
        }
        return numerator / denominator;
    }

    /**
     * SCALED_RATIO: (leaf0 / leaf1) * K, where K is a literal constant
     * extracted from the AST.
     */
    private static Object applyScaledRatio(
        FormulaNormalizer.Result nf,
        Map<Integer, Double> leafValues)
    {
        if (leafValues.size() < 2) {
            return null;
        }
        Double numerator = leafValues.get(0);
        Double denominator = leafValues.get(1);
        if (numerator == null || denominator == null) {
            return null;
        }
        if (denominator == 0.0) {
            return null;
        }
        double scaleFactor = extractScaleFactor(nf);
        return (numerator / denominator) * scaleFactor;
    }

    /**
     * SCALED_VALUE: leaf0 * K, where K is a literal constant extracted
     * from the AST.
     */
    private static Object applyScaledValue(
        FormulaNormalizer.Result nf,
        Map<Integer, Double> leafValues)
    {
        if (leafValues.isEmpty()) {
            return null;
        }
        Double value = leafValues.get(0);
        if (value == null) {
            return null;
        }
        double scaleFactor = extractScaleFactor(nf);
        return value * scaleFactor;
    }

    /**
     * ADDITIVE: leaf0 + leaf1 (or leaf0 − leaf1 when operator is "-").
     * The operator is determined from the normalized expression.
     */
    private static Object applyAdditive(
        FormulaNormalizer.Result nf,
        Map<Integer, Double> leafValues)
    {
        if (leafValues.size() < 2) {
            return null;
        }
        Double a = leafValues.get(0);
        Double b = leafValues.get(1);
        if (a == null || b == null) {
            return null;
        }

        Exp exp = nf.normalizedExp;
        if (exp instanceof FunCall) {
            String fn = ((FunCall) exp).getFunName();
            if ("-".equals(fn)) {
                return a - b;
            }
        }
        return a + b; // default: addition
    }

    /**
     * SINGLE_REF: pass leaf0 through unchanged.
     */
    private static Object applySingleRef(Map<Integer, Double> leafValues) {
        if (leafValues.isEmpty()) {
            return null;
        }
        return leafValues.get(0);
    }

    // -----------------------------------------------------------------------
    // Scale-factor extraction
    // -----------------------------------------------------------------------

    /**
     * Extracts the literal scale factor from a multiply expression in the
     * normalized AST.
     *
     * <p>For SCALED_RATIO {@code (a / b) * K} and SCALED_VALUE {@code a * K},
     * finds the {@link Literal} argument of the top-level {@code *} call and
     * returns its numeric value. Returns {@code 1.0} if no literal is found.
     */
    static double extractScaleFactor(FormulaNormalizer.Result nf) {
        Exp exp = nf.normalizedExp;
        if (!(exp instanceof FunCall)) {
            return 1.0;
        }
        FunCall fc = (FunCall) exp;
        if (!"*".equals(fc.getFunName())) {
            return 1.0;
        }
        for (Exp arg : fc.getArgs()) {
            if (arg instanceof Literal) {
                Object val = ((Literal) arg).getValue();
                if (val instanceof Number) {
                    return ((Number) val).doubleValue();
                }
            }
        }
        return 1.0;
    }

    // -----------------------------------------------------------------------
    // Class-ID lookup
    // -----------------------------------------------------------------------

    /**
     * Scans all coordinate class plans to find which classId contains the
     * given {@link PhysicalValueRequest} (matched by physicalMeasureId).
     *
     * @return the classId, or {@code null} if not found
     */
    private static String findClassId(
        PhysicalValueRequest req,
        Map<String, CoordinateClassPlan> classPlans)
    {
        String measureId = req.getPhysicalMeasureId();
        for (Map.Entry<String, CoordinateClassPlan> entry
                : classPlans.entrySet())
        {
            for (PhysicalValueRequest r : entry.getValue().getRequests()) {
                if (measureId.equals(r.getPhysicalMeasureId())) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }
}

// End PostProcessEvaluator.java
