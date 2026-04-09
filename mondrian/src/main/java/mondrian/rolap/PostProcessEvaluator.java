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

import mondrian.calc.Calc;
import mondrian.olap.fun.FunUtil;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.Map;

/**
 * Phase D.3: Evaluates PostProcess formulas by delegating to compiled
 * Mondrian Calc trees with prefetched leaf values via ContextBackedCellReader.
 *
 * <p>Instead of pattern-matching on formula shapes (RATIO, SCALED_RATIO, etc.),
 * this evaluator compiles the calculated member's MDX expression into a Calc
 * tree (the same tree Mondrian uses internally) and evaluates it against a
 * ContextBackedCellReader that supplies the physical leaf values already
 * fetched by the SQL phase (D.1-D.2).
 *
 * <p>This handles all scalar current-cell formulas correctly, including
 * {@code (A-B)/A*K} which pattern matching got wrong.
 */
public class PostProcessEvaluator {

    private static final Logger LOGGER =
        LogManager.getLogger(PostProcessEvaluator.class);

    private PostProcessEvaluator() {}

    /**
     * Evaluates a PostProcess formula using compiled Calc.
     *
     * <p>Creates a child evaluator with a ContextBackedCellReader installed,
     * then delegates to the Calc tree. The Calc tree resolves measure
     * references by setting the evaluator's context to each leaf measure
     * and calling {@code evaluator.evaluateCurrent()}, which routes through
     * the ContextBackedCellReader to look up prefetched values.
     *
     * @param calc                      compiled Calc tree for the calculated
     *                                  measure
     * @param templateEvaluator         the NQE's RolapEvaluator; a child copy
     *                                  is pushed so the original is not
     *                                  mutated
     * @param context                   query result context filled by SQL
     *                                  execution (Phase D.1-D.2)
     * @param projectedKeyByGranClassId per-granularity-class projected keys
     *                                  for the current axis tuple
     * @param granClassIdByBaseClassId  map from base classId to
     *                                  granularity classId, or {@code null}
     *                                  for legacy path
     * @param classPlanMap              map from base classId to
     *                                  {@link CoordinateClassPlan}
     * @return computed value, or {@code null} if evaluation fails or
     *         produces NaN/DoubleNull
     */
    public static Object evaluateWithCalc(
        Calc calc,
        RolapEvaluator templateEvaluator,
        NativeQueryResultContext context,
        Map<String, String> projectedKeyByGranClassId,
        Map<String, String> granClassIdByBaseClassId,
        Map<String, CoordinateClassPlan> classPlanMap)
    {
        if (calc == null || templateEvaluator == null) {
            return null;
        }

        RolapEvaluator childEvaluator = templateEvaluator.push();
        CellReader cellReader = new ContextBackedCellReader(
            context, projectedKeyByGranClassId,
            granClassIdByBaseClassId, classPlanMap);
        childEvaluator.setCellReader(cellReader);

        try {
            Object result = calc.evaluate(childEvaluator);
            if (result instanceof Double) {
                double d = (Double) result;
                if (Double.isNaN(d) || d == FunUtil.DoubleNull) {
                    return null;
                }
            }
            return result;
        } catch (Exception e) {
            LOGGER.warn(
                "PostProcessEvaluator: Calc evaluation failed: {}",
                e.getMessage());
            return null;
        }
    }
}

// End PostProcessEvaluator.java
