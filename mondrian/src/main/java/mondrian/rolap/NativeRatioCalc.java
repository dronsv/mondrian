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
import mondrian.calc.impl.GenericCalc;
import mondrian.olap.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Evaluates a ratio calculated measure via batch SQL.
 * Generates one SQL query covering all axis members, caches results,
 * returns cached values for individual evaluate() calls.
 *
 * <p>Stub implementation — returns null from tryCreate until
 * SQL generation is implemented in Task 4.
 */
public class NativeRatioCalc extends GenericCalc {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeRatioCalc.class);

    private NativeRatioCalc(Exp exp) {
        super(exp, new Calc[0]);
    }

    /**
     * Factory: resolves measures, validates, returns calc or null.
     * Returns null until SQL generation is implemented.
     */
    static Calc tryCreate(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root,
        NativeRatioConfig.RatioMeasureDef def)
    {
        LOGGER.info(
            "NativeRatioCalc.tryCreate: stub — not yet implemented for '{}'",
            member.getName());
        return null;
    }

    @Override
    public Object evaluate(Evaluator evaluator) {
        throw new UnsupportedOperationException(
            "NativeRatioCalc.evaluate not yet implemented");
    }
}
