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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Registry for native ratio measure evaluators.
 * Checks schema annotations on calculated members to find ratio
 * definitions, then creates NativeRatioCalc instances.
 */
public class NativeRatioRegistry {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeRatioRegistry.class);

    private static final NativeRatioRegistry INSTANCE =
        new NativeRatioRegistry();

    private NativeRatioRegistry() {
    }

    public static NativeRatioRegistry instance() {
        return INSTANCE;
    }

    /**
     * Attempts to create a native ratio Calc for the given calculated member.
     * Checks the global enable flag, then looks for nativeRatio.* annotations
     * on the member.
     *
     * @param member the calculated member to evaluate
     * @param root   the evaluator root
     * @return a NativeRatioCalc if annotations are present, or null
     */
    public Calc tryCreateCalc(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root)
    {
        if (!NativeRatioConfig.isEnabled()) {
            return null;
        }
        final NativeRatioConfig.RatioMeasureDef def =
            NativeRatioConfig.fromAnnotations(member);
        if (def == null) {
            return null;
        }
        LOGGER.info(
            "NativeRatio: matched measure '{}' via annotations "
                + "(num={}, denom={}, reset={}, mult={})",
            member.getName(),
            def.getNumeratorMeasureName(),
            def.getDenominatorMeasureName(),
            def.getResetHierarchyNames(),
            def.getMultiplier());
        return NativeRatioCalc.tryCreate(member, root, def);
    }
}
