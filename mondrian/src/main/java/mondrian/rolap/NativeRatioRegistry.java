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
 * Reads config from MondrianProperties and matches calculated members
 * to ratio definitions.
 */
public class NativeRatioRegistry {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeRatioRegistry.class);

    private static final NativeRatioRegistry INSTANCE =
        new NativeRatioRegistry();

    private volatile NativeRatioConfig config;

    private NativeRatioRegistry() {
    }

    public static NativeRatioRegistry instance() {
        return INSTANCE;
    }

    private NativeRatioConfig getConfig() {
        if (config == null) {
            config = NativeRatioConfig.fromMondrianProperties();
        }
        return config;
    }

    /**
     * Reloads configuration. Called when properties change.
     */
    public void reload() {
        config = null;
    }

    /**
     * Attempts to create a native ratio Calc for the given calculated member.
     *
     * @param member the calculated member to evaluate
     * @param root   the evaluator root (provides schema, dialect, result)
     * @return a NativeRatioCalc if the member matches a configured ratio
     *         measure, or null if not configured or not applicable
     */
    public Calc tryCreateCalc(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root)
    {
        final NativeRatioConfig cfg = getConfig();
        if (!cfg.isEnabled()) {
            return null;
        }
        final String memberName = member.getName();
        final NativeRatioConfig.RatioMeasureDef def =
            cfg.getDefinition(memberName);
        if (def == null) {
            return null;
        }
        LOGGER.info(
            "NativeRatio: matched measure '{}' to ratio config "
                + "(num={}, denom={}, reset={}, mult={})",
            memberName,
            def.getNumeratorMeasureName(),
            def.getDenominatorMeasureName(),
            def.getResetHierarchyNames(),
            def.getMultiplier());
        return NativeRatioCalc.tryCreate(member, root, def);
    }
}
