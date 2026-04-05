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
 * Registry for native SQL measure evaluators.
 * Checks schema annotations on calculated members to find nativeSql.*
 * definitions, then creates NativeSqlCalc instances.
 */
public class NativeSqlRegistry {
    private static final Logger LOGGER =
        LogManager.getLogger(NativeSqlRegistry.class);

    private static final NativeSqlRegistry INSTANCE =
        new NativeSqlRegistry();

    private NativeSqlRegistry() {}

    public static NativeSqlRegistry instance() {
        return INSTANCE;
    }

    /**
     * Attempts to create a native SQL Calc for the given calculated member.
     * Checks the global enable flag, then looks for nativeSql.* annotations
     * on the member.
     *
     * @param member the calculated member to evaluate
     * @param root   the evaluator root
     * @return a NativeSqlCalc if annotations are present, or null
     */
    public Calc tryCreateCalc(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root)
    {
        if (!NativeSqlConfig.isGloballyEnabled()) {
            return null;
        }
        try {
            NativeSqlConfig.NativeSqlDef def =
                NativeSqlConfig.fromMember(member);
            if (def == null) {
                return null;
            }
            LOGGER.debug(
                "NativeSql: creating calc for [{}], template length={}",
                def.getMeasureName(), def.getTemplate().length());
            return NativeSqlCalc.create(member, root, def);
        } catch (Exception e) {
            LOGGER.warn(
                "NativeSql: failed to create calc for [{}], "
                + "falling back to MDX",
                member.getName(), e);
            return null;
        }
    }
}
