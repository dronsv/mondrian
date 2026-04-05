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
import mondrian.olap.Evaluator;

/**
 * Evaluates a native SQL calculated measure via a batch SQL query.
 *
 * <p>Full implementation in Task 3. This stub satisfies compile-time
 * dependencies for NativeSqlRegistry.
 */
public class NativeSqlCalc extends GenericCalc {
    private NativeSqlCalc(RolapCalculatedMember member) {
        super(member.getExpression(), new Calc[0]);
    }

    /**
     * Factory method called by NativeSqlRegistry.
     * Returns null (stub) until Task 3 provides the real implementation.
     */
    static Calc create(
        RolapCalculatedMember member,
        RolapEvaluatorRoot root,
        NativeSqlConfig.NativeSqlDef def)
    {
        // Stub — full implementation in Task 3
        return null;
    }

    @Override
    public Object evaluate(Evaluator evaluator) {
        return null;
    }
}
