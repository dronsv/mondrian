/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2026 Hitachi Vantara..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.calc.Calc;
import mondrian.calc.impl.GenericCalc;
import mondrian.calc.DummyExp;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;

/**
 * Wrapper calc that applies a temporary peer-hierarchy reset plan before
 * evaluating the underlying calculation.
 */
class PeerHierarchyResetCalc extends GenericCalc {
    private final Calc delegate;
    private final ShareMeasurePeerHierarchyResetPlanner.ResetPlan resetPlan;

    PeerHierarchyResetCalc(
        Calc delegate,
        ShareMeasurePeerHierarchyResetPlanner.ResetPlan resetPlan)
    {
        super(new DummyExp(delegate.getType()), new Calc[] {delegate});
        this.delegate = delegate;
        this.resetPlan = resetPlan;
    }

    @Override
    public Object evaluate(Evaluator evaluator) {
        final Member[] resetMembers = resetPlan.getResetMembers();
        boolean requiresReset = false;
        for (Member resetMember : resetMembers) {
            if (resetMember != evaluator.getContext(resetMember.getHierarchy())) {
                requiresReset = true;
                break;
            }
        }
        if (!requiresReset) {
            return delegate.evaluate(evaluator);
        }

        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setContext(resetMembers);
            return delegate.evaluate(evaluator);
        } finally {
            evaluator.restore(savepoint);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return super.isWrapperFor(iface) || delegate.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        if (super.isWrapperFor(iface)) {
            return super.unwrap(iface);
        }
        return delegate.unwrap(iface);
    }
}
