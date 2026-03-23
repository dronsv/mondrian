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

import junit.framework.TestCase;
import mondrian.calc.Calc;
import mondrian.olap.Evaluator;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.type.ScalarType;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PeerHierarchyResetCalcTest extends TestCase {
    public void testEvaluateAppliesResetMembersAroundDelegateEvaluation() {
        final Hierarchy skuHierarchy = mock(RolapHierarchy.class);
        final Member skuAll = mock(RolapMember.class);
        when(skuAll.getHierarchy()).thenReturn(skuHierarchy);

        final Calc delegate = mock(Calc.class);
        when(delegate.getType()).thenReturn(new ScalarType());
        when(delegate.evaluate(mock(Evaluator.class))).thenReturn(null);

        final Evaluator evaluator = mock(Evaluator.class);
        when(evaluator.getContext(skuHierarchy)).thenReturn(mock(Member.class));
        when(delegate.evaluate(evaluator)).thenReturn(42);

        final PeerHierarchyResetCalc calc =
            new PeerHierarchyResetCalc(
                delegate,
                new ShareMeasurePeerHierarchyResetPlanner.ResetPlan(
                    Arrays.asList(skuAll)));

        assertEquals(42, calc.evaluate(evaluator));
        verify(evaluator).savepoint();
        final ArgumentCaptor<Member[]> membersCaptor =
            ArgumentCaptor.forClass(Member[].class);
        verify(evaluator).setContext(membersCaptor.capture());
        assertEquals(1, membersCaptor.getValue().length);
        assertSame(skuAll, membersCaptor.getValue()[0]);
        verify(evaluator).restore(0);
    }

    public void testEvaluateSkipsSavepointWhenNothingNeedsReset() {
        final Hierarchy skuHierarchy = mock(RolapHierarchy.class);
        final Member skuAll = mock(RolapMember.class);
        when(skuAll.getHierarchy()).thenReturn(skuHierarchy);

        final Calc delegate = mock(Calc.class);
        when(delegate.getType()).thenReturn(new ScalarType());

        final Evaluator evaluator = mock(Evaluator.class);
        when(evaluator.getContext(skuHierarchy)).thenReturn(skuAll);
        when(delegate.evaluate(evaluator)).thenReturn(7);

        final PeerHierarchyResetCalc calc =
            new PeerHierarchyResetCalc(
                delegate,
                new ShareMeasurePeerHierarchyResetPlanner.ResetPlan(
                    Arrays.asList(skuAll)));

        assertEquals(7, calc.evaluate(evaluator));
        verify(evaluator, never()).savepoint();
        verify(evaluator, never()).setContext(new Member[] {skuAll});
        verify(evaluator, never()).restore(0);
    }
}
