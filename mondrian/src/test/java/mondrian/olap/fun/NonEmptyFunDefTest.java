/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import junit.framework.TestCase;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.type.Type;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NonEmptyFunDefTest extends TestCase {

    public void testPrioritizeRightTuplesMovesStoredBeforeCalculated()
    {
        final Member storedMeasure =
            measure(false, mock(Exp.class));
        final Member calculatedMeasure =
            measure(true, resolvedCall("IIf", new MemberExpr(storedMeasure)));
        final Member validMeasureCalc =
            measure(
                true,
                resolvedCall(
                    "IIf",
                    resolvedCall(
                        ValidMeasureFunDef.instance,
                        new MemberExpr(storedMeasure))));

        final TupleList tuples = TupleCollections.createList(1, 0);
        tuples.addTuple(validMeasureCalc);
        tuples.addTuple(calculatedMeasure);
        tuples.addTuple(storedMeasure);

        final TupleList reordered = NonEmptyFunDef.prioritizeRightTuples(tuples);

        assertSame(storedMeasure, reordered.get(0).get(0));
        assertSame(calculatedMeasure, reordered.get(1).get(0));
        assertSame(validMeasureCalc, reordered.get(2).get(0));
    }

    public void testPrioritizeRightTuplesKeepsStableOrderWithinSameCost()
    {
        final Member storedMeasure0 = measure(false, mock(Exp.class));
        final Member storedMeasure1 = measure(false, mock(Exp.class));

        final TupleList tuples = TupleCollections.createList(1, 0);
        tuples.addTuple(storedMeasure0);
        tuples.addTuple(storedMeasure1);

        final TupleList reordered = NonEmptyFunDef.prioritizeRightTuples(tuples);

        assertSame(storedMeasure0, reordered.get(0).get(0));
        assertSame(storedMeasure1, reordered.get(1).get(0));
    }

    private Member measure(boolean calculated, Exp expression) {
        final Member member = mock(Member.class);
        when(member.isMeasure()).thenReturn(true);
        when(member.isCalculated()).thenReturn(calculated);
        when(member.getExpression()).thenReturn(expression);
        return member;
    }

    private ResolvedFunCall resolvedCall(String name, Exp... args) {
        final FunDef funDef = funDef(name, args.length);
        return resolvedCall(funDef, args);
    }

    private ResolvedFunCall resolvedCall(FunDef funDef, Exp... args) {
        return new ResolvedFunCall(
            funDef,
            Arrays.copyOf(args, args.length),
            mock(Type.class));
    }

    private FunDef funDef(String name, int argCount) {
        final FunDef funDef = mock(FunDef.class);
        when(funDef.getName()).thenReturn(name);
        when(funDef.getSignature()).thenReturn(name + "(...)");
        when(funDef.getDescription()).thenReturn(name);
        when(funDef.getSyntax()).thenReturn(Syntax.Function);
        when(funDef.getReturnCategory()).thenReturn(0);
        when(funDef.getParameterCategories()).thenReturn(new int[argCount]);
        return funDef;
    }
}
