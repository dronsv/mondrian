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
import mondrian.calc.BooleanCalc;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.IterCalc;
import mondrian.calc.ListCalc;
import mondrian.calc.ResultStyle;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Member;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.SchemaReader;
import mondrian.olap.Syntax;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.Type;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapMember;
import mondrian.server.Execution;
import sun.misc.Unsafe;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FilterFunDefTest extends TestCase {

    public void testContextAwareNotIsEmptyPrefersNativeListEvaluator()
        throws Exception
    {
        final Scenario scenario = scenario();
        final ListCalc setCalc = mock(ListCalc.class);
        when(setCalc.getResultStyle()).thenReturn(ResultStyle.LIST);

        final BooleanCalc booleanCalc = mock(BooleanCalc.class);
        final ExpCompiler compiler = mock(ExpCompiler.class);
        when(compiler.compileList(eq(scenario.setExp), eq(false)))
            .thenReturn(setCalc);
        when(compiler.compileBoolean(any(Exp.class))).thenReturn(booleanCalc);

        final NativeEvaluator nativeEvaluator = mock(NativeEvaluator.class);
        final TupleList nativeResult = TupleCollections.createList(1, 0);
        nativeResult.addTuple(scenario.axisMember);
        when(nativeEvaluator.execute(ResultStyle.LIST)).thenReturn(nativeResult);
        when(scenario.schemaReader.getNativeSetEvaluator(
            eq(FilterFunDef.instance),
            any(Exp[].class),
            any(Evaluator.class),
            any(Calc.class)))
            .thenReturn(nativeEvaluator);

        final ListCalc calc =
            FilterFunDef.instance.compileCallList(scenario.filterCall, compiler);
        final TupleList result = calc.evaluateList(scenario.evaluator);

        assertSame(nativeResult, result);
        verify(nativeEvaluator).execute(ResultStyle.LIST);
        verify(setCalc, never()).evaluateList(any(Evaluator.class));
    }

    public void testContextAwareNotIsEmptyPrefersNativeIterableEvaluator()
        throws Exception
    {
        final Scenario scenario = scenario();
        final IterCalc setCalc = mock(IterCalc.class);
        when(setCalc.getResultStyle()).thenReturn(ResultStyle.ITERABLE);

        final BooleanCalc booleanCalc = mock(BooleanCalc.class);
        final ExpCompiler compiler = mock(ExpCompiler.class);
        when(compiler.compileAs(
            eq(scenario.setExp),
            eq((Type) null),
            eq(ResultStyle.ITERABLE_LIST_MUTABLELIST)))
            .thenReturn(setCalc);
        when(compiler.compileBoolean(any(Exp.class))).thenReturn(booleanCalc);

        final NativeEvaluator nativeEvaluator = mock(NativeEvaluator.class);
        final TupleList nativeResult = TupleCollections.createList(1, 0);
        nativeResult.addTuple(scenario.axisMember);
        when(nativeEvaluator.execute(ResultStyle.ITERABLE))
            .thenReturn(nativeResult);
        when(scenario.schemaReader.getNativeSetEvaluator(
            eq(FilterFunDef.instance),
            any(Exp[].class),
            any(Evaluator.class),
            any(Calc.class)))
            .thenReturn(nativeEvaluator);

        final IterCalc calc =
            FilterFunDef.instance.compileCallIterable(
                scenario.filterCall,
                compiler);
        final TupleIterable result = calc.evaluateIterable(scenario.evaluator);

        assertSame(nativeResult, result);
        verify(nativeEvaluator).execute(ResultStyle.ITERABLE);
        verify(setCalc, never()).evaluateIterable(any(Evaluator.class));
    }

    private Scenario scenario() throws Exception {
        final Scenario scenario = new Scenario();
        scenario.schemaReader = mock(SchemaReader.class);

        final RolapHierarchy hierarchy = mock(RolapHierarchy.class);

        scenario.axisMember = mock(RolapMember.class);
        when(scenario.axisMember.isEvaluated()).thenReturn(false);
        when(scenario.axisMember.isMeasure()).thenReturn(false);
        when(scenario.axisMember.isAll()).thenReturn(false);
        when(scenario.axisMember.getHierarchy()).thenReturn(hierarchy);

        final Member measure = mock(Member.class);
        when(measure.isMeasure()).thenReturn(true);
        when(measure.isCalculated()).thenReturn(false);

        scenario.setExp = mock(Exp.class);
        scenario.filterCall =
            new ResolvedFunCall(
                FilterFunDef.instance,
                new Exp[] {
                    scenario.setExp,
                    resolvedCall(
                        "Not",
                        resolvedCall(
                            "IsEmpty",
                            new MemberExpr(measure)))
                },
                new SetType(MemberType.Unknown));

        scenario.evaluator =
            newRolapEvaluator(scenario.schemaReader, scenario.axisMember);
        return scenario;
    }

    private RolapEvaluator newRolapEvaluator(
        SchemaReader schemaReader,
        RolapMember axisMember)
        throws Exception
    {
        final Class<?> rootClass =
            Class.forName("mondrian.rolap.RolapEvaluatorRoot");
        final Object root = unsafe().allocateInstance(rootClass);
        setField(rootClass, root, "schemaReader", schemaReader);
        setField(rootClass, root, "defaultMembers", new RolapMember[] {axisMember});
        setField(rootClass, root, "nonAllPositions", new int[] {0});
        setField(rootClass, root, "nonAllPositionCount", Integer.valueOf(1));
        setField(rootClass, root, "query", null);
        setField(rootClass, root, "execution", Execution.NONE);

        final Constructor<RolapEvaluator> constructor =
            RolapEvaluator.class.getConstructor(rootClass);
        return constructor.newInstance(root);
    }

    private ResolvedFunCall resolvedCall(String name, Exp... args) {
        return new ResolvedFunCall(
            functionDef(name),
            args,
            mock(Type.class));
    }

    private FunDef functionDef(String name) {
        final FunDef funDef = mock(FunDef.class);
        when(funDef.getName()).thenReturn(name);
        when(funDef.getSignature()).thenReturn(name + "(...)");
        when(funDef.getDescription()).thenReturn(name);
        when(funDef.getSyntax()).thenReturn(Syntax.Function);
        when(funDef.getReturnCategory()).thenReturn(0);
        when(funDef.getParameterCategories()).thenReturn(new int[0]);
        return funDef;
    }

    private Unsafe unsafe() throws Exception {
        final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        return (Unsafe) unsafeField.get(null);
    }

    private void setField(
        Class<?> targetClass,
        Object target,
        String fieldName,
        Object value)
        throws Exception
    {
        final Field field = targetClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class Scenario {
        private SchemaReader schemaReader;
        private RolapEvaluator evaluator;
        private RolapMember axisMember;
        private Exp setExp;
        private ResolvedFunCall filterCall;
    }
}
