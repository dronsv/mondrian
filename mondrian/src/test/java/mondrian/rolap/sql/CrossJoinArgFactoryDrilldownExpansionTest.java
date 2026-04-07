/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import junit.framework.TestCase;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Syntax;
import mondrian.olap.type.Type;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrossJoinArgFactoryDrilldownExpansionTest extends TestCase {

    public void testShouldNotExpandDrilldownSetWhenExpandNonNativeDisabled()
        throws Exception
    {
        final MondrianProperties props = MondrianProperties.instance();
        final boolean previous = props.ExpandNonNative.get();
        props.ExpandNonNative.set(false);
        try {
            assertFalse(
                invokeShouldExpandNonEmpty(
                    new CrossJoinArgFactory(false),
                    hierarchize(braced(drilldownLevel()))));
        } finally {
            props.ExpandNonNative.set(previous);
        }
    }

    public void testShouldExpandDrilldownSetWhenExpandNonNativeEnabled()
        throws Exception
    {
        final MondrianProperties props = MondrianProperties.instance();
        final boolean previous = props.ExpandNonNative.get();
        props.ExpandNonNative.set(true);
        try {
            assertTrue(
                invokeShouldExpandNonEmpty(
                    new CrossJoinArgFactory(false),
                    hierarchize(braced(drilldownLevel()))));
        } finally {
            props.ExpandNonNative.set(previous);
        }
    }

    public void testShouldNotExpandUnsupportedDrilldownLevelOperand()
        throws Exception
    {
        assertFalse(
            invokeShouldExpandUnsupportedNativeOperand(
                new CrossJoinArgFactory(false),
                hierarchize(braced(drilldownLevel()))));
    }

    public void testShouldNotExpandUnsupportedDrilldownMemberOperand()
        throws Exception
    {
        assertFalse(
            invokeShouldExpandUnsupportedNativeOperand(
                new CrossJoinArgFactory(false),
                hierarchize(drilldownMember())));
    }

    public void testShouldExpandNonDrilldownOperand()
        throws Exception
    {
        assertTrue(
            invokeShouldExpandUnsupportedNativeOperand(
                new CrossJoinArgFactory(false),
                hierarchize(braced(resolvedCall("Members")))));
    }

    private static boolean invokeShouldExpandNonEmpty(
        CrossJoinArgFactory factory,
        Exp exp)
        throws Exception
    {
        final Method method =
            CrossJoinArgFactory.class.getDeclaredMethod(
                "shouldExpandNonEmpty",
                Exp.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(factory, exp);
    }

    private static boolean invokeShouldExpandUnsupportedNativeOperand(
        CrossJoinArgFactory factory,
        Exp exp)
        throws Exception
    {
        final Method method =
            CrossJoinArgFactory.class.getDeclaredMethod(
                "shouldExpandUnsupportedNativeOperand",
                Exp.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(factory, exp);
    }

    private static ResolvedFunCall hierarchize(Exp arg) {
        return resolvedCall("Hierarchize", arg);
    }

    private static ResolvedFunCall braced(Exp arg) {
        return resolvedCall("{}", arg);
    }

    private static ResolvedFunCall drilldownLevel() {
        return resolvedCall("DrilldownLevel");
    }

    private static ResolvedFunCall drilldownMember() {
        return resolvedCall("DrilldownMember");
    }

    private static ResolvedFunCall resolvedCall(String name, Exp... args) {
        return new ResolvedFunCall(
            funDef(name, args.length),
            Arrays.copyOf(args, args.length),
            mock(Type.class));
    }

    private static FunDef funDef(String name, int argCount) {
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
