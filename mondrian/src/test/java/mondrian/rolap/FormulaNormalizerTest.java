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
import mondrian.mdx.MemberExpr;
import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Literal;
import mondrian.olap.Member;

import java.math.BigDecimal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FormulaNormalizer}.
 */
public class FormulaNormalizerTest extends TestCase {

    // -----------------------------------------------------------------------
    // Basic pattern tests
    // -----------------------------------------------------------------------

    /** a / b → RATIO pattern, 2 leaf refs, no guard stripped */
    public void testIdentifiesSimpleRatio() {
        Exp a = mockMeasureExpr("sales_rub");
        Exp b = mockMeasureExpr("akb");
        FunCall divide = mockFunCall("/", a, b);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(divide);
        assertEquals(FormulaNormalizer.Pattern.RATIO, r.pattern);
        assertEquals(2, r.leafRefs.size());
        assertFalse(r.guardStripped);
    }

    /** (a / b) * 100 → SCALED_RATIO */
    public void testIdentifiesScaledRatio() {
        Exp a = mockMeasureExpr("sales_rub");
        Exp b = mockMeasureExpr("akb");
        FunCall divide = mockFunCall("/", a, b);
        Literal hundred = Literal.create(BigDecimal.valueOf(100));
        FunCall multiply = mockFunCall("*", divide, hundred);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(multiply);
        assertEquals(FormulaNormalizer.Pattern.SCALED_RATIO, r.pattern);
        assertEquals(2, r.leafRefs.size());
        assertFalse(r.guardStripped);
    }

    /** 100 * (a / b) → SCALED_RATIO (literal first) */
    public void testIdentifiesScaledRatioLiteralFirst() {
        Exp a = mockMeasureExpr("sales_rub");
        Exp b = mockMeasureExpr("akb");
        FunCall divide = mockFunCall("/", a, b);
        Literal hundred = Literal.create(BigDecimal.valueOf(100));
        FunCall multiply = mockFunCall("*", hundred, divide);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(multiply);
        assertEquals(FormulaNormalizer.Pattern.SCALED_RATIO, r.pattern);
        assertEquals(2, r.leafRefs.size());
    }

    /** a * 0.001 → SCALED_VALUE */
    public void testIdentifiesScaledValue() {
        Exp a = mockMeasureExpr("sales_rub");
        Literal scale = Literal.create(BigDecimal.valueOf(0.001));
        FunCall multiply = mockFunCall("*", a, scale);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(multiply);
        assertEquals(FormulaNormalizer.Pattern.SCALED_VALUE, r.pattern);
        assertEquals(1, r.leafRefs.size());
    }

    /** a + b → ADDITIVE */
    public void testIdentifiesAdditive() {
        Exp a = mockMeasureExpr("x");
        Exp b = mockMeasureExpr("y");
        FunCall plus = mockFunCall("+", a, b);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(plus);
        assertEquals(FormulaNormalizer.Pattern.ADDITIVE, r.pattern);
        assertEquals(2, r.leafRefs.size());
    }

    /** a - b → ADDITIVE */
    public void testIdentifiesSubtractive() {
        Exp a = mockMeasureExpr("x");
        Exp b = mockMeasureExpr("y");
        FunCall minus = mockFunCall("-", a, b);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(minus);
        assertEquals(FormulaNormalizer.Pattern.ADDITIVE, r.pattern);
        assertEquals(2, r.leafRefs.size());
    }

    /** single measure reference → SINGLE_REF */
    public void testSingleMeasureRef() {
        Exp a = mockMeasureExpr("sales_qty");

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(a);
        assertEquals(FormulaNormalizer.Pattern.SINGLE_REF, r.pattern);
        assertEquals(1, r.leafRefs.size());
        assertFalse(r.guardStripped);
    }

    /** null expression → UNSUPPORTED */
    public void testNullExpression() {
        FormulaNormalizer.Result r = FormulaNormalizer.normalize(null);
        assertEquals(FormulaNormalizer.Pattern.UNSUPPORTED, r.pattern);
        assertTrue(r.leafRefs.isEmpty());
        assertFalse(r.guardStripped);
    }

    // -----------------------------------------------------------------------
    // IIF null-guard stripping — IsEmpty variant
    // -----------------------------------------------------------------------

    /** IIF(IsEmpty(x), NULL, x / y) → RATIO, guardStripped=true */
    public void testStripsIifIsEmptyGuardNullFirst() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall isEmpty = mockFunCall("IsEmpty", x);
        FunCall divide = mockFunCall("/", x, y);
        // true branch = NULL, false branch = divide
        FunCall iif = mockFunCall("IIf", isEmpty, Literal.nullValue, divide);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(iif);
        assertEquals(FormulaNormalizer.Pattern.RATIO, r.pattern);
        assertTrue(r.guardStripped);
        assertEquals(2, r.leafRefs.size());
    }

    /** IIF(IsEmpty(x), x / y, NULL) → RATIO, guardStripped=true (inverted form) */
    public void testStripsIifIsEmptyGuardNullSecond() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall isEmpty = mockFunCall("IsEmpty", x);
        FunCall divide = mockFunCall("/", x, y);
        // true branch = divide, false branch = NULL
        FunCall iif = mockFunCall("IIf", isEmpty, divide, Literal.nullValue);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(iif);
        assertEquals(FormulaNormalizer.Pattern.RATIO, r.pattern);
        assertTrue(r.guardStripped);
    }

    // -----------------------------------------------------------------------
    // IIF null-guard stripping — zero-equality variant
    // -----------------------------------------------------------------------

    /** IIF(y = 0, NULL, x / y) → RATIO, guardStripped=true */
    public void testStripsIifZeroGuard() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall eqZero = mockFunCall("=", y, Literal.zero);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", eqZero, Literal.nullValue, divide);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(iif);
        assertEquals(FormulaNormalizer.Pattern.RATIO, r.pattern);
        assertTrue(r.guardStripped);
    }

    /** IIF(0 = y, NULL, x / y) → RATIO (zero on left side) */
    public void testStripsIifZeroGuardZeroOnLeft() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall eqZero = mockFunCall("=", Literal.zero, y);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", eqZero, Literal.nullValue, divide);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(iif);
        assertEquals(FormulaNormalizer.Pattern.RATIO, r.pattern);
        assertTrue(r.guardStripped);
    }

    // -----------------------------------------------------------------------
    // IIF with OR condition
    // -----------------------------------------------------------------------

    /** IIF(IsEmpty(x) OR y=0, NULL, x/y) → RATIO, guardStripped=true */
    public void testStripsIifOrGuard() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall isEmpty = mockFunCall("IsEmpty", x);
        FunCall eqZero = mockFunCall("=", y, Literal.zero);
        FunCall or = mockFunCall("OR", isEmpty, eqZero);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", or, Literal.nullValue, divide);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(iif);
        assertEquals(FormulaNormalizer.Pattern.RATIO, r.pattern);
        assertTrue(r.guardStripped);
    }

    // -----------------------------------------------------------------------
    // IIF that is NOT a null guard should not be stripped
    // -----------------------------------------------------------------------

    /** IIF(some_condition, expr1, expr2) where neither branch is NULL → UNSUPPORTED, no strip */
    public void testDoesNotStripNonNullGuardIif() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall cond = mockFunCall("IsEmpty", x);
        // Both branches are expressions, not NULL
        FunCall iif = mockFunCall("IIf", cond, x, y);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(iif);
        // The IIf is not a null guard, so it should not be stripped
        assertFalse(r.guardStripped);
        // The inner expression is an IIf funCall — classified as UNSUPPORTED
        assertEquals(FormulaNormalizer.Pattern.UNSUPPORTED, r.pattern);
    }

    // -----------------------------------------------------------------------
    // normalizedExp field
    // -----------------------------------------------------------------------

    /** After stripping, normalizedExp is the inner expression (not the IIF) */
    public void testNormalizedExpIsInnerAfterStrip() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall isEmpty = mockFunCall("IsEmpty", x);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", isEmpty, Literal.nullValue, divide);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(iif);
        assertSame(divide, r.normalizedExp);
    }

    /** Without guard, normalizedExp is the original expression */
    public void testNormalizedExpIsOriginalWhenNoGuard() {
        Exp a = mockMeasureExpr("a");
        Exp b = mockMeasureExpr("b");
        FunCall divide = mockFunCall("/", a, b);

        FormulaNormalizer.Result r = FormulaNormalizer.normalize(divide);
        assertSame(divide, r.normalizedExp);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Create a MemberExpr mock wrapping a measure member. */
    private MemberExpr mockMeasureExpr(String name) {
        Member member = mock(Member.class);
        when(member.isMeasure()).thenReturn(true);
        when(member.getName()).thenReturn(name);

        MemberExpr expr = mock(MemberExpr.class);
        when(expr.getMember()).thenReturn(member);
        return expr;
    }

    /** Create a FunCall mock. */
    private FunCall mockFunCall(String name, Exp... args) {
        FunCall fc = mock(FunCall.class);
        when(fc.getFunName()).thenReturn(name);
        when(fc.getArgs()).thenReturn(args);
        when(fc.getArgCount()).thenReturn(args.length);
        for (int i = 0; i < args.length; i++) {
            when(fc.getArg(i)).thenReturn(args[i]);
        }
        return fc;
    }
}

// End FormulaNormalizerTest.java
