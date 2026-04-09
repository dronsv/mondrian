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
import mondrian.olap.Syntax;

import java.math.BigDecimal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FormulaAnalyzer}.
 */
public class FormulaAnalyzerTest extends TestCase {

    // -----------------------------------------------------------------------
    // Eligible formula tests
    // -----------------------------------------------------------------------

    /** a / b → eligible, 2 leaf refs, no guard stripped */
    public void testSimpleRatioIsEligible() {
        Exp a = mockMeasureExpr("sales_rub");
        Exp b = mockMeasureExpr("akb");
        FunCall divide = mockFunCall("/", a, b);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(divide);
        assertTrue(r.isEligibleForPostProcess());
        assertNull(r.unsupportedReason);
        assertEquals(2, r.leafRefs.size());
        assertFalse(r.guardStripped);
    }

    /** (a / b) * 100 → eligible */
    public void testScaledRatioIsEligible() {
        Exp a = mockMeasureExpr("sales_rub");
        Exp b = mockMeasureExpr("akb");
        FunCall divide = mockFunCall("/", a, b);
        Literal hundred = Literal.create(BigDecimal.valueOf(100));
        FunCall multiply = mockFunCall("*", divide, hundred);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(multiply);
        assertTrue(r.isEligibleForPostProcess());
        assertEquals(2, r.leafRefs.size());
        assertFalse(r.guardStripped);
    }

    /** a * 0.001 → eligible */
    public void testScaledValueIsEligible() {
        Exp a = mockMeasureExpr("sales_rub");
        Literal scale = Literal.create(BigDecimal.valueOf(0.001));
        FunCall multiply = mockFunCall("*", a, scale);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(multiply);
        assertTrue(r.isEligibleForPostProcess());
        assertEquals(1, r.leafRefs.size());
    }

    /** a + b → eligible */
    public void testAdditiveIsEligible() {
        Exp a = mockMeasureExpr("x");
        Exp b = mockMeasureExpr("y");
        FunCall plus = mockFunCall("+", a, b);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(plus);
        assertTrue(r.isEligibleForPostProcess());
        assertEquals(2, r.leafRefs.size());
    }

    /** a - b → eligible */
    public void testSubtractiveIsEligible() {
        Exp a = mockMeasureExpr("x");
        Exp b = mockMeasureExpr("y");
        FunCall minus = mockFunCall("-", a, b);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(minus);
        assertTrue(r.isEligibleForPostProcess());
        assertEquals(2, r.leafRefs.size());
    }

    /** single measure reference → eligible */
    public void testSingleMeasureRefIsEligible() {
        Exp a = mockMeasureExpr("sales_qty");

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(a);
        assertTrue(r.isEligibleForPostProcess());
        assertEquals(1, r.leafRefs.size());
        assertFalse(r.guardStripped);
    }

    /** (a - b) / a * 100 → eligible (the formula that broke FormulaNormalizer) */
    public void testCompoundDifferenceRatioIsEligible() {
        Exp a = mockMeasureExpr("total");
        Exp b = mockMeasureExpr("part");
        FunCall subtract = mockFunCall("-", a, b);
        FunCall divide = mockFunCall("/", subtract, a);
        Literal hundred = Literal.create(BigDecimal.valueOf(100));
        FunCall multiply = mockFunCall("*", divide, hundred);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(multiply);
        assertTrue(
            "Compound (A-B)/A*100 must be eligible",
            r.isEligibleForPostProcess());
        // 'a' appears twice: in (a-b) and in /(.,a). collectLeafRefs
        // will find both occurrences.
        assertTrue(r.leafRefs.size() >= 2);
    }

    // -----------------------------------------------------------------------
    // Ineligible formula tests
    // -----------------------------------------------------------------------

    /** null expression → ineligible */
    public void testNullExpression() {
        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(null);
        assertFalse(r.isEligibleForPostProcess());
        assertNotNull(r.unsupportedReason);
        assertTrue(r.leafRefs.isEmpty());
        assertFalse(r.guardStripped);
    }

    /** Aggregate(x) → ineligible (unsafe function) */
    public void testAggregateIsIneligible() {
        Exp x = mockMeasureExpr("x");
        FunCall agg = mockFunCall("Aggregate", x);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(agg);
        assertFalse(r.isEligibleForPostProcess());
        assertNotNull(r.unsupportedReason);
        assertTrue(r.unsupportedReason.contains("unsafe function"));
    }

    /** Sum({set}, measure) → ineligible */
    public void testSumIsIneligible() {
        Exp x = mockMeasureExpr("x");
        FunCall sum = mockFunCall("Sum", x);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(sum);
        assertFalse(r.isEligibleForPostProcess());
        assertTrue(r.unsupportedReason.contains("unsafe function"));
    }

    /** Filter(set, condition) → ineligible */
    public void testFilterIsIneligible() {
        Exp x = mockMeasureExpr("x");
        FunCall filter = mockFunCall("Filter", x);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(filter);
        assertFalse(r.isEligibleForPostProcess());
        assertTrue(r.unsupportedReason.contains("unsafe function"));
    }

    /** PrevMember → ineligible (navigation) */
    public void testPrevMemberIsIneligible() {
        Exp x = mockMeasureExpr("x");
        FunCall prev = mockFunCall("PrevMember", x);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(prev);
        assertFalse(r.isEligibleForPostProcess());
        assertTrue(r.unsupportedReason.contains("unsafe function"));
    }

    /** Nested unsafe: a / Sum(set) → ineligible */
    public void testNestedUnsafeFunctionIsIneligible() {
        Exp a = mockMeasureExpr("a");
        Exp b = mockMeasureExpr("b");
        FunCall sum = mockFunCall("Sum", b);
        FunCall divide = mockFunCall("/", a, sum);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(divide);
        assertFalse(r.isEligibleForPostProcess());
        assertTrue(r.unsupportedReason.contains("unsafe function"));
    }

    // -----------------------------------------------------------------------
    // IIF null-guard stripping — IsEmpty variant
    // -----------------------------------------------------------------------

    /** IIF(IsEmpty(x), NULL, x / y) → eligible, guardStripped=true */
    public void testStripsIifIsEmptyGuardNullFirst() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall isEmpty = mockFunCall("IsEmpty", x);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", isEmpty, Literal.nullValue, divide);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(iif);
        assertTrue(r.isEligibleForPostProcess());
        assertTrue(r.guardStripped);
        assertEquals(2, r.leafRefs.size());
    }

    /** IIF(IsEmpty(x), x / y, NULL) → eligible, guardStripped=true (inverted) */
    public void testStripsIifIsEmptyGuardNullSecond() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall isEmpty = mockFunCall("IsEmpty", x);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", isEmpty, divide, Literal.nullValue);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(iif);
        assertTrue(r.isEligibleForPostProcess());
        assertTrue(r.guardStripped);
    }

    // -----------------------------------------------------------------------
    // IIF null-guard stripping — zero-equality variant
    // -----------------------------------------------------------------------

    /** IIF(y = 0, NULL, x / y) → eligible, guardStripped=true */
    public void testStripsIifZeroGuard() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall eqZero = mockFunCall("=", y, Literal.zero);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", eqZero, Literal.nullValue, divide);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(iif);
        assertTrue(r.isEligibleForPostProcess());
        assertTrue(r.guardStripped);
    }

    /** IIF(0 = y, NULL, x / y) → eligible (zero on left side) */
    public void testStripsIifZeroGuardZeroOnLeft() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall eqZero = mockFunCall("=", Literal.zero, y);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", eqZero, Literal.nullValue, divide);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(iif);
        assertTrue(r.isEligibleForPostProcess());
        assertTrue(r.guardStripped);
    }

    // -----------------------------------------------------------------------
    // IIF with OR condition
    // -----------------------------------------------------------------------

    /** IIF(IsEmpty(x) OR y=0, NULL, x/y) → eligible, guardStripped=true */
    public void testStripsIifOrGuard() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall isEmpty = mockFunCall("IsEmpty", x);
        FunCall eqZero = mockFunCall("=", y, Literal.zero);
        FunCall or = mockFunCall("OR", isEmpty, eqZero);
        FunCall divide = mockFunCall("/", x, y);
        FunCall iif = mockFunCall("IIf", or, Literal.nullValue, divide);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(iif);
        assertTrue(r.isEligibleForPostProcess());
        assertTrue(r.guardStripped);
    }

    // -----------------------------------------------------------------------
    // IIF that is NOT a null guard should not be stripped
    // -----------------------------------------------------------------------

    /** IIF(some_condition, expr1, expr2) where neither branch is NULL → not stripped */
    public void testDoesNotStripNonNullGuardIif() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall cond = mockFunCall("IsEmpty", x);
        // Both branches are expressions, not NULL
        FunCall iif = mockFunCall("IIf", cond, x, y);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(iif);
        // The IIf is not a null guard, so it should not be stripped
        assertFalse(r.guardStripped);
        // IIf itself is not unsafe — it's eligible because both branches
        // are simple measure refs
        assertTrue(r.isEligibleForPostProcess());
    }

    // -----------------------------------------------------------------------
    // Nested null guards
    // -----------------------------------------------------------------------

    /** IIF(IsEmpty(x), NULL, IIF(y=0, NULL, x / y)) → eligible, nested guards stripped */
    public void testStripsNestedNullGuards() {
        Exp x = mockMeasureExpr("x");
        Exp y = mockMeasureExpr("y");
        FunCall divide = mockFunCall("/", x, y);
        FunCall eqZero = mockFunCall("=", y, Literal.zero);
        FunCall innerIif = mockFunCall("IIf", eqZero, Literal.nullValue, divide);
        FunCall isEmpty = mockFunCall("IsEmpty", x);
        FunCall outerIif = mockFunCall("IIf", isEmpty, Literal.nullValue, innerIif);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(outerIif);
        assertTrue(r.isEligibleForPostProcess());
        assertTrue(r.guardStripped);
        assertSame(divide, r.normalizedExp);
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

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(iif);
        assertSame(divide, r.normalizedExp);
    }

    /** Without guard, normalizedExp is the original expression */
    public void testNormalizedExpIsOriginalWhenNoGuard() {
        Exp a = mockMeasureExpr("a");
        Exp b = mockMeasureExpr("b");
        FunCall divide = mockFunCall("/", a, b);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(divide);
        assertSame(divide, r.normalizedExp);
    }

    // -----------------------------------------------------------------------
    // UNSAFE_FUNCTIONS coverage
    // -----------------------------------------------------------------------

    /** Verify all major unsafe function categories are rejected */
    public void testUnsafeFunctionCategories() {
        String[] unsafeFunctions = {
            // Set-producing
            "Members", "Descendants", "Filter", "TopCount",
            "NonEmpty", "CrossJoin",
            // Set-aggregating
            "Sum", "Avg", "Count", "Aggregate",
            // Navigation
            "PrevMember", "Lag", "Lead", "ParallelPeriod",
            "StrToMember"
        };
        for (String fn : unsafeFunctions) {
            Exp x = mockMeasureExpr("x");
            FunCall fc = mockFunCall(fn, x);
            FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(fc);
            assertFalse(
                fn + " should be ineligible",
                r.isEligibleForPostProcess());
            assertNotNull(
                fn + " should have unsupportedReason",
                r.unsupportedReason);
        }
    }

    // -----------------------------------------------------------------------
    // Coordinate-changing tuple detection
    // -----------------------------------------------------------------------

    /** Tuple with non-measure dimension member → ineligible */
    public void testTupleWithNonMeasureDimensionIsIneligible() {
        Member measureMember = mock(Member.class);
        when(measureMember.isMeasure()).thenReturn(true);
        MemberExpr measureExpr = mock(MemberExpr.class);
        when(measureExpr.getMember()).thenReturn(measureMember);

        Member dimMember = mock(Member.class);
        when(dimMember.isMeasure()).thenReturn(false);
        MemberExpr dimExpr = mock(MemberExpr.class);
        when(dimExpr.getMember()).thenReturn(dimMember);

        FunCall tuple = mockTupleFunCall(measureExpr, dimExpr);

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(tuple);
        assertFalse(r.isEligibleForPostProcess());
        assertNotNull(r.unsupportedReason);
        assertTrue(r.unsupportedReason.contains("coordinate-changing tuple"));
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
        // Default syntax: Function (not Parentheses)
        when(fc.getSyntax()).thenReturn(Syntax.Function);
        return fc;
    }

    /** Create a FunCall mock with Parentheses syntax (tuple constructor). */
    private FunCall mockTupleFunCall(Exp... args) {
        FunCall fc = mock(FunCall.class);
        when(fc.getFunName()).thenReturn("()");
        when(fc.getArgs()).thenReturn(args);
        when(fc.getArgCount()).thenReturn(args.length);
        for (int i = 0; i < args.length; i++) {
            when(fc.getArg(i)).thenReturn(args[i]);
        }
        when(fc.getSyntax()).thenReturn(Syntax.Parentheses);
        return fc;
    }
}

// End FormulaAnalyzerTest.java
