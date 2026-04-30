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

import mondrian.mdx.MemberExpr;
import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Hierarchy;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FormulaAnalyzer#detectCoordinatePinTuple(Exp)}.
 *
 * <p>The recognizer is purely structural — these tests use mocks for
 * {@link Exp}, {@link FunCall}, {@link MemberExpr}, {@link Member}, and
 * {@link Hierarchy} rather than spinning up a Mondrian
 * {@code TestContext}.
 */
class CoordinatePinTupleAnalyzerTest {

    /** ([Measures].[X], [H1].[All], [H2].[All]) → recognised. */
    @Test
    void detectsBareTuple() {
        Member measure = mockMeasure();
        Hierarchy hA = mock(Hierarchy.class);
        Hierarchy hB = mock(Hierarchy.class);
        FunCall tuple = mockTuple(
            measureExpr(measure),
            allMemberExpr(hA),
            allMemberExpr(hB));

        FormulaAnalyzer.CoordinatePinTuple result =
            FormulaAnalyzer.detectCoordinatePinTuple(tuple);

        assertNotNull(result);
        assertSame(measure, result.innerMeasure);
        assertEquals(2, result.pinnedHierarchies.size());
        assertTrue(result.pinnedHierarchies.contains(hA));
        assertTrue(result.pinnedHierarchies.contains(hB));
    }

    /** IIF(IsEmpty(x), NULL, (measure, h.[All])) → recognised. */
    @Test
    void detectsIifWrappedTuple() {
        Member measure = mockMeasure();
        Hierarchy hA = mock(Hierarchy.class);
        FunCall tuple = mockTuple(measureExpr(measure), allMemberExpr(hA));
        // IIF(IsEmpty(x), NULL, tuple) — match the form
        // FormulaAnalyzer.isNullGuardIif() understands.
        Member guardMember = mock(Member.class);
        when(guardMember.isMeasure()).thenReturn(true);
        MemberExpr guardExpr = mock(MemberExpr.class);
        when(guardExpr.getMember()).thenReturn(guardMember);
        FunCall isEmpty = mockFunCall("IsEmpty", guardExpr);
        FunCall iif =
            mockFunCall("IIf", isEmpty, Literal.nullValue, tuple);

        FormulaAnalyzer.CoordinatePinTuple result =
            FormulaAnalyzer.detectCoordinatePinTuple(iif);

        assertNotNull(result);
        assertSame(measure, result.innerMeasure);
        assertEquals(1, result.pinnedHierarchies.size());
        assertTrue(result.pinnedHierarchies.contains(hA));
    }

    /** (measure, [h.NonAll]) → rejected (member is not All). */
    @Test
    void rejectsNonAllMember() {
        Member measure = mockMeasure();
        Hierarchy hA = mock(Hierarchy.class);
        Member nonAllMember = mock(Member.class);
        when(nonAllMember.isAll()).thenReturn(false);
        when(nonAllMember.getHierarchy()).thenReturn(hA);
        MemberExpr nonAll = mock(MemberExpr.class);
        when(nonAll.getMember()).thenReturn(nonAllMember);

        FunCall tuple = mockTuple(measureExpr(measure), nonAll);

        assertNull(FormulaAnalyzer.detectCoordinatePinTuple(tuple));
    }

    /** (nonMeasure, h.[All]) → rejected (first arg is not a measure). */
    @Test
    void rejectsNonMeasureFirstArg() {
        Member nonMeasure = mock(Member.class);
        when(nonMeasure.isMeasure()).thenReturn(false);
        MemberExpr first = mock(MemberExpr.class);
        when(first.getMember()).thenReturn(nonMeasure);
        Hierarchy hA = mock(Hierarchy.class);

        FunCall tuple = mockTuple(first, allMemberExpr(hA));

        assertNull(FormulaAnalyzer.detectCoordinatePinTuple(tuple));
    }

    /** (measure, h.[All], h.[All]) → rejected (duplicate hierarchy). */
    @Test
    void rejectsDuplicateHierarchy() {
        Member measure = mockMeasure();
        Hierarchy hA = mock(Hierarchy.class);

        FunCall tuple = mockTuple(
            measureExpr(measure),
            allMemberExpr(hA),
            allMemberExpr(hA));

        assertNull(FormulaAnalyzer.detectCoordinatePinTuple(tuple));
    }

    /** Bare measure ref (no tuple wrapper) → rejected. */
    @Test
    void rejectsBareMeasureRef() {
        Member measure = mockMeasure();
        MemberExpr ref = measureExpr(measure);

        assertNull(FormulaAnalyzer.detectCoordinatePinTuple(ref));
    }

    /** {@code null} → returns {@code null} (no NPE). */
    @Test
    void rejectsNull() {
        assertNull(FormulaAnalyzer.detectCoordinatePinTuple(null));
    }

    /** A pin tuple inside {@code analyze()} clears unsupportedReason. */
    @Test
    void analyzeClearsUnsupportedReasonOnPinMatch() {
        Member measure = mockMeasure();
        Hierarchy hA = mock(Hierarchy.class);
        FunCall tuple = mockTuple(measureExpr(measure), allMemberExpr(hA));

        FormulaAnalyzer.Result r = FormulaAnalyzer.analyze(tuple);

        assertNotNull(r.coordinatePinTuple);
        assertSame(measure, r.coordinatePinTuple.innerMeasure);
        assertNull(
            r.unsupportedReason,
            "pin recognition takes precedence over "
            + "'coordinate-changing tuple' rejection");
    }

    // --- helpers ---

    private static Member mockMeasure() {
        Member m = mock(Member.class);
        when(m.isMeasure()).thenReturn(true);
        return m;
    }

    private static MemberExpr measureExpr(Member m) {
        MemberExpr me = mock(MemberExpr.class);
        when(me.getMember()).thenReturn(m);
        return me;
    }

    private static MemberExpr allMemberExpr(Hierarchy h) {
        Member m = mock(Member.class);
        when(m.isAll()).thenReturn(true);
        when(m.getHierarchy()).thenReturn(h);
        MemberExpr me = mock(MemberExpr.class);
        when(me.getMember()).thenReturn(m);
        return me;
    }

    /** Build a parenthesised tuple FunCall mock matching the AST shape. */
    private static FunCall mockTuple(Exp... args) {
        FunCall fc = mock(FunCall.class);
        when(fc.getFunName()).thenReturn("()");
        when(fc.getSyntax()).thenReturn(Syntax.Parentheses);
        when(fc.getArgCount()).thenReturn(args.length);
        when(fc.getArgs()).thenReturn(args);
        for (int i = 0; i < args.length; i++) {
            when(fc.getArg(i)).thenReturn(args[i]);
        }
        return fc;
    }

    /**
     * Build a generic FunCall mock (default {@link Syntax#Function}). Used
     * for things like IIF / IsEmpty wrappers that are not tuple
     * constructors.
     */
    private static FunCall mockFunCall(String name, Exp... args) {
        FunCall fc = mock(FunCall.class);
        when(fc.getFunName()).thenReturn(name);
        when(fc.getSyntax()).thenReturn(Syntax.Function);
        when(fc.getArgCount()).thenReturn(args.length);
        when(fc.getArgs()).thenReturn(args);
        for (int i = 0; i < args.length; i++) {
            when(fc.getArg(i)).thenReturn(args[i]);
        }
        return fc;
    }
}

// End CoordinatePinTupleAnalyzerTest.java
