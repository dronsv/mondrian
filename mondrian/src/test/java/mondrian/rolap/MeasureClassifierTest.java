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
import mondrian.olap.Member;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link MeasureClassifier}.
 */
public class MeasureClassifierTest extends TestCase {

    // -----------------------------------------------------------------------
    // Single-measure classify() tests
    // -----------------------------------------------------------------------

    /** A non-measure member must be classified as EVALUATOR with a red flag. */
    public void testNonMeasureClassifiedAsEvaluator() {
        Member member = mock(Member.class);
        when(member.isMeasure()).thenReturn(false);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(member);
        assertEquals(MeasureClassifier.CandidateClass.EVALUATOR, c.candidateClass);
        assertTrue(c.redFlags.contains("not a measure"));
        assertNull(c.normalizedFormula);
    }

    /** A stored (non-calculated) measure must be DIRECT_PUSH_STORED. */
    public void testStoredMeasureClassifiedAsDirectPush() {
        Member measure = mock(Member.class);
        when(measure.isMeasure()).thenReturn(true);
        when(measure.isCalculated()).thenReturn(false);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(measure);
        assertEquals(MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED, c.candidateClass);
        assertNull(c.normalizedFormula);
        assertTrue(c.redFlags.isEmpty());
    }

    /**
     * A calculated measure whose formula is null must be EVALUATOR,
     * because there is nothing to normalise.
     */
    public void testCalcMeasureWithNullFormulaClassifiedAsEvaluator() {
        Member measure = mock(Member.class);
        when(measure.isMeasure()).thenReturn(true);
        when(measure.isCalculated()).thenReturn(true);
        when(measure.getExpression()).thenReturn(null);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(measure);
        assertEquals(MeasureClassifier.CandidateClass.EVALUATOR, c.candidateClass);
        assertTrue(c.redFlags.contains("null formula expression"));
    }

    /**
     * A calculated measure with a simple ratio formula (a / b) must be
     * POST_PROCESS_CANDIDATE with RATIO pattern.
     */
    public void testCalcMeasureWithRatioFormulaClassifiedAsPostProcess() {
        Member measure = mock(Member.class);
        when(measure.isMeasure()).thenReturn(true);
        when(measure.isCalculated()).thenReturn(true);

        MemberExpr a = mockMeasureExpr("sales_rub");
        MemberExpr b = mockMeasureExpr("akb");
        FunCall divide = mockFunCall("/", a, b);
        when(measure.getExpression()).thenReturn(divide);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(measure);
        assertEquals(MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE, c.candidateClass);
        assertNotNull(c.normalizedFormula);
        assertTrue(c.normalizedFormula.isEligibleForPostProcess());
        assertTrue(c.redFlags.isEmpty());
    }

    /**
     * A calculated measure with an additive formula (a + b) must be
     * POST_PROCESS_CANDIDATE with ADDITIVE pattern.
     */
    public void testCalcMeasureWithAdditiveFormulaClassifiedAsPostProcess() {
        Member measure = mock(Member.class);
        when(measure.isMeasure()).thenReturn(true);
        when(measure.isCalculated()).thenReturn(true);

        MemberExpr a = mockMeasureExpr("sales_qty");
        MemberExpr b = mockMeasureExpr("returns_qty");
        FunCall plus = mockFunCall("+", a, b);
        when(measure.getExpression()).thenReturn(plus);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(measure);
        assertEquals(MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE, c.candidateClass);
        assertNotNull(c.normalizedFormula);
        assertTrue(c.normalizedFormula.isEligibleForPostProcess());
    }

    /**
     * A calculated measure with an unsupported formula pattern (e.g. Aggregate)
     * must be EVALUATOR with an appropriate red flag.
     */
    public void testCalcMeasureWithUnsupportedPatternClassifiedAsEvaluator() {
        Member measure = mock(Member.class);
        when(measure.isMeasure()).thenReturn(true);
        when(measure.isCalculated()).thenReturn(true);

        MemberExpr x = mockMeasureExpr("x");
        FunCall agg = mockFunCall("Aggregate", x);
        when(measure.getExpression()).thenReturn(agg);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(measure);
        assertEquals(MeasureClassifier.CandidateClass.EVALUATOR, c.candidateClass);
        assertNotNull(c.normalizedFormula);
        assertFalse(c.normalizedFormula.isEligibleForPostProcess());
        assertNotNull(c.normalizedFormula.unsupportedReason);
        assertFalse(c.redFlags.isEmpty());
    }

    /**
     * A calculated measure that is NOT a RolapMember (so the nativeSql
     * instanceof check is skipped) and has a SINGLE_REF formula must be
     * POST_PROCESS_CANDIDATE.
     */
    public void testCalcMeasureWithSingleRefFormulaClassifiedAsPostProcess() {
        Member measure = mock(Member.class);
        when(measure.isMeasure()).thenReturn(true);
        when(measure.isCalculated()).thenReturn(true);

        MemberExpr a = mockMeasureExpr("sales_qty");
        when(measure.getExpression()).thenReturn(a);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(measure);
        assertEquals(MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE, c.candidateClass);
        assertNotNull(c.normalizedFormula);
        assertTrue(c.normalizedFormula.isEligibleForPostProcess());
    }

    // -----------------------------------------------------------------------
    // Candidate field invariants
    // -----------------------------------------------------------------------

    /** Candidate.measure must reference the original measure object. */
    public void testCandidateMeasureRefIsOriginal() {
        Member measure = mock(Member.class);
        when(measure.isMeasure()).thenReturn(true);
        when(measure.isCalculated()).thenReturn(false);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(measure);
        assertSame(measure, c.measure);
    }

    /** redFlags list must be unmodifiable. */
    public void testRedFlagsListIsUnmodifiable() {
        Member member = mock(Member.class);
        when(member.isMeasure()).thenReturn(false);

        MeasureClassifier.Candidate c = MeasureClassifier.classify(member);
        try {
            c.redFlags.add("extra");
            fail("Expected UnsupportedOperationException for unmodifiable list");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    // -----------------------------------------------------------------------
    // classifyAll() tests
    // -----------------------------------------------------------------------

    /** All stored measures → eligible list returned (non-null). */
    public void testAllStoredMeasuresEligible() {
        Set<Member> measures = new LinkedHashSet<Member>();
        measures.add(mockStoredMeasure("sales_qty"));
        measures.add(mockStoredMeasure("sales_rub"));

        List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        assertNotNull("All stored measures should be eligible", candidates);
        assertEquals(2, candidates.size());
        for (MeasureClassifier.Candidate c : candidates) {
            assertEquals(
                MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
                c.candidateClass);
        }
    }

    /** A single EVALUATOR measure poisons the entire query (null result). */
    public void testEvaluatorMeasurePoisonsQuery() {
        Set<Member> measures = new LinkedHashSet<Member>();
        measures.add(mockStoredMeasure("sales_qty"));
        measures.add(mockEvaluatorMeasure());

        List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        assertNull("Evaluator measure should poison entire query", candidates);
    }

    /** Empty set → empty eligible list (not null). */
    public void testEmptySetReturnsEmptyList() {
        Set<Member> measures = new LinkedHashSet<Member>();

        List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        assertNotNull(candidates);
        assertTrue(candidates.isEmpty());
    }

    /** Mixed stored + post-process measures → both in eligible list. */
    public void testMixedStoredAndPostProcessEligible() {
        Set<Member> measures = new LinkedHashSet<Member>();
        measures.add(mockStoredMeasure("sales_qty"));

        // calculated ratio measure
        Member ratio = mock(Member.class);
        when(ratio.isMeasure()).thenReturn(true);
        when(ratio.isCalculated()).thenReturn(true);
        MemberExpr a = mockMeasureExpr("sales_rub");
        MemberExpr b = mockMeasureExpr("akb");
        FunCall divide = mockFunCall("/", a, b);
        when(ratio.getExpression()).thenReturn(divide);
        measures.add(ratio);

        List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        assertNotNull(candidates);
        assertEquals(2, candidates.size());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private Member mockStoredMeasure(String name) {
        Member m = mock(Member.class);
        when(m.getName()).thenReturn(name);
        when(m.isMeasure()).thenReturn(true);
        when(m.isCalculated()).thenReturn(false);
        return m;
    }

    private Member mockEvaluatorMeasure() {
        // calculated measure with null formula → EVALUATOR
        Member m = mock(Member.class);
        when(m.isMeasure()).thenReturn(true);
        when(m.isCalculated()).thenReturn(true);
        when(m.getExpression()).thenReturn(null);
        return m;
    }

    private MemberExpr mockMeasureExpr(String name) {
        Member member = mock(Member.class);
        when(member.isMeasure()).thenReturn(true);
        when(member.getName()).thenReturn(name);

        MemberExpr expr = mock(MemberExpr.class);
        when(expr.getMember()).thenReturn(member);
        return expr;
    }

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

// End MeasureClassifierTest.java
