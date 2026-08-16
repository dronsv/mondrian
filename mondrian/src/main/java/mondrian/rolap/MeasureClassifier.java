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
import mondrian.olap.Member;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Phase A: classifies each requested measure into a candidate
 * execution class for NativeQueryEngine.
 *
 * <p>Contract: may inspect the immediate formula AST of the candidate
 * measure to extract leaf references and detect red flags.
 * MUST NOT recursively resolve those leaf references to their own
 * dependencies — that is DependencyResolver's job.
 */
public class MeasureClassifier {

    private MeasureClassifier() {}

    // -----------------------------------------------------------------------
    // Public API types
    // -----------------------------------------------------------------------

    /**
     * Candidate execution class for a single measure.
     */
    public enum CandidateClass {
        /**
         * Stored (non-calculated) measure — can be pushed directly to SQL
         * as a column reference.
         */
        DIRECT_PUSH_STORED,
        /**
         * Calculated measure with {@code nativeSql.*} annotations — the
         * native SQL template replaces MDX evaluation.
         */
        DIRECT_PUSH_NATIVE,
        /**
         * Calculated measure with a supported formula pattern (ratio,
         * additive, scaled, etc.) — needs post-processing after SQL but
         * does not require the MDX evaluator.
         */
        POST_PROCESS_CANDIDATE,
        /**
         * Anything else — the entire query must fall back to the standard
         * MDX evaluator.
         */
        EVALUATOR
    }

    /**
     * Classification result for a single measure.
     */
    public static class Candidate {
        /** The measure member that was classified. */
        public final Member measure;
        /** Execution class assigned to this measure. */
        public final CandidateClass candidateClass;
        /**
         * Normalized formula, or {@code null} for stored/native measures
         * and for EVALUATOR candidates where normalization was not reached.
         */
        public final FormulaAnalyzer.Result normalizedFormula;
        /**
         * Reasons why a higher-priority class was rejected.
         * Always non-null; empty when classification succeeded without
         * degradation.
         */
        public final List<String> redFlags;

        Candidate(
            Member measure,
            CandidateClass candidateClass,
            FormulaAnalyzer.Result normalizedFormula,
            List<String> redFlags)
        {
            this.measure = measure;
            this.candidateClass = candidateClass;
            this.normalizedFormula = normalizedFormula;
            this.redFlags = redFlags == null
                ? Collections.<String>emptyList()
                : Collections.unmodifiableList(
                    new ArrayList<String>(redFlags));
        }
    }

    // -----------------------------------------------------------------------
    // Public methods
    // -----------------------------------------------------------------------

    /**
     * Classifies a single measure.
     *
     * @param measure  the measure to classify; must not be {@code null}
     * @return a {@link Candidate} — never {@code null}
     */
    public static Candidate classify(Member measure) {
        // Only measure members can be handled natively.
        if (!measure.isMeasure()) {
            return new Candidate(
                measure,
                CandidateClass.EVALUATOR,
                null,
                Collections.singletonList("not a measure"));
        }

        // Stored (non-calculated) measures map directly to SQL columns.
        if (!measure.isCalculated()) {
            return new Candidate(
                measure,
                CandidateClass.DIRECT_PUSH_STORED,
                null,
                null);
        }

        // Check for nativeSql annotation on RolapMember instances.
        if (measure instanceof RolapMember) {
            RolapCalculatedMember nativeMember =
                NativeSqlConfig.findNativeSqlMember((RolapMember) measure);
            if (nativeMember != null) {
                return new Candidate(
                    measure,
                    CandidateClass.DIRECT_PUSH_NATIVE,
                    null,
                    null);
            }
        }

        // Calculated measure — inspect and normalise the formula.
        Exp formula = measure.getExpression();
        if (formula == null) {
            return new Candidate(
                measure,
                CandidateClass.EVALUATOR,
                null,
                Collections.singletonList("null formula expression"));
        }

        FormulaAnalyzer.Result analyzed = FormulaAnalyzer.analyze(formula);

        // Coordinate-pin tuple recognition: when FormulaAnalyzer detected
        // the (measure, hierA.[All], hierB.[All], ...) shape and the
        // inner measure is a stored measure, classify as DIRECT_PUSH_STORED
        // so DependencyResolver inlines it as a pinned PhysicalValueRequest.
        if (analyzed.coordinatePinTuple != null) {
            Member inner = analyzed.coordinatePinTuple.innerMeasure;
            if (inner instanceof RolapStoredMeasure) {
                return new Candidate(
                    measure,
                    CandidateClass.DIRECT_PUSH_STORED,
                    analyzed,
                    null);
            }
            // Inner is itself calc-on-calc — leave to existing path.
        }

        if (!analyzed.isEligibleForPostProcess()) {
            // Before giving up, try to inline through the formula.
            // Handles cases like: IIF(IsEmpty(x), NULL, ValidMeasure([StoredMeasure]))
            // After null-guard stripping + ValidMeasure unwrap, we may reach
            // a stored measure that can be pushed directly.
            CandidateClass inlinedClass = tryInlineToDirectPush(analyzed);
            if (inlinedClass != null) {
                return new Candidate(
                    measure, inlinedClass, analyzed, null);
            }
            return new Candidate(
                measure,
                CandidateClass.EVALUATOR,
                analyzed,
                Collections.singletonList(
                    analyzed.unsupportedReason != null
                        ? analyzed.unsupportedReason
                        : "not eligible for POST_PROCESS"));
        }

        String unsafeLeaf = findUnsupportedCalculatedLeaf(analyzed);
        if (unsafeLeaf != null) {
            return new Candidate(
                measure,
                CandidateClass.EVALUATOR,
                analyzed,
                Collections.singletonList(unsafeLeaf));
        }

        // Formula is eligible — Phase B (DependencyResolver) will
        // confirm or further degrade the classification.
        return new Candidate(
            measure,
            CandidateClass.POST_PROCESS_CANDIDATE,
            analyzed,
            null);
    }

    /**
     * Partitioned result of {@link #classifyAll}: {@code ownable}
     * candidates feed dependency resolution and SQL planning;
     * {@code evaluatorOnly} candidates (e.g. Excel's
     * {@code __XLRelated}/{@code __XLPath} helper calc members) carry
     * no NQE-ownable work and participate ONLY in execution-mode
     * classification, where their presence caps the mode at
     * PREFETCH_ONLY. Encoding the split in the type keeps future
     * consumers from accidentally planning SQL for evaluator measures
     * (emondrian-clickhouse#84 follow-up).
     */
    public static final class ClassificationResult {
        /** Non-EVALUATOR candidates, in request iteration order. */
        public final List<Candidate> ownable;
        /** EVALUATOR candidates, in request iteration order. */
        public final List<Candidate> evaluatorOnly;

        ClassificationResult(
            List<Candidate> ownable,
            List<Candidate> evaluatorOnly)
        {
            this.ownable = Collections.unmodifiableList(ownable);
            this.evaluatorOnly = Collections.unmodifiableList(evaluatorOnly);
        }

        /**
         * All candidates — the input for
         * {@code NativeQueryEngine.classifyExecutionMode}, which must
         * see EVALUATOR entries to cap the mode (FULL_RESULT with a
         * partial measure set would leave their cells empty).
         */
        public List<Candidate> all() {
            final List<Candidate> all = new ArrayList<Candidate>(
                ownable.size() + evaluatorOnly.size());
            all.addAll(ownable);
            all.addAll(evaluatorOnly);
            return all;
        }
    }

    /**
     * Classifies all measures in {@code requestedMeasures} and
     * partitions them into NQE-ownable and evaluator-only candidates.
     * EVALUATOR measures do not poison the query — the historic
     * whole-query poison predated the PREFETCH_ONLY mode and starved
     * every Excel discovery query of prefetch. Callers decide
     * eligibility from {@code ownable.isEmpty()}.
     *
     * @param requestedMeasures  the measures requested by the query
     * @return the partitioned classification; never {@code null}
     */
    public static ClassificationResult classifyAll(
        Set<Member> requestedMeasures)
    {
        List<Candidate> ownable =
            new ArrayList<Candidate>(requestedMeasures.size());
        List<Candidate> evaluatorOnly = new ArrayList<Candidate>();
        for (Member m : requestedMeasures) {
            Candidate c = classify(m);
            if (c.candidateClass == CandidateClass.EVALUATOR) {
                evaluatorOnly.add(c);
            } else {
                ownable.add(c);
            }
        }
        return new ClassificationResult(ownable, evaluatorOnly);
    }
    // -----------------------------------------------------------------------
    // Inline helpers
    // -----------------------------------------------------------------------

    /**
     * Tries to inline a calculated measure with UNSUPPORTED pattern
     * down to a DirectPush classification.
     *
     * <p>Handles patterns like {@code IIF(IsEmpty(x), NULL,
     * ValidMeasure([StoredMeasure]))} where the normalized expression
     * (after guard stripping) is {@code ValidMeasure([StoredMeasure])}.
     * ValidMeasure is unwrapped, and if the inner expression is a
     * stored measure reference, returns DIRECT_PUSH_STORED.
     *
     * @return the inlined CandidateClass, or {@code null} if inlining fails
     */
    private static CandidateClass tryInlineToDirectPush(
        FormulaAnalyzer.Result normalized)
    {
        Exp inner = normalized.normalizedExp;
        if (inner == null) {
            return null;
        }

        // Unwrap ValidMeasure(expr) → expr
        inner = unwrapValidMeasure(inner);

        // Check if we now have a member reference
        if (inner instanceof MemberExpr) {
            Member m = ((MemberExpr) inner).getMember();
            // Unwrap delegating wrappers (VirtualCube)
            while (m instanceof DelegatingRolapMember) {
                m = ((DelegatingRolapMember) m).member;
            }
            if (m.isMeasure() && !m.isCalculated()) {
                return CandidateClass.DIRECT_PUSH_STORED;
            }
            if (m instanceof RolapStoredMeasure) {
                return CandidateClass.DIRECT_PUSH_STORED;
            }
        }

        return null;
    }

    /**
     * POST_PROCESS evaluates the compiled Calc against prefetched leaf
     * values. A calculated leaf without a native SQL contract may carry
     * evaluator semantics that cannot be represented as a scalar lookup.
     */
    private static String findUnsupportedCalculatedLeaf(
        FormulaAnalyzer.Result analyzed)
    {
        for (Exp leafRef : analyzed.leafRefs) {
            if (!(leafRef instanceof MemberExpr)) {
                continue;
            }
            Member member = ((MemberExpr) leafRef).getMember();
            while (member instanceof DelegatingRolapMember) {
                member = ((DelegatingRolapMember) member).member;
            }
            if (member == null
                || !member.isMeasure()
                || !member.isCalculated())
            {
                continue;
            }
            if (member instanceof RolapMember
                && NativeSqlConfig.findNativeSqlMember((RolapMember) member)
                    != null)
            {
                continue;
            }
            return "calculated leaf measure not safe for POST_PROCESS: "
                + member.getUniqueName();
        }
        return null;
    }

    /**
     * Unwraps {@code ValidMeasure(expr)} to {@code expr}.
     */
    private static Exp unwrapValidMeasure(Exp exp) {
        if (exp instanceof FunCall) {
            FunCall fc = (FunCall) exp;
            if ("ValidMeasure".equalsIgnoreCase(fc.getFunName())
                && fc.getArgCount() == 1)
            {
                return fc.getArg(0);
            }
        }
        return exp;
    }
}

// End MeasureClassifier.java
