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
        public final FormulaNormalizer.Result normalizedFormula;
        /**
         * Reasons why a higher-priority class was rejected.
         * Always non-null; empty when classification succeeded without
         * degradation.
         */
        public final List<String> redFlags;

        Candidate(
            Member measure,
            CandidateClass candidateClass,
            FormulaNormalizer.Result normalizedFormula,
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

        FormulaNormalizer.Result normalized = FormulaNormalizer.normalize(formula);

        if (normalized.pattern == FormulaNormalizer.Pattern.UNSUPPORTED) {
            // Before giving up, try to inline through the formula.
            // Handles cases like: IIF(IsEmpty(x), NULL, ValidMeasure([StoredMeasure]))
            // After null-guard stripping + ValidMeasure unwrap, we may reach
            // a stored measure that can be pushed directly.
            CandidateClass inlinedClass = tryInlineToDirectPush(normalized);
            if (inlinedClass != null) {
                return new Candidate(
                    measure, inlinedClass, normalized, null);
            }
            return new Candidate(
                measure,
                CandidateClass.EVALUATOR,
                normalized,
                Collections.singletonList("unsupported formula pattern"));
        }

        // Formula pattern is supported — Phase B (DependencyResolver) will
        // confirm or further degrade the classification.
        return new Candidate(
            measure,
            CandidateClass.POST_PROCESS_CANDIDATE,
            normalized,
            null);
    }

    /**
     * Classifies all measures in {@code requestedMeasures}.
     *
     * <p>If any measure is classified as {@link CandidateClass#EVALUATOR},
     * the entire query is considered ineligible for native execution and
     * {@code null} is returned ("poison" semantics). Otherwise returns the
     * full list of candidates in iteration order.
     *
     * @param requestedMeasures  the measures requested by the query
     * @return list of candidates, or {@code null} if any measure is EVALUATOR
     */
    public static List<Candidate> classifyAll(Set<Member> requestedMeasures) {
        List<Candidate> candidates =
            new ArrayList<Candidate>(requestedMeasures.size());
        for (Member m : requestedMeasures) {
            Candidate c = classify(m);
            if (c.candidateClass == CandidateClass.EVALUATOR) {
                return null; // poison: whole query falls back
            }
            candidates.add(c);
        }
        return candidates;
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
        FormulaNormalizer.Result normalized)
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
