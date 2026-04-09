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
import mondrian.olap.Category;
import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Id;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.Syntax;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Analyzes MDX formula ASTs for NativeQueryEngine eligibility.
 *
 * <p>Strips null-guard wrappers (IIF/IIf with IsEmpty or zero-equality
 * conditions), identifies leaf measure references, and checks for
 * unsafe constructs that prevent post-process evaluation.
 *
 * <p>Unlike the previous pattern-based classification (FormulaNormalizer),
 * this class uses an eligibility model: any scalar current-cell formula
 * whose AST contains only safe operations and at least one leaf measure
 * reference is eligible for POST_PROCESS evaluation via the Calc framework.
 *
 * <p>This is a Phase A component: it operates on a single formula AST and
 * does not require Mondrian runtime context. It is used by
 * {@code MeasureClassifier} to decide whether a calculated measure can be
 * evaluated natively.
 */
public class FormulaAnalyzer {

    /**
     * Functions that produce or consume sets, navigate the hierarchy,
     * or otherwise require context that POST_PROCESS cannot provide.
     *
     * <p>Any formula containing one of these (case-insensitive) is
     * ineligible for post-process evaluation.
     */
    private static final Set<String> UNSAFE_FUNCTIONS;
    static {
        Set<String> s = new HashSet<String>(Arrays.asList(
            // Set-producing functions
            "Members", "AllMembers", "Children", "Descendants",
            "Ascendants", "Ancestors", "Siblings", "DrilldownLevel",
            "DrilldownMember", "Filter", "TopCount", "BottomCount",
            "TopPercent", "BottomPercent", "Head", "Tail",
            "Subset", "Order", "Hierarchize", "Distinct",
            "Except", "Intersect", "Union", "CrossJoin",
            "NonEmptyCrossJoin", "NonEmpty", "Generate", "Exists",
            "AddCalculatedMembers", "StripCalculatedMembers",
            "VisualTotals", "PeriodsToDate", "Ytd", "Qtd", "Mtd",
            "Wtd", "LastPeriods",
            // Set-aggregating functions
            "Sum", "Avg", "Count", "Min", "Max",
            "Aggregate", "Median", "Var", "Stdev",
            "SetToStr", "TupleToStr",
            // Navigation functions
            "PrevMember", "NextMember", "Lag", "Lead",
            "ParallelPeriod", "OpeningPeriod", "ClosingPeriod",
            "Cousin", "Ancestor",
            "StrToMember", "StrToTuple", "StrToSet",
            "LookupCube",
            // Rank / ordinal
            "Rank", "Item"
        ));
        UNSAFE_FUNCTIONS = Collections.unmodifiableSet(s);
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * The result of analyzing a formula expression.
     */
    public static class Result {
        /**
         * The inner expression after stripping any null-guard wrapper.
         * Equals the original expression when no guard was stripped.
         */
        public final Exp normalizedExp;
        /**
         * Leaf measure (or identifier) references found in
         * {@link #normalizedExp}.
         */
        public final List<Exp> leafRefs;
        /** {@code true} when an IIF null-guard wrapper was removed. */
        public final boolean guardStripped;
        /**
         * Human-readable reason why the formula is ineligible, or
         * {@code null} when the formula is eligible for post-process.
         */
        public final String unsupportedReason;

        Result(
            Exp normalizedExp,
            List<Exp> leafRefs,
            boolean guardStripped,
            String unsupportedReason)
        {
            this.normalizedExp = normalizedExp;
            this.leafRefs =
                Collections.unmodifiableList(new ArrayList<Exp>(leafRefs));
            this.guardStripped = guardStripped;
            this.unsupportedReason = unsupportedReason;
        }

        /**
         * Returns {@code true} when the formula is eligible for
         * post-process evaluation.
         */
        public boolean isEligibleForPostProcess() {
            return unsupportedReason == null && !leafRefs.isEmpty();
        }
    }

    /**
     * Analyzes {@code exp} for post-process eligibility.
     *
     * @param exp  MDX expression to analyze; may be {@code null}
     * @return a {@link Result} — never {@code null}
     */
    public static Result analyze(Exp exp) {
        if (exp == null) {
            return new Result(
                null,
                Collections.<Exp>emptyList(),
                false,
                "null expression");
        }

        boolean guardStripped = false;
        Exp inner = exp;

        while (isNullGuardIif(inner)) {
            inner = extractGuardedExpression(inner);
            guardStripped = true;
        }

        // Check for unsafe constructs
        String unsafeReason = findUnsafeConstruct(inner);
        if (unsafeReason != null) {
            // Still collect leaf refs for diagnostic purposes
            List<Exp> leafRefs = new ArrayList<Exp>();
            collectLeafRefs(inner, leafRefs);
            return new Result(inner, leafRefs, guardStripped, unsafeReason);
        }

        List<Exp> leafRefs = new ArrayList<Exp>();
        collectLeafRefs(inner, leafRefs);

        if (leafRefs.isEmpty()) {
            return new Result(
                inner, leafRefs, guardStripped,
                "no leaf measure references");
        }

        return new Result(inner, leafRefs, guardStripped, null);
    }

    // -----------------------------------------------------------------------
    // Unsafe construct detection
    // -----------------------------------------------------------------------

    /**
     * Recursively walks the AST looking for unsafe functions or
     * coordinate-changing tuples.
     *
     * @param exp the expression to inspect
     * @return a human-readable reason string if an unsafe construct is
     *         found, or {@code null} if the expression is safe
     */
    static String findUnsafeConstruct(Exp exp) {
        if (exp == null) {
            return null;
        }
        if (!(exp instanceof FunCall)) {
            return null;
        }
        FunCall fc = (FunCall) exp;
        String fn = fc.getFunName();

        // Check function name against the unsafe set (case-insensitive)
        for (String unsafe : UNSAFE_FUNCTIONS) {
            if (unsafe.equalsIgnoreCase(fn)) {
                return "unsafe function: " + fn;
            }
        }

        // Detect coordinate-changing tuples: (Member, Member, ...)
        // where at least one member is from a non-Measures dimension
        if (isTupleWithNonMeasureDimension(fc)) {
            return "coordinate-changing tuple";
        }

        // Recurse into arguments
        for (Exp arg : fc.getArgs()) {
            String reason = findUnsafeConstruct(arg);
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    /**
     * Returns {@code true} when {@code fc} is a tuple constructor
     * (parentheses syntax with 2+ arguments) containing at least one
     * member expression from a non-Measures dimension.
     *
     * <p>Such tuples change the evaluation coordinate and cannot be
     * handled by post-process evaluation.
     */
    static boolean isTupleWithNonMeasureDimension(FunCall fc) {
        Syntax syntax = fc.getSyntax();
        if (syntax == null || syntax != Syntax.Parentheses) {
            return false;
        }
        if (fc.getArgCount() < 2) {
            return false;
        }
        for (Exp arg : fc.getArgs()) {
            if (arg instanceof MemberExpr) {
                Member m = ((MemberExpr) arg).getMember();
                if (!m.isMeasure()) {
                    return true;
                }
            }
        }
        return false;
    }

    // -----------------------------------------------------------------------
    // Null-guard detection
    // -----------------------------------------------------------------------

    /**
     * Returns {@code true} if {@code exp} is an IIF call whose condition is an
     * IsEmpty check or a zero-equality check, and one of the value branches is
     * the NULL literal.
     *
     * <p>Accepted forms:
     * <ul>
     *   <li>{@code IIF(IsEmpty(x), NULL, expr)}</li>
     *   <li>{@code IIF(x = 0, NULL, expr)}</li>
     *   <li>{@code IIF(IsEmpty(x) OR x = 0, NULL, expr)}</li>
     *   <li>...and the inverted variants where {@code NULL} is the false branch.</li>
     * </ul>
     */
    static boolean isNullGuardIif(Exp exp) {
        if (!(exp instanceof FunCall)) {
            return false;
        }
        FunCall fc = (FunCall) exp;
        if (!"IIf".equalsIgnoreCase(fc.getFunName())) {
            return false;
        }
        if (fc.getArgCount() != 3) {
            return false;
        }
        Exp condition   = fc.getArg(0);
        Exp trueBranch  = fc.getArg(1);
        Exp falseBranch = fc.getArg(2);

        boolean nullInTrue  = isNullLiteral(trueBranch);
        boolean nullInFalse = isNullLiteral(falseBranch);

        if (!nullInTrue && !nullInFalse) {
            return false;
        }
        return isEmptyOrZeroCheck(condition);
    }

    static boolean isNullLiteral(Exp exp) {
        if (exp instanceof Literal) {
            Literal lit = (Literal) exp;
            return lit == Literal.nullValue
                || lit.getCategory() == Category.Null;
        }
        return false;
    }

    /**
     * Returns {@code true} when {@code condition} is recognised as a guard
     * that checks for empty or zero — i.e. it is safe to strip the IIF.
     */
    static boolean isEmptyOrZeroCheck(Exp condition) {
        if (!(condition instanceof FunCall)) {
            return false;
        }
        FunCall fc = (FunCall) condition;
        String fn = fc.getFunName();

        if ("IsEmpty".equalsIgnoreCase(fn)) {
            return true;
        }
        if ("=".equals(fn) && fc.getArgCount() == 2) {
            return isZeroLiteral(fc.getArg(0))
                || isZeroLiteral(fc.getArg(1));
        }
        if ("OR".equalsIgnoreCase(fn) && fc.getArgCount() == 2) {
            return isEmptyOrZeroCheck(fc.getArg(0))
                || isEmptyOrZeroCheck(fc.getArg(1));
        }
        return false;
    }

    static boolean isZeroLiteral(Exp exp) {
        if (exp instanceof Literal) {
            Object val = ((Literal) exp).getValue();
            if (val instanceof Number) {
                return ((Number) val).doubleValue() == 0.0;
            }
        }
        return false;
    }

    /**
     * Returns the non-NULL value branch of a recognised null-guard IIF call.
     */
    static Exp extractGuardedExpression(Exp iifExp) {
        FunCall fc = (FunCall) iifExp;
        Exp trueBranch  = fc.getArg(1);
        Exp falseBranch = fc.getArg(2);
        // Return whichever branch is NOT the NULL literal
        return isNullLiteral(trueBranch) ? falseBranch : trueBranch;
    }

    // -----------------------------------------------------------------------
    // Leaf reference collection
    // -----------------------------------------------------------------------

    /**
     * Recursively collects measure references ({@link MemberExpr} where
     * {@link Member#isMeasure()} is {@code true}) and bare {@link Id}
     * references into {@code refs}.
     */
    static void collectLeafRefs(Exp exp, List<Exp> refs) {
        if (exp == null) {
            return;
        }
        if (exp instanceof MemberExpr) {
            Member m = ((MemberExpr) exp).getMember();
            if (m.isMeasure()) {
                refs.add(exp);
            }
            return;
        }
        if (exp instanceof Id) {
            refs.add(exp);
            return;
        }
        if (exp instanceof FunCall) {
            for (Exp arg : ((FunCall) exp).getArgs()) {
                collectLeafRefs(arg, refs);
            }
        }
    }
}

// End FormulaAnalyzer.java
