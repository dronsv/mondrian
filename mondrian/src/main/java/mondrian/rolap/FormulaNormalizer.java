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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Normalizes MDX formula ASTs for NativeQueryEngine classification.
 *
 * <p>Strips null-guard wrappers (IIF/IIf with IsEmpty or zero-equality
 * conditions), identifies leaf measure references, and classifies the
 * formula into a supported {@link Pattern}.
 *
 * <p>This is a Phase A component: it operates on a single formula AST and
 * does not require Mondrian runtime context. It is used by
 * {@code MeasureClassifier} to decide whether a calculated measure can be
 * evaluated natively.
 */
public class FormulaNormalizer {

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Patterns that NativeQueryEngine knows how to handle.
     */
    public enum Pattern {
        /** Simple division: {@code a / b} */
        RATIO,
        /** Division scaled by a constant: {@code (a / b) * K} or {@code K * (a / b)} */
        SCALED_RATIO,
        /** A single measure multiplied by a constant: {@code a * K} or {@code K * a} */
        SCALED_VALUE,
        /** Addition or subtraction: {@code a + b} or {@code a - b} */
        ADDITIVE,
        /** A bare measure reference with no operator */
        SINGLE_REF,
        /** Any formula not covered by the patterns above */
        UNSUPPORTED
    }

    /**
     * The result of normalizing a formula expression.
     */
    public static class Result {
        /** Classified pattern of the (possibly stripped) inner expression. */
        public final Pattern pattern;
        /**
         * The inner expression after stripping any null-guard wrapper.
         * Equals the original expression when no guard was stripped.
         */
        public final Exp normalizedExp;
        /**
         * Leaf measure (or identifier) references found in {@link #normalizedExp}.
         */
        public final List<Exp> leafRefs;
        /** {@code true} when an IIF null-guard wrapper was removed. */
        public final boolean guardStripped;

        Result(
            Pattern pattern,
            Exp normalizedExp,
            List<Exp> leafRefs,
            boolean guardStripped)
        {
            this.pattern = pattern;
            this.normalizedExp = normalizedExp;
            this.leafRefs =
                Collections.unmodifiableList(new ArrayList<Exp>(leafRefs));
            this.guardStripped = guardStripped;
        }
    }

    /**
     * Normalizes {@code exp}.
     *
     * @param exp  MDX expression to normalize; may be {@code null}
     * @return a {@link Result} — never {@code null}
     */
    public static Result normalize(Exp exp) {
        if (exp == null) {
            return new Result(
                Pattern.UNSUPPORTED, null,
                Collections.<Exp>emptyList(), false);
        }

        boolean guardStripped = false;
        Exp inner = exp;

        if (isNullGuardIif(exp)) {
            inner = extractGuardedExpression(exp);
            guardStripped = true;
        }

        List<Exp> leafRefs = new ArrayList<Exp>();
        collectLeafRefs(inner, leafRefs);

        Pattern pattern = classifyPattern(inner, leafRefs);

        return new Result(pattern, inner, leafRefs, guardStripped);
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
     *   <li>…and the inverted variants where {@code NULL} is the false branch.</li>
     * </ul>
     */
    private static boolean isNullGuardIif(Exp exp) {
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
        Exp condition  = fc.getArg(0);
        Exp trueBranch = fc.getArg(1);
        Exp falseBranch = fc.getArg(2);

        boolean nullInTrue  = isNullLiteral(trueBranch);
        boolean nullInFalse = isNullLiteral(falseBranch);

        if (!nullInTrue && !nullInFalse) {
            return false;
        }
        return isEmptyOrZeroCheck(condition);
    }

    private static boolean isNullLiteral(Exp exp) {
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
    private static boolean isEmptyOrZeroCheck(Exp condition) {
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

    private static boolean isZeroLiteral(Exp exp) {
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
    private static Exp extractGuardedExpression(Exp iifExp) {
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
    private static void collectLeafRefs(Exp exp, List<Exp> refs) {
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

    // -----------------------------------------------------------------------
    // Pattern classification
    // -----------------------------------------------------------------------

    private static Pattern classifyPattern(Exp exp, List<Exp> leafRefs) {
        if (exp == null || leafRefs.isEmpty()) {
            return Pattern.UNSUPPORTED;
        }

        // Bare reference with no wrapping operator
        if (!(exp instanceof FunCall)) {
            return leafRefs.size() == 1
                ? Pattern.SINGLE_REF
                : Pattern.UNSUPPORTED;
        }

        FunCall fc = (FunCall) exp;
        String fn = fc.getFunName();

        // a / b
        if ("/".equals(fn) && fc.getArgCount() == 2) {
            return Pattern.RATIO;
        }

        // a * K  or  K * a  or  (a/b) * K  or  K * (a/b)
        if ("*".equals(fn) && fc.getArgCount() == 2) {
            Exp left  = fc.getArg(0);
            Exp right = fc.getArg(1);
            boolean leftIsLit  = left  instanceof Literal;
            boolean rightIsLit = right instanceof Literal;

            if (leftIsLit || rightIsLit) {
                Exp nonLiteral = leftIsLit ? right : left;
                if (nonLiteral instanceof FunCall
                    && "/".equals(((FunCall) nonLiteral).getFunName()))
                {
                    return Pattern.SCALED_RATIO;
                }
                return Pattern.SCALED_VALUE;
            }
        }

        // a + b  or  a - b
        if (("+".equals(fn) || "-".equals(fn)) && fc.getArgCount() == 2) {
            return Pattern.ADDITIVE;
        }

        return Pattern.UNSUPPORTED;
    }
}

// End FormulaNormalizer.java
