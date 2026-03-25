/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2026 Hitachi Vantara..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.mdx.UnresolvedFunCall;
import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.type.Type;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Rewrites tuple-shaped companion denominator expressions by injecting missing
 * same-dimension peer members before compilation.
 */
class ShareMeasurePeerHierarchyTupleNormalizer {
    static Exp normalize(
        Exp expression,
        ShareMeasurePeerHierarchyResetPlanner.InjectionPlan injectionPlan,
        RolapConnection connection)
    {
        if (expression == null
            || injectionPlan == null
            || injectionPlan.isEmpty()
            || connection == null)
        {
            return expression;
        }

        final String rewrittenMdx =
            toNormalizedTupleMdx(expression, injectionPlan);
        if (rewrittenMdx == null) {
            return expression;
        }
        try {
            return connection.parseExpression(rewrittenMdx);
        } catch (Exception e) {
            return expression;
        }
    }

    static String toNormalizedTupleMdx(
        Exp expression,
        ShareMeasurePeerHierarchyResetPlanner.InjectionPlan injectionPlan)
    {
        if (expression == null || injectionPlan == null || injectionPlan.isEmpty()) {
            return null;
        }
        final Exp[] tupleArgs = tupleArgs(expression);
        if (tupleArgs == null) {
            return null;
        }
        final List<Member> membersToInject = filterInjectedMembers(
            tupleArgs,
            injectionPlan.getInjectedMembers());
        if (membersToInject.isEmpty()) {
            return null;
        }
        final int measureIndex = findMeasureIndex(tupleArgs);
        final List<String> rewrittenArgs =
            new ArrayList<String>(tupleArgs.length + membersToInject.size());
        final int insertAt = measureIndex < 0 ? tupleArgs.length : measureIndex;

        for (int i = 0; i < insertAt; i++) {
            rewrittenArgs.add(toMdx(tupleArgs[i]));
        }
        for (Member memberToInject : membersToInject) {
            rewrittenArgs.add(toInjectedMdx(memberToInject));
        }
        for (int i = insertAt; i < tupleArgs.length; i++) {
            rewrittenArgs.add(toMdx(tupleArgs[i]));
        }
        return "(" + join(rewrittenArgs) + ")";
    }

    static String toNormalizedExpressionMdx(
        Exp expression,
        ShareMeasurePeerHierarchyResetPlanner.InjectionPlan injectionPlan)
    {
        if (expression == null || injectionPlan == null || injectionPlan.isEmpty()) {
            return null;
        }
        return rewriteExpression(expression, injectionPlan);
    }

    private static List<Member> filterInjectedMembers(
        Exp[] tupleArgs,
        Member[] injectedMembers)
    {
        final Set<Hierarchy> tupleHierarchies = tupleHierarchies(tupleArgs);
        final List<Member> filtered = new ArrayList<Member>(injectedMembers.length);
        for (Member injectedMember : injectedMembers) {
            if (injectedMember == null || injectedMember.getHierarchy() == null) {
                continue;
            }
            if (tupleReferencesHierarchy(tupleArgs, tupleHierarchies, injectedMember.getHierarchy())) {
                continue;
            }
            filtered.add(injectedMember);
        }
        return filtered;
    }

    private static boolean tupleReferencesHierarchy(
        Exp[] tupleArgs,
        Set<Hierarchy> tupleHierarchies,
        Hierarchy hierarchy)
    {
        if (hierarchy == null) {
            return false;
        }
        if (tupleHierarchies.contains(hierarchy)) {
            return true;
        }
        final String hierarchyUniqueName = hierarchy.getUniqueName();
        final String hierarchyNeedle =
            hierarchyUniqueName == null
                ? null
                : hierarchyUniqueName.toLowerCase(Locale.ROOT);
        for (Exp tupleArg : tupleArgs) {
            if (tupleArg == null) {
                continue;
            }
            if (ExplicitHierarchyReferenceFinder.find(tupleArg).contains(hierarchy)) {
                return true;
            }
            if (hierarchyNeedle != null) {
                final String tupleArgMdx = toMdx(tupleArg).toLowerCase(Locale.ROOT);
                if (tupleArgMdx.contains(hierarchyNeedle)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String toInjectedMdx(Member injectedMember) {
        final Hierarchy hierarchy =
            injectedMember == null ? null : injectedMember.getHierarchy();
        if (hierarchy != null) {
            final Member defaultMember = hierarchy.getDefaultMember();
            if (defaultMember != null && defaultMember.equals(injectedMember)) {
                return hierarchy.getUniqueName() + ".DefaultMember";
            }
        }
        return injectedMember == null ? null : injectedMember.getUniqueName();
    }

    private static String toMdx(Exp expression) {
        final StringWriter stringWriter = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(stringWriter);
        expression.unparse(printWriter);
        printWriter.flush();
        return stringWriter.toString();
    }

    private static String join(List<String> parts) {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(parts.get(i));
        }
        return builder.toString();
    }

    private static String rewriteExpression(
        Exp expression,
        ShareMeasurePeerHierarchyResetPlanner.InjectionPlan injectionPlan)
    {
        final String tupleRewrite =
            toNormalizedTupleMdx(expression, injectionPlan);
        if (tupleRewrite != null) {
            return tupleRewrite;
        }
        if (expression instanceof ResolvedFunCall) {
            return rewriteIif((ResolvedFunCall) expression, injectionPlan);
        }
        if (expression instanceof UnresolvedFunCall) {
            return rewriteIif((UnresolvedFunCall) expression, injectionPlan);
        }
        return null;
    }

    private static String rewriteIif(
        ResolvedFunCall call,
        ShareMeasurePeerHierarchyResetPlanner.InjectionPlan injectionPlan)
    {
        if (!isIif(call.getFunName()) || call.getArgCount() != 3) {
            return null;
        }
        return rewriteIifArgs(call.getArgs(), injectionPlan);
    }

    private static String rewriteIif(
        UnresolvedFunCall call,
        ShareMeasurePeerHierarchyResetPlanner.InjectionPlan injectionPlan)
    {
        if (!isIif(call.getFunName()) || call.getArgCount() != 3) {
            return null;
        }
        return rewriteIifArgs(call.getArgs(), injectionPlan);
    }

    private static String rewriteIifArgs(
        Exp[] args,
        ShareMeasurePeerHierarchyResetPlanner.InjectionPlan injectionPlan)
    {
        if (conditionReferencesInjectedHierarchy(args[0], injectionPlan)) {
            return null;
        }
        final String thenRewrite = rewriteExpression(args[1], injectionPlan);
        final String elseRewrite = rewriteExpression(args[2], injectionPlan);
        if (thenRewrite == null && elseRewrite == null) {
            return null;
        }
        return "IIf("
            + toMdx(args[0])
            + ", "
            + (thenRewrite == null ? toMdx(args[1]) : thenRewrite)
            + ", "
            + (elseRewrite == null ? toMdx(args[2]) : elseRewrite)
            + ")";
    }

    private static boolean conditionReferencesInjectedHierarchy(
        Exp condition,
        ShareMeasurePeerHierarchyResetPlanner.InjectionPlan injectionPlan)
    {
        if (condition == null || injectionPlan == null || injectionPlan.isEmpty()) {
            return false;
        }
        final Set<Hierarchy> explicitHierarchies =
            ExplicitHierarchyReferenceFinder.find(condition);
        final String conditionMdx = toMdx(condition).toLowerCase(Locale.ROOT);
        for (Member injectedMember : injectionPlan.getInjectedMembers()) {
            if (injectedMember == null || injectedMember.getHierarchy() == null) {
                continue;
            }
            final Hierarchy hierarchy = injectedMember.getHierarchy();
            if (explicitHierarchies.contains(hierarchy)) {
                return true;
            }
            final String uniqueName = hierarchy.getUniqueName();
            if (uniqueName != null
                && conditionMdx.contains(uniqueName.toLowerCase(Locale.ROOT)))
            {
                return true;
            }
        }
        return false;
    }

    private static boolean isIif(String funName) {
        return funName != null && "iif".equalsIgnoreCase(funName);
    }

    private static Set<Hierarchy> tupleHierarchies(Exp[] tupleArgs) {
        final Set<Hierarchy> hierarchies = new LinkedHashSet<Hierarchy>();
        for (Exp tupleArg : tupleArgs) {
            final Hierarchy hierarchy = hierarchyOf(tupleArg);
            if (hierarchy != null) {
                hierarchies.add(hierarchy);
            }
        }
        return hierarchies;
    }

    private static int findMeasureIndex(Exp[] tupleArgs) {
        for (int i = 0; i < tupleArgs.length; i++) {
            final Dimension dimension = dimensionOf(tupleArgs[i]);
            if (dimension != null && dimension.isMeasures()) {
                return i;
            }
        }
        return -1;
    }

    private static Dimension dimensionOf(Exp expression) {
        if (expression instanceof MemberExpr) {
            final Member member = ((MemberExpr) expression).getMember();
            return member == null ? null : member.getDimension();
        }
        try {
            final Type type = expression.getType();
            return type == null ? null : type.getDimension();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    private static Hierarchy hierarchyOf(Exp expression) {
        if (expression instanceof MemberExpr) {
            final Member member = ((MemberExpr) expression).getMember();
            return member == null ? null : member.getHierarchy();
        }
        try {
            final Type type = expression.getType();
            return type == null ? null : type.getHierarchy();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    private static Exp[] tupleArgs(Exp expression) {
        if (expression instanceof ResolvedFunCall) {
            final ResolvedFunCall call = (ResolvedFunCall) expression;
            if (call.getSyntax() == Syntax.Parentheses && call.getArgCount() > 1) {
                return call.getArgs();
            }
        } else if (expression instanceof UnresolvedFunCall) {
            final UnresolvedFunCall call = (UnresolvedFunCall) expression;
            if (call.getSyntax() == Syntax.Parentheses && call.getArgCount() > 1) {
                return call.getArgs();
            }
        }
        return null;
    }
}
