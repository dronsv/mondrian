/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2026 Hitachi Vantara..  All rights reserved.
*/

package mondrian.calc.impl;

import mondrian.olap.Evaluator;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.rolap.RolapEvaluator;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Applies query subcube masking for scalar tuple evaluation when the tuple
 * explicitly resets one or more hierarchies to their all/default members.
 */
final class ExplicitTupleSubcubeMaskSupport {
    private ExplicitTupleSubcubeMaskSupport() {
    }

    static void apply(Evaluator evaluator, Member member) {
        if (member == null) {
            return;
        }
        apply(evaluator, new Member[] {member});
    }

    static void apply(Evaluator evaluator, Member[] members) {
        if (!(evaluator instanceof RolapEvaluator)
            || members == null
            || members.length == 0)
        {
            return;
        }
        final Set<Hierarchy> ignoredHierarchies =
            collectIgnoredHierarchies(members);
        if (ignoredHierarchies.isEmpty()) {
            return;
        }
        ((RolapEvaluator) evaluator).setIgnoredSubcubeHierarchies(
            ignoredHierarchies);
    }

    private static Set<Hierarchy> collectIgnoredHierarchies(Member[] members) {
        final LinkedHashSet<Hierarchy> hierarchies =
            new LinkedHashSet<Hierarchy>();
        for (Member member : members) {
            if (member == null || member.getHierarchy() == null) {
                continue;
            }
            final Hierarchy hierarchy = member.getHierarchy();
            final Member defaultMember = hierarchy.getDefaultMember();
            if (member.isAll()
                || (defaultMember != null && defaultMember.equals(member)))
            {
                hierarchies.add(hierarchy);
            }
        }
        return hierarchies.isEmpty()
            ? Collections.<Hierarchy>emptySet()
            : Collections.unmodifiableSet(hierarchies);
    }
}
