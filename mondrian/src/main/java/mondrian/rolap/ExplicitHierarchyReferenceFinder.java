/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2026 Hitachi Vantara..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.mdx.DimensionExpr;
import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.LevelExpr;
import mondrian.mdx.MdxVisitorImpl;
import mondrian.mdx.MemberExpr;
import mondrian.olap.Dimension;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Collects hierarchies explicitly referenced by an MDX expression.
 *
 * <p>The result is intentionally conservative. If a dimension is explicitly
 * referenced, all of its hierarchies are treated as explicit and therefore
 * excluded from auto-reset.</p>
 */
class ExplicitHierarchyReferenceFinder extends MdxVisitorImpl {
    private final Set<Hierarchy> hierarchies = new LinkedHashSet<Hierarchy>();

    static Set<Hierarchy> find(Exp expression) {
        if (expression == null) {
            return Collections.emptySet();
        }
        final ExplicitHierarchyReferenceFinder finder =
            new ExplicitHierarchyReferenceFinder();
        expression.accept(finder);
        return finder.hierarchies;
    }

    @Override
    public Object visit(DimensionExpr dimensionExpr) {
        final Dimension dimension = dimensionExpr.getDimension();
        if (dimension != null) {
            Collections.addAll(hierarchies, dimension.getHierarchies());
        }
        return null;
    }

    @Override
    public Object visit(HierarchyExpr hierarchyExpr) {
        final Hierarchy hierarchy = hierarchyExpr.getHierarchy();
        if (hierarchy != null) {
            hierarchies.add(hierarchy);
        }
        return null;
    }

    @Override
    public Object visit(LevelExpr levelExpr) {
        final Level level = levelExpr.getLevel();
        if (level != null && level.getHierarchy() != null) {
            hierarchies.add(level.getHierarchy());
        }
        return null;
    }

    @Override
    public Object visit(MemberExpr memberExpr) {
        final Member member = memberExpr.getMember();
        if (member != null && member.getHierarchy() != null) {
            hierarchies.add(member.getHierarchy());
        }
        return null;
    }
}
