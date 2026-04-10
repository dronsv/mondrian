/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.mdx;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.impl.ConstantCalc;
import mondrian.olap.*;
import mondrian.olap.type.HierarchyType;
import mondrian.olap.type.Type;

/**
 * Usage of a {@link mondrian.olap.Hierarchy} as an MDX expression.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public class HierarchyExpr extends ExpBase implements Exp {
    private final Hierarchy hierarchy;

    /**
     * Creates a hierarchy expression.
     *
     * @param hierarchy Hierarchy
     * @pre hierarchy != null
     */
    public HierarchyExpr(Hierarchy hierarchy) {
        Util.assertPrecondition(hierarchy != null, "hierarchy != null");
        this.hierarchy = hierarchy;
    }

    /**
     * Returns the hierarchy.
     *
     * @post return != null
     */
    public Hierarchy getHierarchy() {
        return hierarchy;
    }

    @Override public String toString() {
        return hierarchy.getUniqueName();
    }

    @Override public Type getType() {
        return HierarchyType.forHierarchy(hierarchy);
    }

    @Override public HierarchyExpr clone() {
        return new HierarchyExpr(hierarchy);
    }

    @Override public int getCategory() {
        return Category.Hierarchy;
    }

    @Override public Exp accept(Validator validator) {
        return this;
    }

    @Override public Calc accept(ExpCompiler compiler) {
        return ConstantCalc.constantHierarchy(hierarchy);
    }

    @Override public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
    }
}

// End HierarchyExpr.java
