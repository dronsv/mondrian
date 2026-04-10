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
import mondrian.olap.type.MemberType;
import mondrian.olap.type.Type;

/**
 * Usage of a {@link mondrian.olap.Member} as an MDX expression.
 *
 * @author jhyde
 * @since Sep 26, 2005
 */
public class MemberExpr extends ExpBase implements Exp {
    private final Member member;
    private MemberType type;

    /**
     * Creates a member expression.
     *
     * @param member Member
     * @pre member != null
     */
    public MemberExpr(Member member) {
        Util.assertPrecondition(member != null, "member != null");
        this.member = member;
    }

    /**
     * Returns the member.
     *
     * @post return != null
     */
    public Member getMember() {
        return member;
    }

    @Override public String toString() {
        return member.getUniqueName();
    }

    @Override public Type getType() {
        if (type == null) {
            type = MemberType.forMember(member);
        }
        return type;
    }

    @Override public MemberExpr clone() {
        return new MemberExpr(member);
    }

    @Override public int getCategory() {
        return Category.Member;
    }

    @Override public Exp accept(Validator validator) {
        return this;
    }

    @Override public Calc accept(ExpCompiler compiler) {
        return ConstantCalc.constantMember(member);
    }

    @Override public Object accept(MdxVisitor visitor) {
        return visitor.visit(this);
    }
}

// End MemberExpr.java
