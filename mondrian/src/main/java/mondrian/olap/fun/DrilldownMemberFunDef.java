/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// Copyright (C) 2021-2024 Sergei Semenkov
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.*;

/**
 * Definition of the <code>DrilldownMember</code> MDX function.
 *
 * @author Grzegorz Lojek
 * @since 6 December, 2004
 */
class DrilldownMemberFunDef extends FunDefBase {
    static final String[] reservedWords = new String[] {"RECURSIVE"};
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "DrilldownMember",
            "DrilldownMember(<Set1>, <Set2>[, RECURSIVE | <Hierarchy>])",
            "Drills down the members in a set that are present in a second specified set.",
            new String[]{"fxxx", "fxxxy", "fxxxeey", "fxxxh"},
            DrilldownMemberFunDef.class,
            reservedWords);

    public DrilldownMemberFunDef(FunDef funDef) {
        super(funDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));

        // Third argument can be RECURSIVE (symbol) or a Hierarchy
        // (Excel SSAS extension for specifying which hierarchy to drill
        // in a crossjoin).
        String literalArg = "";
        final HierarchyCalc hierarchyCalc;
        if (call.getArgCount() == 3) {
            Exp arg2 = call.getArg(2);
            if (arg2.getType() instanceof mondrian.olap.type.HierarchyType) {
                hierarchyCalc = compiler.compileHierarchy(arg2);
            } else {
                hierarchyCalc = null;
                literalArg = getLiteralArg(call, 2, "", reservedWords);
            }
        } else {
            hierarchyCalc = null;
        }
        final boolean recursive = literalArg.equals("RECURSIVE");

        return new AbstractListCalc(
            call,
            new Calc[] {listCalc1, listCalc2})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                final TupleList list1 = listCalc1.evaluateList(evaluator);
                final TupleList list2 = listCalc2.evaluateList(evaluator);
                Hierarchy drillHierarchy = hierarchyCalc != null
                    ? hierarchyCalc.evaluateHierarchy(evaluator) : null;
                return drilldownMember(
                    list1, list2, evaluator, drillHierarchy);
            }

            /**
             * Drills down an element. Standard behavior: if a tuple
             * member is in memberSet, expand that member's children.
             */
            protected void drillDownObj(
                Evaluator evaluator,
                Member[] tuple,
                Set<Member> memberSet,
                TupleList resultList)
            {
                for (int k = 0; k < tuple.length; k++) {
                    Member member = tuple[k];
                    if (memberSet.contains(member)) {
                        List<Member> children =
                            evaluator.getSchemaReader().getMemberChildren(member);
                        final Member[] tuple2 = tuple.clone();
                        for (Member childMember : children) {
                            tuple2[k] = childMember;
                            resultList.addTuple(tuple2);
                            if (recursive) {
                                drillDownObj(
                                    evaluator, tuple2, memberSet, resultList);
                            }
                        }
                        break;
                    }
                }
            }

            /**
             * Cross-hierarchy drill: if a tuple member (from one
             * hierarchy) is in memberSet, expand the member from
             * drillHierarchy in the same tuple.
             */
            protected void drillDownCrossHierarchy(
                Evaluator evaluator,
                Member[] tuple,
                Set<Member> memberSet,
                Hierarchy drillHierarchy,
                TupleList resultList)
            {
                // Check if any member in the tuple is in the drill set
                boolean shouldDrill = false;
                for (Member member : tuple) {
                    if (memberSet.contains(member)) {
                        shouldDrill = true;
                        break;
                    }
                }
                if (!shouldDrill) {
                    return;
                }
                // Find the position of drillHierarchy in the tuple
                for (int k = 0; k < tuple.length; k++) {
                    if (tuple[k].getHierarchy().getUniqueName()
                        .equals(drillHierarchy.getUniqueName()))
                    {
                        List<Member> children =
                            evaluator.getSchemaReader()
                                .getMemberChildren(tuple[k]);
                        final Member[] tuple2 = tuple.clone();
                        for (Member childMember : children) {
                            tuple2[k] = childMember;
                            resultList.addTuple(tuple2);
                        }
                        break;
                    }
                }
            }

            private TupleList drilldownMember(
                TupleList v0,
                TupleList v1,
                Evaluator evaluator,
                Hierarchy drillHierarchy)
            {
                assert v1.getArity() == 1;
                if (v0.isEmpty() || v1.isEmpty()) {
                    return v0;
                }

                Set<Member> set1 = new HashSet<Member>(v1.slice(0));

                TupleList result = TupleCollections.createList(v0.getArity());
                int i = 0, n = v0.size();
                final Member[] members = new Member[v0.getArity()];
                while (i < n) {
                    List<Member> o = v0.get(i++);
                    o.toArray(members);
                    result.add(o);
                    if (drillHierarchy != null) {
                        drillDownCrossHierarchy(
                            evaluator, members, set1,
                            drillHierarchy, result);
                    } else {
                        drillDownObj(evaluator, members, set1, result);
                    }
                }
                return result;
            }
        };
    }
}

// End DrilldownMemberFunDef.java
