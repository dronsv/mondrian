/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (c) 2025 Sergei Semenkov
 * All rights reserved.
 */

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.UnaryTupleList;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Definition of the <code>DrillUpLevel</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><pre>
 * DrillUpLevel(Set_Expression[, Level_Expression])
 * </pre></blockquote>
 *
 * Opposite of DrilldownLevel: replaces members at a given level
 * with their parents, optionally including calculated members.
 */
class DrillupLevelFunDef extends FunDefBase {
    public static String INCLUDE_CALC_MEMBERS = "INCLUDE_CALC_MEMBERS";

    static final ReflectiveMultiResolver Resolver =
            new ReflectiveMultiResolver(
                    "DrillupLevel",
                    "DrillupLevel(<Set>[, <Level>])",
                    "Drills up the members of a set, at a specified level, to one level above.",
                    new String[]{"fxx", "fxxl"},
                    DrillupLevelFunDef.class,
                    new String[]{INCLUDE_CALC_MEMBERS}
            );

    public DrillupLevelFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc = compiler.compileList(call.getArg(0));
        final LevelCalc levelCalc =
                call.getArgCount() > 1
                        && call.getArg(1).getType() instanceof mondrian.olap.type.LevelType
                        ? compiler.compileLevel(call.getArg(1))
                        : null;

        final boolean includeCalcMembers =
                (call.getArgCount() == 2
                        && call.getArg(1) != null
                        && call.getArg(1) instanceof Literal
                        && INCLUDE_CALC_MEMBERS.equals(((Literal)call.getArg(1)).getValue())
                )
                        || (call.getArgCount() == 3
                        && call.getArg(2) != null
                        && call.getArg(2) instanceof Literal
                        && INCLUDE_CALC_MEMBERS.equals(((Literal)call.getArg(2)).getValue())
                );

        return new AbstractListCalc(call, new Calc[]{listCalc, levelCalc}) {
            public TupleList evaluateList(Evaluator evaluator) {
                TupleList list = listCalc.evaluateList(evaluator);
                if (list.isEmpty()) {
                    return list;
                }
                int searchDepth = -1;
                if (levelCalc != null) {
                    Level level = levelCalc.evaluateLevel(evaluator);
                    searchDepth = level.getDepth();
                }
                return new UnaryTupleList(
                        drillUp(searchDepth, list.slice(0), evaluator, includeCalcMembers)
                );
            }
        };
    }

    HashMap<Member, List<Member>> getCalcParentsByChild(Hierarchy hierarchy, Evaluator evaluator, boolean includeCalcMembers) {
        List<Member> calculatedMembers;
        if (includeCalcMembers) {
            calculatedMembers = evaluator.getSchemaReader().getCalculatedMembers(hierarchy);
        } else {
            calculatedMembers = new ArrayList<Member>();
        }
        HashMap<Member, List<Member>> calcParentsByChild = new HashMap<Member, List<Member>>();
        for (Member member : calculatedMembers) {
            Member parent = member.getParentMember();
            if (parent != null) {
                calcParentsByChild
                        .computeIfAbsent(member, k -> new ArrayList<Member>())
                        .add(parent);
            }
        }
        return calcParentsByChild;
    }

    List<Member> drillUp(int searchDepth, List<Member> list, Evaluator evaluator, boolean includeCalcMembers) {
        HashMap<Member, List<Member>> calcParentsByChild = getCalcParentsByChild(
                list.get(0).getHierarchy(),
                evaluator,
                includeCalcMembers
        );

        if (searchDepth == -1) {
            searchDepth = list.get(0).getLevel().getDepth();
            for (int i = 1, m = list.size(); i < m; i++) {
                int depth = list.get(i).getLevel().getDepth();
                if (depth < searchDepth) {
                    searchDepth = depth;
                }
            }
        }

        List<Member> drilledSet = new ArrayList<Member>();
        for (Member member : list) {
            int memberLevelDepth = member.getLevel().getDepth();
            while(searchDepth != -1 && memberLevelDepth > searchDepth) {
                member = member.getParentMember();
                if(member != null) {
                    memberLevelDepth = member.getLevel().getDepth();
                }
                else {
                    memberLevelDepth = -1;
                }
            }
            if (!drilledSet.contains(member)) {
                drilledSet.add(member);
            }
        }
        return drilledSet;
    }
}
