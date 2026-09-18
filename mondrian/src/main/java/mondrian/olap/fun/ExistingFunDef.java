/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2014-2017 Hitachi Vantara
// All rights reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.IterCalc;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleIterable;
import mondrian.calc.TupleList;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.Level;
import mondrian.olap.Dimension;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Existing keyword limits a set to what exists within the current context, ie
 * as if context members of the same dimension as the set were in the slicer.
 */
public class ExistingFunDef extends FunDefBase {

    static final ExistingFunDef instance = new ExistingFunDef();

    protected ExistingFunDef() {
      super(
          "Existing",
          "Existing <Set>",
          "Forces the set to be evaluated within the current context.",
          "Pxx");
    }

    @Override
    public Type getResultType(Validator validator, Exp[] args) {
        return args[0].getType();
    }

    @Override
    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final IterCalc setArg = compiler.compileIter(call.getArg(0));
        final Type myType = call.getArg(0).getType();

        return new AbstractListCalc(call, new Calc[] {setArg}) {
            @Override
            public boolean dependsOn(Hierarchy hierarchy) {
                return myType.usesDimension(hierarchy.getDimension(), false);
            }

            @Override
            public TupleList evaluateList(Evaluator evaluator) {
                // EXISTING tests dimension membership, not populated cells.
                Evaluator dimensionEvaluator = evaluator.push();
                dimensionEvaluator.setNonEmpty(false);
                TupleIterable setTuples = setArg.evaluateIterable(dimensionEvaluator);
                TupleList result = TupleCollections.createList(setTuples.getArity());
                List<Member> contextMembers = Arrays.asList(evaluator.getMembers());
                List<Hierarchy> contextDims = contextMembers.stream().map(Member::getHierarchy).toList();
                Map<List<Level>, Set<List<Member>>> admitted = new HashMap<>();

                for (List<Member> tuple : setTuples) {
                    Map<Dimension, List<Member>> dimensions = new LinkedHashMap<>();
                    boolean visible = true;
                    for (Member member : tuple) {
                        if (member.isNull()) {
                            visible = false;
                            break;
                        }
                        if (member.getDimension().isMeasures() || member.isCalculated()) {
                            if (!existsInTuple(List.of(member), contextMembers,
                                List.of(member.getHierarchy()), contextDims, evaluator))
                            {
                                visible = false;
                                break;
                            }
                        } else {
                            dimensions.computeIfAbsent(member.getDimension(), ignored -> new ArrayList<>()).add(member);
                        }
                    }
                    for (List<Member> members : dimensions.values()) {
                        if (!visible) {
                            break;
                        }
                        List<Level> levels = members.stream().map(Member::getLevel).toList();
                        visible = admitted.computeIfAbsent(levels, ignored -> new HashSet<>(
                            evaluator.getSchemaReader().getMemberTuplesInDimensionContext(levels, evaluator)))
                            .contains(members);
                    }
                    if (visible) {
                        result.add(tuple);
                    }
                }
                return result;
            }
        };
    }


}
// End ExistingFunDef.java