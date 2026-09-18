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
import mondrian.olap.Dimension;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.Validator;
import mondrian.olap.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
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
                ContextMembership membership = new ContextMembership(evaluator);
                for (List<Member> tuple : setTuples) {
                    if (membership.contains(tuple)) {
                        result.add(tuple);
                    }
                }
                return result;
            }
        };
    }

    /** Tests tuples against the dimension rows visible in one evaluation context. */
    private static final class ContextMembership {
        private final Evaluator evaluator;
        private final List<Member> contextMembers;
        private final List<Hierarchy> contextHierarchies;
        /** Visible member tuples per level combination: one query per combination. */
        private final Map<List<Level>, Set<List<Member>>> visible = new HashMap<>();

        ContextMembership(Evaluator evaluator) {
            this.evaluator = evaluator;
            this.contextMembers = Arrays.asList(evaluator.getMembers());
            this.contextHierarchies = contextMembers.stream().map(Member::getHierarchy).toList();
        }

        boolean contains(List<Member> tuple) {
            Map<Dimension, List<Member>> byDimension = new LinkedHashMap<>();
            for (Member member : tuple) {
                if (member.isNull()) {
                    return false;
                }
                if (member.getDimension().isMeasures() || member.isCalculated()) {
                    // No dimension rows to test: keep the hierarchy-chain check.
                    if (!existsInTuple(List.of(member), contextMembers,
                        List.of(member.getHierarchy()), contextHierarchies, evaluator))
                    {
                        return false;
                    }
                } else {
                    byDimension.computeIfAbsent(member.getDimension(), ignored -> new ArrayList<>()).add(member);
                }
            }
            for (List<Member> members : byDimension.values()) {
                List<Level> levels = members.stream().map(Member::getLevel).toList();
                if (!visible.computeIfAbsent(levels, key -> new HashSet<>(
                    evaluator.getSchemaReader().getMemberTuplesInDimensionContext(key, evaluator)))
                    .contains(members))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
// End ExistingFunDef.java