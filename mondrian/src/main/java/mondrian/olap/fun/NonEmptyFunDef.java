/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2021 Sergei Semenkov
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.rolap.SqlConstraintUtils;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.util.CancellationChecker;

import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class NonEmptyFunDef extends FunDefBase {
    private static final Logger LOGGER = LogManager.getLogger( NonEmptyFunDef.class );
    static final ReflectiveMultiResolver Resolver =
            new ReflectiveMultiResolver(
                    "NonEmpty",
                    "NonEmpty(<Set1>[, <Set2>])",
                    "Returns the set of tuples that are not empty from a specified set, based on the cross product " +
                            "of the specified set with a second set.",
                    new String[] {"fxx", "fxxx"},
                    NonEmptyFunDef.class);

    public NonEmptyFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        return args[0].getType();
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        ListCalc listCalc2 = null;
        if(call.getArgCount() == 2) {
            listCalc2 = compiler.compileList(call.getArg(1));
        }

        return new NonEmptyListCalcImpl(call, listCalc1, listCalc2);
    }

    private static class NonEmptyListCalcImpl extends AbstractListCalc {
        private final ListCalc listCalc1;
        private final ListCalc listCalc2;

        public NonEmptyListCalcImpl(
                ResolvedFunCall call,
                ListCalc listCalc1,
                ListCalc listCalc2)
        {
            super(call, new Calc[]{listCalc1, listCalc2});
            this.listCalc1 = listCalc1;
            this.listCalc2 = listCalc2;
        }

        public TupleList evaluateList(Evaluator evaluator) {
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(false);

                TupleList rightTuples = null;
                if(this.listCalc2 != null) {
                    rightTuples = listCalc2.evaluateList(evaluator);
                    rightTuples = prioritizeRightTuples(rightTuples);
                }

                evaluator.setNonEmpty(true);

                TupleList leftTuples = listCalc1.evaluateList(evaluator);
                if (leftTuples.isEmpty()) {
                    return TupleCollections.emptyList(leftTuples.getArity());
                }
                TupleList result =
                        TupleCollections.createList(leftTuples.getArity());


                for (List<Member> leftTuple : leftTuples) {
                    if(this.listCalc2 != null) {
                        if (rightTuples != null && !rightTuples.isEmpty()) {
                            for (List<Member> rightTuple : rightTuples) {
                                evaluator.setContext(leftTuple);
                                evaluator.setContext(rightTuple);
                                Object tupleResult = evaluator.evaluateCurrent();
                                if (tupleResult != null) {
                                    result.add(leftTuple);
                                    break;
                                }
                            }
                        }
                    }
                    else {
                        evaluator.setContext(leftTuple);
                        Object tupleResult = evaluator.evaluateCurrent();
                        if (tupleResult != null)
                        {
                            result.add(leftTuple);
                        }
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
            }
        }

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }
    }

    static TupleList prioritizeRightTuples(TupleList rightTuples) {
        if (rightTuples == null || rightTuples.size() < 2) {
            return rightTuples;
        }

        final List<List<Member>> tuples = new ArrayList<List<Member>>(rightTuples);
        Collections.sort(
            tuples,
            new Comparator<List<Member>>() {
                public int compare(List<Member> left, List<Member> right) {
                    return Integer.compare(tupleEvaluationCost(left), tupleEvaluationCost(right));
                }
            });

        boolean changed = false;
        for (int i = 0; i < tuples.size(); i++) {
            if (tuples.get(i) != rightTuples.get(i)) {
                changed = true;
                break;
            }
        }
        if (!changed) {
            return rightTuples;
        }

        final TupleList reordered =
            TupleCollections.createList(rightTuples.getArity(), rightTuples.size());
        reordered.addAll(tuples);
        return reordered;
    }

    private static int tupleEvaluationCost(List<Member> tuple) {
        int cost = 0;
        for (Member member : tuple) {
            if (member == null || !member.isMeasure()) {
                continue;
            }
            if (!member.isCalculated()) {
                continue;
            }
            cost = Math.max(cost, 1);
            final Exp expression = member.getExpression();
            if (expression != null && SqlConstraintUtils.containsValidMeasure(expression)) {
                cost = Math.max(cost, 2);
            }
        }
        return cost;
    }
}
