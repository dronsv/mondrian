/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// Copyright (C) 2004-2005 SAS Institute, Inc.
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.CellReadAnalysis;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapEvaluator;


/**
 * Definition of the <code>NonEmptyCrossJoin</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 *
 * author 16 December, 2004
 */
public class NonEmptyCrossJoinFunDef extends CrossJoinFunDef {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
        "NonEmptyCrossJoin",
            "NonEmptyCrossJoin(<Set1>, <Set2>)",
            "Returns the cross product of two sets, excluding empty tuples and tuples without associated fact table data.",
            new String[]{"fxxx"},
            NonEmptyCrossJoinFunDef.class);

    public NonEmptyCrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    @Override
    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
        final ListCalc crossings = new AbstractListCalc(
            call, new Calc[] {listCalc1, listCalc2}, false)
        {
            @Override
            public TupleList evaluateList(Evaluator evaluator) {
                SchemaReader schemaReader = evaluator.getSchemaReader();

                // Evaluate the arguments in non empty mode, but remove from
                // the slicer any members that will be overridden by args to
                // the NonEmptyCrossjoin function. For example, in
                //
                //   SELECT NonEmptyCrossJoin(
                //       [Store].[USA].Children,
                //       [Product].[Beer].Children)
                //    FROM [Sales]
                //    WHERE [Store].[Mexico]
                //
                // we want all beers, not just those sold in Mexico.
                final int savepoint = evaluator.savepoint();
                try {
                    evaluator.setNonEmpty(true);
                    for (Member member
                        : ((RolapEvaluator) evaluator).getSlicerMembers())
                    {
                        if (getType().getElementType().usesHierarchy(
                                member.getHierarchy(), true))
                        {
                            evaluator.setContext(
                                member.getHierarchy().getAllMember());
                        }
                    }

                    NativeEvaluator nativeEvaluator =
                        schemaReader.getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), evaluator, this);
                    if (nativeEvaluator != null) {
                        evaluator.restore(savepoint);
                        final TupleList tuples = (TupleList)
                            nativeEvaluator.execute(ResultStyle.LIST);
                        if (!judgeCellsEnabled()) {
                            return nativeResultIsFinal(evaluator, call)
                                ? tuples
                                : judgedCrossings(evaluator, tuples,
                                    CellReadAnalysis.Judges.crossJoin(
                                        call.getArgs()));
                        }
                        // Fact presence bounds the candidates, but neither
                        // a calculation nor a nullable stored measure must
                        // have a value at every fact-backed crossing.
                        //
                        // This judges after the restore above, while the
                        // interpreted path below judges inside the try. The
                        // widening in the try only replaces slicer members of
                        // hierarchies this call's element type uses, and every
                        // one of those is set again by the tuple being judged;
                        // setNonEmpty steers enumeration, not the value of a
                        // cell at a fixed coordinate. So the two judging
                        // contexts agree, and each stays where its own
                        // candidates were produced.
                        return judgedCrossings(evaluator, tuples,
                            CellReadAnalysis.Judges.crossJoin(call.getArgs()));
                    }

                    final TupleList list1 = listCalc1.evaluateList(evaluator);
                    if (list1.isEmpty()) {
                        evaluator.restore(savepoint);
                        return list1;
                    }
                    final TupleList list2 = listCalc2.evaluateList(evaluator);
                    TupleList result = mutableCrossJoin(list1, list2);

                    // remove any remaining empty crossings from the result.
                    // Judged here, inside the try: these candidates were
                    // produced under the widened context above, and the
                    // native branch's comment explains why judging under it
                    // is the same as judging after the restore.
                    return nonEmptyCrossJoinList(evaluator, result, call);
                } finally {
                    evaluator.restore(savepoint);
                }
            }

            @Override
            public boolean dependsOn(Hierarchy hierarchy) {
                if (super.dependsOn(hierarchy)) {
                    return true;
                }
                // Member calculations generate members, which mask the actual
                // expression from the inherited context.
                if (listCalc1.getType().usesHierarchy(hierarchy, true)) {
                    return false;
                }
                if (listCalc2.getType().usesHierarchy(hierarchy, true)) {
                    return false;
                }
                // The implicit value expression, executed to figure out
                // whether a given tuple is empty, depends upon all dimensions.
                return true;
            }
        };

        // Judging reads a cell per crossing, and RolapResult evaluates one
        // axis more than once: a batch-load pass that only registers the
        // cell requests, the pass that answers with those cells loaded, and
        // the axis-construction pass, which repeats the answer exactly.
        //
        // The answer is a function of this call, the coordinate it is
        // evaluated at and the cells that coordinate reads, which is what
        // the query's own expression result cache is keyed and invalidated
        // by: getCachedResult stores a result as valid only when the cell
        // reader was clean and missed nothing while it was computed, so a
        // load pass's answer (every candidate kept, because an unloaded
        // cell reads back as a non-null placeholder) is discarded with its
        // phase and only the pass that read loaded cells is reused. The
        // hierarchies of the key are this calc's own dependsOn, so a
        // coordinate the judging reads is never cached away.
        //
        // The cache lives in mondrian.rolap and is reached through the
        // public Evaluator.getCachedResult, which CacheFunDef and
        // RankFunDef already use from this package: the dirty/miss-count
        // test is encapsulated by it rather than exposed, so neither the
        // judging nor a CellReader predicate has to move packages.
        final ExpCacheDescriptor judged =
            new ExpCacheDescriptor(call, crossings, compiler.getEvaluator());
        return new AbstractListCalc(call, new Calc[] {crossings}, false) {
            @Override
            public TupleList evaluateList(Evaluator evaluator) {
                return (TupleList) evaluator.getCachedResult(judged);
            }
        };
    }

    /**
     * Whether a native result needs no cell of its own: it joined the fact
     * that bounds every judge of this call. A dimension-only enumeration, or
     * one over a virtual cube's base facts, only lists the candidates.
     *
     * <p>Only consulted when the final judging is switched off; with it on,
     * the fact bounds the candidates but does not decide the result.
     */
    private static boolean nativeResultIsFinal(
        Evaluator evaluator, ResolvedFunCall call)
    {
        return !((RolapCube) evaluator.getCube()).isVirtual()
            && !CellReadAnalysis.of(evaluator).needsFactlessEnumeration(
                evaluator, CellReadAnalysis.Judges.crossJoin(call.getArgs()));
    }
}

// End NonEmptyCrossJoinFunDef.java
