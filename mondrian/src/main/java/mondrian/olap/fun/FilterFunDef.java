/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.metrics.FilterExecutionMetrics;
import mondrian.metrics.FilterPathMetrics;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapStoredMeasure;
import mondrian.rolap.SqlConstraintUtils;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.util.CancellationChecker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Definition of the <code>Filter</code> MDX function.
 *
 * <p>Syntax:
 * <blockquote><code>Filter(&lt;Set&gt;, &lt;Search
 * Condition&gt;)</code></blockquote>
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class FilterFunDef extends FunDefBase {

    private static final String TIMING_NAME =
        FilterFunDef.class.getSimpleName();
    private static final Logger LOGGER =
        LogManager.getLogger(FilterFunDef.class);
    private static final String PATH_FAST_EXACT_NOT_ISEMPTY =
        "fastpath.exact_not_isempty";
    private static final String PATH_FAST_EXACT_AND_NOT_ISEMPTY =
        "fastpath.exact_and_not_isempty";
    private static final String PATH_FAST_AND_NOT_ISEMPTY =
        "fastpath.and_contains_not_isempty";
    private static final String PATH_FAST_EXACT_OR_MEASURE_NULL =
        "fastpath.exact_or_measure_null";
    private static final String PATH_NATIVE_EVALUATOR =
        "native.set_evaluator";
    private static final String PATH_CONTEXT_AWARE_MEASURE_PASS =
        "fallback.context_aware_measure_pass";
    private static final String PATH_FALLBACK =
        "fallback.standard";
    private static final Map<Query, Set<String>>
        LOGGED_FILTER_FALLBACK_EVENTS_BY_QUERY =
        Collections.synchronizedMap(new WeakHashMap<Query, Set<String>>());
    private static final Map<Query, Set<String>>
        LOGGED_FILTER_PATH_EVENTS_BY_QUERY =
        Collections.synchronizedMap(new WeakHashMap<Query, Set<String>>());

    static final FilterFunDef instance = new FilterFunDef();

    private FilterFunDef() {
        super(
            "Filter",
            "Returns the set resulting from filtering a set based on a search condition.",
            "fxxb");
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        // Ignore the caller's priority. We prefer to return iterable, because
        // it makes NamedSet.CurrentOrdinal work.
        List<ResultStyle> styles = compiler.getAcceptableResultStyles();
        if (call.getArg(0) instanceof ResolvedFunCall
            && ((ResolvedFunCall) call.getArg(0)).getFunName().equals("AS"))
        {
            styles = ResultStyle.ITERABLE_ONLY;
        }
        if (styles.contains(ResultStyle.ITERABLE)
            || styles.contains(ResultStyle.ANY))
        {
            return compileCallIterable(call, compiler);
        } else if (styles.contains(ResultStyle.LIST)
            || styles.contains(ResultStyle.MUTABLE_LIST))
        {
            return compileCallList(call, compiler);
        } else {
            throw ResultStyleException.generate(
                ResultStyle.ITERABLE_LIST_MUTABLELIST_ANY,
                styles);
        }
    }

    /**
     * Returns an IterCalc.
     *
     * <p>Here we would like to get either a IterCalc or ListCalc (mutable)
     * from the inner expression. For the IterCalc, its Iterator
     * can be wrapped with another Iterator that filters each element.
     * For the mutable list, remove all members that are filtered.
     *
     * @param call Call
     * @param compiler Compiler
     * @return Implementation of this function call in the Iterable result style
     */
    protected IterCalc compileCallIterable(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        // want iterable, mutable list or immutable list in that order
        Calc imlcalc = compiler.compileAs(
            call.getArg(0), null, ResultStyle.ITERABLE_LIST_MUTABLELIST);
        final Exp residualPredicate =
            preparePredicateForBooleanEvaluation(call);
        BooleanCalc bcalc = compiler.compileBoolean(residualPredicate);
        Calc[] calcs = new Calc[] {imlcalc, bcalc};

        // check returned calc ResultStyles
        checkIterListResultStyles(imlcalc);

        if (imlcalc.getResultStyle() == ResultStyle.ITERABLE) {
            return new IterIterCalc(call, calcs);
        } else if (imlcalc.getResultStyle() == ResultStyle.LIST) {
            return new ImmutableIterCalc(call, calcs);
        } else {
            return new MutableIterCalc(call, calcs);
        }
    }

    private static abstract class BaseIterCalc extends AbstractIterCalc {
        protected BaseIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }

        public TupleIterable evaluateIterable(Evaluator evaluator) {
            evaluator.getTiming().markStart(TIMING_NAME);
            try {
                ResolvedFunCall call = (ResolvedFunCall) exp;
                final NonEmptyPredicateAnalysis nonEmptyAnalysis =
                    analyzeNonEmptyPredicate(call);
                final MeasureNullPredicateDisjunction measureNullOrAnalysis =
                    analyzeMeasureNullOrPredicate(call.getArg(1));
                if (nonEmptyAnalysis != null
                    && requiresContextAwareMeasurePass(evaluator))
                {
                    final SchemaReader schemaReader =
                        evaluator.getSchemaReader();
                    // Push carrier stored measure — see evaluateList for
                    // detailed explanation.
                    final int nativeSavepoint = evaluator.savepoint();
                    NativeEvaluator nativeEvaluator = null;
                    try {
                        pushCarrierMeasure(evaluator, nonEmptyAnalysis);
                        nativeEvaluator =
                            schemaReader.getNativeSetEvaluator(
                                call.getFunDef(), call.getArgs(),
                                evaluator, this);
                    } finally {
                        evaluator.restore(nativeSavepoint);
                    }
                    if (nativeEvaluator != null) {
                        recordFilterPath(
                            evaluator,
                            call,
                            call.getArg(1),
                            PATH_NATIVE_EVALUATOR,
                            "axis_context_not_isempty_native");
                        return (TupleIterable)
                            nativeEvaluator.execute(ResultStyle.ITERABLE);
                    }
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_CONTEXT_AWARE_MEASURE_PASS,
                        "axis_context_not_isempty");
                    TupleIterable candidates = evaluateInputSet(evaluator);
                    candidates = filterByMeasuresNonEmpty(
                        evaluator,
                        candidates,
                        nonEmptyAnalysis.getMeasures());
                    if (nonEmptyAnalysis.isExact()) {
                        return candidates;
                    }
                    return filterByPredicate(
                        evaluator,
                        candidates,
                        (BooleanCalc) getCalcs()[1]);
                }
                if (nonEmptyAnalysis != null && nonEmptyAnalysis.isExact()) {
                    final Member primaryMeasure =
                        nonEmptyAnalysis.getPrimaryMeasure();
                    if (primaryMeasure == null) {
                        return makeIterable(evaluator);
                    }
                    if (nonEmptyAnalysis.getMeasures().size() == 1) {
                        recordFilterPath(
                            evaluator,
                            call,
                            call.getArg(1),
                            PATH_FAST_EXACT_NOT_ISEMPTY,
                            "exact_not_isempty");
                        return evaluateBySetNonEmpty(
                            evaluator,
                            primaryMeasure);
                    }
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_FAST_EXACT_AND_NOT_ISEMPTY,
                        "exact_and_not_isempty");
                    return filterByMeasuresNonEmpty(
                        evaluator,
                        evaluateBySetNonEmpty(evaluator, primaryMeasure),
                        nonEmptyAnalysis.getAdditionalMeasures());
                }
                // Use a native evaluator, if more efficient.
                // TODO: Figure this out at compile time.
                SchemaReader schemaReader = evaluator.getSchemaReader();
                NativeEvaluator nativeEvaluator =
                    schemaReader.getNativeSetEvaluator(
                        call.getFunDef(), call.getArgs(), evaluator, this);
                if (nativeEvaluator != null) {
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_NATIVE_EVALUATOR,
                        "native_set_evaluator");
                    return (TupleIterable)
                        nativeEvaluator.execute(ResultStyle.ITERABLE);
                }
                if (measureNullOrAnalysis != null) {
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_FAST_EXACT_OR_MEASURE_NULL,
                        measureNullOrAnalysis.isTautology()
                            ? "exact_or_measure_null_tautology"
                            : "exact_or_measure_null");
                    final TupleIterable candidates = evaluateInputSet(evaluator);
                    if (measureNullOrAnalysis.isTautology()) {
                        return candidates;
                    }
                    return filterByMeasureNullPredicatesDisjunction(
                        evaluator,
                        candidates,
                        measureNullOrAnalysis);
                }
                if (nonEmptyAnalysis != null) {
                    final Member primaryMeasure =
                        nonEmptyAnalysis.getPrimaryMeasure();
                    if (primaryMeasure == null) {
                        return makeIterable(evaluator);
                    }
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "and_contains_not_isempty");
                    TupleIterable candidates = evaluateBySetNonEmpty(
                        evaluator,
                        primaryMeasure);
                    final List<Member> additionalMeasures =
                        nonEmptyAnalysis.getAdditionalMeasures();
                    if (!additionalMeasures.isEmpty()) {
                        candidates = filterByMeasuresNonEmpty(
                            evaluator,
                            candidates,
                            additionalMeasures);
                    }
                    return filterByPredicate(
                        evaluator,
                        candidates,
                        (BooleanCalc) getCalcs()[1]);
                }
                final String fallbackReason =
                    determineFilterFastPathFallbackReason(call.getArg(1));
                recordFilterPath(
                    evaluator,
                    call,
                    call.getArg(1),
                    PATH_FALLBACK,
                    fallbackReason == null
                        ? "general"
                        : fallbackReason);
                maybeLogFilterFastPathFallback(
                    evaluator,
                    call,
                    call.getArg(1),
                    fallbackReason);
                return makeIterable(evaluator);
            } finally {
                evaluator.getTiming().markEnd(TIMING_NAME);
            }
        }

        private TupleIterable evaluateBySetNonEmpty(
            Evaluator evaluator,
            Member nonEmptyMeasure)
        {
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(true);
                evaluator.setContext(nonEmptyMeasure);
                final Calc setCalc = getCalcs()[0];
                if (setCalc instanceof IterCalc) {
                    return ((IterCalc) setCalc).evaluateIterable(evaluator);
                }
                if (setCalc instanceof ListCalc) {
                    return ((ListCalc) setCalc).evaluateList(evaluator);
                }
                throw new IllegalStateException(
                    "Unexpected set calc type in Filter iterable fast-path: "
                    + setCalc.getClass().getName());
            } finally {
                evaluator.restore(savepoint);
            }
        }

        private TupleIterable evaluateInputSet(Evaluator evaluator) {
            final Calc setCalc = getCalcs()[0];
            if (setCalc instanceof IterCalc) {
                return ((IterCalc) setCalc).evaluateIterable(evaluator);
            }
            if (setCalc instanceof ListCalc) {
                return ((ListCalc) setCalc).evaluateList(evaluator);
            }
            throw new IllegalStateException(
                "Unexpected set calc type in Filter iterable fast-path: "
                + setCalc.getClass().getName());
        }

        private TupleIterable filterByPredicate(
            Evaluator evaluator,
            final TupleIterable iterable,
            final BooleanCalc bcalc)
        {
            final Evaluator evaluator2 = evaluator.push();
            evaluator2.setNonEmpty(false);
            return new AbstractTupleIterable(iterable.getArity()) {
                public TupleCursor tupleCursor() {
                    return new AbstractTupleCursor(iterable.getArity()) {
                        final TupleCursor cursor = iterable.tupleCursor();
                        final Execution execution = evaluator2.getQuery()
                            .getStatement().getCurrentExecution();
                        int currentIteration = 0;

                        public boolean forward() {
                            while (cursor.forward()) {
                                CancellationChecker.checkCancelOrTimeout(
                                    currentIteration++, execution);
                                cursor.setContext(evaluator2);
                                if (bcalc.evaluateBoolean(evaluator2)) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        public List<Member> current() {
                            return cursor.current();
                        }
                    };
                }
            };
        }

        private TupleIterable filterByMeasuresNonEmpty(
            Evaluator evaluator,
            final TupleIterable iterable,
            final List<Member> measures)
        {
            if (measures == null || measures.isEmpty()) {
                return iterable;
            }
            final Evaluator evaluator2 = evaluator.push();
            evaluator2.setNonEmpty(false);
            return new AbstractTupleIterable(iterable.getArity()) {
                public TupleCursor tupleCursor() {
                    return new AbstractTupleCursor(iterable.getArity()) {
                        final TupleCursor cursor = iterable.tupleCursor();
                        final Execution execution = evaluator2.getQuery()
                            .getStatement().getCurrentExecution();
                        int currentIteration = 0;

                        public boolean forward() {
                            while (cursor.forward()) {
                                CancellationChecker.checkCancelOrTimeout(
                                    currentIteration++, execution);
                                cursor.setContext(evaluator2);
                                boolean keep = true;
                                for (Member measure : measures) {
                                    evaluator2.setContext(measure);
                                    if (evaluator2.evaluateCurrent() == null) {
                                        keep = false;
                                        break;
                                    }
                                }
                                if (keep) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        public List<Member> current() {
                            return cursor.current();
                        }
                    };
                }
            };
        }

        private TupleIterable filterByMeasureNullPredicatesDisjunction(
            Evaluator evaluator,
            final TupleIterable iterable,
            final MeasureNullPredicateDisjunction disjunction)
        {
            if (disjunction == null || disjunction.isEmpty()) {
                return iterable;
            }
            final Evaluator evaluator2 = evaluator.push();
            evaluator2.setNonEmpty(false);
            return new AbstractTupleIterable(iterable.getArity()) {
                public TupleCursor tupleCursor() {
                    return new AbstractTupleCursor(iterable.getArity()) {
                        final TupleCursor cursor = iterable.tupleCursor();
                        final Execution execution = evaluator2.getQuery()
                            .getStatement().getCurrentExecution();
                        int currentIteration = 0;

                        public boolean forward() {
                            while (cursor.forward()) {
                                CancellationChecker.checkCancelOrTimeout(
                                    currentIteration++, execution);
                                cursor.setContext(evaluator2);
                                boolean keep = false;
                                for (Member measure : disjunction.getMeasures()) {
                                    evaluator2.setContext(measure);
                                    if (disjunction.matches(
                                        measure,
                                        evaluator2.evaluateCurrent()))
                                    {
                                        keep = true;
                                        break;
                                    }
                                }
                                if (keep) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        public List<Member> current() {
                            return cursor.current();
                        }
                    };
                }
            };
        }

        protected abstract TupleIterable makeIterable(Evaluator evaluator);

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }
    }

    private static class MutableIterCalc extends BaseIterCalc {
        MutableIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleIterable makeIterable(Evaluator evaluator) {
            evaluator.getTiming().markStart(TIMING_NAME);
            final int savepoint = evaluator.savepoint();
            try {
                Calc[] calcs = getCalcs();
                ListCalc lcalc = (ListCalc) calcs[0];
                BooleanCalc bcalc = (BooleanCalc) calcs[1];

                TupleList list = lcalc.evaluateList(evaluator);

                // make list mutable; guess selectivity .5
                TupleList result =
                    TupleCollections.createList(
                        list.getArity(), list.size() / 2);
                evaluator.setNonEmpty(false);
                TupleCursor cursor = list.tupleCursor();
                int currentIteration = 0;
                Execution execution =
                    evaluator.getQuery().getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
                evaluator.getTiming().markEnd(TIMING_NAME);
            }
        }
    }

    private static class ImmutableIterCalc extends BaseIterCalc {
        ImmutableIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleIterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];
            TupleList members = lcalc.evaluateList(evaluator);

            // Not mutable, must create new list
            TupleList result = members.cloneList(members.size() / 2);
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(false);
                TupleCursor cursor = members.tupleCursor();
                int currentIteration = 0;
                Execution execution =
                    evaluator.getQuery().getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
            }
        }
    }

    private static class IterIterCalc
        extends BaseIterCalc
    {
        IterIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof IterCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleIterable makeIterable(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            IterCalc icalc = (IterCalc) calcs[0];
            final BooleanCalc bcalc = (BooleanCalc) calcs[1];

            // This does dynamics, just in time,
            // as needed filtering
            final TupleIterable iterable =
                icalc.evaluateIterable(evaluator);
            final Evaluator evaluator2 = evaluator.push();
            evaluator2.setNonEmpty(false);
            return new AbstractTupleIterable(iterable.getArity()) {
                public TupleCursor tupleCursor() {
                    return new AbstractTupleCursor(iterable.getArity()) {
                        final TupleCursor cursor = iterable.tupleCursor();

                        public boolean forward() {
                            int currentIteration = 0;
                            Execution execution = Locus.peek().execution;
                            while (cursor.forward()) {
                                CancellationChecker.checkCancelOrTimeout(
                                    currentIteration++, execution);
                                cursor.setContext(evaluator2);
                                if (bcalc.evaluateBoolean(evaluator2)) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        public List<Member> current() {
                            return cursor.current();
                        }
                    };
                }
            };
        }
    }


    /**
     * Returns a ListCalc.
     *
     * @param call Call
     * @param compiler Compiler
     * @return Implementation of this function call in the List result style
     */
    protected ListCalc compileCallList(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        Calc ilcalc = compiler.compileList(call.getArg(0), false);
        final Exp residualPredicate =
            preparePredicateForBooleanEvaluation(call);
        BooleanCalc bcalc = compiler.compileBoolean(residualPredicate);
        Calc[] calcs = new Calc[] {ilcalc, bcalc};

        // Note that all of the ListCalc's return will be mutable
        switch (ilcalc.getResultStyle()) {
        case LIST:
            return new ImmutableListCalc(call, calcs);
        case MUTABLE_LIST:
            return new MutableListCalc(call, calcs);
        }
        throw ResultStyleException.generateBadType(
            ResultStyle.MUTABLELIST_LIST,
            ilcalc.getResultStyle());
    }

    private static abstract class BaseListCalc extends AbstractListCalc {
        protected BaseListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }

        public TupleList evaluateList(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            final NonEmptyPredicateAnalysis nonEmptyAnalysis =
                analyzeNonEmptyPredicate(call);
            final MeasureNullPredicateDisjunction measureNullOrAnalysis =
                analyzeMeasureNullOrPredicate(call.getArg(1));
            if (nonEmptyAnalysis != null
                && requiresContextAwareMeasurePass(evaluator))
            {
                final SchemaReader schemaReader = evaluator.getSchemaReader();
                // Push carrier stored measure into evaluator before native
                // evaluation attempt.  This ensures:
                // 1. measuresConflictWithMembers sees stored measure (no
                //    embedded dimension refs) instead of calc measure formula
                // 2. Stable cache key across re-evaluations during segment
                //    loading (same stored measure each time = cache hits)
                final int nativeSavepoint = evaluator.savepoint();
                NativeEvaluator nativeEvaluator = null;
                try {
                    pushCarrierMeasure(evaluator, nonEmptyAnalysis);
                    nativeEvaluator =
                        schemaReader.getNativeSetEvaluator(
                            call.getFunDef(), call.getArgs(), evaluator, this);
                } finally {
                    evaluator.restore(nativeSavepoint);
                }
                if (nativeEvaluator != null) {
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_NATIVE_EVALUATOR,
                        "axis_context_not_isempty_native");
                    return (TupleList)
                        nativeEvaluator.execute(ResultStyle.LIST);
                }
                recordFilterPath(
                    evaluator,
                    call,
                    call.getArg(1),
                    PATH_CONTEXT_AWARE_MEASURE_PASS,
                    "axis_context_not_isempty");
                TupleList candidates =
                    evaluateInputSet(evaluator, PATH_CONTEXT_AWARE_MEASURE_PASS);
                candidates = filterByMeasuresNonEmpty(
                    evaluator,
                    candidates,
                    nonEmptyAnalysis.getMeasures(),
                    PATH_CONTEXT_AWARE_MEASURE_PASS);
                if (nonEmptyAnalysis.isExact()) {
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_CONTEXT_AWARE_MEASURE_PASS,
                        "output");
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_CONTEXT_AWARE_MEASURE_PASS,
                        "output",
                        candidates.size());
                    return candidates;
                }
                final TupleList result = filterByPredicate(
                    evaluator,
                    candidates,
                    (BooleanCalc) getCalcs()[1],
                    null);
                FilterExecutionMetrics.recordStageExecution(
                    PATH_CONTEXT_AWARE_MEASURE_PASS,
                    "output");
                FilterExecutionMetrics.recordStageTuples(
                    PATH_CONTEXT_AWARE_MEASURE_PASS,
                    "output",
                    result.size());
                return result;
            }
            if (nonEmptyAnalysis != null && nonEmptyAnalysis.isExact()) {
                final Member primaryMeasure =
                    nonEmptyAnalysis.getPrimaryMeasure();
                if (primaryMeasure != null
                    && nonEmptyAnalysis.getMeasures().size() == 1)
                {
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_FAST_EXACT_NOT_ISEMPTY,
                        "exact_not_isempty");
                    final TupleList result = evaluateBySetNonEmpty(
                        evaluator,
                        primaryMeasure,
                        PATH_FAST_EXACT_NOT_ISEMPTY);
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_FAST_EXACT_NOT_ISEMPTY,
                        "output");
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_FAST_EXACT_NOT_ISEMPTY,
                        "output",
                        result.size());
                    return result;
                }
                if (primaryMeasure != null
                    && nonEmptyAnalysis.getMeasures().size() > 1)
                {
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_FAST_EXACT_AND_NOT_ISEMPTY,
                        "exact_and_not_isempty");
                    TupleList result = evaluateBySetNonEmpty(
                        evaluator,
                        primaryMeasure,
                        PATH_FAST_EXACT_AND_NOT_ISEMPTY);
                    result = filterByMeasuresNonEmpty(
                        evaluator,
                        result,
                        nonEmptyAnalysis.getAdditionalMeasures(),
                        PATH_FAST_EXACT_AND_NOT_ISEMPTY);
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_FAST_EXACT_AND_NOT_ISEMPTY,
                        "output");
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_FAST_EXACT_AND_NOT_ISEMPTY,
                        "output",
                        result.size());
                    return result;
                }
            }
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                schemaReader.getNativeSetEvaluator(
                    call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                recordFilterPath(
                    evaluator,
                    call,
                    call.getArg(1),
                    PATH_NATIVE_EVALUATOR,
                    "native_set_evaluator");
                return (TupleList) nativeEvaluator.execute(
                    ResultStyle.ITERABLE);
            }
            if (measureNullOrAnalysis != null) {
                recordFilterPath(
                    evaluator,
                    call,
                    call.getArg(1),
                    PATH_FAST_EXACT_OR_MEASURE_NULL,
                    measureNullOrAnalysis.isTautology()
                        ? "exact_or_measure_null_tautology"
                        : "exact_or_measure_null");
                final TupleList candidates =
                    evaluateInputSet(evaluator, PATH_FAST_EXACT_OR_MEASURE_NULL);
                if (measureNullOrAnalysis.isTautology()) {
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_FAST_EXACT_OR_MEASURE_NULL,
                        "residual_null_tautology");
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_FAST_EXACT_OR_MEASURE_NULL,
                        "residual_output",
                        candidates.size());
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_FAST_EXACT_OR_MEASURE_NULL,
                        "output");
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_FAST_EXACT_OR_MEASURE_NULL,
                        "output",
                        candidates.size());
                    return candidates;
                }
                final TupleList result = filterByMeasureNullPredicatesDisjunction(
                    evaluator,
                    candidates,
                    measureNullOrAnalysis,
                    PATH_FAST_EXACT_OR_MEASURE_NULL);
                FilterExecutionMetrics.recordStageExecution(
                    PATH_FAST_EXACT_OR_MEASURE_NULL,
                    "output");
                FilterExecutionMetrics.recordStageTuples(
                    PATH_FAST_EXACT_OR_MEASURE_NULL,
                    "output",
                    result.size());
                return result;
            }
            if (nonEmptyAnalysis != null) {
                final Member primaryMeasure =
                    nonEmptyAnalysis.getPrimaryMeasure();
                if (primaryMeasure == null) {
                    return makeList(evaluator);
                }
                recordFilterPath(
                    evaluator,
                    call,
                    call.getArg(1),
                    PATH_FAST_AND_NOT_ISEMPTY,
                    "and_contains_not_isempty");
                TupleList candidates = evaluateBySetNonEmpty(
                    evaluator,
                    primaryMeasure,
                    PATH_FAST_AND_NOT_ISEMPTY);
                final List<Member> additionalMeasures =
                    nonEmptyAnalysis.getAdditionalMeasures();
                final Exp residualPredicateExp =
                    preparePredicateForBooleanEvaluation(call);
                final NumericMeasurePredicateConjunction numericPredicates =
                    extractNumericMeasurePredicateConjunction(
                        residualPredicateExp);
                final MeasureNullPredicateConjunction nullPredicates =
                    extractMeasureNullPredicateConjunction(
                        residualPredicateExp);
                final NumericMeasurePredicate numericPredicate =
                    numericPredicates == null
                        ? extractNumericMeasurePredicate(residualPredicateExp)
                        : numericPredicates.getSinglePredicate();
                final Set<Member> foldEligibleMeasures =
                    new HashSet<Member>(additionalMeasures);
                foldEligibleMeasures.add(primaryMeasure);
                final boolean foldNumericIntoMeasurePass =
                    numericPredicates != null
                        && !numericPredicates.isEmpty()
                        && foldEligibleMeasures.containsAll(
                            numericPredicates.getMeasures());
                if (nullPredicates != null && nullPredicates.isUnsatisfiable()) {
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "residual_null_contradiction");
                    final TupleList result = candidates.cloneList(0);
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "residual_output",
                        result.size());
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "output");
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "output",
                        result.size());
                    return result;
                }
                if (!additionalMeasures.isEmpty() || foldNumericIntoMeasurePass) {
                    candidates = foldNumericIntoMeasurePass
                        ? filterByMeasuresAndNumericPredicates(
                            evaluator,
                            candidates,
                            primaryMeasure,
                            additionalMeasures,
                            numericPredicates,
                            PATH_FAST_AND_NOT_ISEMPTY)
                        : filterByMeasuresNonEmpty(
                            evaluator,
                            candidates,
                            additionalMeasures,
                            PATH_FAST_AND_NOT_ISEMPTY);
                }
                if (foldNumericIntoMeasurePass) {
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "output");
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "output",
                        candidates.size());
                    return candidates;
                }
                if (nullPredicates != null && !nullPredicates.isEmpty()) {
                    final TupleList result = filterByMeasureNullPredicates(
                        evaluator,
                        candidates,
                        nullPredicates,
                        PATH_FAST_AND_NOT_ISEMPTY);
                    FilterExecutionMetrics.recordStageExecution(
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "output");
                    FilterExecutionMetrics.recordStageTuples(
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "output",
                        result.size());
                    return result;
                }
                final TupleList result = filterByPredicate(
                    evaluator,
                    candidates,
                    (BooleanCalc) getCalcs()[1],
                    numericPredicate);
                FilterExecutionMetrics.recordStageExecution(
                    PATH_FAST_AND_NOT_ISEMPTY,
                    "output");
                FilterExecutionMetrics.recordStageTuples(
                    PATH_FAST_AND_NOT_ISEMPTY,
                    "output",
                    result.size());
                return result;
            }
            final String fallbackReason =
                determineFilterFastPathFallbackReason(call.getArg(1));
            recordFilterPath(
                evaluator,
                call,
                call.getArg(1),
                PATH_FALLBACK,
                fallbackReason == null
                    ? "general"
                    : fallbackReason);
            maybeLogFilterFastPathFallback(
                evaluator,
                call,
                call.getArg(1),
                fallbackReason);
            return makeList(evaluator);
        }

        private TupleList evaluateInputSet(Evaluator evaluator, String path) {
            final int savepoint = evaluator.savepoint();
            final long startNanos = System.nanoTime();
            try {
                FilterExecutionMetrics.recordStageExecution(
                    path,
                    "preprune_input");
                final ListCalc lcalc = (ListCalc) getCalcs()[0];
                final TupleList result = lcalc.evaluateList(evaluator);
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "preprune_input",
                    result.size());
                return result;
            } finally {
                FilterExecutionMetrics.recordStageDurationNanos(
                    path,
                    "preprune_input",
                    System.nanoTime() - startNanos);
                evaluator.restore(savepoint);
            }
        }

        /**
         * Fast path for common pattern:
         * Filter(setExpr, NOT IsEmpty([Measures].X)).
         */
        private TupleList evaluateBySetNonEmpty(
            Evaluator evaluator,
            Member nonEmptyMeasure,
            String path)
        {
            final int savepoint = evaluator.savepoint();
            final long startNanos = System.nanoTime();
            try {
                evaluator.setNonEmpty(true);
                evaluator.setContext(nonEmptyMeasure);
                ListCalc lcalc = (ListCalc) getCalcs()[0];
                final TupleList result = lcalc.evaluateList(evaluator);
                FilterExecutionMetrics.recordStageExecution(
                    path,
                    "preprune_nonempty");
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "preprune_nonempty",
                    result.size());
                return result;
            } finally {
                FilterExecutionMetrics.recordStageDurationNanos(
                    path,
                    "preprune_nonempty",
                    System.nanoTime() - startNanos);
                evaluator.restore(savepoint);
            }
        }

        private TupleList filterByPredicate(
            Evaluator evaluator,
            TupleList candidates,
            BooleanCalc bcalc,
            NumericMeasurePredicate numericPredicate)
        {
            final int savepoint = evaluator.savepoint();
            final long startNanos = System.nanoTime();
            try {
                final boolean useNumericPredicate =
                    numericPredicate != null
                        && !numericPredicate.measure.isCalculated();
                final String stageName =
                    useNumericPredicate
                        ? "residual_eval_numeric_measure"
                        : "residual_eval";
                FilterExecutionMetrics.recordStageExecution(
                    PATH_FAST_AND_NOT_ISEMPTY,
                    stageName);
                FilterExecutionMetrics.recordStageTuples(
                    PATH_FAST_AND_NOT_ISEMPTY,
                    "residual_input",
                    candidates.size());
                evaluator.setNonEmpty(false);
                final TupleList result =
                    candidates.cloneList(candidates.size() / 2);
                final TupleCursor cursor = candidates.tupleCursor();
                int currentIteration = 0;
                final Execution execution = evaluator.getQuery()
                    .getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    if (useNumericPredicate) {
                        evaluator.setContext(numericPredicate.measure);
                        final Object value = evaluator.evaluateCurrent();
                        if (numericPredicate.matches(value)) {
                            result.addCurrent(cursor);
                        }
                    } else if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                FilterExecutionMetrics.recordStageTuples(
                    PATH_FAST_AND_NOT_ISEMPTY,
                    "residual_output",
                    result.size());
                return result;
            } finally {
                FilterExecutionMetrics.recordStageDurationNanos(
                    PATH_FAST_AND_NOT_ISEMPTY,
                    numericPredicate != null
                        ? "residual_eval_numeric_measure"
                        : "residual_eval",
                    System.nanoTime() - startNanos);
                evaluator.restore(savepoint);
            }
        }

        private TupleList filterByMeasuresAndNumericPredicates(
            Evaluator evaluator,
            TupleList candidates,
            Member primaryMeasure,
            List<Member> additionalMeasures,
            NumericMeasurePredicateConjunction numericPredicates,
            String path)
        {
            if (numericPredicates == null || numericPredicates.isEmpty())
            {
                return candidates;
            }
            final int savepoint = evaluator.savepoint();
            final long startNanos = System.nanoTime();
            try {
                FilterExecutionMetrics.recordStageExecution(
                    path,
                    "measure_nonempty_residual_numeric_conjunction");
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "measure_nonempty_input",
                    candidates.size());
                evaluator.setNonEmpty(false);
                final TupleList result =
                    candidates.cloneList(candidates.size() / 2);
                final TupleCursor cursor = candidates.tupleCursor();
                int currentIteration = 0;
                final Execution execution = evaluator.getQuery()
                    .getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    boolean keep = true;
                    for (Member measure : additionalMeasures) {
                        evaluator.setContext(measure);
                        final Object value = evaluator.evaluateCurrent();
                        if (value == null) {
                            keep = false;
                            break;
                        }
                        if (!numericPredicates.matches(measure, value))
                        {
                            keep = false;
                            break;
                        }
                    }
                    if (keep
                        && primaryMeasure != null
                        && numericPredicates.hasMeasure(primaryMeasure))
                    {
                        evaluator.setContext(primaryMeasure);
                        if (!numericPredicates.matches(
                            primaryMeasure,
                            evaluator.evaluateCurrent()))
                        {
                            keep = false;
                        }
                    }
                    if (keep) {
                        result.addCurrent(cursor);
                    }
                }
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "measure_nonempty_output",
                    result.size());
                return result;
            } finally {
                FilterExecutionMetrics.recordStageDurationNanos(
                    path,
                    "measure_nonempty_residual_numeric_conjunction",
                    System.nanoTime() - startNanos);
                evaluator.restore(savepoint);
            }
        }

        private TupleList filterByMeasuresNonEmpty(
            Evaluator evaluator,
            TupleList candidates,
            List<Member> measures,
            String path)
        {
            if (measures == null || measures.isEmpty()) {
                return candidates;
            }
            final int savepoint = evaluator.savepoint();
            final long startNanos = System.nanoTime();
            try {
                FilterExecutionMetrics.recordStageExecution(
                    path,
                    "measure_nonempty_residual");
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "measure_nonempty_input",
                    candidates.size());
                evaluator.setNonEmpty(false);
                final TupleList result =
                    candidates.cloneList(candidates.size() / 2);
                final TupleCursor cursor = candidates.tupleCursor();
                int currentIteration = 0;
                final Execution execution = evaluator.getQuery()
                    .getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    boolean keep = true;
                    for (Member measure : measures) {
                        evaluator.setContext(measure);
                        if (evaluator.evaluateCurrent() == null) {
                            keep = false;
                            break;
                        }
                    }
                    if (keep) {
                        result.addCurrent(cursor);
                    }
                }
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "measure_nonempty_output",
                    result.size());
                return result;
            } finally {
                FilterExecutionMetrics.recordStageDurationNanos(
                    path,
                    "measure_nonempty_residual",
                    System.nanoTime() - startNanos);
                evaluator.restore(savepoint);
            }
        }

        private TupleList filterByMeasureNullPredicates(
            Evaluator evaluator,
            TupleList candidates,
            MeasureNullPredicateConjunction nullPredicates,
            String path)
        {
            if (nullPredicates == null || nullPredicates.isEmpty()) {
                return candidates;
            }
            final int savepoint = evaluator.savepoint();
            final long startNanos = System.nanoTime();
            try {
                FilterExecutionMetrics.recordStageExecution(
                    path,
                    "residual_eval_measure_null");
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "residual_input",
                    candidates.size());
                evaluator.setNonEmpty(false);
                final TupleList result =
                    candidates.cloneList(candidates.size() / 2);
                final TupleCursor cursor = candidates.tupleCursor();
                int currentIteration = 0;
                final Execution execution = evaluator.getQuery()
                    .getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    boolean keep = true;
                    for (Member measure : nullPredicates.getMeasures()) {
                        evaluator.setContext(measure);
                        if (!nullPredicates.matches(
                            measure,
                            evaluator.evaluateCurrent()))
                        {
                            keep = false;
                            break;
                        }
                    }
                    if (keep) {
                        result.addCurrent(cursor);
                    }
                }
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "residual_output",
                    result.size());
                return result;
            } finally {
                FilterExecutionMetrics.recordStageDurationNanos(
                    path,
                    "residual_eval_measure_null",
                    System.nanoTime() - startNanos);
                evaluator.restore(savepoint);
            }
        }

        private TupleList filterByMeasureNullPredicatesDisjunction(
            Evaluator evaluator,
            TupleList candidates,
            MeasureNullPredicateDisjunction nullPredicates,
            String path)
        {
            if (nullPredicates == null || nullPredicates.isEmpty()) {
                return candidates;
            }
            final int savepoint = evaluator.savepoint();
            final long startNanos = System.nanoTime();
            try {
                FilterExecutionMetrics.recordStageExecution(
                    path,
                    "residual_eval_measure_null_or");
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "residual_input",
                    candidates.size());
                evaluator.setNonEmpty(false);
                final TupleList result =
                    candidates.cloneList(candidates.size() / 2);
                final TupleCursor cursor = candidates.tupleCursor();
                int currentIteration = 0;
                final Execution execution = evaluator.getQuery()
                    .getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    boolean keep = false;
                    for (Member measure : nullPredicates.getMeasures()) {
                        evaluator.setContext(measure);
                        if (nullPredicates.matches(
                            measure,
                            evaluator.evaluateCurrent()))
                        {
                            keep = true;
                            break;
                        }
                    }
                    if (keep) {
                        result.addCurrent(cursor);
                    }
                }
                FilterExecutionMetrics.recordStageTuples(
                    path,
                    "residual_output",
                    result.size());
                return result;
            } finally {
                FilterExecutionMetrics.recordStageDurationNanos(
                    path,
                    "residual_eval_measure_null_or",
                    System.nanoTime() - startNanos);
                evaluator.restore(savepoint);
            }
        }

        protected abstract TupleList makeList(Evaluator evaluator);

        public boolean dependsOn(Hierarchy hierarchy) {
            return anyDependsButFirst(getCalcs(), hierarchy);
        }
    }

    /**
     * Extracts [Measures].X from predicate NOT IsEmpty([Measures].X).
     */
    private static Member extractNotIsEmptyMeasure(Exp predicate) {
        if (!(predicate instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall notCall = (ResolvedFunCall) predicate;
        if (!"Not".equalsIgnoreCase(notCall.getFunName())
            || notCall.getArgCount() != 1)
        {
            return null;
        }
        final Exp isEmptyExp = notCall.getArg(0);
        if (!(isEmptyExp instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall isEmptyCall = (ResolvedFunCall) isEmptyExp;
        final String fn = isEmptyCall.getFunName();
        if (!"IsEmpty".equalsIgnoreCase(fn)
            && !"IS EMPTY".equalsIgnoreCase(fn))
        {
            return null;
        }
        if (isEmptyCall.getArgCount() != 1) {
            return null;
        }
        final Exp arg0 = isEmptyCall.getArg(0);
        if (!(arg0 instanceof MemberExpr)) {
            return null;
        }
        final Member member = ((MemberExpr) arg0).getMember();
        // Keep this optimization conservative: calculated measures stay on
        // fallback path to preserve evaluation semantics.
        return member != null
            && member.isMeasure()
            && !member.isCalculated()
            ? member
            : null;
    }

    /**
     * Analyzes filter predicate for patterns that can be pre-pruned by
     * evaluator non-empty context.
     *
     * <p>Exact means the whole predicate is exactly NOT IsEmpty([Measures].M).
     * Non-exact means predicate is a conjunction that contains such atom.
     */
    private static NonEmptyPredicateAnalysis analyzeNonEmptyPredicate(
        ResolvedFunCall filterCall)
    {
        if (filterCall.getArgCount() < 2) {
            return null;
        }
        return analyzeNonEmptyPredicateExp(filterCall.getArg(1));
    }

    private static NonEmptyPredicateAnalysis analyzeNonEmptyPredicateExp(
        Exp predicate)
    {
        final Member directMeasure = extractNotIsEmptyMeasure(predicate);
        if (directMeasure != null) {
            return new NonEmptyPredicateAnalysis(
                Collections.singletonList(directMeasure),
                true);
        }
        if (!(predicate instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall call = (ResolvedFunCall) predicate;
        if (!isAndCall(call)) {
            return null;
        }
        final List<Member> measures = new ArrayList<Member>();
        boolean exact = true;
        for (Exp arg : call.getArgs()) {
            final NonEmptyPredicateAnalysis nested =
                analyzeNonEmptyPredicateExp(arg);
            if (nested == null) {
                exact = false;
                continue;
            }
            if (!nested.getMeasures().isEmpty()) {
                for (Member m : nested.getMeasures()) {
                    if (!measures.contains(m)) {
                        measures.add(m);
                    }
                }
            } else {
                exact = false;
            }
            if (!nested.isExact()) {
                exact = false;
            }
        }
        return measures.isEmpty()
            ? null
            : new NonEmptyPredicateAnalysis(measures, exact);
    }

    private static boolean isAndCall(ResolvedFunCall call) {
        final String name = call.getFunName();
        return "AND".equalsIgnoreCase(name) || "And".equalsIgnoreCase(name);
    }

    private static boolean isOrCall(ResolvedFunCall call) {
        final String name = call.getFunName();
        return "OR".equalsIgnoreCase(name) || "Or".equalsIgnoreCase(name);
    }

    private static final class NonEmptyPredicateAnalysis {
        private final List<Member> measures;
        private final boolean exact;

        private NonEmptyPredicateAnalysis(List<Member> measures, boolean exact) {
            this.measures = measures == null
                ? Collections.<Member>emptyList()
                : Collections.unmodifiableList(new ArrayList<Member>(measures));
            this.exact = exact;
        }

        private Member getPrimaryMeasure() {
            return measures.isEmpty() ? null : measures.get(0);
        }

        private List<Member> getAdditionalMeasures() {
            if (measures.size() <= 1) {
                return Collections.emptyList();
            }
            return measures.subList(1, measures.size());
        }

        private List<Member> getMeasures() {
            return measures;
        }

        private boolean isExact() {
            return exact;
        }
    }

    private static boolean requiresContextAwareMeasurePass(
        Evaluator evaluator)
    {
        if (!(evaluator instanceof RolapEvaluator)) {
            return false;
        }
        final RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
        final Set<Hierarchy> slicerHierarchies = new HashSet<Hierarchy>();
        for (Member slicerMember : rolapEvaluator.getSlicerMembers()) {
            if (slicerMember != null
                && !slicerMember.isMeasure()
                && !slicerMember.isAll())
            {
                slicerHierarchies.add(slicerMember.getHierarchy());
            }
        }
        for (Member member : evaluator.getNonAllMembers()) {
            if (member == null
                || member.isMeasure()
                || member.isAll()
                || slicerHierarchies.contains(member.getHierarchy()))
            {
                continue;
            }
            return true;
        }
        return false;
    }

    /**
     * If the NOT-IsEmpty predicate references a measure that can be resolved
     * to a stored measure from a single base cube, push that stored measure
     * into the evaluator context.  This replaces a calculated measure (whose
     * formula may reference dimension members that cause false-positive
     * conflicts in {@code measuresConflictWithMembers}) with a simple stored
     * measure that has no embedded dimension references.
     *
     * <p>The caller is responsible for saving and restoring the evaluator
     * via {@link Evaluator#savepoint()} / {@link Evaluator#restore(int)}.
     */
    private static void pushCarrierMeasure(
        Evaluator evaluator,
        NonEmptyPredicateAnalysis nonEmptyAnalysis)
    {
        final Member primaryMeasure = nonEmptyAnalysis.getPrimaryMeasure();
        if (primaryMeasure == null) {
            return;
        }
        final RolapStoredMeasure carrierMeasure =
            SqlConstraintUtils.resolveStoredMeasureCarrier(primaryMeasure);
        if (carrierMeasure != null) {
            evaluator.setContext(carrierMeasure);
        }
    }

    /**
     * For non-exact AND predicates, compile boolean residual without already
     * handled NOT IsEmpty([Measures].M) atoms.
     */
    private static Exp preparePredicateForBooleanEvaluation(
        ResolvedFunCall filterCall)
    {
        if (filterCall == null || filterCall.getArgCount() < 2) {
            return filterCall == null ? null : filterCall.getArg(1);
        }
        final Exp predicate = filterCall.getArg(1);
        final NonEmptyPredicateAnalysis analysis =
            analyzeNonEmptyPredicate(filterCall);
        if (analysis == null
            || analysis.isExact()
            || analysis.getMeasures().isEmpty())
        {
            return predicate;
        }
        final Set<Member> handledMeasures =
            new HashSet<Member>(analysis.getMeasures());
        if (containsIsEmptyOnMeasures(predicate, handledMeasures)) {
            return predicate;
        }
        final Exp residual =
            stripHandledNotIsEmptyFromAnd(predicate, handledMeasures);
        return residual == null ? predicate : residual;
    }

    private static boolean containsIsEmptyOnMeasures(
        Exp exp,
        Set<Member> measures)
    {
        if (exp == null || measures == null || measures.isEmpty()) {
            return false;
        }
        final Member isEmptyMeasure = extractIsEmptyMeasure(exp);
        if (isEmptyMeasure != null && measures.contains(isEmptyMeasure)) {
            return true;
        }
        if (!(exp instanceof ResolvedFunCall)) {
            return false;
        }
        final ResolvedFunCall call = (ResolvedFunCall) exp;
        for (Exp arg : call.getArgs()) {
            if (containsIsEmptyOnMeasures(arg, measures)) {
                return true;
            }
        }
        return false;
    }

    private static Exp stripHandledNotIsEmptyFromAnd(
        Exp exp,
        Set<Member> handledMeasures)
    {
        if (exp == null || handledMeasures == null || handledMeasures.isEmpty()) {
            return exp;
        }
        final Member atomMeasure = extractNotIsEmptyMeasure(exp);
        if (atomMeasure != null && handledMeasures.contains(atomMeasure)) {
            return null;
        }
        if (!(exp instanceof ResolvedFunCall)) {
            return exp;
        }
        final ResolvedFunCall call = (ResolvedFunCall) exp;
        if (!isAndCall(call)) {
            return exp;
        }
        final List<Exp> keptArgs = new ArrayList<Exp>(call.getArgCount());
        boolean changed = false;
        for (Exp arg : call.getArgs()) {
            final Exp kept = stripHandledNotIsEmptyFromAnd(
                arg,
                handledMeasures);
            if (kept == null) {
                changed = true;
                continue;
            }
            if (kept != arg) {
                changed = true;
            }
            keptArgs.add(kept);
        }
        if (keptArgs.isEmpty()) {
            return null;
        }
        if (!changed) {
            return exp;
        }
        if (keptArgs.size() == 1) {
            return keptArgs.get(0);
        }
        return new ResolvedFunCall(
            call.getFunDef(),
            keptArgs.toArray(new Exp[keptArgs.size()]),
            call.getType());
    }

    private static void maybeLogFilterFastPathFallback(
        Evaluator evaluator,
        ResolvedFunCall call,
        Exp predicate,
        String precomputedReason)
    {
        if (!LOGGER.isWarnEnabled()) {
            return;
        }
        final String reason =
            precomputedReason == null
                ? determineFilterFastPathFallbackReason(predicate)
                : precomputedReason;
        if (reason == null) {
            return;
        }
        final String shape = predicateShape(predicate, 0);
        if (!shouldLogFilterFastPathFallback(evaluator, reason, shape)) {
            return;
        }
        LOGGER.warn(
            "Filter fast-path fallback: reason={}, function={}, predicateShape={}",
            reason,
            call == null ? "Filter" : call.getFunName(),
            shape);
    }

    private static void recordFilterPath(
        Evaluator evaluator,
        ResolvedFunCall call,
        Exp predicate,
        String path,
        String reason)
    {
        final String reasonCode =
            reason == null || reason.length() == 0
                ? "none"
                : reason;
        FilterPathMetrics.recordPathSelection(path, reasonCode);
        if (!LOGGER.isInfoEnabled()) {
            return;
        }
        final String shape = predicateShape(predicate, 0);
        if (!shouldLogFilterPathSelection(evaluator, path, reasonCode, shape)) {
            return;
        }
        LOGGER.info(
            "Filter path selected: path={}, reason={}, function={}, predicateShape={}",
            path,
            reasonCode,
            call == null ? "Filter" : call.getFunName(),
            shape);
    }

    private static boolean shouldLogFilterPathSelection(
        Evaluator evaluator,
        String path,
        String reasonCode,
        String shape)
    {
        if (evaluator == null || evaluator.getQuery() == null) {
            return true;
        }
        final Query query = evaluator.getQuery();
        final String key =
            String.valueOf(path)
                + '|'
                + String.valueOf(reasonCode)
                + '|'
                + String.valueOf(shape);
        synchronized (LOGGED_FILTER_PATH_EVENTS_BY_QUERY) {
            Set<String> keys = LOGGED_FILTER_PATH_EVENTS_BY_QUERY.get(query);
            if (keys == null) {
                keys = new HashSet<String>();
                LOGGED_FILTER_PATH_EVENTS_BY_QUERY.put(query, keys);
            }
            return keys.add(key);
        }
    }

    private static String determineFilterFastPathFallbackReason(Exp predicate) {
        if (containsNotIsEmptyOnCalculatedMeasure(predicate)) {
            return "calculated_measure_nonempty_atom";
        }
        if (containsUnsupportedBooleanForm(predicate)) {
            return "unsupported_boolean_form";
        }
        return null;
    }

    private static boolean shouldLogFilterFastPathFallback(
        Evaluator evaluator,
        String reason,
        String shape)
    {
        if (evaluator == null || evaluator.getQuery() == null) {
            return true;
        }
        final Query query = evaluator.getQuery();
        final String key =
            String.valueOf(reason) + '|' + String.valueOf(shape);
        synchronized (LOGGED_FILTER_FALLBACK_EVENTS_BY_QUERY) {
            Set<String> keys =
                LOGGED_FILTER_FALLBACK_EVENTS_BY_QUERY.get(query);
            if (keys == null) {
                keys = new HashSet<String>();
                LOGGED_FILTER_FALLBACK_EVENTS_BY_QUERY.put(query, keys);
            }
            return keys.add(key);
        }
    }

    private static boolean containsNotIsEmptyOnCalculatedMeasure(Exp exp) {
        if (!(exp instanceof ResolvedFunCall)) {
            return false;
        }
        final ResolvedFunCall call = (ResolvedFunCall) exp;
        if ("Not".equalsIgnoreCase(call.getFunName())
            && call.getArgCount() == 1)
        {
            final Exp inner = call.getArg(0);
            if (inner instanceof ResolvedFunCall) {
                final ResolvedFunCall innerCall = (ResolvedFunCall) inner;
                final String innerFn = innerCall.getFunName();
                if (("IsEmpty".equalsIgnoreCase(innerFn)
                    || "IS EMPTY".equalsIgnoreCase(innerFn))
                    && innerCall.getArgCount() == 1
                    && innerCall.getArg(0) instanceof MemberExpr)
                {
                    final Member m =
                        ((MemberExpr) innerCall.getArg(0)).getMember();
                    return m != null && m.isMeasure() && m.isCalculated();
                }
            }
        }
        for (Exp arg : call.getArgs()) {
            if (containsNotIsEmptyOnCalculatedMeasure(arg)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsUnsupportedBooleanForm(Exp exp) {
        if (!(exp instanceof ResolvedFunCall)) {
            return false;
        }
        final ResolvedFunCall call = (ResolvedFunCall) exp;
        final String fn = call.getFunName();
        if ("OR".equalsIgnoreCase(fn)) {
            return analyzeMeasureNullOrPredicate(exp) == null;
        }
        if ("IIF".equalsIgnoreCase(fn)
            || "CASE".equalsIgnoreCase(fn))
        {
            return true;
        }
        if ("NOT".equalsIgnoreCase(fn)) {
            return extractNotIsEmptyMeasure(call) == null;
        }
        for (Exp arg : call.getArgs()) {
            if (containsUnsupportedBooleanForm(arg)) {
                return true;
            }
        }
        return false;
    }

    private static String predicateShape(Exp exp, int depth) {
        if (exp == null) {
            return "<null>";
        }
        if (depth >= 3) {
            return "...";
        }
        if (exp instanceof ResolvedFunCall) {
            final ResolvedFunCall call = (ResolvedFunCall) exp;
            final StringBuilder sb = new StringBuilder();
            sb.append(call.getFunName()).append('(');
            final Exp[] args = call.getArgs();
            for (int i = 0; i < args.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(predicateShape(args[i], depth + 1));
            }
            sb.append(')');
            return sb.toString();
        }
        if (exp instanceof MemberExpr) {
            return "MemberExpr";
        }
        return exp.getClass().getSimpleName();
    }

    private enum NumericComparisonOp {
        GT,
        GE,
        LT,
        LE,
        EQ,
        NE
    }

    private static final class NumericMeasurePredicate {
        private final Member measure;
        private final NumericComparisonOp op;
        private final double literalValue;

        private NumericMeasurePredicate(
            Member measure,
            NumericComparisonOp op,
            double literalValue)
        {
            this.measure = measure;
            this.op = op;
            this.literalValue = literalValue;
        }

        private boolean matches(Object value) {
            final Double numericValue = toDouble(value);
            if (numericValue == null) {
                return false;
            }
            final double v = numericValue.doubleValue();
            switch (op) {
            case GT:
                return v > literalValue;
            case GE:
                return v >= literalValue;
            case LT:
                return v < literalValue;
            case LE:
                return v <= literalValue;
            case EQ:
                return v == literalValue;
            case NE:
                return v != literalValue;
            default:
                return false;
            }
        }
    }

    private static final class MeasureNullPredicate {
        private final Member measure;
        private final boolean expectNull;

        private MeasureNullPredicate(Member measure, boolean expectNull) {
            this.measure = measure;
            this.expectNull = expectNull;
        }
    }

    private static final class MeasureNullPredicateConjunction {
        private final Map<Member, Boolean> expectationsByMeasure =
            new HashMap<Member, Boolean>();
        private boolean unsatisfiable;

        private void addPredicate(MeasureNullPredicate predicate) {
            final Boolean existing =
                expectationsByMeasure.get(predicate.measure);
            if (existing != null
                && existing.booleanValue() != predicate.expectNull)
            {
                unsatisfiable = true;
                return;
            }
            expectationsByMeasure.put(
                predicate.measure,
                Boolean.valueOf(predicate.expectNull));
        }

        private void addAll(MeasureNullPredicateConjunction other) {
            if (other.unsatisfiable) {
                unsatisfiable = true;
                return;
            }
            for (Map.Entry<Member, Boolean> entry
                : other.expectationsByMeasure.entrySet())
            {
                addPredicate(
                    new MeasureNullPredicate(
                        entry.getKey(),
                        entry.getValue().booleanValue()));
            }
        }

        private boolean matches(Member measure, Object value) {
            final Boolean expectNull = expectationsByMeasure.get(measure);
            if (expectNull == null) {
                return true;
            }
            return expectNull.booleanValue()
                ? value == null
                : value != null;
        }

        private Set<Member> getMeasures() {
            return expectationsByMeasure.keySet();
        }

        private boolean isEmpty() {
            return expectationsByMeasure.isEmpty() && !unsatisfiable;
        }

        private boolean isUnsatisfiable() {
            return unsatisfiable;
        }
    }

    private static final class MeasureNullPredicateDisjunction {
        private static final int FLAG_EXPECT_NULL = 1;
        private static final int FLAG_EXPECT_NON_NULL = 2;

        private final Map<Member, Integer> expectationsByMeasure =
            new HashMap<Member, Integer>();
        private boolean tautology;

        private void addPredicate(MeasureNullPredicate predicate) {
            addExpectation(
                predicate.measure,
                predicate.expectNull
                    ? FLAG_EXPECT_NULL
                    : FLAG_EXPECT_NON_NULL);
        }

        private void addAll(MeasureNullPredicateDisjunction other) {
            if (other == null) {
                return;
            }
            if (other.tautology) {
                tautology = true;
            }
            for (Map.Entry<Member, Integer> entry
                : other.expectationsByMeasure.entrySet())
            {
                addExpectation(
                    entry.getKey(),
                    entry.getValue().intValue());
            }
        }

        private void addExpectation(Member measure, int expectedFlags) {
            final Integer existing = expectationsByMeasure.get(measure);
            final int combinedFlags =
                (existing == null ? 0 : existing.intValue()) | expectedFlags;
            expectationsByMeasure.put(
                measure,
                Integer.valueOf(combinedFlags));
            if ((combinedFlags & FLAG_EXPECT_NULL) != 0
                && (combinedFlags & FLAG_EXPECT_NON_NULL) != 0)
            {
                tautology = true;
            }
        }

        private boolean matches(Member measure, Object value) {
            final Integer expectedFlags = expectationsByMeasure.get(measure);
            if (expectedFlags == null) {
                return false;
            }
            final int flags = expectedFlags.intValue();
            if (value == null) {
                return (flags & FLAG_EXPECT_NULL) != 0;
            }
            return (flags & FLAG_EXPECT_NON_NULL) != 0;
        }

        private Set<Member> getMeasures() {
            return expectationsByMeasure.keySet();
        }

        private boolean isEmpty() {
            return expectationsByMeasure.isEmpty() && !tautology;
        }

        private boolean isTautology() {
            return tautology;
        }
    }

    private static MeasureNullPredicateConjunction
    extractMeasureNullPredicateConjunction(Exp predicate)
    {
        if (!(predicate instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall call = (ResolvedFunCall) predicate;
        if (isAndCall(call)) {
            final MeasureNullPredicateConjunction conjunction =
                new MeasureNullPredicateConjunction();
            for (Exp arg : call.getArgs()) {
                final MeasureNullPredicateConjunction nested =
                    extractMeasureNullPredicateConjunction(arg);
                if (nested == null) {
                    return null;
                }
                conjunction.addAll(nested);
            }
            return conjunction;
        }
        final MeasureNullPredicate atom =
            extractMeasureNullPredicate(predicate);
        if (atom == null) {
            return null;
        }
        final MeasureNullPredicateConjunction conjunction =
            new MeasureNullPredicateConjunction();
        conjunction.addPredicate(atom);
        return conjunction;
    }

    private static MeasureNullPredicateDisjunction
    analyzeMeasureNullOrPredicate(Exp predicate)
    {
        if (!(predicate instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall call = (ResolvedFunCall) predicate;
        if (!isOrCall(call)) {
            return null;
        }
        return extractMeasureNullPredicateDisjunction(predicate);
    }

    private static MeasureNullPredicateDisjunction
    extractMeasureNullPredicateDisjunction(Exp predicate)
    {
        if (!(predicate instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall call = (ResolvedFunCall) predicate;
        if (isOrCall(call)) {
            final MeasureNullPredicateDisjunction disjunction =
                new MeasureNullPredicateDisjunction();
            for (Exp arg : call.getArgs()) {
                final MeasureNullPredicateDisjunction nested =
                    extractMeasureNullPredicateDisjunction(arg);
                if (nested == null) {
                    return null;
                }
                disjunction.addAll(nested);
            }
            return disjunction;
        }
        final MeasureNullPredicate atom =
            extractMeasureNullPredicate(predicate);
        if (atom == null) {
            return null;
        }
        final MeasureNullPredicateDisjunction disjunction =
            new MeasureNullPredicateDisjunction();
        disjunction.addPredicate(atom);
        return disjunction;
    }

    private static MeasureNullPredicate extractMeasureNullPredicate(Exp exp) {
        final Member isEmptyMeasure = extractIsEmptyMeasure(exp);
        if (isEmptyMeasure != null) {
            return new MeasureNullPredicate(isEmptyMeasure, true);
        }
        final Member notIsEmptyMeasure = extractNotIsEmptyMeasure(exp);
        if (notIsEmptyMeasure != null) {
            return new MeasureNullPredicate(notIsEmptyMeasure, false);
        }
        return null;
    }

    private static Member extractIsEmptyMeasure(Exp exp) {
        if (!(exp instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall call = (ResolvedFunCall) exp;
        final String fn = call.getFunName();
        if (!"IsEmpty".equalsIgnoreCase(fn)
            && !"IS EMPTY".equalsIgnoreCase(fn))
        {
            return null;
        }
        if (call.getArgCount() != 1) {
            return null;
        }
        return extractStoredMeasure(call.getArg(0));
    }

    private static final class NumericMeasurePredicateConjunction {
        private final Map<Member, List<NumericMeasurePredicate>> predicatesByMeasure =
            new HashMap<Member, List<NumericMeasurePredicate>>();
        private int count;

        private void addPredicate(NumericMeasurePredicate predicate) {
            List<NumericMeasurePredicate> predicates =
                predicatesByMeasure.get(predicate.measure);
            if (predicates == null) {
                predicates = new ArrayList<NumericMeasurePredicate>();
                predicatesByMeasure.put(predicate.measure, predicates);
            }
            predicates.add(predicate);
            count++;
        }

        private void addAll(NumericMeasurePredicateConjunction other) {
            for (List<NumericMeasurePredicate> predicates
                : other.predicatesByMeasure.values())
            {
                for (NumericMeasurePredicate predicate : predicates) {
                    addPredicate(predicate);
                }
            }
        }

        private boolean matches(Member measure, Object value) {
            final List<NumericMeasurePredicate> predicates =
                predicatesByMeasure.get(measure);
            if (predicates == null || predicates.isEmpty()) {
                return true;
            }
            for (NumericMeasurePredicate predicate : predicates) {
                if (!predicate.matches(value)) {
                    return false;
                }
            }
            return true;
        }

        private boolean hasMeasure(Member measure) {
            return predicatesByMeasure.containsKey(measure);
        }

        private Set<Member> getMeasures() {
            return predicatesByMeasure.keySet();
        }

        private boolean isEmpty() {
            return count == 0;
        }

        private NumericMeasurePredicate getSinglePredicate() {
            if (count != 1) {
                return null;
            }
            return predicatesByMeasure.values().iterator().next().get(0);
        }
    }

    private static NumericMeasurePredicateConjunction
    extractNumericMeasurePredicateConjunction(Exp predicate)
    {
        if (!(predicate instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall call = (ResolvedFunCall) predicate;
        if (isAndCall(call)) {
            final NumericMeasurePredicateConjunction conjunction =
                new NumericMeasurePredicateConjunction();
            for (Exp arg : call.getArgs()) {
                final NumericMeasurePredicateConjunction nested =
                    extractNumericMeasurePredicateConjunction(arg);
                if (nested == null || nested.isEmpty()) {
                    return null;
                }
                conjunction.addAll(nested);
            }
            return conjunction.isEmpty() ? null : conjunction;
        }
        final NumericMeasurePredicate atom =
            extractNumericMeasurePredicate(predicate);
        if (atom == null) {
            return null;
        }
        final NumericMeasurePredicateConjunction conjunction =
            new NumericMeasurePredicateConjunction();
        conjunction.addPredicate(atom);
        return conjunction;
    }

    private static NumericMeasurePredicate extractNumericMeasurePredicate(
        Exp predicate)
    {
        if (!(predicate instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall call = (ResolvedFunCall) predicate;
        if (isAndCall(call)) {
            return null;
        }
        final NumericComparisonOp op = parseNumericComparisonOp(call);
        if (op == null || call.getArgCount() != 2) {
            return null;
        }
        final Member leftMeasure = extractStoredMeasure(call.getArg(0));
        final Member rightMeasure = extractStoredMeasure(call.getArg(1));
        final Double leftLiteral = extractNumericLiteral(call.getArg(0));
        final Double rightLiteral = extractNumericLiteral(call.getArg(1));

        if (leftMeasure != null && rightLiteral != null) {
            return new NumericMeasurePredicate(
                leftMeasure,
                op,
                rightLiteral.doubleValue());
        }
        if (rightMeasure != null && leftLiteral != null) {
            return new NumericMeasurePredicate(
                rightMeasure,
                invertComparison(op),
                leftLiteral.doubleValue());
        }
        return null;
    }

    private static NumericComparisonOp parseNumericComparisonOp(
        ResolvedFunCall call)
    {
        final String fn = call.getFunName();
        if (fn == null) {
            return null;
        }
        if (">".equals(fn) || "GT".equalsIgnoreCase(fn)) {
            return NumericComparisonOp.GT;
        }
        if (">=".equals(fn) || "GE".equalsIgnoreCase(fn)) {
            return NumericComparisonOp.GE;
        }
        if ("<".equals(fn) || "LT".equalsIgnoreCase(fn)) {
            return NumericComparisonOp.LT;
        }
        if ("<=".equals(fn) || "LE".equalsIgnoreCase(fn)) {
            return NumericComparisonOp.LE;
        }
        if ("=".equals(fn) || "EQ".equalsIgnoreCase(fn)) {
            return NumericComparisonOp.EQ;
        }
        if ("<>".equals(fn) || "!=".equals(fn) || "NE".equalsIgnoreCase(fn)) {
            return NumericComparisonOp.NE;
        }
        return null;
    }

    private static NumericComparisonOp invertComparison(NumericComparisonOp op) {
        switch (op) {
        case GT:
            return NumericComparisonOp.LT;
        case GE:
            return NumericComparisonOp.LE;
        case LT:
            return NumericComparisonOp.GT;
        case LE:
            return NumericComparisonOp.GE;
        default:
            return op;
        }
    }

    private static Member extractStoredMeasure(Exp exp) {
        if (!(exp instanceof MemberExpr)) {
            return null;
        }
        final Member member = ((MemberExpr) exp).getMember();
        if (member == null
            || !member.isMeasure()
            || member.isCalculated())
        {
            return null;
        }
        return member;
    }

    private static Double extractNumericLiteral(Exp exp) {
        if (!(exp instanceof Literal)) {
            return null;
        }
        final Object value = ((Literal) exp).getValue();
        return toDouble(value);
    }

    private static Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            try {
                return Double.valueOf((String) value);
            } catch (NumberFormatException ignore) {
                return null;
            }
        }
        return null;
    }

    private static class MutableListCalc extends BaseListCalc {
        MutableListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleList makeList(Evaluator evaluator) {
            Calc[] calcs = getCalcs();
            ListCalc lcalc = (ListCalc) calcs[0];
            BooleanCalc bcalc = (BooleanCalc) calcs[1];
            TupleList members0 = lcalc.evaluateList(evaluator);

            // make list mutable;
            // for capacity planning, guess selectivity = .5
            TupleList result = members0.cloneList(members0.size() / 2);
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(false);
                final TupleCursor cursor = members0.tupleCursor();
                int currentIteration = 0;
                Execution execution =
                    evaluator.getQuery().getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
            }
        }
    }

    private static class ImmutableListCalc extends BaseListCalc {
        ImmutableListCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
            assert calcs[0] instanceof ListCalc;
            assert calcs[1] instanceof BooleanCalc;
        }

        protected TupleList makeList(Evaluator evaluator) {
            evaluator.getTiming().markStart(TIMING_NAME);
            final int savepoint = evaluator.savepoint();
            try {
                Calc[] calcs = getCalcs();
                ListCalc lcalc = (ListCalc) calcs[0];
                BooleanCalc bcalc = (BooleanCalc) calcs[1];
                TupleList members0 = lcalc.evaluateList(evaluator);

                // Not mutable, must create new list;
                // for capacity planning, guess selectivity = .5
                TupleList result = members0.cloneList(members0.size() / 2);
                evaluator.setNonEmpty(false);
                final TupleCursor cursor = members0.tupleCursor();
                int currentIteration = 0;
                Execution execution = evaluator.getQuery()
                    .getStatement().getCurrentExecution();
                while (cursor.forward()) {
                    CancellationChecker.checkCancelOrTimeout(
                        currentIteration++, execution);
                    cursor.setContext(evaluator);
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
                evaluator.restore(savepoint);
                evaluator.getTiming().markEnd(TIMING_NAME);
            }
        }
    }
}

// End FilterFunDef.java
