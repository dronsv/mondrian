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
import mondrian.metrics.FilterPathMetrics;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.util.CancellationChecker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
    private static final String PATH_FAST_AND_NOT_ISEMPTY =
        "fastpath.and_contains_not_isempty";
    private static final String PATH_NATIVE_EVALUATOR =
        "native.set_evaluator";
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
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
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
                if (nonEmptyAnalysis != null && nonEmptyAnalysis.isExact()) {
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_FAST_EXACT_NOT_ISEMPTY,
                        "exact_not_isempty");
                    return evaluateBySetNonEmpty(
                        evaluator,
                        nonEmptyAnalysis.getMeasure());
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
                if (nonEmptyAnalysis != null) {
                    recordFilterPath(
                        evaluator,
                        call,
                        call.getArg(1),
                        PATH_FAST_AND_NOT_ISEMPTY,
                        "and_contains_not_isempty");
                    return filterByPredicate(
                        evaluator,
                        evaluateBySetNonEmpty(
                            evaluator,
                            nonEmptyAnalysis.getMeasure()),
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
        BooleanCalc bcalc = compiler.compileBoolean(call.getArg(1));
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
            if (nonEmptyAnalysis != null && nonEmptyAnalysis.isExact()) {
                recordFilterPath(
                    evaluator,
                    call,
                    call.getArg(1),
                    PATH_FAST_EXACT_NOT_ISEMPTY,
                    "exact_not_isempty");
                return evaluateBySetNonEmpty(
                    evaluator,
                    nonEmptyAnalysis.getMeasure());
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
            if (nonEmptyAnalysis != null) {
                recordFilterPath(
                    evaluator,
                    call,
                    call.getArg(1),
                    PATH_FAST_AND_NOT_ISEMPTY,
                    "and_contains_not_isempty");
                return filterByPredicate(
                    evaluator,
                    evaluateBySetNonEmpty(
                        evaluator,
                        nonEmptyAnalysis.getMeasure()),
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
            return makeList(evaluator);
        }

        /**
         * Fast path for common pattern:
         * Filter(setExpr, NOT IsEmpty([Measures].X)).
         */
        private TupleList evaluateBySetNonEmpty(
            Evaluator evaluator,
            Member nonEmptyMeasure)
        {
            final int savepoint = evaluator.savepoint();
            try {
                evaluator.setNonEmpty(true);
                evaluator.setContext(nonEmptyMeasure);
                ListCalc lcalc = (ListCalc) getCalcs()[0];
                return lcalc.evaluateList(evaluator);
            } finally {
                evaluator.restore(savepoint);
            }
        }

        private TupleList filterByPredicate(
            Evaluator evaluator,
            TupleList candidates,
            BooleanCalc bcalc)
        {
            final int savepoint = evaluator.savepoint();
            try {
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
                    if (bcalc.evaluateBoolean(evaluator)) {
                        result.addCurrent(cursor);
                    }
                }
                return result;
            } finally {
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
            return new NonEmptyPredicateAnalysis(directMeasure, true);
        }
        if (!(predicate instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall call = (ResolvedFunCall) predicate;
        if (!isAndCall(call)) {
            return null;
        }
        Member selectedMeasure = null;
        for (Exp arg : call.getArgs()) {
            final NonEmptyPredicateAnalysis nested =
                analyzeNonEmptyPredicateExp(arg);
            if (nested != null && nested.getMeasure() != null) {
                selectedMeasure = nested.getMeasure();
                break;
            }
        }
        return selectedMeasure == null
            ? null
            : new NonEmptyPredicateAnalysis(selectedMeasure, false);
    }

    private static boolean isAndCall(ResolvedFunCall call) {
        final String name = call.getFunName();
        return "AND".equalsIgnoreCase(name) || "And".equalsIgnoreCase(name);
    }

    private static final class NonEmptyPredicateAnalysis {
        private final Member measure;
        private final boolean exact;

        private NonEmptyPredicateAnalysis(Member measure, boolean exact) {
            this.measure = measure;
            this.exact = exact;
        }

        private Member getMeasure() {
            return measure;
        }

        private boolean isExact() {
            return exact;
        }
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
        if ("OR".equalsIgnoreCase(fn)
            || "IIF".equalsIgnoreCase(fn)
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
