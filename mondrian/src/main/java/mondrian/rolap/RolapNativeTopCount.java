/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.MemberExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.type.ScalarType;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.*;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

/**
 * Computes a TopCount in SQL.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class RolapNativeTopCount extends RolapNativeSet {

    public RolapNativeTopCount() {
        super.setEnabled(
            MondrianProperties.instance().EnableNativeTopCount.get());
    }

    static class TopCountConstraint extends SetConstraint {
        Exp orderByExpr;
        boolean ascending;
        Integer topCount;

        public TopCountConstraint(
            int count,
            CrossJoinArg[] args, RolapEvaluator evaluator,
            Exp orderByExpr, boolean ascending)
        {
            // An explicit ranking selects its own scalar measure context.
            super(args, evaluator, true, orderByExpr == null
                ? CellReadAnalysis.Judges.AXIS : CellReadAnalysis.Judges.CONTEXT);
            this.orderByExpr = orderByExpr;
            this.ascending = ascending;
            this.topCount = new Integer(count);
        }

        /**
         * If the orderByExpr is not present than we're dealing with
         * the 2 arg form of TC.  The 2 arg form cannot be evaluated
         * with a join to the fact.  Because of this, it's only valid
         * to apply a native constraint for the 2 arg form if a single CJ
         * arg is in place.  Otherwise we'd need to join the dims together
         * via the fact table, which could eliminate tuples that should
         * be returned.
         */
        protected boolean isValid() {
            if (orderByExpr == null) {
                return args.length == 1
                    && canApplyCrossJoinArgConstraint(args[0]);
            }
            return true;
        }

        /**
         * {@inheritDoc}
         *
         * <p>TopCount needs to join the fact table if a top count expression
         * is present (i.e. orderByExpr).  If not present than the results of
         * TC should be the natural ordering of the set in the first argument,
         * which cannot use a join to the fact table without potentially
         * eliminating empty tuples.
         */
        @Override
        protected boolean isJoinRequired() {
            return orderByExpr != null;
        }

        @Override
        public boolean supportsAggTables() {
            // We can only safely use agg tables if we can limit
            // results to those with data (i.e. if we would be
            // joining to a fact table).
            return isJoinRequired();
        }

        @Override
        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            assert isValid();
            if (orderByExpr != null) {
                RolapNativeSql sql =
                    new RolapNativeSql(
                        sqlQuery, aggStar, getEvaluator(), null);
                final String orderBySql =
                    sql.generateTopCountOrderBy(orderByExpr);
                boolean nullable =
                    deduceNullability(orderByExpr);
                final String orderByAlias =
                    sqlQuery.addSelect(orderBySql, null);
                sqlQuery.addOrderBy(
                    orderBySql,
                    orderByAlias,
                    ascending,
                    true,
                    nullable,
                    true);
            }
            if (isJoinRequired()) {
                super.addConstraint(sqlQuery, baseCube, aggStar);
            } else if (args.length == 1) {
                args[0].addConstraint(sqlQuery, baseCube, null);
            }
            addRowLimitIfPossible(sqlQuery);
        }

        private void addRowLimitIfPossible(SqlQuery sqlQuery) {
            final String skipReason =
                getRowLimitPushdownSkipReason(
                    topCount == null ? 0 : topCount.intValue(),
                    sqlQuery.getDialect().requiresDrillthroughMaxRowsInLimit(),
                    isVirtualCubeQuery());
            if (skipReason == null) {
                sqlQuery.addRowLimit(topCount.intValue());
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "Native TopCount SQL LIMIT pushdown skipped: {}",
                    skipReason);
            }
        }

        static String getRowLimitPushdownSkipReason(
            int topCount,
            boolean dialectRequiresLimitClause,
            boolean virtualCubeQuery)
        {
            if (topCount <= 0) {
                return "count<=0";
            }
            if (!dialectRequiresLimitClause) {
                return "dialect-limit-clause-disabled";
            }
            if (virtualCubeQuery) {
                return "virtual-cube-union-path";
            }
            return null;
        }

        private boolean isVirtualCubeQuery() {
            if (getEvaluator() == null
                || !(getEvaluator().getCube() instanceof RolapCube))
            {
                return false;
            }
            return ((RolapCube) getEvaluator().getCube()).isVirtual();
        }

        private boolean deduceNullability(Exp expr) {
            if (!(expr instanceof MemberExpr)) {
                return true;
            }
            final MemberExpr memberExpr = (MemberExpr) expr;
            if (!(memberExpr.getMember() instanceof RolapStoredMeasure)) {
                return true;
            }
            final RolapStoredMeasure measure =
                (RolapStoredMeasure) memberExpr.getMember();
            return measure.getAggregator() != RolapAggregator.DistinctCount;
        }

        @Override
        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            key.add(super.getCacheKey());
            // Note: need to use string in order for caching to work
            if (orderByExpr != null) {
                key.add(orderByExpr.toString());
            }
            key.add(ascending);
            key.add(topCount);
            key.add(this.getEvaluator().isNonEmpty());

            if (this.getEvaluator() instanceof RolapEvaluator) {
                key.add(
                    ((RolapEvaluator)this.getEvaluator())
                        .getSlicerMembers());
            }
            return key;
        }
    }

    @Override
    protected boolean restrictMemberTypes() {
        return true;
    }

    @Override
    NativeEvaluator createEvaluator(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        // #86: the measure-member-conflict part of the context check is
        // deferred until the ranking expression is known (see below).
        String funName = fun.getName();

        // #88: Head(Order(set, expr, BDESC), N) is the hand-written
        // spelling of TopCount(set, N, expr). Rewrite it here so it
        // reaches the native path instead of materializing the full
        // ordered set in Java. Fail closed on any non-conforming piece —
        // before the context walk, since every Head(...) comes here.
        if ("Head".equalsIgnoreCase(funName)) {
            if (!isHeadRewriteEnabled()) {
                return null;
            }
            final HeadOrderRewrite rewrite = rewriteHeadOrderToTopCount(args);
            if (rewrite == null) {
                return null;
            }
            funName = rewrite.funName();
            args = rewrite.args();
        }

        if (!isEnabled()
            || !isValidContext(evaluator, /*checkMeasureConflicts*/ false))
        {
            return null;
        }

        // is this "TopCount(<set>, <count>, [<numeric expr>])"
        boolean ascending;
        if ("TopCount".equalsIgnoreCase(funName)) {
            ascending = false;
        } else if ("BottomCount".equalsIgnoreCase(funName)) {
            ascending = true;
        } else {
            return null;
        }
        if (args.length < 2 || args.length > 3) {
            return null;
        }

        // extract the set expression
        List<CrossJoinArg[]> allArgs =
            crossJoinArgFactory().checkCrossJoinArg(evaluator, args[0]);

        // checkCrossJoinArg returns a list of CrossJoinArg arrays.  The first
        // array is the CrossJoin dimensions.  The second array, if any,
        // contains additional constraints on the dimensions. If either the list
        // or the first array is null, then native cross join is not feasible.
        if (allArgs == null || allArgs.isEmpty() || allArgs.get(0) == null) {
            alertNonNativeTopCount(
                "Set in 1st argument does not support native eval.");
            return null;
        }

        CrossJoinArg[] cjArgs = allArgs.get(0);
        if (isPreferInterpreter(cjArgs, false)) {
            alertNonNativeTopCount(
                "One or more args prefer non-native.");
            return null;
        }

        // extract count
        if (!(args[1] instanceof Literal)) {
            alertNonNativeTopCount(
                "TopCount value cannot be determined.");
            return null;
        }
        int count = ((Literal) args[1]).getIntValue();
        if (count <= 0) {
            return null;
        }

        // extract "order by" expression
        SchemaReader schemaReader = evaluator.getSchemaReader();
        DataSource ds = schemaReader.getDataSource();

        // generate the ORDER BY Clause
        // Need to generate top count order by to determine whether
        // or not it can be created. The top count
        // could change to use an aggregate table later in evaulation
        SqlQuery sqlQuery = SqlQuery.newQuery(ds, "NativeTopCount");
        RolapNativeSql sql =
            new RolapNativeSql(
                sqlQuery, null, evaluator, null);
        Exp orderByExpr = null;
        if (args.length == 3) {
            orderByExpr = args[2];
            String orderBySQL = sql.generateTopCountOrderBy(args[2]);
            if (orderBySQL == null) {
                alertNonNativeTopCount(
                    "Cannot convert order by expression to SQL.");
                return null;
            }
        }

        // A literal-only ranking carries no stored measure, so overrideContext
        // leaves a calculated context measure in place, and the strict
        // constraint cannot restrict SQL to a calculation. One over stored
        // measures keeps the Java path; a fact-less one never gets that far.
        if (orderByExpr != null
            && sql.getStoredMeasure() == null
            && evaluator.getMembers()[0].isCalculated()
            && !SqlConstraintUtils.isFactlessContext(
                evaluator, CellReadAnalysis.Judges.CONTEXT))
        {
            alertNonNativeTopCount(
                "Ranking has no stored measure to replace the calculated"
                + " context measure.");
            return null;
        }

        // #86: a calc measure on the query that pins coordinates outside
        // the context (e.g. an All-pinned twin) conflicts with native
        // evaluation in two ways. A non-stored-only ranking can pull
        // those coordinates into the TopN SQL, so it keeps the veto. A
        // stored-only ranking is safe to rank natively, but the conflict
        // can still make an empty-ranked member survive NON EMPTY, so the
        // result must be padded to N like the Java path.
        final boolean measureConflict =
            !isValidContext(evaluator, /*checkMeasureConflicts*/ true, cjArgs);
        // A ranking of literals only (e.g. TopCount(set, N, 1)) compiles to
        // SQL but carries no stored measure, so overrideContext would leave
        // the conflicting calc measure in the context and pull its pinned
        // coordinates into the constraint — what the veto exists to prevent
        // (#33). The carve-out therefore needs a stored measure to rank by.
        final boolean storedOnlyRanking =
            isStoredOnlyRanking(orderByExpr) && sql.getStoredMeasure() != null;
        if (measureConflict && !storedOnlyRanking) {
            alertNonNativeTopCount(
                "Calc measures conflict with context members and the"
                + " ranking expression is not stored-only.");
            return null;
        }
        final boolean needsPadding =
            !evaluator.isNonEmpty() || measureConflict
                || SqlConstraintUtils.hasUnboundedNonEmptyMeasure(evaluator);
        // Null-value padding reads members of a single level only
        // (RolapNativeSet.SetEvaluator), so a multi-hierarchy set that
        // may need it stays on the Java path. Head always evaluates with
        // NON EMPTY off (#88), so this covers every Head(Order(CrossJoin)).
        if (needsPadding && cjArgs.length > 1) {
            // Includes TopCount(CrossJoin(...), N, m) on a plain axis, which
            // reached the broken single-level padding before #88 (#31).
            LOGGER.debug(
                "no native TopCount: {} levels need null padding, which reads"
                + " a single level only",
                cjArgs.length);
            alertNonNativeTopCount(
                "Null-value padding supports a single-level set only.");
            return null;
        }

        // The padding reader cannot carry the query's subselect restrictions.
        // Dense outputs can retain NULL-ranked members even under NON EMPTY.
        // The sole subselect axis being resolved is exempt: re-entry skips
        // that whole expression, leaving no restriction for padding to lose.
        // Static siblings and nested axes still restrict the ranking, so
        // they must keep the veto. A provisional Java ranking is detected
        // by Query's miss counter and makes NQE fall back safely.
        if (needsPadding
            && evaluator.getQuery().getSubcube() != null
            && evaluator.getQuery().getSubcube().getSubcube() != null
            && !evaluator.getQuery().isResolvingOnlySubcubeAxis())
        {
            alertNonNativeTopCount(
                "Null-value padding cannot preserve subselect restrictions.");
            return null;
        }

        if (needsPadding && hasSiblingHierarchyContext(evaluator, cjArgs)) {
            alertNonNativeTopCount(
                "Null-value padding cannot preserve sibling hierarchy restrictions.");
            return null;
        }

        final int savepoint = evaluator.savepoint();
        try {
            overrideContext(evaluator, cjArgs, sql.getStoredMeasure());

            CrossJoinArg[] predicateArgs = null;
            if (allArgs.size() == 2) {
                predicateArgs = allArgs.get(1);
            }

            CrossJoinArg[] combinedArgs;
            if (predicateArgs != null) {
                // Combined the CJ and the additional predicate args
                // to form the TupleConstraint.
                combinedArgs =
                    Util.appendArrays(cjArgs, predicateArgs);
            } else {
                combinedArgs = cjArgs;
            }
            TopCountConstraint constraint =
                new TopCountConstraint(
                    count, combinedArgs, evaluator, orderByExpr, ascending);
            if (!constraint.isValid()) {
                alertNonNativeTopCount(
                    "Constraint constructed cannot be used for native eval.");
                return null;
            }
            LOGGER.debug("using native topcount");
            SetEvaluator sev =
                new SetEvaluator(cjArgs, schemaReader, constraint);
            sev.setMaxRows(count);
            sev.setCompleteWithNullValues(needsPadding);
            return sev;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    /** The padding reader applies the set and roles, but no sibling context. */
    private boolean hasSiblingHierarchyContext(
        RolapEvaluator evaluator, CrossJoinArg[] args)
    {
        for (CrossJoinArg arg : args) {
            final RolapLevel level = arg.getLevel();
            if (level == null) {
                continue;
            }
            for (Member member : evaluator.getMembers()) {
                if (!member.isAll()
                    && member.getDimension().equals(level.getDimension())
                    && !member.getHierarchy().equals(level.getHierarchy()))
                {
                    return true;
                }
            }
        }
        return false;
    }

    private void alertNonNativeTopCount(String msg) {
        RolapUtil.alertNonNative("TopCount", msg);
    }

    // package-local visibility for testing purposes
    boolean isValidContext(
        RolapEvaluator evaluator, boolean checkMeasureConflicts)
    {
        return isValidContext(evaluator, checkMeasureConflicts, null);
    }

    /**
     * @param cjArgs the ranked set: a formula shift conflicts only where it
     * is enumerated, or with a constrained context; null for any hierarchy
     */
    boolean isValidContext(
        RolapEvaluator evaluator, boolean checkMeasureConflicts,
        CrossJoinArg[] cjArgs)
    {
        return TopCountConstraint.isValidContext(
            evaluator,
            /*disallowVirtualCube*/ true,
            cjArgs == null ? null : collectLevels(cjArgs),
            restrictMemberTypes(),
            checkMeasureConflicts);
    }

    /**
     * Returns true when {@code Head(Order(...))} may be rewritten to native
     * TopCount ({@code mondrian.native.head.enable}, default true). Read per
     * call so an operator can roll the rewrite back on its own, without also
     * disabling native TopCount (#30).
     */
    static boolean isHeadRewriteEnabled() {
        return MondrianProperties.instance().EnableNativeHead.get();
    }

    /** Result of {@link #rewriteHeadOrderToTopCount}: the equivalent
     *  TopCount spelling of a conforming Head(Order(...), N). */
    record HeadOrderRewrite(String funName, Exp[] args) {}

    /**
     * Rewrites {@code Head(Order(set, expr, BDESC), N)} into the
     * equivalent {@code TopCount(set, N, expr)} argument shape (#88).
     *
     * <p>Returns null (fail closed — keep the Java path) unless ALL of:
     * Head has exactly 2 args; N is a literal; the set argument is a
     * direct 3-arg {@code Order} call; the direction is the
     * break-hierarchy {@code BDESC}. Hierarchical {@code DESC}/{@code ASC}
     * (including the 2-arg Order default) sort within parent groups,
     * which TopCount does not reproduce. {@code BASC} is not rewritten:
     * Java Order sorts empty values first there, while native
     * BottomCount ranks non-empty values and pads empties last.
     */
    static HeadOrderRewrite rewriteHeadOrderToTopCount(Exp[] args) {
        if (args == null || args.length != 2) {
            return null;
        }
        if (!(args[1] instanceof Literal)) {
            return null;
        }
        if (!(args[0] instanceof ResolvedFunCall order)
            || !"Order".equalsIgnoreCase(order.getFunName()))
        {
            return null;
        }
        final Exp[] orderArgs = order.getArgs();
        if (orderArgs.length != 3) {
            return null;
        }
        if (!(orderArgs[2] instanceof Literal direction)
            || !(direction.getValue() instanceof String directionName))
        {
            return null;
        }
        if (!"BDESC".equalsIgnoreCase(directionName)) {
            return null;
        }
        return new HeadOrderRewrite(
            "TopCount", new Exp[] {orderArgs[0], args[1], orderArgs[1]});
    }

    /**
     * Returns true when the TopCount/BottomCount ranking expression
     * references stored measures only (literals and scalar function
     * calls over them included). Such a ranking builds its TopN SQL
     * purely from the set argument plus the stored measure — calculated
     * members elsewhere on the query never enter that SQL (#86).
     *
     * <p>{@code null} (the 2-arg TopCount form), calculated members,
     * non-measure member references and any member-, tuple- or
     * set-typed sub-expression (tuple pins, including zero-argument
     * member functions such as {@code ParallelPeriod()}) return false
     * and keep the full veto.
     */
    static boolean isStoredOnlyRanking(Exp exp) {
        if (exp == null) {
            return false;
        }
        if (exp instanceof Literal) {
            return true;
        }
        if (exp instanceof MemberExpr) {
            return ((MemberExpr) exp).getMember()
                instanceof RolapStoredMeasure;
        }
        if (exp instanceof ResolvedFunCall call) {
            if (!(call.getType() instanceof ScalarType)) {
                return false;
            }
            for (Exp arg : call.getArgs()) {
                if (!isStoredOnlyRanking(arg)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}

// End RolapNativeTopCount.java
