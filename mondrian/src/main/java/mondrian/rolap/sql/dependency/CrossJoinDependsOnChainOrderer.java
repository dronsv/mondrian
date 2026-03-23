/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql.dependency;

import mondrian.calc.TupleList;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.sql.CrossJoinArg;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Applies a stable tuple reordering for native crossjoin results when the
 * projected levels are linked by validated {@code drilldown.dependsOnChain}
 * rules.
 *
 * <p>The implementation intentionally stays conservative: if no validated
 * chain relation is present among the projected levels, the tuple list is
 * returned unchanged.</p>
 *
 * <p>"Earlier in tuple" does not mean "adjacent column". For each dependent
 * projected level, the orderer considers the full earlier tuple prefix and
 * builds a composite determinant signature from every applicable validated
 * chain rule.</p>
 */
public final class CrossJoinDependsOnChainOrderer {
    private CrossJoinDependsOnChainOrderer() {
    }

    public static TupleList maybeOrder(
        TupleList tupleList,
        CrossJoinArg[] args,
        DependencyPruningContext context)
    {
        if (!MondrianProperties.instance().CrossJoinOrderByDependsOnChain.get()
            || tupleList == null
            || tupleList.size() <= 1
            || args == null
            || args.length <= 1
            || context == null
            || context.getRegistry() == null)
        {
            return tupleList;
        }

        final OrderingPlan orderingPlan = buildOrderingPlan(args, context);
        if (!orderingPlan.hasApplicableChain()) {
            return tupleList;
        }

        final TupleList fixedList = tupleList.fix();
        fixedList.sort(
            new TupleOrderComparator(fixedList.getArity(), orderingPlan));
        return fixedList;
    }

    static OrderingPlan buildOrderingPlan(
        CrossJoinArg[] args,
        DependencyPruningContext context)
    {
        if (args == null || args.length <= 1 || context == null) {
            return OrderingPlan.empty(args == null ? 0 : args.length);
        }
        final DependencyRegistry registry = context.getRegistry();
        if (registry == null) {
            return OrderingPlan.empty(args.length);
        }

        final int[][] determinantColumns = new int[args.length][];
        boolean hasApplicableChain = false;
        for (int dependentIndex = 1; dependentIndex < args.length; dependentIndex++) {
            final RolapLevel dependentLevel = args[dependentIndex].getLevel();
            if (dependentLevel == null) {
                determinantColumns[dependentIndex] = new int[0];
                continue;
            }
            final DependencyRegistry.LevelDependencyDescriptor descriptor =
                registry.getLevelDescriptor(dependentLevel.getUniqueName());
            if (descriptor == null
                || !descriptor.isChainDeclared()
                || descriptor.getRules() == null
                || descriptor.getRules().isEmpty())
            {
                determinantColumns[dependentIndex] = new int[0];
                continue;
            }

            final List<Integer> matchedDeterminants = new ArrayList<Integer>();
            for (int determinantIndex = 0;
                 determinantIndex < dependentIndex;
                 determinantIndex++)
            {
                final RolapLevel determinantLevel = args[determinantIndex].getLevel();
                if (determinantLevel == null) {
                    continue;
                }
                if (hasApplicableRule(descriptor, determinantLevel, context)) {
                    matchedDeterminants.add(Integer.valueOf(determinantIndex));
                }
            }
            determinantColumns[dependentIndex] =
                toIntArray(matchedDeterminants);
            hasApplicableChain =
                hasApplicableChain || determinantColumns[dependentIndex].length > 0;
        }
        return new OrderingPlan(determinantColumns, hasApplicableChain);
    }

    private static boolean hasApplicableRule(
        DependencyRegistry.LevelDependencyDescriptor descriptor,
        RolapLevel determinantLevel,
        DependencyPruningContext context)
    {
        if (descriptor == null
            || determinantLevel == null
            || context == null
            || descriptor.getRules() == null)
        {
            return false;
        }
        for (DependencyRegistry.CompiledDependencyRule rule : descriptor.getRules()) {
            if (rule == null
                || !rule.isValidated()
                || rule.isAmbiguousJoinPath()
                || !determinantLevel.getUniqueName().equals(rule.getDeterminantLevelName()))
            {
                continue;
            }
            if (rule.requiresTimeFilter() && !context.hasRequiredTimeFilter()) {
                continue;
            }
            return true;
        }
        return false;
    }

    private static final class TupleOrderComparator
        implements Comparator<List<Member>> {
        private final int arity;
        private final OrderingPlan orderingPlan;

        private TupleOrderComparator(
            int arity,
            OrderingPlan orderingPlan)
        {
            this.arity = arity;
            this.orderingPlan = orderingPlan;
        }

        public int compare(List<Member> left, List<Member> right) {
            if (left == right) {
                return 0;
            }
            if (left == null) {
                return -1;
            }
            if (right == null) {
                return 1;
            }

            final int width =
                Math.min(arity, Math.min(left.size(), right.size()));
            for (int i = 0; i < width; i++) {
                for (int determinantColumn
                    : orderingPlan.getDeterminantColumns(i))
                {
                    if (determinantColumn >= width) {
                        continue;
                    }
                    final int determinantComparison =
                        compareMembers(
                            left.get(determinantColumn),
                            right.get(determinantColumn));
                    if (determinantComparison != 0) {
                        return determinantComparison;
                    }
                }
                final int comparison = compareMembers(left.get(i), right.get(i));
                if (comparison != 0) {
                    return comparison;
                }
            }
            if (left.size() == right.size()) {
                return 0;
            }
            return left.size() < right.size() ? -1 : 1;
        }
    }

    private static int[] toIntArray(List<Integer> values) {
        if (values == null || values.isEmpty()) {
            return new int[0];
        }
        final int[] result = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = values.get(i).intValue();
        }
        return result;
    }

    static int compareMembers(Member left, Member right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }

        final int orderKeyComparison =
            compareComparableValues(left.getOrderKey(), right.getOrderKey());
        if (orderKeyComparison != 0) {
            return orderKeyComparison;
        }

        final int leftOrdinal = left.getOrdinal();
        final int rightOrdinal = right.getOrdinal();
        if (leftOrdinal != rightOrdinal) {
            return leftOrdinal < rightOrdinal ? -1 : 1;
        }

        return String.valueOf(left.getUniqueName()).compareTo(
            String.valueOf(right.getUniqueName()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compareComparableValues(
        Comparable left,
        Comparable right)
    {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left instanceof Number && right instanceof Number) {
            return new BigDecimal(left.toString()).compareTo(
                new BigDecimal(right.toString()));
        }
        if (left.getClass().equals(right.getClass())) {
            return left.compareTo(right);
        }
        return left.toString().compareTo(right.toString());
    }

    static final class OrderingPlan {
        private final int[][] determinantColumnsByIndex;
        private final boolean applicableChain;

        private OrderingPlan(
            int[][] determinantColumnsByIndex,
            boolean applicableChain)
        {
            this.determinantColumnsByIndex =
                determinantColumnsByIndex == null
                    ? new int[0][]
                    : determinantColumnsByIndex;
            this.applicableChain = applicableChain;
        }

        static OrderingPlan empty(int size) {
            return new OrderingPlan(new int[size][], false);
        }

        boolean hasApplicableChain() {
            return applicableChain;
        }

        int[] getDeterminantColumns(int index) {
            if (index < 0 || index >= determinantColumnsByIndex.length) {
                return new int[0];
            }
            final int[] result = determinantColumnsByIndex[index];
            return result == null ? new int[0] : result;
        }
    }
}
