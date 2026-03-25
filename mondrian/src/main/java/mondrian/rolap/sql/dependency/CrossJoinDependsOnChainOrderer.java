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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger LOGGER =
        LogManager.getLogger(CrossJoinDependsOnChainOrderer.class);
    private static final String DEBUG_PROPERTY =
        "mondrian.native.crossjoin.orderByDependsOnChain.debug";

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
            logDiagnostic(
                "skip-preconditions",
                tupleList,
                args,
                context,
                null);
            return tupleList;
        }

        final DynamicDrilldepHierarchyPlan hierarchyPlan =
            buildHierarchyPlan(args, context);
        if (!hierarchyPlan.hasApplicableChain()) {
            logDiagnostic(
                "skip-no-applicable-chain",
                tupleList,
                args,
                context,
                hierarchyPlan);
            return tupleList;
        }

        final TupleList fixedList = tupleList.fix();
        fixedList.sort(
            new TupleOrderComparator(fixedList.getArity(), hierarchyPlan));
        logDiagnostic(
            "applied",
            fixedList,
            args,
            context,
            hierarchyPlan);
        return fixedList;
    }

    static DynamicDrilldepHierarchyPlan buildHierarchyPlan(
        CrossJoinArg[] args,
        DependencyPruningContext context)
    {
        return DynamicDrilldepHierarchyPlan.build(args, context);
    }

    static boolean isApplicableOrderingRule(
        DependencyRegistry.CompiledDependencyRule rule,
        DependencyPruningContext context)
    {
        if (rule == null || context == null) {
            return false;
        }
        if (rule.requiresTimeFilter() && !context.hasRequiredTimeFilter()) {
            return false;
        }
        if (rule.isValidated() && !rule.isAmbiguousJoinPath()) {
            return true;
        }
        // Ordering works on already materialized member properties, so an
        // explicit property-mapped rule remains safe even when pruning had to
        // reject it for ambiguous schema join-path reasons.
        if (rule.getMappingType()
            != DependencyRegistry.DependencyMappingType.PROPERTY
            || rule.getMappingProperty() == null
            || rule.getMappingProperty().isEmpty())
        {
            return false;
        }
        return DependencyRegistry.DependencyIssueCodes
            .AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH.equals(
                rule.getValidationCode());
    }

    private static final class TupleOrderComparator
        implements Comparator<List<Member>> {
        private final int arity;
        private final DynamicDrilldepHierarchyPlan hierarchyPlan;

        private TupleOrderComparator(
            int arity,
            DynamicDrilldepHierarchyPlan hierarchyPlan)
        {
            this.arity = arity;
            this.hierarchyPlan = hierarchyPlan;
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
                    : hierarchyPlan.getDeterminantColumns(i))
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
                for (String determinantProperty
                    : hierarchyPlan.getHiddenDeterminantProperties(i))
                {
                    final int determinantComparison =
                        compareComparableValues(
                            toComparable(
                                getMemberPropertyValue(left.get(i), determinantProperty)),
                            toComparable(
                                getMemberPropertyValue(right.get(i), determinantProperty)));
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

    private static Object getMemberPropertyValue(
        Member member,
        String propertyName)
    {
        if (member == null || propertyName == null || propertyName.isEmpty()) {
            return null;
        }
        return member.getPropertyValue(propertyName, false);
    }

    private static Comparable toComparable(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Comparable) {
            return (Comparable) value;
        }
        return value.toString();
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

    private static void logDiagnostic(
        String event,
        TupleList tupleList,
        CrossJoinArg[] args,
        DependencyPruningContext context,
        DynamicDrilldepHierarchyPlan hierarchyPlan)
    {
        if (!isDiagnosticEnabled()) {
            return;
        }
        LOGGER.info(
            "orderByDependsOnChain event={} tupleCount={} args={} plan={} shapeClass={} registryPresent={} timeFilter={}",
            event,
            tupleList == null ? -1 : tupleList.size(),
            formatArgs(args),
            formatPlan(hierarchyPlan),
            hierarchyPlan == null ? "<none>" : hierarchyPlan.getShapeClass(),
            context != null && context.getRegistry() != null,
            context != null && context.hasRequiredTimeFilter());
    }

    private static boolean isDiagnosticEnabled() {
        final String rawValue =
            MondrianProperties.instance().getProperty(DEBUG_PROPERTY);
        return rawValue != null
            && !"false".equalsIgnoreCase(rawValue.trim())
            && !"0".equals(rawValue.trim());
    }

    private static String formatArgs(CrossJoinArg[] args) {
        if (args == null || args.length == 0) {
            return "<none>";
        }
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                builder.append(" | ");
            }
            final RolapLevel level = args[i] == null ? null : args[i].getLevel();
            builder.append(i)
                .append(':')
                .append(level == null ? "<null>" : level.getUniqueName());
        }
        return builder.toString();
    }

    private static String formatPlan(DynamicDrilldepHierarchyPlan plan) {
        if (plan == null) {
            return "<none>";
        }
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < plan.size(); i++) {
            if (i > 0) {
                builder.append(" | ");
            }
            builder.append(i)
                .append(":[v=");
            appendIntArray(builder, plan.getDeterminantColumns(i));
            builder.append(",h=");
            appendStringArray(builder, plan.getHiddenDeterminantProperties(i));
            builder.append(']');
        }
        return builder.toString();
    }

    private static void appendIntArray(StringBuilder builder, int[] values) {
        if (values == null || values.length == 0) {
            builder.append('-');
            return;
        }
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(values[i]);
        }
    }

    private static void appendStringArray(StringBuilder builder, String[] values) {
        if (values == null || values.length == 0) {
            builder.append('-');
            return;
        }
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(values[i]);
        }
    }
}
