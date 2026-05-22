/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// Copyright (C) 2021-2024 Sergei Semenkov
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.rolap.MeasureExecutionKind;
import mondrian.rolap.NativeNonEmptyFilter;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.SyntheticFlatHierarchy;
import mondrian.rolap.SyntheticFlatHierarchySupport;

import java.util.*;

/**
 * Definition of the <code>DrilldownMember</code> MDX function.
 *
 * @author Grzegorz Lojek
 * @since 6 December, 2004
 */
class DrilldownMemberFunDef extends FunDefBase {
    static final String[] reservedWords = new String[] {"RECURSIVE"};
    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "DrilldownMember",
            "DrilldownMember(<Set1>, <Set2>[, RECURSIVE | <Hierarchy>])",
            "Drills down the members in a set that are present in a second specified set.",
            new String[]{"fxxx", "fxxxy", "fxxxeey", "fxxxh"},
            DrilldownMemberFunDef.class,
            reservedWords);

    public DrilldownMemberFunDef(FunDef funDef) {
        super(funDef);
    }

    @Override
    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));

        // Third argument can be RECURSIVE (symbol) or a Hierarchy
        // (Excel SSAS extension for specifying which hierarchy to drill
        // in a crossjoin).
        String literalArg = "";
        final HierarchyCalc hierarchyCalc;
        if (call.getArgCount() == 3) {
            Exp arg2 = call.getArg(2);
            if (arg2.getType() instanceof mondrian.olap.type.HierarchyType) {
                hierarchyCalc = compiler.compileHierarchy(arg2);
            } else {
                hierarchyCalc = null;
                literalArg = getLiteralArg(call, 2, "", reservedWords);
            }
        } else {
            hierarchyCalc = null;
        }
        final boolean recursive = literalArg.equals("RECURSIVE");

        return new AbstractListCalc(
            call,
            new Calc[] {listCalc1, listCalc2})
        {
            @Override
            public TupleList evaluateList(Evaluator evaluator) {
                final TupleList list1 = listCalc1.evaluateList(evaluator);
                final TupleList list2 = listCalc2.evaluateList(evaluator);
                Hierarchy drillHierarchy = hierarchyCalc != null
                    ? hierarchyCalc.evaluateHierarchy(evaluator) : null;
                return drilldownMember(
                    list1, list2, evaluator, drillHierarchy);
            }

            /**
             * Drills down an element. Standard behavior: if a tuple
             * member is in memberSet, expand that member's children.
             */
            protected void drillDownObj(
                Evaluator evaluator,
                Member[] tuple,
                Set<Member> memberSet,
                TupleList resultList)
            {
                for (int k = 0; k < tuple.length; k++) {
                    Member member = tuple[k];
                    if (memberSet.contains(member)) {
                        List<Member> children =
                            evaluator.getSchemaReader().getMemberChildren(member);
                        final Member[] tuple2 = tuple.clone();
                        for (Member childMember : children) {
                            tuple2[k] = childMember;
                            resultList.addTuple(tuple2);
                            if (recursive) {
                                drillDownObj(
                                    evaluator, tuple2, memberSet, resultList);
                            }
                        }
                        break;
                    }
                }
            }

            /**
             * Cross-hierarchy drill: if a tuple member (from one
             * hierarchy) is in memberSet, expand the member from
             * drillHierarchy in the same tuple.
             *
             * <p>When the drill target is a {@link SyntheticFlatHierarchy}
             * and one or more other tuple positions belong to sibling
             * synthetic-flat hierarchies projecting different levels of
             * the same source hierarchy, the cross-hierarchy drill is
             * Cartesian by default — each sibling level has no parent-
             * child structure connecting it to the others, so every
             * candidate child is paired with every sibling member,
             * producing invalid (sourceparent-mismatched) tuples and
             * blowing past `-Xmx` on Excel "+ expand" pivots. See #78.
             *
             * <p>The filter below derives source-path constraints from
             * the sibling tuple positions and keeps only candidate
             * children whose synthetic-flat ancestor properties match
             * those constraints — emitting just the valid source-
             * hierarchy parent-child paths.
             */
            protected void drillDownCrossHierarchy(
                Evaluator evaluator,
                Member[] tuple,
                Set<Member> memberSet,
                Hierarchy drillHierarchy,
                TupleList resultList)
            {
                // Check if any member in the tuple is in the drill set
                boolean shouldDrill = false;
                for (Member member : tuple) {
                    if (memberSet.contains(member)) {
                        shouldDrill = true;
                        break;
                    }
                }
                if (!shouldDrill) {
                    return;
                }
                // Find the position of drillHierarchy in the tuple
                for (int k = 0; k < tuple.length; k++) {
                    if (tuple[k].getHierarchy().getUniqueName()
                        .equals(drillHierarchy.getUniqueName()))
                    {
                        List<Member> children =
                            evaluator.getSchemaReader()
                                .getMemberChildren(tuple[k]);
                        List<Member> filteredChildren =
                            filterChildrenBySourcePath(
                                tuple, k, drillHierarchy, children);
                        final Member[] tuple2 = tuple.clone();
                        for (Member childMember : filteredChildren) {
                            tuple2[k] = childMember;
                            resultList.addTuple(tuple2);
                        }
                        break;
                    }
                }
            }

            /**
             * #78 source-path correlation: when drilling a synthetic-
             * flat hierarchy in a tuple whose other positions hold
             * sibling synthetic-flats projecting the same source
             * hierarchy, restrict candidate children to those whose
             * source-hierarchy ancestor identities match the sibling
             * tuple members. When the prerequisites don't hold
             * (non-synthetic-flat drill, no sibling synthetic-flats),
             * returns {@code children} unchanged — preserves the
             * existing behavior for every non-#78 caller.
             */
            private List<Member> filterChildrenBySourcePath(
                Member[] tuple,
                int drillIndex,
                Hierarchy drillHierarchy,
                List<Member> children)
            {
                if (children == null || children.isEmpty()) {
                    return children;
                }
                final SyntheticFlatHierarchy drillSF =
                    SyntheticFlatHierarchySupport.resolveSyntheticFlat(
                        drillHierarchy);
                if (drillSF == null) {
                    return children;
                }

                // Step 2: scan sibling positions for source-path constraints
                List<String> constraintProps = null;
                List<Object> constraintKeys = null;
                for (int j = 0; j < tuple.length; j++) {
                    if (j == drillIndex) {
                        continue;
                    }
                    final Member sibling = tuple[j];
                    if (sibling == null || sibling.isAll()) {
                        continue;
                    }
                    final SyntheticFlatHierarchy siblingSF =
                        SyntheticFlatHierarchySupport.resolveSyntheticFlat(
                            sibling.getHierarchy());
                    if (siblingSF == null) {
                        continue;
                    }
                    // Find a SourceLink on the sibling whose hierarchy
                    // is also linked from the drill side AT A GREATER
                    // DEPTH (sibling is the ancestor, drill is the
                    // descendant).
                    for (SyntheticFlatHierarchy.SourceLink detLink
                        : siblingSF.getSourceLinks())
                    {
                        SyntheticFlatHierarchy.SourceLink depLink =
                            drillSF.findLinkForHierarchy(
                                detLink.hierarchy());
                        if (depLink == null
                            || depLink.depth() <= detLink.depth())
                        {
                            continue;
                        }
                        final Object reqKey = sibling.getPropertyValue(
                            Property.KEY.getName());
                        // For a synthetic-flat sibling, the level key
                        // surfaces as the standard KEY property; if it
                        // is null, fall back to getName() (rare — but
                        // safer than skipping the constraint).
                        final Object actualKey = reqKey != null
                            ? reqKey
                            : sibling.getName();
                        if (actualKey == null) {
                            break;
                        }
                        if (constraintProps == null) {
                            constraintProps = new ArrayList<>(2);
                            constraintKeys = new ArrayList<>(2);
                        }
                        constraintProps.add(
                            SyntheticFlatHierarchySupport
                                .ANCESTOR_PROPERTY_PREFIX
                                + detLink.level().getName());
                        constraintKeys.add(actualKey);
                        break;
                    }
                }
                if (constraintProps == null) {
                    return children;
                }

                // Step 3: per-child filter
                final List<Member> filtered =
                    new ArrayList<>(children.size());
                outer:
                for (Member child : children) {
                    for (int i = 0; i < constraintProps.size(); i++) {
                        Object actual = child.getPropertyValue(
                            constraintProps.get(i));
                        if (!SyntheticFlatHierarchySupport.equalsTolerant(
                                actual, constraintKeys.get(i)))
                        {
                            continue outer;
                        }
                    }
                    filtered.add(child);
                }
                return filtered;
            }

            private TupleList drilldownMember(
                TupleList v0,
                TupleList v1,
                Evaluator evaluator,
                Hierarchy drillHierarchy)
            {
                assert v1.getArity() == 1;
                if (v0.isEmpty() || v1.isEmpty()) {
                    return v0;
                }

                Set<Member> set1 = new HashSet<Member>(v1.slice(0));

                TupleList result = TupleCollections.createList(v0.getArity());
                int i = 0, n = v0.size();
                final Member[] members = new Member[v0.getArity()];
                while (i < n) {
                    List<Member> o = v0.get(i++);
                    o.toArray(members);
                    result.add(o);
                    if (drillHierarchy != null) {
                        drillDownCrossHierarchy(
                            evaluator, members, set1,
                            drillHierarchy, result);
                    } else {
                        drillDownObj(evaluator, members, set1, result);
                    }
                }
                return tryPruneExpandedDrilldownMember(evaluator, result);
            }
        };
    }

    static TupleList tryPruneExpandedDrilldownMember(
        Evaluator evaluator,
        TupleList result)
    {
        if (result == null
            || result.isEmpty()
            || evaluator == null
            || !evaluator.isNonEmpty()
            || !MondrianProperties.instance().NativeNonEmptyFilterEnable.get()
            || !(evaluator instanceof RolapEvaluator))
        {
            return result;
        }

        final Query query = evaluator.getQuery();
        if (query == null) {
            return result;
        }

        final Set<Member> measures = query.getMeasuresMembers();
        if (!hasOnlyNativeNonEmptySafeMeasures(measures)) {
            return result;
        }

        final TupleList pruned = NativeNonEmptyFilter.tryPrune(
            (RolapEvaluator) evaluator,
            result,
            measures);
        return pruned == null ? result : pruned;
    }

    private static boolean hasOnlyNativeNonEmptySafeMeasures(
        Set<Member> measures)
    {
        if (measures == null || measures.isEmpty()) {
            return false;
        }
        for (Member measure : measures) {
            if (measure == null || !measure.isMeasure()) {
                return false;
            }
            final MeasureExecutionKind executionKind =
                MeasureExecutionKind.forMember(measure);
            if (executionKind != MeasureExecutionKind.STORED
                    && executionKind
                        != MeasureExecutionKind.CALCULATED_NATIVE_SQL)
            {
                return false;
            }
        }
        return true;
    }
}

// End DrilldownMemberFunDef.java
