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

import mondrian.rolap.RolapLevel;
import mondrian.rolap.sql.CrossJoinArg;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Query-scoped dynamic drilldep hierarchy plan synthesized from the projected
 * crossjoin levels and validated dependency metadata.
 *
 * <p>This plan is intentionally backend-agnostic. It describes which projected
 * columns depend on earlier projected columns and which projected columns may
 * derive a hidden determinant from an already loaded member property. Native
 * row-axis post-ordering is one consumer of this plan.</p>
 */
final class DynamicDrilldepHierarchyPlan {
    enum ShapeClass {
        NO_CHAIN,
        PROJECTED_PREFIX,
        HIDDEN_DETERMINANT,
        MIXED
    }

    private final int[][] determinantColumnsByIndex;
    private final String[][] hiddenDeterminantPropertiesByIndex;
    private final boolean applicableChain;
    private final ShapeClass shapeClass;

    private DynamicDrilldepHierarchyPlan(
        int[][] determinantColumnsByIndex,
        String[][] hiddenDeterminantPropertiesByIndex,
        boolean applicableChain,
        ShapeClass shapeClass)
    {
        this.determinantColumnsByIndex =
            determinantColumnsByIndex == null
                ? new int[0][]
                : determinantColumnsByIndex;
        this.hiddenDeterminantPropertiesByIndex =
            hiddenDeterminantPropertiesByIndex == null
                ? new String[0][]
                : hiddenDeterminantPropertiesByIndex;
        this.applicableChain = applicableChain;
        this.shapeClass =
            shapeClass == null ? ShapeClass.NO_CHAIN : shapeClass;
    }

    static DynamicDrilldepHierarchyPlan build(
        CrossJoinArg[] args,
        DependencyPruningContext context)
    {
        if (args == null || args.length <= 1 || context == null) {
            return empty(args == null ? 0 : args.length);
        }
        final DependencyRegistry registry = context.getRegistry();
        if (registry == null) {
            return empty(args.length);
        }

        final int[][] determinantColumns = new int[args.length][];
        final String[][] hiddenDeterminantProperties = new String[args.length][];
        boolean hasApplicableChain = false;
        boolean hasVisibleDeterminants = false;
        boolean hasHiddenDeterminants = false;
        for (int dependentIndex = 0; dependentIndex < args.length; dependentIndex++) {
            final RolapLevel dependentLevel = args[dependentIndex].getLevel();
            if (dependentLevel == null) {
                determinantColumns[dependentIndex] = new int[0];
                hiddenDeterminantProperties[dependentIndex] = new String[0];
                continue;
            }
            final DependencyRegistry.LevelDependencyDescriptor descriptor =
                registry.getLevelDescriptor(dependentLevel.getUniqueName());
            if (descriptor == null
                || descriptor.getRules() == null
                || descriptor.getRules().isEmpty())
            {
                determinantColumns[dependentIndex] = new int[0];
                hiddenDeterminantProperties[dependentIndex] = new String[0];
                continue;
            }

            final Set<Integer> matchedDeterminants =
                new LinkedHashSet<Integer>();
            final List<String> hiddenDeterminants = new ArrayList<String>();
            for (DependencyRegistry.CompiledDependencyRule rule
                : descriptor.getRules())
            {
                if (!CrossJoinDependsOnChainOrderer.isApplicableOrderingRule(
                    rule,
                    context))
                {
                    continue;
                }

                boolean matchedVisibleDeterminant = false;
                for (int determinantIndex = 0;
                     determinantIndex < dependentIndex;
                     determinantIndex++)
                {
                    final RolapLevel determinantLevel =
                        args[determinantIndex].getLevel();
                    if (determinantLevel == null) {
                        continue;
                    }
                    if (determinantLevel.getUniqueName().equals(
                        rule.getDeterminantLevelName()))
                    {
                        matchedDeterminants.add(Integer.valueOf(determinantIndex));
                        matchedVisibleDeterminant = true;
                        break;
                    }
                }
                if (!matchedVisibleDeterminant
                    && rule.getMappingType()
                        == DependencyRegistry.DependencyMappingType.PROPERTY
                    && rule.getMappingProperty() != null
                    && !rule.getMappingProperty().isEmpty())
                {
                    hiddenDeterminants.add(rule.getMappingProperty());
                }
            }
            determinantColumns[dependentIndex] =
                toIntArray(matchedDeterminants);
            hiddenDeterminantProperties[dependentIndex] =
                hiddenDeterminants.toArray(
                    new String[hiddenDeterminants.size()]);
            hasApplicableChain =
                hasApplicableChain
                    || determinantColumns[dependentIndex].length > 0
                    || hiddenDeterminantProperties[dependentIndex].length > 0;
            hasVisibleDeterminants =
                hasVisibleDeterminants
                    || determinantColumns[dependentIndex].length > 0;
            hasHiddenDeterminants =
                hasHiddenDeterminants
                    || hiddenDeterminantProperties[dependentIndex].length > 0;
        }
        return new DynamicDrilldepHierarchyPlan(
            determinantColumns,
            hiddenDeterminantProperties,
            hasApplicableChain,
            deriveShapeClass(
                hasApplicableChain,
                hasVisibleDeterminants,
                hasHiddenDeterminants));
    }

    static DynamicDrilldepHierarchyPlan empty(int size) {
        return new DynamicDrilldepHierarchyPlan(
            new int[size][],
            new String[size][],
            false,
            ShapeClass.NO_CHAIN);
    }

    boolean hasApplicableChain() {
        return applicableChain;
    }

    ShapeClass getShapeClass() {
        return shapeClass;
    }

    int[] getDeterminantColumns(int index) {
        if (index < 0 || index >= determinantColumnsByIndex.length) {
            return new int[0];
        }
        final int[] result = determinantColumnsByIndex[index];
        return result == null ? new int[0] : result;
    }

    String[] getHiddenDeterminantProperties(int index) {
        if (index < 0 || index >= hiddenDeterminantPropertiesByIndex.length) {
            return new String[0];
        }
        final String[] result = hiddenDeterminantPropertiesByIndex[index];
        return result == null ? new String[0] : result;
    }

    int size() {
        return determinantColumnsByIndex.length;
    }

    private static ShapeClass deriveShapeClass(
        boolean hasApplicableChain,
        boolean hasVisibleDeterminants,
        boolean hasHiddenDeterminants)
    {
        if (!hasApplicableChain) {
            return ShapeClass.NO_CHAIN;
        }
        if (hasVisibleDeterminants && hasHiddenDeterminants) {
            return ShapeClass.MIXED;
        }
        if (hasHiddenDeterminants) {
            return ShapeClass.HIDDEN_DETERMINANT;
        }
        return ShapeClass.PROJECTED_PREFIX;
    }

    private static int[] toIntArray(Set<Integer> values) {
        if (values == null || values.isEmpty()) {
            return new int[0];
        }
        final int[] result = new int[values.size()];
        int i = 0;
        for (Integer value : values) {
            result[i++] = value == null ? -1 : value.intValue();
        }
        return result;
    }
}
