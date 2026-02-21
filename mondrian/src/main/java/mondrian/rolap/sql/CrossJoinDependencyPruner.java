/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.Annotation;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Applies conservative dependency-driven pruning to {@link MemberListCrossJoinArg} inputs.
 *
 * <p>Pruning can be configured via level annotation {@code drilldown.dependsOn}:
 * <ul>
 *   <li>{@code [Level Unique Name]}</li>
 *   <li>{@code [Level Unique Name]|ancestor}</li>
 *   <li>{@code [Level Unique Name]|property:PropertyName}</li>
 * </ul>
 *
 * <p>Multiple rules may be separated with ',' or ';'.</p>
 */
public final class CrossJoinDependencyPruner {
    static final String DEPENDS_ON_ANNOTATION = "drilldown.dependsOn";

    private static final Logger LOGGER =
        LogManager.getLogger(CrossJoinDependencyPruner.class);

    private CrossJoinDependencyPruner() {
    }

    public static CrossJoinArg[] prune(
        CrossJoinArg[] args,
        RolapEvaluator evaluator)
    {
        if (args == null || args.length < 2) {
            return args;
        }
        CrossJoinArg[] prunedArgs = args.clone();
        boolean changed = false;

        for (int determinantIndex = 0;
             determinantIndex < prunedArgs.length;
             determinantIndex++)
        {
            if (!(prunedArgs[determinantIndex] instanceof MemberListCrossJoinArg)) {
                continue;
            }
            final MemberListCrossJoinArg determinantArg =
                (MemberListCrossJoinArg) prunedArgs[determinantIndex];
            if (!isPrunableArg(determinantArg)) {
                continue;
            }

            final List<RolapMember> originalMembers = determinantArg.getMembers();
            if (originalMembers.size() <= 1) {
                continue;
            }

            List<RolapMember> filteredMembers = originalMembers;
            boolean dependencyApplied = false;

            for (int dependentIndex = 0;
                 dependentIndex < prunedArgs.length;
                 dependentIndex++)
            {
                if (dependentIndex == determinantIndex
                    || !(prunedArgs[dependentIndex] instanceof MemberListCrossJoinArg))
                {
                    continue;
                }
                final MemberListCrossJoinArg dependentArg =
                    (MemberListCrossJoinArg) prunedArgs[dependentIndex];
                if (!isPrunableArg(dependentArg)) {
                    continue;
                }
                final Set<Object> allowedDeterminantKeys =
                    deriveDeterminantKeys(dependentArg, determinantArg);
                if (allowedDeterminantKeys == null) {
                    continue;
                }

                dependencyApplied = true;
                filteredMembers =
                    filterMembersByKey(filteredMembers, allowedDeterminantKeys);
                if (filteredMembers.isEmpty()) {
                    break;
                }
            }

            if (!dependencyApplied
                || filteredMembers.size() >= originalMembers.size())
            {
                continue;
            }

            final CrossJoinArg filteredArg =
                MemberListCrossJoinArg.create(
                    evaluator,
                    filteredMembers,
                    determinantArg.isRestrictMemberTypes(),
                    determinantArg.isExclude());
            if (filteredArg instanceof MemberListCrossJoinArg) {
                prunedArgs[determinantIndex] = filteredArg;
                changed = true;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "Pruned crossjoin level {} from {} to {} members",
                        determinantArg.getLevel().getUniqueName(),
                        originalMembers.size(),
                        filteredMembers.size());
                }
            }
        }

        return changed ? prunedArgs : args;
    }

    private static boolean isPrunableArg(MemberListCrossJoinArg arg) {
        return !arg.isExclude()
            && !arg.hasAllMember()
            && !arg.hasCalcMembers()
            && !arg.isEmptyCrossJoinArg()
            && arg.getLevel() != null;
    }

    private static Set<Object> deriveDeterminantKeys(
        MemberListCrossJoinArg dependentArg,
        MemberListCrossJoinArg determinantArg)
    {
        final RolapLevel dependentLevel = dependentArg.getLevel();
        final RolapLevel determinantLevel = determinantArg.getLevel();
        if (dependentLevel == null || determinantLevel == null) {
            return null;
        }

        final DependencyRule explicitRule =
            findExplicitRule(dependentLevel, determinantLevel);
        if (explicitRule != null) {
            return explicitRule.mappingType == MappingType.PROPERTY
                ? collectPropertyKeys(
                    dependentArg.getMembers(),
                    explicitRule.mappingProperty)
                : collectAncestorKeys(
                    dependentArg.getMembers(),
                    determinantLevel);
        }

        if (isHierarchyAncestorDependency(dependentLevel, determinantLevel)) {
            return collectAncestorKeys(
                dependentArg.getMembers(),
                determinantLevel);
        }
        return null;
    }

    static boolean isHierarchyAncestorDependency(
        RolapLevel dependentLevel,
        RolapLevel determinantLevel)
    {
        return dependentLevel.getHierarchy().equals(determinantLevel.getHierarchy())
            && dependentLevel.getDepth() > determinantLevel.getDepth();
    }

    static Set<Object> collectAncestorKeys(
        List<RolapMember> dependentMembers,
        RolapLevel determinantLevel)
    {
        final Set<Object> keys = new LinkedHashSet<Object>();
        for (RolapMember dependentMember : dependentMembers) {
            if (dependentMember == null || dependentMember.isCalculated()) {
                return null;
            }
            RolapMember current = dependentMember;
            while (current != null && !determinantLevel.equals(current.getLevel())) {
                current = current.getParentMember();
            }
            if (current == null || current.getKey() == null) {
                return null;
            }
            keys.add(current.getKey());
        }
        return keys;
    }

    static Set<Object> collectPropertyKeys(
        List<RolapMember> dependentMembers,
        String propertyName)
    {
        if (propertyName == null || propertyName.isEmpty()) {
            return null;
        }
        final Set<Object> keys = new LinkedHashSet<Object>();
        for (RolapMember dependentMember : dependentMembers) {
            if (dependentMember == null || dependentMember.isCalculated()) {
                return null;
            }
            final Object value = dependentMember.getPropertyValue(propertyName);
            if (value == null) {
                return null;
            }
            keys.add(value);
        }
        return keys;
    }

    private static List<RolapMember> filterMembersByKey(
        List<RolapMember> members,
        Set<Object> allowedKeys)
    {
        if (allowedKeys.isEmpty()) {
            return Collections.emptyList();
        }
        final List<RolapMember> result = new ArrayList<RolapMember>(members.size());
        for (RolapMember member : members) {
            if (member == null || member.getKey() == null) {
                continue;
            }
            if (containsKey(allowedKeys, member.getKey())) {
                result.add(member);
            }
        }
        return result;
    }

    private static boolean containsKey(Set<Object> allowedKeys, Object key) {
        if (allowedKeys.contains(key)) {
            return true;
        }
        final String keyString = String.valueOf(key);
        for (Object allowedKey : allowedKeys) {
            if (allowedKey != null
                && keyString.equals(String.valueOf(allowedKey)))
            {
                return true;
            }
        }
        return false;
    }

    private static DependencyRule findExplicitRule(
        RolapLevel dependentLevel,
        RolapLevel determinantLevel)
    {
        for (DependencyRule rule : getDependencyRules(dependentLevel)) {
            if (rule.matches(determinantLevel)) {
                return rule;
            }
        }
        return null;
    }

    private static List<DependencyRule> getDependencyRules(RolapLevel level) {
        final Map<String, Annotation> annotationMap = level.getAnnotationMap();
        if (annotationMap == null || annotationMap.isEmpty()) {
            return Collections.emptyList();
        }
        final Annotation annotation = annotationMap.get(DEPENDS_ON_ANNOTATION);
        if (annotation == null || annotation.getValue() == null) {
            return Collections.emptyList();
        }
        return parseDependencyRules(String.valueOf(annotation.getValue()));
    }

    static List<DependencyRule> parseDependencyRules(String annotationValue) {
        if (annotationValue == null) {
            return Collections.emptyList();
        }
        final String trimmedValue = annotationValue.trim();
        if (trimmedValue.isEmpty()) {
            return Collections.emptyList();
        }
        final String[] tokens = trimmedValue.split("[;,]");
        final List<DependencyRule> rules = new ArrayList<DependencyRule>(tokens.length);
        for (String token : tokens) {
            final DependencyRule rule = parseRuleToken(token);
            if (rule != null) {
                rules.add(rule);
            }
        }
        return rules;
    }

    private static DependencyRule parseRuleToken(String token) {
        if (token == null) {
            return null;
        }
        final String trimmedToken = token.trim();
        if (trimmedToken.isEmpty()) {
            return null;
        }

        final int separator = trimmedToken.indexOf('|');
        if (separator < 0) {
            return new DependencyRule(trimmedToken, MappingType.ANCESTOR, null);
        }
        final String levelName = trimmedToken.substring(0, separator).trim();
        if (levelName.isEmpty()) {
            return null;
        }
        final String mappingConfig =
            trimmedToken.substring(separator + 1).trim();
        if (mappingConfig.isEmpty()
            || "ancestor".equalsIgnoreCase(mappingConfig))
        {
            return new DependencyRule(levelName, MappingType.ANCESTOR, null);
        }

        final String lowerMappingConfig = mappingConfig.toLowerCase();
        final String propertyPrefix1 = "property:";
        final String propertyPrefix2 = "property=";
        if (lowerMappingConfig.startsWith(propertyPrefix1)) {
            final String propertyName =
                mappingConfig.substring(propertyPrefix1.length()).trim();
            return propertyName.isEmpty()
                ? null
                : new DependencyRule(
                    levelName,
                    MappingType.PROPERTY,
                    propertyName);
        }
        if (lowerMappingConfig.startsWith(propertyPrefix2)) {
            final String propertyName =
                mappingConfig.substring(propertyPrefix2.length()).trim();
            return propertyName.isEmpty()
                ? null
                : new DependencyRule(
                    levelName,
                    MappingType.PROPERTY,
                    propertyName);
        }
        LOGGER.debug("Ignoring unsupported dependency mapping '{}'", token);
        return null;
    }

    enum MappingType {
        ANCESTOR,
        PROPERTY
    }

    static class DependencyRule {
        final String determinantLevel;
        final MappingType mappingType;
        final String mappingProperty;

        DependencyRule(
            String determinantLevel,
            MappingType mappingType,
            String mappingProperty)
        {
            this.determinantLevel = determinantLevel;
            this.mappingType = mappingType;
            this.mappingProperty = mappingProperty;
        }

        boolean matches(RolapLevel level) {
            return determinantLevel.equals(level.getUniqueName())
                || determinantLevel.equals(level.getName());
        }
    }
}

// End CrossJoinDependencyPruner.java
