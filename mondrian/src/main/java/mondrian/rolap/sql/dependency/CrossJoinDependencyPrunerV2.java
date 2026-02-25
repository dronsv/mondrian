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

import mondrian.metrics.CrossJoinDependencyPruningMetrics;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.RolapMember;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.CrossJoinDependencyPruner;
import mondrian.rolap.sql.MemberListCrossJoinArg;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * V2 wrapper for dependency-driven pruning.
 *
 * <p>V2 consumes {@link DependencyRegistry} for validated explicit dependency
 * hints and preserves current behavior via RELAXED same-hierarchy fallback.</p>
 */
public final class CrossJoinDependencyPrunerV2 {
    private static final Logger LOGGER =
        LogManager.getLogger(CrossJoinDependencyPrunerV2.class);

    private CrossJoinDependencyPrunerV2() {
    }

    public static CrossJoinArg[] prune(
        CrossJoinArg[] args,
        RolapEvaluator evaluator)
    {
        if (evaluator == null) {
            return args;
        }
        return prune(args, DependencyPruningContext.fromEvaluator(evaluator));
    }

    public static CrossJoinArg[] prune(
        CrossJoinArg[] args,
        DependencyPruningContext context)
    {
        if (context == null
            || context.getEvaluator() == null
            || context.getPolicy() == DependencyRegistry.DependencyPruningPolicy.OFF)
        {
            return args;
        }
        if (args == null || args.length < 2) {
            return args;
        }

        final DependencyRegistry registry = context.getRegistry();
        final CrossJoinArg[] prunedArgs = args.clone();
        boolean changed = false;
        int explicitRuleApplications = 0;
        int relaxedFallbackApplications = 0;
        int determinantPruneCount = 0;
        final RuntimeCounters counters = new RuntimeCounters();

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

                final KeyDerivationResult derivation =
                    deriveDeterminantKeys(
                        dependentArg,
                        determinantArg,
                        context,
                        registry,
                        counters);
                if (derivation == null || derivation.keys == null) {
                    continue;
                }

                if (derivation.source == DerivationSource.EXPLICIT_RULE) {
                    explicitRuleApplications++;
                } else if (derivation.source == DerivationSource.RELAXED_ANCESTOR_FALLBACK) {
                    relaxedFallbackApplications++;
                }
                dependencyApplied = true;
                filteredMembers =
                    filterMembersByKey(
                        filteredMembers,
                        AllowedKeys.from(derivation.keys));
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
                    context.getEvaluator(),
                    filteredMembers,
                    determinantArg.isRestrictMemberTypes(),
                    determinantArg.isExclude());
            if (filteredArg instanceof MemberListCrossJoinArg) {
                prunedArgs[determinantIndex] = filteredArg;
                changed = true;
                determinantPruneCount++;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "V2 pruned crossjoin level {} from {} to {} members (policy={})",
                        determinantArg.getLevel().getUniqueName(),
                        originalMembers.size(),
                        filteredMembers.size(),
                        context.getPolicy());
                }
            }
        }

        if (LOGGER.isDebugEnabled() && (changed || counters.hasRuleSkips())) {
            LOGGER.debug(
                "V2 dependency pruning summary (determinantPruned={}, explicitRules={}, relaxedFallbacks={}, skippedByReason={}, policy={})",
                determinantPruneCount,
                explicitRuleApplications,
                relaxedFallbackApplications,
                counters.getRuleSkipsByReason(),
                context.getPolicy());
        }
        recordMetrics(
            context,
            determinantPruneCount,
            explicitRuleApplications,
            relaxedFallbackApplications,
            counters);
        return changed ? prunedArgs : args;
    }

    private static void recordMetrics(
        DependencyPruningContext context,
        int determinantPruneCount,
        int explicitRuleApplications,
        int relaxedFallbackApplications,
        RuntimeCounters counters)
    {
        final String policy = context == null || context.getPolicy() == null
            ? null
            : context.getPolicy().name();
        CrossJoinDependencyPruningMetrics.recordDeterminantPrunes(
            policy,
            determinantPruneCount);
        CrossJoinDependencyPruningMetrics.recordRuleApplications(
            policy,
            "explicit",
            explicitRuleApplications);
        CrossJoinDependencyPruningMetrics.recordRuleApplications(
            policy,
            "relaxed_fallback",
            relaxedFallbackApplications);
        CrossJoinDependencyPruningMetrics.recordRuleSkips(
            policy,
            counters == null ? null : counters.getRuleSkipsByReason());
    }

    private static boolean isPrunableArg(MemberListCrossJoinArg arg) {
        return !arg.isExclude()
            && !arg.hasAllMember()
            && !arg.hasCalcMembers()
            && !arg.isEmptyCrossJoinArg()
            && arg.getLevel() != null;
    }

    private static KeyDerivationResult deriveDeterminantKeys(
        MemberListCrossJoinArg dependentArg,
        MemberListCrossJoinArg determinantArg,
        DependencyPruningContext context,
        DependencyRegistry registry,
        RuntimeCounters counters)
    {
        final RolapLevel dependentLevel = dependentArg.getLevel();
        final RolapLevel determinantLevel = determinantArg.getLevel();
        if (dependentLevel == null || determinantLevel == null) {
            return null;
        }

        final DependencyRegistry.CompiledDependencyRule explicitRule =
            findValidatedExplicitRule(
                registry,
                dependentLevel,
                determinantLevel,
                counters);
        if (explicitRule != null) {
            if (explicitRule.requiresTimeFilter() && !context.hasRequiredTimeFilter()) {
                counters.incrementRuleSkip(
                    DependencyRegistry.DependencyIssueCodes
                        .RUNTIME_MISSING_REQUIRED_TIME_FILTER);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "V2 skipped explicit dependency rule {} -> {} (code={}, requiresTimeFilter=true, no time filter in context)",
                        dependentLevel.getUniqueName(),
                        determinantLevel.getUniqueName(),
                        DependencyRegistry.DependencyIssueCodes
                            .RUNTIME_MISSING_REQUIRED_TIME_FILTER);
                }
                return null;
            }
            final Set<Object> keys =
                explicitRule.getMappingType()
                == DependencyRegistry.DependencyMappingType.PROPERTY
                ? collectPropertyKeys(
                    dependentArg.getMembers(),
                    explicitRule.getMappingProperty())
                : collectAncestorKeys(
                    dependentArg.getMembers(),
                    determinantLevel);
            if (keys == null) {
                counters.incrementRuleSkip(
                    DependencyRegistry.DependencyIssueCodes
                        .RUNTIME_EXPLICIT_RULE_KEY_DERIVATION_FAILED);
            }
            return KeyDerivationResult.of(
                keys,
                DerivationSource.EXPLICIT_RULE);
        }

        if (context.getPolicy() == DependencyRegistry.DependencyPruningPolicy.RELAXED
            && isHierarchyAncestorDependency(dependentLevel, determinantLevel))
        {
            return KeyDerivationResult.of(
                collectAncestorKeys(
                    dependentArg.getMembers(),
                    determinantLevel),
                DerivationSource.RELAXED_ANCESTOR_FALLBACK);
        }
        return null;
    }

    private static DependencyRegistry.CompiledDependencyRule findValidatedExplicitRule(
        DependencyRegistry registry,
        RolapLevel dependentLevel,
        RolapLevel determinantLevel,
        RuntimeCounters counters)
    {
        if (registry == null || dependentLevel == null || determinantLevel == null) {
            return null;
        }
        final DependencyRegistry.LevelDependencyDescriptor descriptor =
            registry.getLevelDescriptor(dependentLevel.getUniqueName());
        if (descriptor == null) {
            return null;
        }
        for (DependencyRegistry.CompiledDependencyRule rule : descriptor.getRules()) {
            if (rule == null
                || !determinantLevel.getUniqueName().equals(rule.getDeterminantLevelName()))
            {
                continue;
            }
            if (rule.isAmbiguousJoinPath()) {
                counters.incrementRuleSkip(
                    coalesceRuleCode(
                        rule,
                        DependencyRegistry.DependencyIssueCodes
                            .AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "V2 skipped explicit dependency rule {} -> {} due to ambiguous join-path validation (code={})",
                        dependentLevel.getUniqueName(),
                        determinantLevel.getUniqueName(),
                        rule.getValidationCode());
                }
                continue;
            }
            if (!rule.isValidated()) {
                counters.incrementRuleSkip(
                    coalesceRuleCode(
                        rule,
                        "RUNTIME_INVALID_EXPLICIT_RULE"));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "V2 skipped invalid explicit dependency rule {} -> {} (code={})",
                        dependentLevel.getUniqueName(),
                        determinantLevel.getUniqueName(),
                        rule.getValidationCode());
                }
                continue;
            }
            if (rule.isValidated()) {
                return rule;
            }
        }
        return null;
    }

    private static String coalesceRuleCode(
        DependencyRegistry.CompiledDependencyRule rule,
        String fallback)
    {
        if (rule != null && rule.getValidationCode() != null) {
            return rule.getValidationCode();
        }
        return fallback;
    }

    private static boolean isHierarchyAncestorDependency(
        RolapLevel dependentLevel,
        RolapLevel determinantLevel)
    {
        return dependentLevel.getHierarchy().equals(determinantLevel.getHierarchy())
            && dependentLevel.getDepth() > determinantLevel.getDepth();
    }

    private static Set<Object> collectAncestorKeys(
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

    private static Set<Object> collectPropertyKeys(
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
        AllowedKeys allowedKeys)
    {
        if (allowedKeys == null || allowedKeys.isEmpty()) {
            return Collections.emptyList();
        }
        final List<RolapMember> result = new ArrayList<RolapMember>(members.size());
        for (RolapMember member : members) {
            if (member == null || member.getKey() == null) {
                continue;
            }
            if (allowedKeys.contains(member.getKey())) {
                result.add(member);
            }
        }
        return result;
    }

    private enum DerivationSource {
        EXPLICIT_RULE,
        RELAXED_ANCESTOR_FALLBACK
    }

    private static final class KeyDerivationResult {
        private final Set<Object> keys;
        private final DerivationSource source;

        private KeyDerivationResult(Set<Object> keys, DerivationSource source) {
            this.keys = keys;
            this.source = source;
        }

        private static KeyDerivationResult of(
            Set<Object> keys,
            DerivationSource source)
        {
            return new KeyDerivationResult(keys, source);
        }
    }

    private static final class RuntimeCounters {
        private final Map<String, Integer> ruleSkipsByReason =
            new LinkedHashMap<String, Integer>();

        private void incrementRuleSkip(String reasonCode) {
            final String code = reasonCode == null
                ? "UNKNOWN_RUNTIME_SKIP_REASON"
                : reasonCode;
            final Integer prev = ruleSkipsByReason.get(code);
            ruleSkipsByReason.put(code, prev == null ? 1 : prev + 1);
        }

        private boolean hasRuleSkips() {
            return !ruleSkipsByReason.isEmpty();
        }

        private Map<String, Integer> getRuleSkipsByReason() {
            return ruleSkipsByReason;
        }
    }

    private static final class AllowedKeys {
        private final Set<Object> exactKeys;
        private final Set<String> stringKeys;

        private AllowedKeys(Set<Object> exactKeys, Set<String> stringKeys) {
            this.exactKeys = exactKeys;
            this.stringKeys = stringKeys;
        }

        private static AllowedKeys from(Set<Object> keys) {
            if (keys == null || keys.isEmpty()) {
                return new AllowedKeys(
                    Collections.<Object>emptySet(),
                    Collections.<String>emptySet());
            }
            final Set<String> stringKeys = new LinkedHashSet<String>(keys.size());
            for (Object key : keys) {
                if (key != null) {
                    stringKeys.add(String.valueOf(key));
                }
            }
            return new AllowedKeys(keys, stringKeys);
        }

        private boolean isEmpty() {
            return exactKeys.isEmpty();
        }

        private boolean contains(Object key) {
            if (key == null) {
                return false;
            }
            return exactKeys.contains(key)
                || stringKeys.contains(String.valueOf(key));
        }
    }
}
