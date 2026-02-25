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

import mondrian.olap.Annotation;
import mondrian.olap.Cube;
import mondrian.olap.DimensionType;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapHierarchy;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.sql.CrossJoinDependencyPruner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds {@link DependencyRegistry} from loaded cube metadata.
 *
 * <p>V2 builder compiles explicit dependency hints ({@code
 * drilldown.dependsOn}) into immutable level descriptors and reports basic
 * validation issues. Join-path analysis and SCD/time-filter diagnostics are
 * intentionally left for later phases.</p>
 */
public class DependencyRegistryBuilder {
    public DependencyRegistry build(RolapCube cube) {
        final String cubeName = cube == null ? "<unknown-cube>" : cube.getUniqueName();
        DependencyRegistry.Builder builder = DependencyRegistry.builder(cubeName);
        if (cube == null) {
            builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                DependencyRegistry.DependencyValidationSeverity.WARN,
                "NULL_CUBE",
                "Dependency registry cannot be built for null cube.",
                cubeName,
                null,
                "Build the registry after cube metadata is available."));
            return builder.build();
        }

        final List<RolapLevel> levels = collectRolapLevels(cube);
        final boolean hasTimeDimension = hasTimeDimension(levels);
        final LevelLookup lookup = buildLevelLookup(levels);
        for (RolapLevel level : levels) {
            final CompiledLevelRules compiled =
                compileRules(level, cubeName, lookup, builder, hasTimeDimension);
            builder.addLevelDescriptor(
                new DependencyRegistry.LevelDependencyDescriptor(
                    level.getUniqueName(),
                    level.getHierarchy() == null
                        ? null
                        : level.getHierarchy().getUniqueName(),
                    level.getDepth(),
                    compiled.rules,
                    compiled.ambiguousJoinPath));
        }

        builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
            DependencyRegistry.DependencyValidationSeverity.INFO,
            "DEPENDENCY_REGISTRY_PARTIAL_VALIDATION",
            "Dependency registry compiled explicit drilldown.dependsOn hints. "
                + "Join-path and SCD/time-filter validation is not implemented yet.",
            cubeName,
            null,
            "Use explicit dependency hints and STRICT mode for safer rollout."));
        return builder.build();
    }

    public DependencyRegistry build(Cube cube) {
        if (cube instanceof RolapCube) {
            return build((RolapCube) cube);
        }
        final String cubeName = cube == null ? "<unknown-cube>" : cube.getUniqueName();
        return DependencyRegistry.builder(cubeName)
            .addIssue(new DependencyRegistry.DependencyValidationIssue(
                DependencyRegistry.DependencyValidationSeverity.WARN,
                "UNSUPPORTED_CUBE_TYPE",
                "Dependency registry is only available for RolapCube in V2 skeleton.",
                cubeName,
                null,
                "Build registry after resolving virtual/base RolapCube metadata."))
            .build();
    }

    private List<RolapLevel> collectRolapLevels(RolapCube cube) {
        if (cube == null) {
            return Collections.emptyList();
        }
        final List<RolapLevel> levels = new ArrayList<RolapLevel>();
        for (RolapHierarchy hierarchy : cube.getHierarchies()) {
            if (hierarchy == null) {
                continue;
            }
            for (Level level : hierarchy.getLevels()) {
                if (level instanceof RolapLevel) {
                    levels.add((RolapLevel) level);
                }
            }
        }
        return levels;
    }

    private LevelLookup buildLevelLookup(List<RolapLevel> levels) {
        final Map<String, RolapLevel> byUniqueName =
            new LinkedHashMap<String, RolapLevel>();
        final Map<String, List<RolapLevel>> byName =
            new HashMap<String, List<RolapLevel>>();
        for (RolapLevel level : levels) {
            if (level == null) {
                continue;
            }
            byUniqueName.put(level.getUniqueName(), level);
            final String name = level.getName();
            if (name == null) {
                continue;
            }
            List<RolapLevel> sameName = byName.get(name);
            if (sameName == null) {
                sameName = new ArrayList<RolapLevel>(1);
                byName.put(name, sameName);
            }
            sameName.add(level);
        }
        return new LevelLookup(byUniqueName, byName);
    }

    private CompiledLevelRules compileRules(
        RolapLevel dependentLevel,
        String cubeName,
        LevelLookup lookup,
        DependencyRegistry.Builder builder,
        boolean hasTimeDimension)
    {
        final String annotationValue = getDependsOnAnnotationValue(dependentLevel);
        if (annotationValue == null || annotationValue.trim().isEmpty()) {
            return CompiledLevelRules.empty();
        }

        final List<ParsedRuleToken> tokens = parseRuleTokens(annotationValue);
        if (tokens.isEmpty()) {
            return CompiledLevelRules.empty();
        }

        final List<DependencyRegistry.CompiledDependencyRule> rules =
            new ArrayList<DependencyRegistry.CompiledDependencyRule>(tokens.size());
        final Map<String, DependencyRegistry.CompiledDependencyRule>
            validatedRulesByDeterminant =
                new LinkedHashMap<String, DependencyRegistry.CompiledDependencyRule>();
        boolean ambiguousJoinPath = false;
        for (ParsedRuleToken token : tokens) {
            String ruleValidationCode = null;
            if (token.parseError != null) {
                builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                    DependencyRegistry.DependencyValidationSeverity.WARN,
                    DependencyRegistry.DependencyIssueCodes.INVALID_DEPENDENCY_RULE_SYNTAX,
                    token.parseError,
                    cubeName,
                    dependentLevel.getUniqueName(),
                    "Use [Level Unique Name]|ancestor or |property:PropertyName."));
                continue;
            }

            final ResolvedLevelRef resolved =
                resolveDeterminantLevel(token.determinantLevelRef, lookup);
            if (resolved.ambiguous) {
                builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                    DependencyRegistry.DependencyValidationSeverity.WARN,
                    DependencyRegistry.DependencyIssueCodes.AMBIGUOUS_DEPENDENCY_LEVEL_REF,
                    "Dependency rule references level name '"
                        + token.determinantLevelRef
                        + "' that matches multiple levels.",
                    cubeName,
                    dependentLevel.getUniqueName(),
                    "Use determinant level unique name in drilldown.dependsOn."));
                ruleValidationCode =
                    DependencyRegistry.DependencyIssueCodes.AMBIGUOUS_DEPENDENCY_LEVEL_REF;
            } else if (resolved.level == null) {
                builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                    DependencyRegistry.DependencyValidationSeverity.WARN,
                    DependencyRegistry.DependencyIssueCodes.UNKNOWN_DEPENDENCY_LEVEL_REF,
                    "Dependency rule references unknown level '"
                        + token.determinantLevelRef + "'.",
                    cubeName,
                    dependentLevel.getUniqueName(),
                    "Use an existing level name or unique name."));
                ruleValidationCode =
                    DependencyRegistry.DependencyIssueCodes.UNKNOWN_DEPENDENCY_LEVEL_REF;
            } else if (resolved.matchedByName) {
                builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                    DependencyRegistry.DependencyValidationSeverity.INFO,
                    DependencyRegistry.DependencyIssueCodes.UNQUALIFIED_DEPENDENCY_LEVEL_REF,
                    "Dependency rule references level by name '"
                        + token.determinantLevelRef + "'.",
                    cubeName,
                    dependentLevel.getUniqueName(),
                    "Use determinant level unique name in drilldown.dependsOn."));
            }

            boolean validated = resolved.level != null && !resolved.ambiguous;
            if (validated && token.mappingType
                == DependencyRegistry.DependencyMappingType.ANCESTOR)
            {
                ruleValidationCode = validateAncestorRule(
                    dependentLevel,
                    resolved.level,
                    cubeName,
                    builder);
                validated = ruleValidationCode == null;
            } else if (validated && token.mappingType
                == DependencyRegistry.DependencyMappingType.PROPERTY)
            {
                ruleValidationCode = validatePropertyRule(
                    dependentLevel,
                    token.mappingProperty,
                    cubeName,
                    builder);
                validated = ruleValidationCode == null;
            }

            maybeAddTimeFilterHeuristicIssues(
                dependentLevel,
                resolved.level,
                token,
                validated,
                cubeName,
                hasTimeDimension,
                builder);
            boolean ruleAmbiguousJoinPath = false;
            if (validated
                && token.mappingType == DependencyRegistry.DependencyMappingType.PROPERTY
                && resolved.level != null
                && isCrossHierarchyPropertyRule(dependentLevel, resolved.level))
            {
                final boolean joinPathAmbiguous =
                    !dependentLevel.hasStableSingleTableAnchor()
                        || !resolved.level.hasStableSingleTableAnchor();
                if (joinPathAmbiguous) {
                    ambiguousJoinPath = true;
                    ruleAmbiguousJoinPath = true;
                    validated = false;
                    builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                        DependencyRegistry.DependencyValidationSeverity.WARN,
                        DependencyRegistry.DependencyIssueCodes.AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH,
                        "Cross-hierarchy property dependency rule cannot be "
                            + "safely validated from schema metadata (missing stable table anchor).",
                        cubeName,
                        dependentLevel.getUniqueName(),
                        "Use same-hierarchy ancestor rule, add explicit stable level/table mapping, "
                            + "or disable pruning for this level."));
                    ruleValidationCode =
                        DependencyRegistry.DependencyIssueCodes.AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH;
                }
            }

            final String determinantName =
                resolved.level == null
                    ? token.determinantLevelRef
                    : resolved.level.getUniqueName();
            if (validated) {
                final DependencyRegistry.CompiledDependencyRule existingRule =
                    validatedRulesByDeterminant.get(determinantName);
                if (existingRule != null) {
                    final boolean duplicateRule =
                        sameCompiledRuleSemantics(existingRule, token);
                    builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                        DependencyRegistry.DependencyValidationSeverity.WARN,
                        duplicateRule
                            ? DependencyRegistry.DependencyIssueCodes
                                .DUPLICATE_VALIDATED_DEPENDENCY_RULE
                            : DependencyRegistry.DependencyIssueCodes
                                .CONFLICTING_VALIDATED_DEPENDENCY_RULE,
                        duplicateRule
                            ? "Duplicate validated dependency rule for determinant level '"
                                + determinantName + "'."
                            : "Conflicting validated dependency rules for determinant level '"
                                + determinantName + "'. First rule wins.",
                        cubeName,
                        dependentLevel.getUniqueName(),
                        duplicateRule
                            ? "Remove duplicate rule."
                            : "Keep a single explicit rule per determinant level."));
                    validated = false;
                    ruleValidationCode = duplicateRule
                        ? DependencyRegistry.DependencyIssueCodes
                            .DUPLICATE_VALIDATED_DEPENDENCY_RULE
                        : DependencyRegistry.DependencyIssueCodes
                            .CONFLICTING_VALIDATED_DEPENDENCY_RULE;
                }
            }
            final DependencyRegistry.CompiledDependencyRule compiledRule =
                new DependencyRegistry.CompiledDependencyRule(
                determinantName,
                token.mappingType,
                token.mappingProperty,
                validated,
                token.requiresTimeFilter,
                ruleAmbiguousJoinPath,
                ruleValidationCode);
            rules.add(compiledRule);
            if (compiledRule.isValidated()) {
                validatedRulesByDeterminant.put(determinantName, compiledRule);
            }
        }
        return new CompiledLevelRules(rules, ambiguousJoinPath);
    }

    private boolean isCrossHierarchyPropertyRule(
        RolapLevel dependentLevel,
        RolapLevel determinantLevel)
    {
        if (dependentLevel == null || determinantLevel == null) {
            return false;
        }
        final Hierarchy dependentHierarchy = dependentLevel.getHierarchy();
        final Hierarchy determinantHierarchy = determinantLevel.getHierarchy();
        return dependentHierarchy != null
            && determinantHierarchy != null
            && !dependentHierarchy.equals(determinantHierarchy);
    }

    private boolean sameCompiledRuleSemantics(
        DependencyRegistry.CompiledDependencyRule existingRule,
        ParsedRuleToken token)
    {
        if (existingRule == null || token == null) {
            return false;
        }
        if (existingRule.getMappingType() != token.mappingType) {
            return false;
        }
        if (existingRule.requiresTimeFilter() != token.requiresTimeFilter) {
            return false;
        }
        final String existingProperty = existingRule.getMappingProperty();
        final String tokenProperty = token.mappingProperty;
        if (existingProperty == null) {
            return tokenProperty == null;
        }
        return existingProperty.equals(tokenProperty);
    }

    private String getDependsOnAnnotationValue(RolapLevel level) {
        if (level == null) {
            return null;
        }
        final Map<String, Annotation> annotationMap = level.getAnnotationMap();
        if (annotationMap == null || annotationMap.isEmpty()) {
            return null;
        }
        final Annotation annotation =
            annotationMap.get(CrossJoinDependencyPruner.DEPENDS_ON_ANNOTATION);
        if (annotation == null || annotation.getValue() == null) {
            return null;
        }
        return String.valueOf(annotation.getValue());
    }

    private String validateAncestorRule(
        RolapLevel dependentLevel,
        RolapLevel determinantLevel,
        String cubeName,
        DependencyRegistry.Builder builder)
    {
        final Hierarchy dependentHierarchy = dependentLevel.getHierarchy();
        final Hierarchy determinantHierarchy = determinantLevel.getHierarchy();
        if (dependentHierarchy != null
            && dependentHierarchy.equals(determinantHierarchy)
            && dependentLevel.getDepth() > determinantLevel.getDepth())
        {
            return null;
        }
        builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
            DependencyRegistry.DependencyValidationSeverity.WARN,
            DependencyRegistry.DependencyIssueCodes.INVALID_ANCESTOR_DEPENDENCY_RULE,
            "Ancestor dependency requires determinant level to be an ancestor "
                + "in the same hierarchy.",
            cubeName,
            dependentLevel.getUniqueName(),
            "Use same-hierarchy ancestor level or switch to property: mapping."));
        return DependencyRegistry.DependencyIssueCodes.INVALID_ANCESTOR_DEPENDENCY_RULE;
    }

    private String validatePropertyRule(
        RolapLevel dependentLevel,
        String propertyName,
        String cubeName,
        DependencyRegistry.Builder builder)
    {
        if (propertyName == null || propertyName.isEmpty()) {
            builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                DependencyRegistry.DependencyValidationSeverity.WARN,
                DependencyRegistry.DependencyIssueCodes.INVALID_PROPERTY_DEPENDENCY_RULE,
                "Property dependency rule does not specify a property name.",
                cubeName,
                dependentLevel.getUniqueName(),
                "Use property:PropertyName."));
            return DependencyRegistry.DependencyIssueCodes.INVALID_PROPERTY_DEPENDENCY_RULE;
        }
        if (!dependentLevel.hasMemberProperty(propertyName)) {
            builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                DependencyRegistry.DependencyValidationSeverity.WARN,
                DependencyRegistry.DependencyIssueCodes.UNKNOWN_DEPENDENCY_PROPERTY,
                "Dependency rule references missing property '" + propertyName + "'.",
                cubeName,
                dependentLevel.getUniqueName(),
                "Declare the member property on the dependent level."));
            return DependencyRegistry.DependencyIssueCodes.UNKNOWN_DEPENDENCY_PROPERTY;
        }
        if (!dependentLevel.isMemberPropertyFunctionallyDependent(propertyName)) {
            builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                DependencyRegistry.DependencyValidationSeverity.WARN,
                DependencyRegistry.DependencyIssueCodes
                    .PROPERTY_NOT_FUNCTIONALLY_DEPENDENT,
                "Property '" + propertyName + "' is not marked dependsOnLevelValue=true.",
                cubeName,
                dependentLevel.getUniqueName(),
                "Set dependsOnLevelValue=\"true\" for safe dependency pruning."));
            return DependencyRegistry.DependencyIssueCodes
                .PROPERTY_NOT_FUNCTIONALLY_DEPENDENT;
        }
        return null;
    }

    private ResolvedLevelRef resolveDeterminantLevel(
        String determinantLevelRef,
        LevelLookup lookup)
    {
        if (determinantLevelRef == null || determinantLevelRef.isEmpty()) {
            return ResolvedLevelRef.missing();
        }
        final RolapLevel byUniqueName = lookup.byUniqueName.get(determinantLevelRef);
        if (byUniqueName != null) {
            return ResolvedLevelRef.resolved(byUniqueName, false);
        }
        final List<RolapLevel> byName = lookup.byName.get(determinantLevelRef);
        if (byName == null || byName.isEmpty()) {
            return ResolvedLevelRef.missing();
        }
        if (byName.size() == 1) {
            return ResolvedLevelRef.resolved(byName.get(0), true);
        }
        return ResolvedLevelRef.ambiguous();
    }

    private void maybeAddTimeFilterHeuristicIssues(
        RolapLevel dependentLevel,
        RolapLevel determinantLevel,
        ParsedRuleToken token,
        boolean validated,
        String cubeName,
        boolean hasTimeDimension,
        DependencyRegistry.Builder builder)
    {
        if (token == null || dependentLevel == null) {
            return;
        }
        if (token.requiresTimeFilter && !hasTimeDimension) {
            builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                DependencyRegistry.DependencyValidationSeverity.WARN,
                DependencyRegistry.DependencyIssueCodes
                    .REQUIRES_TIME_FILTER_WITHOUT_TIME_DIMENSION,
                "Dependency rule requires time filter but cube has no Time dimension.",
                cubeName,
                dependentLevel.getUniqueName(),
                "Remove requiresTimeFilter or add a Time dimension to the cube."));
        }
        if (!validated
            || token.requiresTimeFilter
            || token.mappingType != DependencyRegistry.DependencyMappingType.PROPERTY
            || determinantLevel == null)
        {
            return;
        }
        final Hierarchy dependentHierarchy = dependentLevel.getHierarchy();
        final Hierarchy determinantHierarchy = determinantLevel.getHierarchy();
        if (dependentHierarchy != null
            && determinantHierarchy != null
            && !dependentHierarchy.equals(determinantHierarchy))
        {
            builder.addIssue(new DependencyRegistry.DependencyValidationIssue(
                DependencyRegistry.DependencyValidationSeverity.INFO,
                DependencyRegistry.DependencyIssueCodes
                    .CROSS_HIERARCHY_PROPERTY_RULE_WITHOUT_TIME_FILTER,
                "Cross-hierarchy property dependency rule does not require a time filter.",
                cubeName,
                dependentLevel.getUniqueName(),
                "Consider adding |requiresTimeFilter for SCD/history-safe pruning."));
        }
    }

    private boolean hasTimeDimension(List<RolapLevel> levels) {
        for (RolapLevel level : levels) {
            if (level == null
                || level.getHierarchy() == null
                || level.getHierarchy().getDimension() == null)
            {
                continue;
            }
            if (level.getHierarchy().getDimension().getDimensionType()
                == DimensionType.TimeDimension)
            {
                return true;
            }
        }
        return false;
    }

    private List<ParsedRuleToken> parseRuleTokens(String annotationValue) {
        final String[] rawTokens = annotationValue.split("[;,]");
        final List<ParsedRuleToken> result =
            new ArrayList<ParsedRuleToken>(rawTokens.length);
        for (String rawToken : rawTokens) {
            final ParsedRuleToken parsed = parseRuleToken(rawToken);
            if (parsed != null) {
                result.add(parsed);
            }
        }
        return result;
    }

    private ParsedRuleToken parseRuleToken(String token) {
        if (token == null) {
            return null;
        }
        final String trimmedToken = token.trim();
        if (trimmedToken.isEmpty()) {
            return null;
        }

        final String[] segments = trimmedToken.split("\\|");
        if (segments.length == 0) {
            return null;
        }

        final String determinantLevelRef = segments[0].trim();
        if (segments.length == 1) {
            return new ParsedRuleToken(
                determinantLevelRef,
                DependencyRegistry.DependencyMappingType.ANCESTOR,
                null,
                false,
                null);
        }
        if (determinantLevelRef.isEmpty()) {
            return ParsedRuleToken.error(
                "Dependency rule is missing determinant level reference: " + trimmedToken);
        }

        DependencyRegistry.DependencyMappingType mappingType =
            DependencyRegistry.DependencyMappingType.ANCESTOR;
        String mappingProperty = null;
        boolean mappingSpecified = false;
        boolean requiresTimeFilter = false;

        for (int i = 1; i < segments.length; i++) {
            final String segment = segments[i] == null
                ? ""
                : segments[i].trim();
            if (segment.isEmpty()) {
                continue;
            }
            final String lowerSegment = segment.toLowerCase();

            if (!mappingSpecified
                && ("ancestor".equals(lowerSegment)
                    || lowerSegment.startsWith("property:")
                    || lowerSegment.startsWith("property=")))
            {
                mappingSpecified = true;
                if ("ancestor".equals(lowerSegment)) {
                    mappingType = DependencyRegistry.DependencyMappingType.ANCESTOR;
                    mappingProperty = null;
                    continue;
                }
                final String propertyName = segment.substring(9).trim();
                if (propertyName.isEmpty()) {
                    return ParsedRuleToken.error(
                        "Property dependency rule is missing property name: "
                            + trimmedToken);
                }
                mappingType = DependencyRegistry.DependencyMappingType.PROPERTY;
                mappingProperty = propertyName;
                continue;
            }

            if (isRequiresTimeFilterOption(lowerSegment)) {
                requiresTimeFilter = parseRequiresTimeFilterValue(lowerSegment);
                continue;
            }

            if (!mappingSpecified) {
                return ParsedRuleToken.error(
                    "Unsupported dependency mapping '" + segment + "' in rule: "
                        + trimmedToken);
            }
            return ParsedRuleToken.error(
                "Unsupported dependency option '" + segment + "' in rule: "
                    + trimmedToken);
        }
        return new ParsedRuleToken(
            determinantLevelRef,
            mappingType,
            mappingProperty,
            requiresTimeFilter,
            null);
    }

    private boolean isRequiresTimeFilterOption(String lowerSegment) {
        return "requirestimefilter".equals(lowerSegment)
            || "requirestimefilter=true".equals(lowerSegment)
            || "requirestimefilter=false".equals(lowerSegment);
    }

    private boolean parseRequiresTimeFilterValue(String lowerSegment) {
        return !"requirestimefilter=false".equals(lowerSegment);
    }

    private static final class LevelLookup {
        private final Map<String, RolapLevel> byUniqueName;
        private final Map<String, List<RolapLevel>> byName;

        private LevelLookup(
            Map<String, RolapLevel> byUniqueName,
            Map<String, List<RolapLevel>> byName)
        {
            this.byUniqueName = byUniqueName;
            this.byName = byName;
        }
    }

    private static final class ResolvedLevelRef {
        private final RolapLevel level;
        private final boolean ambiguous;
        private final boolean matchedByName;

        private ResolvedLevelRef(
            RolapLevel level,
            boolean ambiguous,
            boolean matchedByName)
        {
            this.level = level;
            this.ambiguous = ambiguous;
            this.matchedByName = matchedByName;
        }

        private static ResolvedLevelRef resolved(
            RolapLevel level,
            boolean matchedByName)
        {
            return new ResolvedLevelRef(level, false, matchedByName);
        }

        private static ResolvedLevelRef ambiguous() {
            return new ResolvedLevelRef(null, true, false);
        }

        private static ResolvedLevelRef missing() {
            return new ResolvedLevelRef(null, false, false);
        }
    }

    private static final class ParsedRuleToken {
        private final String determinantLevelRef;
        private final DependencyRegistry.DependencyMappingType mappingType;
        private final String mappingProperty;
        private final boolean requiresTimeFilter;
        private final String parseError;

        private ParsedRuleToken(
            String determinantLevelRef,
            DependencyRegistry.DependencyMappingType mappingType,
            String mappingProperty,
            boolean requiresTimeFilter,
            String parseError)
        {
            this.determinantLevelRef = determinantLevelRef;
            this.mappingType = mappingType;
            this.mappingProperty = mappingProperty;
            this.requiresTimeFilter = requiresTimeFilter;
            this.parseError = parseError;
        }

        private static ParsedRuleToken error(String parseError) {
            return new ParsedRuleToken(
                null,
                DependencyRegistry.DependencyMappingType.ANCESTOR,
                null,
                false,
                parseError);
        }
    }

    private static final class CompiledLevelRules {
        private final List<DependencyRegistry.CompiledDependencyRule> rules;
        private final boolean ambiguousJoinPath;

        private CompiledLevelRules(
            List<DependencyRegistry.CompiledDependencyRule> rules,
            boolean ambiguousJoinPath)
        {
            this.rules = rules == null
                ? Collections.<DependencyRegistry.CompiledDependencyRule>emptyList()
                : rules;
            this.ambiguousJoinPath = ambiguousJoinPath;
        }

        private static CompiledLevelRules empty() {
            return new CompiledLevelRules(
                Collections.<DependencyRegistry.CompiledDependencyRule>emptyList(),
                false);
        }
    }
}
