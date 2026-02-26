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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Precompiled dependency metadata for one cube.
 *
 * <p>V2 skeleton: currently holds immutable descriptors and validation issues,
 * and is designed to be built once at schema/cube load time.</p>
 */
public final class DependencyRegistry {
    private final String cubeUniqueName;
    private final DependencyPruningPolicy policy;
    private final Map<String, LevelDependencyDescriptor> levelDescriptors;
    private final List<DependencyValidationIssue> issues;

    private DependencyRegistry(Builder builder) {
        this.cubeUniqueName = builder.cubeUniqueName;
        this.policy = builder.policy;
        this.levelDescriptors = Collections.unmodifiableMap(
            new LinkedHashMap<String, LevelDependencyDescriptor>(
                builder.levelDescriptors));
        this.issues = Collections.unmodifiableList(
            new ArrayList<DependencyValidationIssue>(builder.issues));
    }

    public static Builder builder(String cubeUniqueName) {
        return new Builder(cubeUniqueName);
    }

    public String getCubeUniqueName() {
        return cubeUniqueName;
    }

    public DependencyPruningPolicy getPolicy() {
        return policy;
    }

    public Map<String, LevelDependencyDescriptor> getLevelDescriptors() {
        return levelDescriptors;
    }

    public LevelDependencyDescriptor getLevelDescriptor(String levelUniqueName) {
        return levelDescriptors.get(levelUniqueName);
    }

    public List<DependencyValidationIssue> getIssues() {
        return issues;
    }

    public boolean hasFatalIssues() {
        for (DependencyValidationIssue issue : issues) {
            if (issue.getSeverity() == DependencyValidationSeverity.FATAL) {
                return true;
            }
        }
        return false;
    }

    public static final class Builder {
        private final String cubeUniqueName;
        private DependencyPruningPolicy policy = DependencyPruningPolicy.RELAXED;
        private final Map<String, LevelDependencyDescriptor> levelDescriptors =
            new LinkedHashMap<String, LevelDependencyDescriptor>();
        private final List<DependencyValidationIssue> issues =
            new ArrayList<DependencyValidationIssue>();

        private Builder(String cubeUniqueName) {
            this.cubeUniqueName = cubeUniqueName;
        }

        public Builder policy(DependencyPruningPolicy policy) {
            if (policy != null) {
                this.policy = policy;
            }
            return this;
        }

        public Builder addLevelDescriptor(LevelDependencyDescriptor descriptor) {
            if (descriptor != null && descriptor.getLevelUniqueName() != null) {
                levelDescriptors.put(descriptor.getLevelUniqueName(), descriptor);
            }
            return this;
        }

        public Builder addIssue(DependencyValidationIssue issue) {
            if (issue != null) {
                issues.add(issue);
            }
            return this;
        }

        public DependencyRegistry build() {
            return new DependencyRegistry(this);
        }
    }

    public enum DependencyPruningPolicy {
        OFF,
        STRICT,
        RELAXED
    }

    public enum DependencyValidationSeverity {
        INFO,
        WARN,
        FATAL
    }

    public enum DependencyMappingType {
        ANCESTOR,
        PROPERTY
    }

    public static final class DependencyIssueCodes {
        public static final String INVALID_DEPENDENCY_RULE_SYNTAX =
            "INVALID_DEPENDENCY_RULE_SYNTAX";
        public static final String AMBIGUOUS_DEPENDENCY_LEVEL_REF =
            "AMBIGUOUS_DEPENDENCY_LEVEL_REF";
        public static final String UNKNOWN_DEPENDENCY_LEVEL_REF =
            "UNKNOWN_DEPENDENCY_LEVEL_REF";
        public static final String UNQUALIFIED_DEPENDENCY_LEVEL_REF =
            "UNQUALIFIED_DEPENDENCY_LEVEL_REF";
        public static final String INVALID_ANCESTOR_DEPENDENCY_RULE =
            "INVALID_ANCESTOR_DEPENDENCY_RULE";
        public static final String INVALID_PROPERTY_DEPENDENCY_RULE =
            "INVALID_PROPERTY_DEPENDENCY_RULE";
        public static final String UNKNOWN_DEPENDENCY_PROPERTY =
            "UNKNOWN_DEPENDENCY_PROPERTY";
        public static final String PROPERTY_NOT_FUNCTIONALLY_DEPENDENT =
            "PROPERTY_NOT_FUNCTIONALLY_DEPENDENT";
        public static final String REQUIRES_TIME_FILTER_WITHOUT_TIME_DIMENSION =
            "REQUIRES_TIME_FILTER_WITHOUT_TIME_DIMENSION";
        public static final String CROSS_HIERARCHY_PROPERTY_RULE_WITHOUT_TIME_FILTER =
            "CROSS_HIERARCHY_PROPERTY_RULE_WITHOUT_TIME_FILTER";
        public static final String AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH =
            "AMBIGUOUS_CROSS_HIERARCHY_JOIN_PATH";
        public static final String DUPLICATE_VALIDATED_DEPENDENCY_RULE =
            "DUPLICATE_VALIDATED_DEPENDENCY_RULE";
        public static final String CONFLICTING_VALIDATED_DEPENDENCY_RULE =
            "CONFLICTING_VALIDATED_DEPENDENCY_RULE";
        public static final String RUNTIME_MISSING_REQUIRED_TIME_FILTER =
            "RUNTIME_MISSING_REQUIRED_TIME_FILTER";
        public static final String RUNTIME_EXPLICIT_RULE_KEY_DERIVATION_FAILED =
            "RUNTIME_EXPLICIT_RULE_KEY_DERIVATION_FAILED";

        private DependencyIssueCodes() {
        }
    }

    public static final class CompiledDependencyRule {
        private final String determinantLevelName;
        private final DependencyMappingType mappingType;
        private final String mappingProperty;
        private final boolean validated;
        private final boolean requiresTimeFilter;
        private final boolean ambiguousJoinPath;
        private final String validationCode;

        public CompiledDependencyRule(
            String determinantLevelName,
            DependencyMappingType mappingType,
            String mappingProperty,
            boolean validated,
            boolean requiresTimeFilter)
        {
            this(
                determinantLevelName,
                mappingType,
                mappingProperty,
                validated,
                requiresTimeFilter,
                false,
                null);
        }

        public CompiledDependencyRule(
            String determinantLevelName,
            DependencyMappingType mappingType,
            String mappingProperty,
            boolean validated,
            boolean requiresTimeFilter,
            boolean ambiguousJoinPath)
        {
            this(
                determinantLevelName,
                mappingType,
                mappingProperty,
                validated,
                requiresTimeFilter,
                ambiguousJoinPath,
                null);
        }

        public CompiledDependencyRule(
            String determinantLevelName,
            DependencyMappingType mappingType,
            String mappingProperty,
            boolean validated,
            boolean requiresTimeFilter,
            boolean ambiguousJoinPath,
            String validationCode)
        {
            this.determinantLevelName = determinantLevelName;
            this.mappingType = mappingType;
            this.mappingProperty = mappingProperty;
            this.validated = validated;
            this.requiresTimeFilter = requiresTimeFilter;
            this.ambiguousJoinPath = ambiguousJoinPath;
            this.validationCode = validationCode;
        }

        public String getDeterminantLevelName() {
            return determinantLevelName;
        }

        public DependencyMappingType getMappingType() {
            return mappingType;
        }

        public String getMappingProperty() {
            return mappingProperty;
        }

        public boolean isValidated() {
            return validated;
        }

        public boolean requiresTimeFilter() {
            return requiresTimeFilter;
        }

        public boolean isAmbiguousJoinPath() {
            return ambiguousJoinPath;
        }

        public String getValidationCode() {
            return validationCode;
        }
    }

    public static final class LevelDependencyDescriptor {
        private final String levelUniqueName;
        private final String hierarchyUniqueName;
        private final int depth;
        private final List<CompiledDependencyRule> rules;
        private final boolean ambiguousJoinPath;
        private final boolean chainDeclared;

        public LevelDependencyDescriptor(
            String levelUniqueName,
            String hierarchyUniqueName,
            int depth,
            List<CompiledDependencyRule> rules,
            boolean ambiguousJoinPath)
        {
            this(
                levelUniqueName,
                hierarchyUniqueName,
                depth,
                rules,
                ambiguousJoinPath,
                false);
        }

        public LevelDependencyDescriptor(
            String levelUniqueName,
            String hierarchyUniqueName,
            int depth,
            List<CompiledDependencyRule> rules,
            boolean ambiguousJoinPath,
            boolean chainDeclared)
        {
            this.levelUniqueName = levelUniqueName;
            this.hierarchyUniqueName = hierarchyUniqueName;
            this.depth = depth;
            this.rules = rules == null
                ? Collections.<CompiledDependencyRule>emptyList()
                : Collections.unmodifiableList(
                    new ArrayList<CompiledDependencyRule>(rules));
            this.ambiguousJoinPath = ambiguousJoinPath;
            this.chainDeclared = chainDeclared;
        }

        public String getLevelUniqueName() {
            return levelUniqueName;
        }

        public String getHierarchyUniqueName() {
            return hierarchyUniqueName;
        }

        public int getDepth() {
            return depth;
        }

        public List<CompiledDependencyRule> getRules() {
            return rules;
        }

        public boolean isAmbiguousJoinPath() {
            return ambiguousJoinPath;
        }

        public boolean isChainDeclared() {
            return chainDeclared;
        }
    }

    public static final class DependencyValidationIssue {
        private final DependencyValidationSeverity severity;
        private final String code;
        private final String message;
        private final String cube;
        private final String level;
        private final String recommendation;

        public DependencyValidationIssue(
            DependencyValidationSeverity severity,
            String code,
            String message,
            String cube,
            String level,
            String recommendation)
        {
            this.severity = severity;
            this.code = code;
            this.message = message;
            this.cube = cube;
            this.level = level;
            this.recommendation = recommendation;
        }

        public DependencyValidationSeverity getSeverity() {
            return severity;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        public String getCube() {
            return cube;
        }

        public String getLevel() {
            return level;
        }

        public String getRecommendation() {
            return recommendation;
        }
    }
}
