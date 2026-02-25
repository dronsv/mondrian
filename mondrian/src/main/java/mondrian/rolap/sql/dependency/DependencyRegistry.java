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

    public static final class CompiledDependencyRule {
        private final String determinantLevelName;
        private final DependencyMappingType mappingType;
        private final String mappingProperty;
        private final boolean validated;
        private final boolean requiresTimeFilter;

        public CompiledDependencyRule(
            String determinantLevelName,
            DependencyMappingType mappingType,
            String mappingProperty,
            boolean validated,
            boolean requiresTimeFilter)
        {
            this.determinantLevelName = determinantLevelName;
            this.mappingType = mappingType;
            this.mappingProperty = mappingProperty;
            this.validated = validated;
            this.requiresTimeFilter = requiresTimeFilter;
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
    }

    public static final class LevelDependencyDescriptor {
        private final String levelUniqueName;
        private final String hierarchyUniqueName;
        private final int depth;
        private final List<CompiledDependencyRule> rules;
        private final boolean ambiguousJoinPath;

        public LevelDependencyDescriptor(
            String levelUniqueName,
            String hierarchyUniqueName,
            int depth,
            List<CompiledDependencyRule> rules,
            boolean ambiguousJoinPath)
        {
            this.levelUniqueName = levelUniqueName;
            this.hierarchyUniqueName = hierarchyUniqueName;
            this.depth = depth;
            this.rules = rules == null
                ? Collections.<CompiledDependencyRule>emptyList()
                : Collections.unmodifiableList(
                    new ArrayList<CompiledDependencyRule>(rules));
            this.ambiguousJoinPath = ambiguousJoinPath;
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

