/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.nativedispatch;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable, hierarchy-keyed snapshot of an MDX query's dimensional
 * shape — the minimal semantic payload that dispatch rules need so
 * they stay declarative and never re-inspect raw MDX.
 *
 * <p><b>Normalized semantic snapshot.</b>  This class is a pure
 * value object that holds the pre-analyzed query shape.  Extraction
 * of this shape from a live {@code Query} / evaluator belongs in a
 * separate analyzer component (e.g. {@code NativeQueryShapeAnalyzer},
 * to be introduced in a follow-up PR).  Do not grow parsing or
 * query-inspection logic into this class.
 *
 * <p><b>Analyzer output, not hand-authored production state.</b>
 * Production code should normally obtain instances from a query-shape
 * analyzer rather than hand-assembling them via the {@link Builder}.
 * The builder API exists primarily for the analyzer implementation
 * and for tests.
 *
 * <p>Design contracts:
 * <ol>
 *   <li><b>Cheap query-level VO.</b>  Built once per query, read many
 *       times by rules.  No cube/star-schema objects leak in — all
 *       identity is unique-name strings.</li>
 *   <li><b>Hierarchy-keyed.</b>  Keyed by hierarchy unique name.  Each
 *       entry is a {@link HierarchyPresence} that captures <em>state</em>
 *       (constrained? projected? what cardinality? what level?) rather
 *       than raw shape fragments.</li>
 *   <li><b>Rules read, never write.</b>  The shape is built by the
 *       analyzer layer and handed to rules as read-only context.</li>
 * </ol>
 */
public final class NativeQueryShape {

    private final Map<String, HierarchyPresence> hierarchies;
    private final Set<String> requestedMeasureUniqueNames;
    private final int nonEmptyAxisCount;

    private NativeQueryShape(Builder builder) {
        this.hierarchies = builder.hierarchies.isEmpty()
            ? Collections.<String, HierarchyPresence>emptyMap()
            : Collections.unmodifiableMap(
                new LinkedHashMap<String, HierarchyPresence>(
                    builder.hierarchies));
        this.requestedMeasureUniqueNames =
            builder.requestedMeasureUniqueNames.isEmpty()
                ? Collections.<String>emptySet()
                : Collections.unmodifiableSet(
                    new LinkedHashSet<String>(
                        builder.requestedMeasureUniqueNames));
        this.nonEmptyAxisCount = builder.nonEmptyAxisCount;
    }

    // -- accessors --

    /**
     * Hierarchy-keyed map of dimensional presences.
     *
     * <p>Keys are hierarchy unique names.  Iteration order matches the
     * order in which hierarchies were added (typically: axes left to
     * right, then slicer, then formula references).
     */
    public Map<String, HierarchyPresence> hierarchies() {
        return hierarchies;
    }

    /**
     * Looks up the presence for a specific hierarchy.
     *
     * @return the {@link HierarchyPresence}, or {@code null} if the
     *         hierarchy does not participate in this query
     */
    public HierarchyPresence hierarchy(String hierarchyUniqueName) {
        return hierarchies.get(hierarchyUniqueName);
    }

    /**
     * Unique names of the measures requested by the query.
     *
     * <p>For dispatch purposes, knowing <em>which</em> measures are
     * requested is usually sufficient; the full classification is in
     * {@code MeasureClassifier.Candidate}.
     */
    public Set<String> requestedMeasureUniqueNames() {
        return requestedMeasureUniqueNames;
    }

    /**
     * Number of axes that carry a NON EMPTY modifier.
     */
    public int nonEmptyAxisCount() {
        return nonEmptyAxisCount;
    }

    // -- convenience queries --

    /** Whether any hierarchy is constrained. */
    public boolean hasConstrainedHierarchy() {
        for (HierarchyPresence hp : hierarchies.values()) {
            if (hp.constrained()) {
                return true;
            }
        }
        return false;
    }

    /** Whether any hierarchy is a set expression. */
    public boolean hasSetExpression() {
        for (HierarchyPresence hp : hierarchies.values()) {
            if (hp.isSetExpression()) {
                return true;
            }
        }
        return false;
    }

    /** Whether the query requests at least one measure. */
    public boolean hasMeasures() {
        return !requestedMeasureUniqueNames.isEmpty();
    }

    /** Total number of hierarchies participating in the query. */
    public int hierarchyCount() {
        return hierarchies.size();
    }

    // -- Object contract --

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NativeQueryShape)) {
            return false;
        }
        NativeQueryShape that = (NativeQueryShape) o;
        return nonEmptyAxisCount == that.nonEmptyAxisCount
            && hierarchies.equals(that.hierarchies)
            && requestedMeasureUniqueNames.equals(
                that.requestedMeasureUniqueNames);
    }

    @Override
    public int hashCode() {
        int h = hierarchies.hashCode();
        h = 31 * h + requestedMeasureUniqueNames.hashCode();
        h = 31 * h + nonEmptyAxisCount;
        return h;
    }

    @Override
    public String toString() {
        return "NativeQueryShape{"
            + "hierarchies=" + hierarchies.size()
            + ", measures=" + requestedMeasureUniqueNames.size()
            + ", nonEmptyAxes=" + nonEmptyAxisCount
            + "}";
    }

    // -- Builder --

    /** Creates a new builder. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Mutable builder for {@link NativeQueryShape}.
     */
    public static final class Builder {
        private final LinkedHashMap<String, HierarchyPresence> hierarchies =
            new LinkedHashMap<String, HierarchyPresence>();
        private final LinkedHashSet<String> requestedMeasureUniqueNames =
            new LinkedHashSet<String>();
        private int nonEmptyAxisCount;

        private Builder() {}

        /**
         * Adds a hierarchy presence.  If a presence with the same unique
         * name already exists, it is replaced (last-write-wins).
         */
        public Builder addHierarchy(HierarchyPresence presence) {
            Objects.requireNonNull(presence, "presence");
            hierarchies.put(presence.hierarchyUniqueName(), presence);
            return this;
        }

        /** Adds a requested measure by unique name. */
        public Builder addMeasure(String measureUniqueName) {
            Objects.requireNonNull(measureUniqueName, "measureUniqueName");
            requestedMeasureUniqueNames.add(measureUniqueName);
            return this;
        }

        /** Sets the number of NON EMPTY axes. */
        public Builder nonEmptyAxisCount(int count) {
            this.nonEmptyAxisCount = count;
            return this;
        }

        public NativeQueryShape build() {
            return new NativeQueryShape(this);
        }
    }
}

// End NativeQueryShape.java
