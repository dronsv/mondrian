/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable semantic snapshot of how one hierarchy participates in a
 * query, capturing enough state so that dispatch rules stay declarative
 * and never need to re-inspect raw MDX.
 *
 * <p>Design contracts:
 * <ol>
 *   <li><b>State, not shape fragments.</b>  Each field answers a
 *       semantic question ("is this hierarchy constrained?") rather
 *       than exposing a syntactic detail ("what axis is it on?").
 *       {@link #locations} is the only field that maps directly to
 *       syntactic position; all others are derived meaning.</li>
 *   <li><b>[All] ≠ single member.</b>  {@link MemberCardinality#ALL_MEMBER}
 *       means <em>unconstrained</em>.  {@link #constrained} is
 *       {@code false} for {@code ALL_MEMBER} and {@code true} for
 *       {@code SINGLE_MEMBER}.  Rules must never conflate the two.</li>
 *   <li><b>Honest imprecision for set expressions.</b>  When
 *       {@link #memberCardinality} is
 *       {@link MemberCardinality#SET_EXPRESSION}, {@link #activeLevel}
 *       may be {@link LevelRef#NONE} — consumers must not assume a
 *       single determinable level.</li>
 *   <li><b>Cheap query-level VO.</b>  No cube/star-schema objects leak
 *       in.  Hierarchy identity is a unique-name string.</li>
 * </ol>
 */
public final class HierarchyPresence {

    private final String hierarchyUniqueName;
    private final Set<QueryLocation> locations;
    private final LevelRef activeLevel;
    private final MemberCardinality memberCardinality;
    private final boolean projected;
    private final boolean constrained;

    private HierarchyPresence(Builder builder) {
        this.hierarchyUniqueName =
            Objects.requireNonNull(
                builder.hierarchyUniqueName, "hierarchyUniqueName");
        this.locations = builder.locations.isEmpty()
            ? Collections.<QueryLocation>emptySet()
            : Collections.unmodifiableSet(
                EnumSet.copyOf(builder.locations));
        this.activeLevel = builder.activeLevel == null
            ? LevelRef.NONE
            : builder.activeLevel;
        this.memberCardinality = builder.memberCardinality == null
            ? MemberCardinality.UNKNOWN
            : builder.memberCardinality;
        this.projected = builder.projected;
        this.constrained = builder.constrained;
    }

    // -- accessors --

    /** Unique name of the hierarchy (identity key for the shape map). */
    public String hierarchyUniqueName() {
        return hierarchyUniqueName;
    }

    /**
     * Where in the query this hierarchy appears.  A hierarchy may appear
     * in multiple locations (e.g. ROWS and CALCULATED_MEMBER_FORMULA).
     * Never null; may be empty if the hierarchy is implied but not
     * syntactically referenced.
     */
    public Set<QueryLocation> locations() {
        return locations;
    }

    /**
     * The active (implied or explicit) level, if determinable.
     *
     * <p>For {@link MemberCardinality#ALL_MEMBER} this is the [All]
     * level (depth 0).  For {@link MemberCardinality#SINGLE_MEMBER}
     * it is the level of that member.  For
     * {@link MemberCardinality#SET_EXPRESSION} it may be
     * {@link LevelRef#NONE} — no single level is guaranteed.
     */
    public LevelRef activeLevel() {
        return activeLevel;
    }

    /**
     * The member cardinality / kind of reference.
     *
     * <p>Key semantic distinction: {@link MemberCardinality#ALL_MEMBER}
     * means absence of constraint (unconstrained), while
     * {@link MemberCardinality#SINGLE_MEMBER} means presence of a
     * specific constraint.
     */
    public MemberCardinality memberCardinality() {
        return memberCardinality;
    }

    /**
     * Whether this hierarchy is projected onto a visible axis
     * (COLUMNS, ROWS, etc.) — i.e. its members appear in the result
     * set.  A hierarchy present only in the slicer or in a calculated
     * member formula is typically <em>not</em> projected.
     */
    public boolean projected() {
        return projected;
    }

    /**
     * Whether this hierarchy imposes an effective constraint on the
     * query.
     *
     * <p>{@code false} when the member cardinality is
     * {@link MemberCardinality#ALL_MEMBER} (the hierarchy is
     * unconstrained).  {@code true} for {@code SINGLE_MEMBER},
     * {@code MULTI_MEMBER}, and most {@code SET_EXPRESSION} cases.
     * May be {@code false} for {@code SET_EXPRESSION} if the set is
     * known to be equivalent to "all members".
     */
    public boolean constrained() {
        return constrained;
    }

    // -- convenience queries --

    /** Shorthand: is the hierarchy on the slicer (WHERE clause)? */
    public boolean onSlicer() {
        return locations.contains(QueryLocation.SLICER);
    }

    /** Shorthand: is the hierarchy on a visible axis (COLUMNS or ROWS)? */
    public boolean onVisibleAxis() {
        return locations.contains(QueryLocation.COLUMNS)
            || locations.contains(QueryLocation.ROWS);
    }

    /** Shorthand: is the member cardinality {@code ALL_MEMBER}? */
    public boolean isAllMember() {
        return memberCardinality == MemberCardinality.ALL_MEMBER;
    }

    /** Shorthand: is the member cardinality {@code SET_EXPRESSION}? */
    public boolean isSetExpression() {
        return memberCardinality == MemberCardinality.SET_EXPRESSION;
    }

    // -- Object contract --

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HierarchyPresence)) {
            return false;
        }
        HierarchyPresence that = (HierarchyPresence) o;
        return projected == that.projected
            && constrained == that.constrained
            && hierarchyUniqueName.equals(that.hierarchyUniqueName)
            && locations.equals(that.locations)
            && activeLevel.equals(that.activeLevel)
            && memberCardinality == that.memberCardinality;
    }

    @Override
    public int hashCode() {
        int h = hierarchyUniqueName.hashCode();
        h = 31 * h + locations.hashCode();
        h = 31 * h + activeLevel.hashCode();
        h = 31 * h + memberCardinality.hashCode();
        h = 31 * h + Boolean.hashCode(projected);
        h = 31 * h + Boolean.hashCode(constrained);
        return h;
    }

    @Override
    public String toString() {
        return "HierarchyPresence{"
            + hierarchyUniqueName
            + ", " + memberCardinality
            + ", locations=" + locations
            + ", level=" + activeLevel
            + ", projected=" + projected
            + ", constrained=" + constrained
            + "}";
    }

    // -- Builder --

    /**
     * Creates a new builder for {@code HierarchyPresence}.
     *
     * @param hierarchyUniqueName  unique name of the hierarchy; required
     */
    public static Builder builder(String hierarchyUniqueName) {
        return new Builder(hierarchyUniqueName);
    }

    /**
     * Mutable builder for {@link HierarchyPresence}.
     */
    public static final class Builder {
        private final String hierarchyUniqueName;
        private final EnumSet<QueryLocation> locations =
            EnumSet.noneOf(QueryLocation.class);
        private LevelRef activeLevel;
        private MemberCardinality memberCardinality;
        private boolean projected;
        private boolean constrained;

        private Builder(String hierarchyUniqueName) {
            this.hierarchyUniqueName = hierarchyUniqueName;
        }

        public Builder addLocation(QueryLocation location) {
            if (location != null) {
                locations.add(location);
            }
            return this;
        }

        public Builder activeLevel(LevelRef activeLevel) {
            this.activeLevel = activeLevel;
            return this;
        }

        public Builder memberCardinality(MemberCardinality cardinality) {
            this.memberCardinality = cardinality;
            return this;
        }

        public Builder projected(boolean projected) {
            this.projected = projected;
            return this;
        }

        public Builder constrained(boolean constrained) {
            this.constrained = constrained;
            return this;
        }

        public HierarchyPresence build() {
            return new HierarchyPresence(this);
        }
    }
}

// End HierarchyPresence.java
