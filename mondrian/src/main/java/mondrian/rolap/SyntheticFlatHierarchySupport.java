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

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.olap.Property;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Public utility methods for working with {@link SyntheticFlatHierarchy}
 * instances across package boundaries. The same primitives are used by
 * {@link mondrian.rolap.sql.CrossJoinDependencyPruner} (for cross-arg
 * pruning during native CrossJoin) and by
 * {@link mondrian.olap.fun.DrilldownMemberFunDef} (for source-path
 * correlation during cross-hierarchy drill).
 *
 * <p>See {@code docs/superpowers/specs/2026-05-22-issue78-drilldown-synthflat-correlation.md}
 * for the design rationale and the algorithm that consumes these methods.
 */
public final class SyntheticFlatHierarchySupport {

    /**
     * Member-property prefix used to expose source-hierarchy ancestor
     * identities on synthetic-flat members. A flat level for source
     * {@code [Product.Category].[Category3]} carries one property per
     * ancestor source level (Category1, Category2), populated at member
     * load time from the source columns.
     *
     * <p>The prefix prevents collision with user-declared properties and
     * marks the properties as internal — callers outside this package
     * should not surface them via {@code XMLA MEMBER_PROPERTIES}.
     */
    public static final String ANCESTOR_PROPERTY_PREFIX =
        "_synth_src_ancestor_";

    private SyntheticFlatHierarchySupport() {
        // no instances
    }

    /**
     * Returns true when {@code propertyName} carries the synthetic-flat
     * ancestor-property sentinel. Single predicate used by both producer
     * (skip emission de-dup) and consumer ({@code XmlaHandler.is\
     * PropertyInternal}) sites so the {@link #ANCESTOR_PROPERTY_PREFIX}
     * convention has one authoritative check.
     */
    public static boolean isAncestorProperty(String propertyName) {
        return propertyName != null
            && propertyName.startsWith(ANCESTOR_PROPERTY_PREFIX);
    }

    /**
     * Builds an ancestor-property descriptor for a synthetic-flat level.
     * Centralises the {@link #ANCESTOR_PROPERTY_PREFIX} prefix invariant
     * (consumed by {@link #filterChildrenBySourcePath}) and defaults the
     * property {@code type} to {@code "String"} when the source datatype
     * is null — preventing the {@code RolapLevel.convertPropertyType\
     * NameToCode} NPE at schema load.
     *
     * <p>Always sets {@code dependsOnLevelValue = true}; callers must
     * ensure the surrounding emission is gated on
     * {@link RolapLevel#isUnique} (otherwise the dependency claim does
     * not hold).
     */
    public static MondrianDef.Property ancestorProperty(
        String ancestorLevelName, String column, String datatype)
    {
        MondrianDef.Property p = new MondrianDef.Property();
        p.name = ANCESTOR_PROPERTY_PREFIX + ancestorLevelName;
        p.column = column;
        p.type = datatype != null ? datatype : "String";
        p.dependsOnLevelValue = Boolean.TRUE;
        return p;
    }

    /**
     * Unwraps a hierarchy to its {@link SyntheticFlatHierarchy}, or
     * returns null if the hierarchy is not synthetic-flat (after one
     * level of {@link RolapCubeHierarchy} unwrap).
     *
     * <p>{@code RolapCubeLevel.getHierarchy()} returns the cube wrapper
     * at runtime, not the underlying {@link SyntheticFlatHierarchy} —
     * callers must use this method rather than a direct instanceof
     * check.
     */
    public static SyntheticFlatHierarchy resolveSyntheticFlat(
        Hierarchy hierarchy)
    {
        Hierarchy inner = RolapCubeHierarchy.unwrap(hierarchy);
        return inner instanceof SyntheticFlatHierarchy sfh ? sfh : null;
    }

    /**
     * Pair returned by {@link #resolveStarLevelTarget}.
     *
     * <p>For a non-synthetic hierarchy {@code requestedLevel} is null
     * and {@code hierarchy} is the result of one {@link RolapCubeHierarchy}
     * unwrap. For a synthetic-flat projection both fields are populated
     * with the underlying source level and its hierarchy, so callers can
     * construct a {@link StarLevelRef} that resolves against the fact
     * star rather than the synthetic wrapper.
     */
    public record StarLevelTarget(
        Hierarchy hierarchy, RolapLevel requestedLevel) { }

    /**
     * Resolves a query-side hierarchy to the pair
     * {@code (underlying schema hierarchy, requested source level)} used
     * to build a {@link StarLevelRef}. Combines the
     * {@link RolapCubeHierarchy#unwrap cube-hierarchy unwrap} with the
     * optional reach to the synthetic-flat source level so callers don't
     * have to repeat the two-step pattern (previously inline in
     * {@code NativeNonEmptyFilter}).
     */
    public static StarLevelTarget resolveStarLevelTarget(Hierarchy h) {
        Hierarchy resolved = RolapCubeHierarchy.unwrap(h);
        if (resolved instanceof SyntheticFlatHierarchy synth) {
            RolapLevel sourceLevel = synth.getSourceLevel();
            if (sourceLevel != null) {
                return new StarLevelTarget(
                    sourceLevel.getHierarchy(), sourceLevel);
            }
        }
        return new StarLevelTarget(resolved, null);
    }

    /**
     * Finds a common source hierarchy between two levels from synthetic
     * flat hierarchies. Iterates ALL {@link SyntheticFlatHierarchy.SourceLink}
     * entries on both sides; returns the dependent-side link if a valid
     * ancestor dependency exists (shared source hierarchy, dependent at
     * a greater depth than determinant), or null otherwise.
     *
     * <p>Returns null if either side is not a synthetic-flat hierarchy.
     */
    public static SyntheticFlatHierarchy.SourceLink findCommonSourceLink(
        RolapLevel dependentLevel,
        RolapLevel determinantLevel)
    {
        SyntheticFlatHierarchy depFlat = resolveSyntheticFlat(
            dependentLevel.getHierarchy());
        SyntheticFlatHierarchy detFlat = resolveSyntheticFlat(
            determinantLevel.getHierarchy());
        if (depFlat == null || detFlat == null) {
            return null;
        }
        for (SyntheticFlatHierarchy.SourceLink detLink
             : detFlat.getSourceLinks())
        {
            SyntheticFlatHierarchy.SourceLink depLink =
                depFlat.findLinkForHierarchy(detLink.hierarchy());
            if (depLink != null
                && depLink.depth() > detLink.depth())
            {
                return depLink;
            }
        }
        return null;
    }

    /**
     * Numeric-and-string tolerant key comparison. Integer(42), Long(42L),
     * String("42") and BigInteger(42) all compare equal. Necessary
     * because synthetic-flat member keys arrive as the JDBC-driver-
     * decided type for the level's key column (often Integer or Long for
     * numeric IDs), but member-property values may come back as a
     * different type from the same column depending on metadata.
     *
     * <p>Falls back to {@link Object#equals} when neither operand is a
     * {@link Number}. Treats null as not-equal to anything (including
     * another null) on the assumption that synthetic-flat key columns
     * are non-nullable.
     */
    public static boolean equalsTolerant(Object a, Object b) {
        if (a == b) {
            return a != null;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a.equals(b)) {
            return true;
        }
        if (a instanceof Number || b instanceof Number) {
            return canonicalNumberString(a)
                .equals(canonicalNumberString(b));
        }
        return false;
    }

    /**
     * #78 source-path correlation: given a tuple about to be expanded by
     * {@code DrilldownMemberFunDef.drillDownCrossHierarchy} at position
     * {@code drillIndex} (whose hierarchy is {@code drillHierarchy}) and
     * the candidate {@code children} returned by the schema reader,
     * restrict the children to those whose source-hierarchy ancestor
     * identities match the sibling tuple members.
     *
     * <p>When the preconditions for source-path filtering don't hold —
     * the drill target is not a synthetic-flat hierarchy, or no sibling
     * tuple position projects an ancestor of the drill level in the same
     * source hierarchy — this method returns {@code children} unchanged
     * so cross-hierarchy drills outside the #78 shape are not perturbed.
     *
     * <p>Lives in {@code mondrian.rolap} so it can read package-internal
     * members ({@link SyntheticFlatHierarchy}, source links) directly and
     * be unit-tested without spinning up a cube. The function-package
     * caller delegates here unchanged.
     */
    public static List<Member> filterChildrenBySourcePath(
        Member[] tuple,
        int drillIndex,
        Hierarchy drillHierarchy,
        List<Member> children)
    {
        if (children == null || children.isEmpty()) {
            return children;
        }
        final SyntheticFlatHierarchy drillSF =
            resolveSyntheticFlat(drillHierarchy);
        if (drillSF == null) {
            return children;
        }

        // The drill side's level is the source of truth for which
        // ancestor properties are actually emitted. If buildSyntheticLevel
        // skipped property emission (non-unique source key, or snowflake
        // ancestor on a different table), the property name will not
        // appear in the level's property list — and a constraint against
        // that name would compare every child's null value to the
        // sibling key and drop the entire result. Snapshot the available
        // names once so each constraint can be vetted before being added.
        final Set<String> emittedAncestorProps =
            collectAncestorPropertyNames(children.get(0).getLevel());

        // Scan sibling positions for source-path constraints.
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
                resolveSyntheticFlat(sibling.getHierarchy());
            if (siblingSF == null) {
                continue;
            }
            // Find a SourceLink on the sibling whose hierarchy is also
            // linked from the drill side at a greater depth (sibling is
            // the ancestor, drill is the descendant).
            for (SyntheticFlatHierarchy.SourceLink detLink
                : siblingSF.getSourceLinks())
            {
                SyntheticFlatHierarchy.SourceLink depLink =
                    drillSF.findLinkForHierarchy(detLink.hierarchy());
                if (depLink == null
                    || depLink.depth() <= detLink.depth())
                {
                    continue;
                }
                final String propName =
                    ANCESTOR_PROPERTY_PREFIX + detLink.level().getName();
                // No emitted property → no constraint. Falling through
                // to unconstrained behavior is the documented degradation
                // path for non-unique / snowflaked source hierarchies.
                if (!emittedAncestorProps.contains(propName)) {
                    break;
                }
                final Object reqKey =
                    sibling.getPropertyValue(Property.KEY.getName());
                final Object actualKey =
                    reqKey != null ? reqKey : sibling.getName();
                if (actualKey == null) {
                    break;
                }
                if (constraintProps == null) {
                    constraintProps = new ArrayList<>(2);
                    constraintKeys = new ArrayList<>(2);
                }
                constraintProps.add(propName);
                constraintKeys.add(actualKey);
                break;
            }
        }
        if (constraintProps == null) {
            return children;
        }

        // Per-child filter
        final List<Member> filtered = new ArrayList<>(children.size());
        outer:
        for (Member child : children) {
            for (int i = 0; i < constraintProps.size(); i++) {
                Object actual = child.getPropertyValue(constraintProps.get(i));
                if (!equalsTolerant(actual, constraintKeys.get(i))) {
                    continue outer;
                }
            }
            filtered.add(child);
        }
        return filtered;
    }

    /**
     * Returns the set of {@link #ANCESTOR_PROPERTY_PREFIX}-prefixed
     * property names actually present on the given level. Used to vet
     * which source-path constraints the filter can safely apply:
     * {@link SyntheticFlatHierarchy#buildSyntheticLevel} skips emission
     * for non-unique source keys and cross-table ancestors, so the
     * level's property list — not the SourceLink topology — is the
     * authoritative source of which constraints are answerable.
     */
    private static Set<String> collectAncestorPropertyNames(Level level) {
        if (level == null) {
            return Set.of();
        }
        Property[] props = level.getProperties();
        if (props == null || props.length == 0) {
            return Set.of();
        }
        Set<String> names = null;
        for (Property p : props) {
            String name = p.getName();
            if (isAncestorProperty(name)) {
                if (names == null) {
                    names = new HashSet<>(props.length);
                }
                names.add(name);
            }
        }
        return names == null ? Set.of() : names;
    }

    private static String canonicalNumberString(Object o) {
        if (o instanceof Number n) {
            // Integer / Long / BigInteger / non-fractional Double:
            // canonicalize via longValue when exact, so 42 == 42L == 42.0.
            double d = n.doubleValue();
            long l = n.longValue();
            if (Double.compare(d, (double) l) == 0) {
                return Long.toString(l);
            }
            return n.toString();
        }
        return o.toString();
    }
}

// End SyntheticFlatHierarchySupport.java
