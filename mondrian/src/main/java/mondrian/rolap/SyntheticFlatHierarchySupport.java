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
        if (hierarchy instanceof RolapCubeHierarchy rch) {
            RolapHierarchy inner = rch.getRolapHierarchy();
            if (inner instanceof SyntheticFlatHierarchy sfh) {
                return sfh;
            }
        }
        if (hierarchy instanceof SyntheticFlatHierarchy sfh) {
            return sfh;
        }
        return null;
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
