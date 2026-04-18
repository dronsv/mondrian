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

/**
 * Coarse query locations relevant for native-dispatch analysis.
 *
 * <p>A hierarchy may appear in multiple locations simultaneously (e.g. on
 * the ROWS axis <em>and</em> in a calculated member formula).
 * {@link HierarchyPresence} stores an {@code EnumSet<QueryLocation>}
 * to capture all locations.
 *
 * <p>This enum captures the coarse syntactic positions that matter for
 * dispatch rule evaluation.  It does not claim to fully model all
 * semantically relevant query positions — future dispatch work may
 * refine or extend these categories.
 */
public enum QueryLocation {

    /** The WHERE / slicer axis. */
    SLICER,

    /** The COLUMNS axis (axis 0). */
    COLUMNS,

    /** The ROWS axis (axis 1). */
    ROWS,

    /** Inside the formula of a WITH-member or calculated measure. */
    CALCULATED_MEMBER_FORMULA,

    /** Inside a sub-SELECT (e.g. FROM (SELECT ... ON COLUMNS FROM ...)). */
    SUBQUERY
}

// End QueryLocation.java
