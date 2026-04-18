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
 * Describes the cardinality / kind of member reference that a hierarchy
 * contributes to a query tuple.
 *
 * <p>Design invariants:
 * <ul>
 *   <li>{@link #ALL_MEMBER} is semantically distinct from
 *       {@link #SINGLE_MEMBER}: it means <em>absence</em> of an effective
 *       constraint, whereas {@code SINGLE_MEMBER} means <em>presence</em>
 *       of a specific constraint.  Rules must never conflate the two.</li>
 *   <li>{@link #SET_EXPRESSION} signals an arbitrary set expression
 *       (union, descendants, generate, etc.) for which a single active
 *       level may not exist.  Consumers must not assume level precision.</li>
 *   <li>{@link #UNKNOWN} is the safe default when the analyzer cannot
 *       determine the shape.  Rules should treat it conservatively.</li>
 * </ul>
 */
public enum MemberCardinality {

    /**
     * The [All] member — semantically unconstrained.
     *
     * <p>This is NOT a special case of {@code SINGLE_MEMBER}.  For
     * dispatch purposes, [All] means the hierarchy imposes no effective
     * filter, while a concrete single member means a definite constraint.
     */
    ALL_MEMBER,

    /**
     * A single concrete (non-All) member.  The hierarchy is constrained
     * to exactly one member at a specific level.
     */
    SINGLE_MEMBER,

    /**
     * Multiple explicit members (e.g. a set literal {@code {[A], [B]}}).
     */
    MULTI_MEMBER,

    /**
     * An arbitrary set expression — union, descendants, generate, filter,
     * etc.  "Single active level" may not be determinable; consumers must
     * not force fake precision.
     */
    SET_EXPRESSION,

    /**
     * Cardinality could not be determined.  Rules should treat this
     * conservatively — typically equivalent to falling back to the
     * standard MDX evaluator.
     */
    UNKNOWN
}

// End MemberCardinality.java
