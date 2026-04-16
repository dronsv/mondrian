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

/**
 * An optional, immutable reference to a hierarchy level.
 *
 * <p>When the analyzer can determine the active level unambiguously
 * (e.g. for {@link MemberCardinality#SINGLE_MEMBER} or
 * {@link MemberCardinality#ALL_MEMBER}), a {@code LevelRef} captures
 * that.  For {@link MemberCardinality#SET_EXPRESSION} or
 * {@link MemberCardinality#UNKNOWN}, no level may be determinable and
 * the field is left {@code null} / {@link #NONE}.
 *
 * <p>Equality is based on the level unique name.  This keeps
 * the shape layer decoupled from the live {@code RolapLevel} object
 * graph — shapes are cheap query-level VOs, not cube/schema objects.
 */
public final class LevelRef {

    /** Sentinel for "no determinable level". */
    public static final LevelRef NONE = new LevelRef(null, -1);

    private final String levelUniqueName;
    private final int depth;

    /**
     * Creates a level reference.
     *
     * @param levelUniqueName  unique name of the level, or {@code null}
     *                         if the level cannot be determined
     * @param depth            ordinal depth of the level within its
     *                         hierarchy (0 = All, 1 = first real level,
     *                         etc.); {@code -1} if unknown
     */
    public LevelRef(String levelUniqueName, int depth) {
        this.levelUniqueName = levelUniqueName;
        this.depth = depth;
    }

    /**
     * Creates a {@code LevelRef} from a unique name, with depth unknown.
     */
    public static LevelRef of(String levelUniqueName) {
        if (levelUniqueName == null) {
            return NONE;
        }
        return new LevelRef(levelUniqueName, -1);
    }

    /**
     * Creates a {@code LevelRef} from a unique name and depth.
     */
    public static LevelRef of(String levelUniqueName, int depth) {
        if (levelUniqueName == null) {
            return NONE;
        }
        return new LevelRef(levelUniqueName, depth);
    }

    /** The unique name of the level, or {@code null} if indeterminate. */
    public String levelUniqueName() {
        return levelUniqueName;
    }

    /** Ordinal depth within the hierarchy, or {@code -1} if unknown. */
    public int depth() {
        return depth;
    }

    /** Whether a concrete level is known. */
    public boolean isPresent() {
        return levelUniqueName != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LevelRef)) {
            return false;
        }
        LevelRef that = (LevelRef) o;
        if (levelUniqueName == null) {
            return that.levelUniqueName == null;
        }
        return levelUniqueName.equals(that.levelUniqueName);
    }

    @Override
    public int hashCode() {
        return levelUniqueName == null ? 0 : levelUniqueName.hashCode();
    }

    @Override
    public String toString() {
        if (levelUniqueName == null) {
            return "LevelRef{NONE}";
        }
        return "LevelRef{" + levelUniqueName
            + (depth >= 0 ? ", depth=" + depth : "")
            + "}";
    }
}

// End LevelRef.java
