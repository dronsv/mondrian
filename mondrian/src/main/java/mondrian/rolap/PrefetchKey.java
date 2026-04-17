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

import java.util.Arrays;

/**
 * Composite key that identifies a single cell value in the NQE prefetch
 * result set.
 *
 * <p>The key consists of the star-schema bit-position of the measure column
 * plus the ordered array of constrained dimension-member values produced by
 * the SQL row. Equality and hashing are value-based.
 *
 * <p>The {@link #MISS} sentinel is returned by
 * {@link PrefetchedCellProvider#lookup} when a key is not present in the
 * prefetch map, distinguishing "not found" from a legitimate {@code null}
 * stored value.
 */
public final class PrefetchKey {

    /**
     * Sentinel returned by {@link PrefetchedCellProvider#lookup} when the
     * key is absent from the prefetch map. Guaranteed to be a distinct object
     * reference — callers must use {@code == PrefetchKey.MISS} to test.
     */
    public static final Object MISS = new Object();

    private final int measureBitPosition;
    private final Object[] constrainedValues;
    private final int hash;

    /**
     * Constructs a PrefetchKey.
     *
     * @param measureBitPosition  bit-position of the measure column in the
     *                            star schema
     * @param constrainedValues   ordered array of dimension-member key values
     *                            that constrain this cell; must not be
     *                            {@code null}
     */
    public PrefetchKey(int measureBitPosition, Object[] constrainedValues) {
        this.measureBitPosition = measureBitPosition;
        this.constrainedValues = constrainedValues;
        this.hash =
            31 * measureBitPosition + Arrays.deepHashCode(constrainedValues);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrefetchKey other)) return false;
        return measureBitPosition == other.measureBitPosition
            && Arrays.deepEquals(constrainedValues, other.constrainedValues);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "PrefetchKey[measure=" + measureBitPosition
            + ", values=" + Arrays.toString(constrainedValues) + "]";
    }
}

// End PrefetchKey.java
