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
 * Read-only view over a pre-populated cell value map produced by the
 * NativeQueryEngine prefetch pass.
 *
 * <p>Implementations must be thread-safe for concurrent reads.
 */
public interface PrefetchedCellProvider {

    /**
     * Looks up the cell value for the given key.
     *
     * @param key  the composite cell key; must not be {@code null}
     * @return the stored value, or {@link PrefetchKey#MISS} if the key is
     *         absent from this provider
     */
    Object lookup(PrefetchKey key);

    /**
     * Returns the number of cell values stored in this provider.
     *
     * @return non-negative size
     */
    int size();
}

// End PrefetchedCellProvider.java
