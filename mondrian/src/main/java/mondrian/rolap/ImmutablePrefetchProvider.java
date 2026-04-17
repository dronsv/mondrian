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
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable, snapshot implementation of {@link PrefetchedCellProvider}.
 *
 * <p>Created by wrapping a {@link Map} that was populated by the NQE prefetch
 * builder. After construction the backing map is never modified; the
 * unmodifiable wrapper is a defence-in-depth safeguard.
 *
 * <p>Thread-safety: safe for concurrent reads from any number of threads once
 * the instance is published (construction is single-threaded in the NQE path).
 */
public final class ImmutablePrefetchProvider implements PrefetchedCellProvider {

    private static final ImmutablePrefetchProvider EMPTY =
        new ImmutablePrefetchProvider(Collections.emptyMap());

    private final Map<PrefetchKey, Object> data;

    /**
     * Package-visible constructor — use {@link #empty()} for the empty case.
     *
     * @param data  map of prefetched cell values; must not be {@code null}.
     *              The caller must not modify the map after passing it here.
     */
    ImmutablePrefetchProvider(Map<PrefetchKey, Object> data) {
        this.data = Collections.unmodifiableMap(new HashMap<>(data));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns {@link PrefetchKey#MISS} when the key is absent, even if
     * the stored value is {@code null}. A {@code null} stored value is
     * therefore indistinguishable from absent — callers should not store
     * {@code null} for meaningful cells.
     */
    @Override
    public Object lookup(PrefetchKey key) {
        Object value = data.get(key);
        return value != null ? value : PrefetchKey.MISS;
    }

    @Override
    public int size() {
        return data.size();
    }

    /**
     * Returns the shared empty provider singleton.
     *
     * @return an {@link ImmutablePrefetchProvider} with no entries
     */
    public static ImmutablePrefetchProvider empty() {
        return EMPTY;
    }
}

// End ImmutablePrefetchProvider.java
