/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.nativesql;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Generic typed cache primitive scoped to one orchestration domain.
 *
 * <p>Per Contract 7 of the design spec: each orchestration domain owns its
 * OWN instance of this cache.  Instances are NOT shared across domains.
 * Two features that need caching (e.g. cell-phase registry and NNEF) each
 * construct their own {@code StatementLocalCache} and never exchange keys.
 * Sharing would allow textual fingerprint collisions across domains to
 * corrupt results whose shapes do not match.
 *
 * <p>Not thread-safe.  Mondrian statements are single-threaded per phase
 * loop; this cache lives on a {@code RolapEvaluatorRoot} and is only
 * accessed from the statement's execution thread.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class StatementLocalCache<K, V> {

    private final Map<K, V> store = new HashMap<>();

    public V get(K key) {
        return store.get(key);
    }

    public void put(K key, V value) {
        Objects.requireNonNull(key, "key");
        store.put(key, value);
    }

    public boolean contains(K key) {
        return store.containsKey(key);
    }

    public int size() {
        return store.size();
    }

    public void clear() {
        store.clear();
    }
}
