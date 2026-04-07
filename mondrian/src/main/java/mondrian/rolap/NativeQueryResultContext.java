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

import java.util.concurrent.ConcurrentHashMap;

/**
 * Query-scoped storage for physical values materialized by
 * NativeQueryEngine SQL queries.
 *
 * Key: (classId, projectedTupleKey, physicalMeasureId)
 * Value: scalar number or null.
 */
public class NativeQueryResultContext {
    private final ConcurrentHashMap<String, Object> data =
        new ConcurrentHashMap<String, Object>();

    private static final Object NULL_SENTINEL = new Object();

    public void put(
        String classId,
        String projectedKey,
        String measureId,
        Object value)
    {
        data.put(
            compositeKey(classId, projectedKey, measureId),
            value == null ? NULL_SENTINEL : value);
    }

    public Object get(
        String classId,
        String projectedKey,
        String measureId)
    {
        Object v = data.get(compositeKey(classId, projectedKey, measureId));
        return v == NULL_SENTINEL ? null : v;
    }

    public boolean containsKey(
        String classId,
        String projectedKey,
        String measureId)
    {
        return data.containsKey(
            compositeKey(classId, projectedKey, measureId));
    }

    public int size() {
        return data.size();
    }

    /**
     * Returns first N composite keys for diagnostics.
     * Uses ~ as display separator (avoids \0 which breaks log output).
     */
    public String dumpKeys(int maxKeys) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (String key : data.keySet()) {
            if (count++ >= maxKeys) break;
            sb.append(key.replace('\0', '~')).append("\n");
        }
        return sb.toString();
    }

    private static String compositeKey(
        String classId,
        String projectedKey,
        String measureId)
    {
        return classId + '\0' + projectedKey + '\0' + measureId;
    }
}
