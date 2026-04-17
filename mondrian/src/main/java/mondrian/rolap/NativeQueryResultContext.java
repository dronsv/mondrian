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

import java.util.ArrayList;
import java.util.List;
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

    /**
     * A decoded entry from the context, exposing the three key parts
     * and the stored value.
     *
     * @param classId       the coordinate class identifier
     * @param projectedKey  the {@code '\0'}-delimited tuple key
     * @param measureId     the physical measure identifier
     * @param value         the stored scalar value (may be {@code null})
     */
    public record Entry(
        String classId,
        String projectedKey,
        String measureId,
        Object value) {}

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

    /**
     * Returns all entries whose composite key starts with the given
     * {@code classId}. The composite key format is
     * {@code classId + '\0' + projectedKey + '\0' + measureId}, so
     * the classId prefix is unambiguous (classIds do not contain
     * {@code '\0'}).
     *
     * @param classId  the coordinate class identifier to filter by
     * @return a list of decoded entries; never {@code null}
     */
    public List<Entry> entriesForClassId(String classId) {
        String prefix = classId + '\0';
        List<Entry> result = new ArrayList<Entry>();
        for (java.util.Map.Entry<String, Object> e : data.entrySet()) {
            String compositeKey = e.getKey();
            if (!compositeKey.startsWith(prefix)) {
                continue;
            }
            // Parse: classId \0 projectedKey \0 measureId
            // The classId part is known (prefix length - 1).
            // Find the last \0 to split projectedKey from measureId.
            String remainder = compositeKey.substring(prefix.length());
            int lastSep = remainder.lastIndexOf('\0');
            String projectedKey;
            String measureId;
            if (lastSep < 0) {
                // No separator in remainder — entire remainder is measureId,
                // projectedKey is empty (zero-dim query)
                projectedKey = "";
                measureId = remainder;
            } else {
                projectedKey = remainder.substring(0, lastSep);
                measureId = remainder.substring(lastSep + 1);
            }
            Object value = e.getValue();
            result.add(new Entry(
                classId,
                projectedKey,
                measureId,
                value == NULL_SENTINEL ? null : value));
        }
        return result;
    }

    private static String compositeKey(
        String classId,
        String projectedKey,
        String measureId)
    {
        return classId + '\0' + projectedKey + '\0' + measureId;
    }
}
