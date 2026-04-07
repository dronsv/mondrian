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

    private static String compositeKey(
        String classId,
        String projectedKey,
        String measureId)
    {
        return classId + '\0' + projectedKey + '\0' + measureId;
    }
}
