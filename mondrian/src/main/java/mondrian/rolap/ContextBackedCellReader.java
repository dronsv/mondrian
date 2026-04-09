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

import mondrian.olap.Member;
import java.util.Map;

/**
 * Read-only CellReader that feeds prefetched leaf measure values from
 * {@link NativeQueryResultContext} into Calc evaluation.
 *
 * <p>Contract: strictly read-only, non-recursive.  Does NOT launch SQL,
 * does NOT fall back to the generic evaluator, does NOT compute unknown
 * measures.  When a requested value is not found in the context, the
 * reader increments {@link #missCount} and returns {@code null}.
 *
 * <p>Used by the Calc-based PostProcessEvaluator (Task 3) to supply
 * physical leaf values while the Calc tree evaluates a calculated
 * measure's formula.
 */
class ContextBackedCellReader implements CellReader {

    private final NativeQueryResultContext context;
    private final Map<String, String> projectedKeyByGranClassId;
    private final Map<String, String> granClassIdByBaseClassId;
    private final Map<String, CoordinateClassPlan> classPlanMap;
    private int missCount;

    /**
     * Creates a ContextBackedCellReader.
     *
     * @param context                   query result context filled by SQL
     *                                  execution (Phase D.1-D.2)
     * @param projectedKeyByGranClassId per-granularity-class projected keys
     *                                  for the current axis tuple
     * @param granClassIdByBaseClassId  map from base classId to
     *                                  granularity classId, or {@code null}
     *                                  for the legacy (non-granularity) path
     * @param classPlanMap              map from base classId to
     *                                  {@link CoordinateClassPlan}
     */
    ContextBackedCellReader(
        NativeQueryResultContext context,
        Map<String, String> projectedKeyByGranClassId,
        Map<String, String> granClassIdByBaseClassId,
        Map<String, CoordinateClassPlan> classPlanMap)
    {
        this.context = context;
        this.projectedKeyByGranClassId = projectedKeyByGranClassId;
        this.granClassIdByBaseClassId = granClassIdByBaseClassId;
        this.classPlanMap = classPlanMap;
        this.missCount = 0;
    }

    @Override
    public Object get(RolapEvaluator evaluator) {
        Member currentMeasure = evaluator.getMembers()[0];
        if (currentMeasure == null || !currentMeasure.isMeasure()) {
            missCount++;
            return null;
        }

        String measureId = currentMeasure.getUniqueName();

        // Find which coordinate class plan contains this measure
        String baseClassId = findClassForMeasure(measureId);
        if (baseClassId == null) {
            missCount++;
            return null;
        }

        // Resolve granularity classId (may differ from base when
        // multi-granularity is active)
        String lookupClassId = baseClassId;
        if (granClassIdByBaseClassId != null) {
            String gran = granClassIdByBaseClassId.get(baseClassId);
            if (gran != null) {
                lookupClassId = gran;
            }
        }

        String projectedKey = projectedKeyByGranClassId.get(lookupClassId);
        if (projectedKey == null) {
            missCount++;
            return null;
        }

        Object value = context.get(lookupClassId, projectedKey, measureId);
        if (value == null) {
            missCount++;
        }
        return value;
    }

    @Override
    public int getMissCount() {
        return missCount;
    }

    @Override
    public boolean isDirty() {
        return false;
    }

    /**
     * Scans all coordinate class plans to find which classId contains the
     * given measure (matched by physicalMeasureId).
     *
     * <p>This mirrors the lookup in
     * {@link PostProcessEvaluator#findClassId}.
     *
     * @param measureId the unique name of the measure to look up
     * @return the classId, or {@code null} if not found
     */
    private String findClassForMeasure(String measureId) {
        for (Map.Entry<String, CoordinateClassPlan> entry
                : classPlanMap.entrySet())
        {
            for (PhysicalValueRequest req
                    : entry.getValue().getRequests())
            {
                if (measureId.equals(req.getPhysicalMeasureId())) {
                    return entry.getKey();
                }
            }
        }
        return null;
    }
}

// End ContextBackedCellReader.java
