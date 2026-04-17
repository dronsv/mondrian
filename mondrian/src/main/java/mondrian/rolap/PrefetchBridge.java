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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Translates NQE execution results from
 * {@link NativeQueryResultContext} coordinates into legacy stored-cell
 * coordinates suitable for {@link PrefetchedCellProvider} lookup.
 *
 * <p>The NQE stores data keyed by {@code (classId, projectedKey, measureId)}
 * where {@code projectedKey} is a {@code '\0'}-delimited string of
 * dimensional values. This bridge:
 * <ol>
 *   <li>Resolves each measure to its {@link RolapStar.Measure} (to obtain
 *       {@code bitPosition})</li>
 *   <li>Decodes the projectedKey string into individual dimension values</li>
 *   <li>Builds a {@link PrefetchKey} using the canonical key builder</li>
 *   <li>Accumulates entries into an {@link ImmutablePrefetchProvider}</li>
 * </ol>
 */
public final class PrefetchBridge {

    private static final Logger LOGGER =
        LogManager.getLogger(PrefetchBridge.class);

    /** Result of a bridge build operation. */
    public record BuildResult(
        PrefetchedCellProvider provider,
        PrefetchBuildMetrics metrics) {}

    private PrefetchBridge() {
        // static utility
    }

    /**
     * Builds a {@link PrefetchedCellProvider} by translating every NQE
     * entry from the supplied {@code context} that matches one of the
     * given {@code plans} into legacy cell coordinates.
     *
     * @param context  the NQE result storage; must not be {@code null}
     * @param plans    the coordinate class plans that describe the shape
     *                 of data in the context; must not be {@code null}
     * @param star     the star schema used to resolve measure bit-positions;
     *                 must not be {@code null}
     * @return a {@link BuildResult} containing the provider and metrics
     */
    public static BuildResult build(
        NativeQueryResultContext context,
        List<CoordinateClassPlan> plans,
        RolapStar star)
    {
        if (plans.isEmpty() || context.size() == 0) {
            return new BuildResult(
                ImmutablePrefetchProvider.empty(),
                new PrefetchBuildMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0));
        }

        final PrefetchKeyBuilder keyBuilder = new PrefetchKeyBuilder();
        final Map<PrefetchKey, Object> cellMap =
            new HashMap<PrefetchKey, Object>();
        final RolapStar.Table factTable = star.getFactTable();

        int nqeRowsProcessed = 0;
        int rowsMapped = 0;
        int rowsRejected = 0;
        int duplicateKeys = 0;
        int decodeRejects = 0;
        int measureResolutionRejects = 0;
        int missingColumnRejects = 0;
        int normalizationRejects = 0;

        // Pre-resolve measures for each plan
        for (CoordinateClassPlan plan : plans) {
            String classId = plan.getClassId();

            // Collect the requests that have resolvable measures
            // (STORED_COLUMN and STATE_AGGREGATE only)
            List<ResolvedMeasureEntry> resolvedMeasures =
                new ArrayList<ResolvedMeasureEntry>();

            for (PhysicalValueRequest request : plan.getRequests()) {
                PhysicalValueRequest.ExpressionProviderKind kind =
                    request.getProviderKind();
                if (kind != PhysicalValueRequest.ExpressionProviderKind
                        .STORED_COLUMN
                    && kind != PhysicalValueRequest.ExpressionProviderKind
                        .STATE_AGGREGATE)
                {
                    // Skip NATIVE_TEMPLATE and other kinds
                    continue;
                }

                String measureId = request.getPhysicalMeasureId();
                String simpleName =
                    FactResolvedTable.extractSimpleName(measureId);
                RolapStar.Measure starMeasure =
                    lookupMeasure(factTable, simpleName, request);

                if (starMeasure == null) {
                    LOGGER.debug(
                        "PrefetchBridge: cannot resolve measure '{}'"
                        + " (simpleName='{}') in star — skipping",
                        measureId, simpleName);
                    // We'll count per-entry rejects below
                    resolvedMeasures.add(
                        new ResolvedMeasureEntry(measureId, null));
                } else {
                    resolvedMeasures.add(
                        new ResolvedMeasureEntry(
                            measureId, starMeasure));
                }
            }

            if (resolvedMeasures.isEmpty()) {
                continue;
            }

            // Determine the number of projected hierarchy dimensions
            // from the first request (all requests in a plan share
            // the same projected hierarchies minus reset)
            PhysicalValueRequest firstReq = plan.getRequests().get(0);
            int projectedDimCount = computeProjectedDimCount(firstReq);

            // Iterate context entries for this classId
            List<NativeQueryResultContext.Entry> entries =
                context.entriesForClassId(classId);

            for (NativeQueryResultContext.Entry entry : entries) {
                nqeRowsProcessed++;

                // Find the resolved measure for this entry
                ResolvedMeasureEntry resolvedEntry =
                    findResolvedMeasure(resolvedMeasures, entry.measureId());

                if (resolvedEntry == null) {
                    // measureId not in our resolvable set — skip silently
                    // (could be a NATIVE_TEMPLATE measure)
                    continue;
                }

                if (resolvedEntry.starMeasure == null) {
                    measureResolutionRejects++;
                    rowsRejected++;
                    continue;
                }

                // Decode the projected key
                Object[] dimValues;
                try {
                    dimValues = decodeProjectedKey(
                        entry.projectedKey(), projectedDimCount);
                } catch (Exception e) {
                    LOGGER.debug(
                        "PrefetchBridge: decode error for key '{}': {}",
                        displayKey(entry.projectedKey()), e.getMessage());
                    decodeRejects++;
                    rowsRejected++;
                    continue;
                }

                // Build the PrefetchKey
                int bitPos =
                    resolvedEntry.starMeasure.getBitPosition();
                PrefetchKey prefetchKey;
                try {
                    prefetchKey = keyBuilder.fromNqeRow(bitPos, dimValues);
                } catch (Exception e) {
                    LOGGER.debug(
                        "PrefetchBridge: normalisation error"
                        + " for measure bitPos={}: {}",
                        bitPos, e.getMessage());
                    normalizationRejects++;
                    rowsRejected++;
                    continue;
                }

                // Accumulate — first value wins on duplicate
                if (cellMap.containsKey(prefetchKey)) {
                    duplicateKeys++;
                } else {
                    cellMap.put(prefetchKey, entry.value());
                    rowsMapped++;
                }
            }
        }

        PrefetchedCellProvider provider;
        if (cellMap.isEmpty()) {
            provider = ImmutablePrefetchProvider.empty();
        } else {
            provider = new ImmutablePrefetchProvider(cellMap);
        }

        PrefetchBuildMetrics metrics = new PrefetchBuildMetrics(
            nqeRowsProcessed,
            rowsMapped,
            rowsRejected,
            duplicateKeys,
            provider.size(),
            decodeRejects,
            measureResolutionRejects,
            missingColumnRejects,
            normalizationRejects);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "PrefetchBridge.build: processed={}, mapped={}, rejected={},"
                + " duplicates={}, providerSize={}",
                nqeRowsProcessed, rowsMapped, rowsRejected,
                duplicateKeys, provider.size());
        }

        return new BuildResult(provider, metrics);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /**
     * Looks up a {@link RolapStar.Measure} by simple name, trying
     * cube-qualified lookup first, then falling back to name-only scan.
     */
    private static RolapStar.Measure lookupMeasure(
        RolapStar.Table factTable,
        String simpleName,
        PhysicalValueRequest request)
    {
        // Try cube-qualified lookup if source cube is available
        String cubeName = request.getSourceCubeName();
        if (cubeName != null) {
            RolapStar.Measure m =
                factTable.lookupMeasureByName(cubeName, simpleName);
            if (m != null) {
                return m;
            }
        }
        // Fallback: scan all measures by simple name
        for (RolapStar.Column col : factTable.getColumns()) {
            if (col instanceof RolapStar.Measure) {
                if (col.getName().equals(simpleName)) {
                    return (RolapStar.Measure) col;
                }
            }
        }
        return null;
    }

    /**
     * Computes the number of projected dimension values expected in
     * each projectedKey string. This is the number of projected
     * hierarchies minus the reset hierarchies.
     */
    private static int computeProjectedDimCount(
        PhysicalValueRequest request)
    {
        int count = request.getProjectedHierarchies().size();
        if (request.getResetHierarchies() != null) {
            count -= request.getResetHierarchies().size();
        }
        return Math.max(count, 0);
    }

    /**
     * Decodes a {@code '\0'}-delimited projected key string into an
     * array of dimension values.
     *
     * @param projectedKey  the composite key string
     * @param expectedCount expected number of dimension values;
     *                      if 0, the key is expected to be empty
     * @return array of dimension value strings
     */
    static Object[] decodeProjectedKey(
        String projectedKey,
        int expectedCount)
    {
        if (expectedCount == 0) {
            return new Object[0];
        }
        if (projectedKey == null || projectedKey.isEmpty()) {
            return new Object[0];
        }
        // Split on \0 — note: String.split with limit -1 to keep
        // trailing empty strings
        String[] parts = projectedKey.split("\0", -1);
        Object[] result = new Object[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = "null".equals(parts[i]) ? null : parts[i];
        }
        return result;
    }

    /**
     * Finds the resolved measure entry for the given measureId.
     */
    private static ResolvedMeasureEntry findResolvedMeasure(
        List<ResolvedMeasureEntry> resolvedMeasures,
        String measureId)
    {
        for (ResolvedMeasureEntry entry : resolvedMeasures) {
            if (entry.measureId.equals(measureId)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Converts a projectedKey to a display-safe string for logging
     * (replaces \0 with ~).
     */
    private static String displayKey(String projectedKey) {
        return projectedKey == null
            ? "null"
            : projectedKey.replace('\0', '~');
    }

    /**
     * Internal holder pairing a measureId with its resolved star measure
     * (which may be null if resolution failed).
     */
    private static final class ResolvedMeasureEntry {
        final String measureId;
        final RolapStar.Measure starMeasure;

        ResolvedMeasureEntry(
            String measureId,
            RolapStar.Measure starMeasure)
        {
            this.measureId = measureId;
            this.starMeasure = starMeasure;
        }
    }
}

// End PrefetchBridge.java
