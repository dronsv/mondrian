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
 * Immutable snapshot of counters collected while building an
 * {@link ImmutablePrefetchProvider} from NQE SQL result rows.
 *
 * <p>Intended for structured logging and diagnostics; all fields are
 * final and exposed directly.
 *
 * @param nqeRowsProcessed       total SQL rows inspected
 * @param rowsMapped             rows that produced a valid map entry
 * @param rowsRejected           rows discarded for any reason
 * @param duplicateKeys          rows where the computed key already existed
 *                               in the map (later value wins)
 * @param providerSize           final {@link PrefetchedCellProvider#size()}
 * @param decodeRejects          rows rejected due to value-decode errors
 * @param measureResolutionRejects rows rejected because the measure column
 *                               could not be resolved
 * @param missingColumnRejects   rows rejected due to missing required columns
 * @param normalizationRejects   rows rejected during value normalisation
 */
public record PrefetchBuildMetrics(
    int nqeRowsProcessed,
    int rowsMapped,
    int rowsRejected,
    int duplicateKeys,
    int providerSize,
    int decodeRejects,
    int measureResolutionRejects,
    int missingColumnRejects,
    int normalizationRejects) {}

// End PrefetchBuildMetrics.java
