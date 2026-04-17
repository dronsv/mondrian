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

import mondrian.rolap.agg.CellRequest;

/**
 * Builds {@link PrefetchKey} instances from two different entry points and
 * ensures they produce identical keys for the same logical cell.
 *
 * <p>Two entry points must produce the same key for the same cell so that the
 * prefetch map built from NQE rows can be looked up using CellRequests from
 * the segment-cache drain path:
 *
 * <ol>
 * <li>{@link #fromCellRequest} — used by the segment-cache consumer
 *     ({@code FastBatchingCellReader}) when probing the prefetch map.</li>
 * <li>{@link #fromNqeRow} — used by the NQE result builder when populating
 *     the prefetch map from SQL result rows.</li>
 * </ol>
 *
 * <p>Value normalisation (e.g. {@code Integer → Long}) is applied in both
 * paths to guarantee symmetry regardless of JDBC driver type mapping.
 */
public final class PrefetchKeyBuilder {

    /**
     * Builds a {@link PrefetchKey} from a {@link CellRequest}.
     *
     * <p>The measure bit-position is taken from
     * {@code request.getMeasure().getBitPosition()} and the constrained
     * values are normalised before hashing.
     *
     * @param request  the cell request; must not be {@code null}
     * @return a normalised PrefetchKey
     */
    public PrefetchKey fromCellRequest(CellRequest request) {
        int bitPos = request.getMeasure().getBitPosition();
        return new PrefetchKey(bitPos, normalizeValues(request.getSingleValues()));
    }

    /**
     * Builds a {@link PrefetchKey} from an NQE SQL result row.
     *
     * @param measureBitPosition   bit-position of the measure column
     * @param constrainedValues    ordered dimension-member key values from
     *                             the SQL row; may be {@code null} (treated
     *                             as empty)
     * @return a normalised PrefetchKey
     */
    public PrefetchKey fromNqeRow(
        int measureBitPosition,
        Object[] constrainedValues)
    {
        return new PrefetchKey(
            measureBitPosition, normalizeValues(constrainedValues));
    }

    // -----------------------------------------------------------------------
    // Normalisation helpers
    // -----------------------------------------------------------------------

    private Object[] normalizeValues(Object[] raw) {
        if (raw == null || raw.length == 0) {
            return new Object[0];
        }
        Object[] normalized = new Object[raw.length];
        for (int i = 0; i < raw.length; i++) {
            normalized[i] = normalizeValue(raw[i]);
        }
        return normalized;
    }

    /**
     * Normalises a single dimension-member key value.
     *
     * <p>JDBC drivers may return {@code Integer} or {@code Long} for the
     * same numeric key column depending on the driver version. Normalising
     * {@code Integer} to {@code Long} ensures that keys built from JDBC
     * result sets (NQE side) and from cached member objects (CellRequest
     * side) always compare equal.
     */
    private Object normalizeValue(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Integer) {
            return ((Integer) v).longValue();
        }
        return v;
    }
}

// End PrefetchKeyBuilder.java
