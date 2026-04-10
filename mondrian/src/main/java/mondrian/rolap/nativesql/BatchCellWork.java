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

import javax.sql.DataSource;

/**
 * {@link CellNativeWork} whose SQL returns a batch of values keyed by cell
 * coordinate.  Used by {@code NativeQueryEngine} Phase D — one query-wide
 * SQL per {@code (plan, granularity)} pair, populating a shared
 * {@code NativeQueryResultContext}.
 *
 * <p>The cached payload is an opaque container — typically a
 * {@code Map<CoordKey, Object>} or an internal NQE structure.
 * {@link #materialize(Object, Object)} extracts the per-cell value at the
 * requested coordinate.
 */
public abstract class BatchCellWork extends CellNativeWork {

    protected BatchCellWork(
        NativeSqlFingerprint fingerprint,
        DataSource dataSource,
        String sql)
    {
        super(fingerprint, CellWorkKind.BATCH, dataSource, sql);
    }

    /**
     * Extract the value at {@code coordKey} from the cached batch payload.
     *
     * @param cachedPayload whatever {@link #consume(java.sql.ResultSet)}
     *                      returned during drain
     * @param coordKey      consumer-specific coordinate identifier
     * @return the per-cell value, or {@code null} if the coordinate is not
     *         present in the batch
     */
    public abstract Object materialize(Object cachedPayload, Object coordKey);
}
