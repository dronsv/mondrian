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
 * {@link NativeSqlWork} whose SQL returns exactly one scalar value per
 * execution.  Used by {@code NativeSqlCalc} — one templated SQL per measure
 * per cell, returning one aggregate scalar.
 *
 * <p>The cached payload is the scalar itself.  {@link #materialize} is
 * typically the identity function but can be overridden for type coercion
 * (e.g., unboxing or decimal normalization).
 */
public abstract class ScalarNativeSqlWork extends NativeSqlWork {

    protected ScalarNativeSqlWork(
        NativeSqlFingerprint fingerprint,
        DataSource dataSource,
        String sql)
    {
        super(fingerprint, NativeSqlWorkKind.SCALAR, dataSource, sql);
    }

    /**
     * Extract the final consumer-visible scalar from the cached payload.
     *
     * <p>Default semantics (when overridden as identity): {@code cachedPayload}
     * was what {@link #consume(java.sql.ResultSet)} returned from the drain
     * loop.  Subclasses return it directly or apply type coercion.
     */
    public abstract Object materialize(Object cachedPayload);
}
