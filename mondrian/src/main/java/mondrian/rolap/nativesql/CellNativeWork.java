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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Abstract base for all cell-phase native work units.  Work units represent
 * one deferred SQL execution request registered with
 * {@link CellPhaseNativeRegistry}.
 *
 * <p>Subclasses are {@link ScalarCellWork} and {@link BatchCellWork}.  The
 * concrete subclass determines the {@link CellWorkKind} and the shape of the
 * cached payload.
 *
 * <p>Consumer override points — ALL have safe defaults:
 * <ul>
 *   <li>{@link #policyAdjust} — default accepts base classification
 *       unchanged.  Override to escalate FALLBACK → PROPAGATE.  PROPAGATE →
 *       FALLBACK downgrades additionally require
 *       {@link #allowsPropagateDowngrade} to return {@code true} (Section 3
 *       of the design spec).</li>
 *   <li>{@link #allowsPropagateDowngrade} — default {@code false}.  Override
 *       only with explicit, documented rationale.</li>
 *   <li>{@link #onError} — default no-op.  Override for metrics/logging.
 *       Implementations MUST NOT throw.</li>
 * </ul>
 */
public abstract class CellNativeWork {

    private final NativeSqlFingerprint fingerprint;
    private final CellWorkKind kind;
    private final DataSource dataSource;
    private final String sql;

    protected CellNativeWork(
        NativeSqlFingerprint fingerprint,
        CellWorkKind kind,
        DataSource dataSource,
        String sql)
    {
        this.fingerprint = Objects.requireNonNull(fingerprint, "fingerprint");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.sql = Objects.requireNonNull(sql, "sql");
    }

    public final NativeSqlFingerprint fingerprint() { return fingerprint; }
    public final CellWorkKind kind()                { return kind; }
    public final DataSource dataSource()            { return dataSource; }
    public final String sql()                        { return sql; }

    /**
     * Read the {@link ResultSet} and return the payload to cache under this
     * work unit's identity.
     *
     * <p>Called once per work unit from {@link CellPhaseNativeRegistry}'s
     * drain loop.  May throw {@link SQLException} (caught, classified, and
     * cached as an error) or any other exception (also caught by drain).
     */
    public abstract Object consume(ResultSet rs) throws SQLException;

    /**
     * Consumer-side classification override hook.  Default: accept
     * {@code base} unchanged.
     *
     * @param t    the failure throwable
     * @param base the classifier's verdict from
     *             {@link NativeSqlError#classify(Throwable)}
     */
    public NativeSqlError.Classification policyAdjust(
        Throwable t,
        NativeSqlError.Classification base)
    {
        return base;
    }

    /**
     * Opt-in flag for PROPAGATE → FALLBACK downgrades in
     * {@link #policyAdjust}.  Default {@code false} — the registry rejects
     * downgrades and logs a warning unless this returns {@code true}.
     */
    public boolean allowsPropagateDowngrade() {
        return false;
    }

    /**
     * Advisory callback invoked from the drain loop after a failure has been
     * cached.  Default: no-op.  Implementations MUST NOT throw; the drain
     * loop catches metrics-hook bugs but they will be reported via
     * {@link NativeSqlTelemetry#onErrorBug}.
     */
    public void onError(Throwable t) {
        // no-op
    }
}
