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
 * Synchronous, one-shot native SQL work unit. Does NOT participate in the
 * pending plane (no register, no drain, no sentinel re-entry, no Contract 5
 * fingerprint-kind uniqueness).
 *
 * <p>Used by {@code SqlMemberSource} consumers wired through
 * {@link NativeSqlRegistry#executeOneShot(NativeSqlOneShotWork)}.
 *
 * <p>Result type {@code R} MUST be immutable, effectively immutable, or a
 * defensive copy before being returned from {@link #consume} or
 * {@link #fallbackValue}. The substrate stores the returned value directly
 * in the process-wide success cache; subsequent cache hits hand the same
 * instance to every caller.
 *
 * <p>Phase 8a permitted payload shapes: scalar boxed primitives, immutable
 * records, {@code Collections.unmodifiableList} wrappers around
 * already-immutable contents. NOT permitted: live {@link ResultSet}
 * objects, mutable maps/lists handed back to the caller, RolapMember
 * graphs, or objects whose identity is tied to a per-call member-cache
 * context.
 *
 * <p>Consumer override points:
 * <ul>
 *   <li>{@link #consume} — required. Read the {@link ResultSet} into the
 *       immutable cache payload.</li>
 *   <li>{@link #fallbackValue} — required. Return the caller-visible
 *       sentinel for FALLBACK-classified errors. Implementations whose
 *       {@link #policyAdjust} always returns
 *       {@link NativeSqlError.Classification#PROPAGATE} should throw
 *       {@link IllegalStateException} here, making the unreachability
 *       explicit at the type level.</li>
 *   <li>{@link #policyAdjust} — default accepts the classifier verdict
 *       unchanged (inherited from {@link NativeSqlPolicy}).</li>
 *   <li>{@link #allowsPropagateDowngrade} — default {@code false}
 *       (inherited from {@link NativeSqlPolicy}).</li>
 * </ul>
 */
public abstract class NativeSqlOneShotWork<R> implements NativeSqlPolicy {

    private final NativeSqlFingerprint fingerprint;
    private final DataSource dataSource;
    private final String sql;

    protected NativeSqlOneShotWork(
        NativeSqlFingerprint fingerprint,
        DataSource dataSource,
        String sql)
    {
        this.fingerprint = Objects.requireNonNull(fingerprint, "fingerprint");
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.sql = Objects.requireNonNull(sql, "sql");
    }

    public final NativeSqlFingerprint fingerprint() { return fingerprint; }
    public final DataSource dataSource()            { return dataSource; }
    public final String sql()                        { return sql; }

    /**
     * Read the {@link ResultSet} into the immutable cache payload.
     */
    public abstract R consume(ResultSet rs) throws SQLException;

    /**
     * Substrate hands you the FALLBACK-classified throwable; return the
     * caller-visible sentinel. PROPAGATE-only implementations should throw
     * {@link IllegalStateException} so unreachability is explicit.
     */
    public abstract R fallbackValue(Throwable t);
}
