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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Stable identity for a native SQL execution request.
 *
 * <p>Per Contract 1 of the cell-phase native registry design, a fingerprint
 * is defined over executable SQL request identity — NOT over evaluator
 * identity. Any evaluator state that can affect the SQL result MUST already
 * be rendered into {@code sql} or {@code params} by the consumer before
 * calling {@link #of}. See Contract 6 (SQL determinism) for the consumer
 * responsibility rule.
 *
 * <p>Participants in the identity key:
 * <ul>
 *   <li>canonicalized SQL (whitespace collapsed, leading/trailing trimmed);</li>
 *   <li>bound parameter values (stable serialization via {@link Objects#toString});</li>
 *   <li>{@code DataSource} object identity (different DS ≠ same result);</li>
 *   <li>session/security context string (caller responsibility).</li>
 * </ul>
 *
 * <p><b>DataSource identity limitation:</b> {@code DataSource} identity is
 * captured via {@link System#identityHashCode}, which avoids imposing an
 * {@code equals} contract on DataSource implementations but is NOT guaranteed
 * unique — two distinct DataSource instances can theoretically collide.
 * Callers must ensure DataSource instances are long-lived singletons (e.g.,
 * from a connection pool) to guarantee hash uniqueness. Short-lived or
 * transient DataSource instances may produce hash collisions and cause
 * incorrect cache sharing.
 */
public final class NativeSqlFingerprint {

    private final String canonicalSql;
    private final String paramsRepr;
    private final int dataSourceIdentityHash;
    private final String session;
    private final int hash;

    private NativeSqlFingerprint(
        String canonicalSql,
        String paramsRepr,
        int dataSourceIdentityHash,
        String session)
    {
        this.canonicalSql = canonicalSql;
        this.paramsRepr = paramsRepr;
        this.dataSourceIdentityHash = dataSourceIdentityHash;
        this.session = session;
        this.hash = Objects.hash(
            canonicalSql, paramsRepr, dataSourceIdentityHash, session);
    }

    public static NativeSqlFingerprint of(
        String sql,
        List<?> params,
        DataSource dataSource,
        String session)
    {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(params, "params");
        Objects.requireNonNull(dataSource, "dataSource");

        return new NativeSqlFingerprint(
            canonicalize(sql),
            serializeParams(params),
            System.identityHashCode(dataSource),
            session);
    }

    private static String canonicalize(String sql) {
        // Collapse all runs of whitespace (including newlines/tabs) to a single
        // space, then trim leading/trailing whitespace.  We intentionally
        // preserve case — SQL identifiers may be case-sensitive under some
        // dialects (e.g. quoted identifiers on ClickHouse).
        return sql.replaceAll("\\s+", " ").trim();
    }

    private static String serializeParams(List<?> params) {
        if (params.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (Object p : params) {
            if (!first) sb.append(",");
            first = false;
            sb.append(p == null ? "null" : p.getClass().getName())
              .append(":")
              .append(Objects.toString(p));
        }
        sb.append("]");
        return sb.toString();
    }

    /** Debug view of the fingerprint inputs — for logging and tests. */
    public Map<String, String> describe() {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("sql", canonicalSql);
        out.put("params", paramsRepr);
        out.put("dataSource", "identityHash=" + dataSourceIdentityHash);
        out.put("session", session == null ? "<null>" : session);
        return Collections.unmodifiableMap(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NativeSqlFingerprint)) return false;
        NativeSqlFingerprint that = (NativeSqlFingerprint) o;
        return dataSourceIdentityHash == that.dataSourceIdentityHash
            && canonicalSql.equals(that.canonicalSql)
            && paramsRepr.equals(that.paramsRepr)
            && Objects.equals(session, that.session);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return "NativeSqlFingerprint{"
            + "sql=" + (canonicalSql.length() > 60
                ? canonicalSql.substring(0, 60) + "..."
                : canonicalSql)
            + ",params=" + paramsRepr
            + ",dsHash=" + dataSourceIdentityHash
            + ",session=" + session
            + "}";
    }
}
