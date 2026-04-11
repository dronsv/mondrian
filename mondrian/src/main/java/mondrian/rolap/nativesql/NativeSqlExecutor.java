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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * JDBC envelope for native SQL execution in the cell-phase registry
 * substrate.  Wraps connection acquisition, statement creation, timeout
 * configuration, and resource cleanup so consumers never touch JDBC directly.
 *
 * <p>Per Contract 4 of the design spec: timeout is passed to
 * {@link Statement#setQueryTimeout(int)}.  Cancellation is handled by the
 * JDBC driver via {@code Statement.cancel()} invoked externally — this
 * class has no special-case handling for it; a cancelled statement throws
 * {@link SQLException} from {@code executeQuery}, which propagates normally.
 *
 * <p><b>TODO (open question from spec Section 10):</b> Integration with
 * Mondrian's query governor / {@code QueryTiming} is not yet wired.  This
 * initial version executes SQL directly without telemetry span begin/end
 * around the execute call.  Before Phase 4 migration of NativeSqlCalc,
 * verify the exact Mondrian hook point and add the span.
 */
public final class NativeSqlExecutor {

    private NativeSqlExecutor() { /* utility */ }

    /** Callback for reading a {@link ResultSet} into a typed payload. */
    @FunctionalInterface
    public interface ResultSetHandler<T> {
        T handle(ResultSet rs) throws SQLException;
    }

    /**
     * Execute {@code sql} against {@code dataSource} and pass the result set
     * to {@code handler} for materialization.  Closes all JDBC resources in
     * finally blocks regardless of outcome.
     *
     * @param sql            SQL to execute; must be non-null
     * @param dataSource     data source to acquire connection from; non-null
     * @param timeoutSeconds query timeout in seconds; zero means no timeout;
     *                       negative is rejected
     * @param handler        result set consumer; non-null
     * @throws SQLException              on any JDBC failure or handler failure
     * @throws IllegalArgumentException  if timeoutSeconds is negative
     * @throws NullPointerException      if sql, dataSource, or handler is null
     */
    public static <T> T run(
        String sql,
        DataSource dataSource,
        int timeoutSeconds,
        ResultSetHandler<T> handler)
        throws SQLException
    {
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(dataSource, "dataSource");
        Objects.requireNonNull(handler, "handler");
        if (timeoutSeconds < 0) {
            throw new IllegalArgumentException(
                "timeoutSeconds must be >= 0, got " + timeoutSeconds);
        }

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement())
        {
            stmt.setQueryTimeout(timeoutSeconds);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                return handler.handle(rs);
            }
        }
    }
}
