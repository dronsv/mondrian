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
 * Descriptor for a SQL source table selected by the NQE resolver for a
 * specific query shape (Phase 2A aggregate-table support).
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>{@link #resolveMeasure} returns the full aggregate SQL expression for
 *       the measure, qualified with the caller-supplied alias.  The expression
 *       is ready to paste into a SELECT clause.</li>
 *   <li>{@link #resolveLevel} returns the qualified column reference plus any
 *       join predicates as immutable data ({@link LevelSql}).  The method
 *       never mutates query state — it is the SQL generator's job to apply
 *       the returned clauses.</li>
 *   <li>{@link #needsRollup} is a Phase 2A table-level approximation: when
 *       {@code true}, the generator must wrap pre-aggregated columns in the
 *       appropriate merge/rollup aggregate function rather than reading them
 *       raw.</li>
 *   <li>Alias assignment is generator-owned.  The resolver receives the alias
 *       as a parameter and never allocates its own.</li>
 *   <li>{@code ResolvedTable} never decides fallback policy.  If the table
 *       cannot satisfy a measure or level it throws
 *       {@link IllegalArgumentException}; the caller decides whether to fall
 *       back to the fact table.</li>
 * </ul>
 */
public interface ResolvedTable {

    /**
     * Physical name of the table (without schema prefix).
     *
     * @return table name, never {@code null}
     */
    String tableName();

    /**
     * Returns the full aggregate SQL expression for {@code measure}, qualified
     * with {@code alias}.
     *
     * @param measure   the measure to resolve; must be reachable from this table
     * @param alias     SQL alias assigned to this table by the generator
     * @return          SQL fragment for the SELECT clause
     * @throws IllegalArgumentException if the measure is not available
     */
    MeasureSql resolveMeasure(MeasureRef measure, String alias);

    /**
     * Returns the qualified column reference and any join predicates for
     * {@code level}, qualified with {@code alias}.
     *
     * @param level     the hierarchy level to resolve
     * @param alias     SQL alias assigned to this table by the generator
     * @return          column expression plus (possibly empty) join clauses
     * @throws IllegalArgumentException if the level is not available
     */
    LevelSql resolveLevel(LevelRef level, String alias);

    /**
     * {@code true} when this table stores pre-aggregated (partially rolled-up)
     * values and measures must be wrapped in a merge/rollup function before use.
     *
     * <p>This is a table-level approximation for Phase 2A; per-measure
     * granularity may be introduced in a later phase.
     *
     * @return whether the SQL generator must apply a rollup wrapper
     */
    boolean isAggregate();

    /**
     * {@code true} when the selected source is a pre-aggregated table whose
     * values require a further rollup/merge step in the generated SQL.
     *
     * @return whether a rollup step is needed
     */
    boolean needsRollup();
}
