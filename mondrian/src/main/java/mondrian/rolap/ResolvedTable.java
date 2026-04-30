package mondrian.rolap;

/**
 * Source contract for NQE SQL generation. Models both fact tables and
 * aggregate tables behind a unified interface.
 *
 * <p>Alias assignment ({@code "f"}) is generator-owned, not a
 * property of the table. ResolvedTable never decides fallback policy.
 */
public interface ResolvedTable {

    /** Physical table name for the FROM clause. */
    String tableName();

    /**
     * Returns the full aggregate SQL expression for a measure.
     *
     * @param measure typed measure reference
     * @param alias   SQL alias assigned by the generator
     * @return measure SQL fragment, or null if measure not found
     */
    MeasureSql resolveMeasure(MeasureRef measure, String alias);

    /**
     * Returns the qualified column expression and any required joins.
     *
     * @param level typed level reference
     * @param alias SQL alias assigned by the generator
     * @return level SQL fragment, or null if level cannot be resolved
     */
    LevelSql resolveLevel(StarLevelRef level, String alias);

    /**
     * Returns the qualified column expression for a WHERE predicate column,
     * plus any JOIN clauses required to make it resolvable.
     *
     * <p>Implementations choose whether to skip a dim-table JOIN when the
     * physical source already has the column inlined. {@link FactResolvedTable}
     * always emits the JOIN (the fact star has no inlined dim columns).
     * {@link AggResolvedTable} skips the JOIN when the agg has the column
     * collapsed into its fact row.
     *
     * @param column the {@link RolapStar.Column} carried by the predicate
     * @param alias  SQL alias assigned by the generator
     * @return predicate SQL fragment, or null if the column cannot be
     *         resolved against this physical source (caller decides fallback)
     */
    PredicateSql resolvePredicateColumn(RolapStar.Column column, String alias);

    /** Whether this is an aggregate table (for diagnostics). */
    boolean isAggregate();

    /**
     * Whether rollup aggregation is required. Phase 2A table-level
     * approximation; may migrate to measure-scoped in future phases.
     */
    boolean needsRollup();
}
