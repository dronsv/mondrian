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
    LevelSql resolveLevel(LevelRef level, String alias);

    /** Whether this is an aggregate table (for diagnostics). */
    boolean isAggregate();

    /**
     * Whether rollup aggregation is required. Phase 2A table-level
     * approximation; may migrate to measure-scoped in future phases.
     */
    boolean needsRollup();
}
