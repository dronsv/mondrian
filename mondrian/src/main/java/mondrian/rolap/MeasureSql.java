package mondrian.rolap;

/**
 * Structural SQL fragment for a resolved measure expression.
 *
 * @param expression the aggregate SQL expression, e.g. {@code SUM(f.sales)}
 */
public record MeasureSql(String expression) {}
