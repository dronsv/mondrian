package mondrian.rolap;

import java.util.Collections;
import java.util.List;

/**
 * Structural SQL fragment for a resolved level column.
 *
 * @param expression  qualified column, e.g. {@code d.brand_name}
 * @param joinClauses JOIN clauses needed, may be empty for inline columns
 */
public record LevelSql(
    String expression,
    List<String> joinClauses)
{
    public LevelSql {
        joinClauses = Collections.unmodifiableList(joinClauses);
    }

    /** Convenience constructor for inline columns (no joins). */
    public LevelSql(String expression) {
        this(expression, Collections.emptyList());
    }
}
