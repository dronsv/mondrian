package mondrian.rolap;

import mondrian.olap.Hierarchy;
import mondrian.olap.Level;

/**
 * Typed reference to a hierarchy level for source-agnostic resolution.
 *
 * @param hierarchy the hierarchy to resolve
 * @param level     the concrete level to resolve, or null for leaf-level
 *                  fallback
 * @param star      the star schema containing column metadata
 */
public record StarLevelRef(Hierarchy hierarchy, Level level, RolapStar star) {
    public StarLevelRef(Hierarchy hierarchy, RolapStar star) {
        this(hierarchy, null, star);
    }
}
