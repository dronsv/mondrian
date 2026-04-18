package mondrian.rolap;

import mondrian.olap.Hierarchy;

/**
 * Typed reference to a hierarchy level for source-agnostic resolution.
 *
 * @param hierarchy the hierarchy to resolve
 * @param star      the star schema containing column metadata
 */
public record StarLevelRef(Hierarchy hierarchy, RolapStar star) {}
