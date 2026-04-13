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

import mondrian.olap.Hierarchy;

/**
 * Stable key that identifies a hierarchy level within an NQE resolution
 * request.
 *
 * <p>Pairs a {@link Hierarchy} (for MDX-layer identity and level ordering)
 * with the {@link RolapStar} it belongs to (needed to locate the physical
 * column in a star or agg table).
 *
 * <p>The resolver uses this key to decide which column in the
 * {@link ResolvedTable} satisfies the level constraint, and whether any
 * extra join clauses to dimension tables are required.
 */
public record LevelRef(
    Hierarchy hierarchy,
    RolapStar star) {}
