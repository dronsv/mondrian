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

import java.util.Collections;
import java.util.List;

/**
 * Structural SQL fragment for a column referenced by a WHERE predicate.
 *
 * <p>Mirrors {@link LevelSql} but is produced by
 * {@link ResolvedTable#resolvePredicateColumn} — the predicate path takes
 * a {@link RolapStar.Column} (carried by
 * {@link mondrian.rolap.agg.ValueColumnPredicate}) instead of a hierarchy.
 *
 * @param qualifiedColumn  alias-qualified column expression, e.g.
 *                         {@code f.region} or {@code dim_x.brand}
 * @param joinClauses      JOIN clauses required to make {@code qualifiedColumn}
 *                         resolvable; empty for columns inlined in the agg
 *                         fact table or columns that already live there
 */
public record PredicateSql(
    String qualifiedColumn,
    List<String> joinClauses)
{
    public PredicateSql {
        joinClauses = Collections.unmodifiableList(joinClauses);
    }

    /** Convenience constructor for inline columns (no joins). */
    public PredicateSql(String qualifiedColumn) {
        this(qualifiedColumn, Collections.emptyList());
    }
}
