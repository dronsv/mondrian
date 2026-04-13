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
 * SQL rendering result for a single hierarchy level resolved against a
 * {@link ResolvedTable}.
 *
 * <p>{@code expression} is the qualified column reference to use in SELECT /
 * GROUP BY / WHERE, e.g. {@code "a0"."brand_name"}.
 *
 * <p>{@code joinClauses} carries zero or more SQL predicates that must be
 * added to the WHERE clause when the level column lives in a dimension table
 * rather than the selected source table directly.  The list is returned as
 * data — the SQL generator appends them; {@link ResolvedTable} never mutates
 * query state.
 *
 * <p>The convenience single-arg constructor produces an empty join-clause list
 * and is the normal case for denormalised agg tables.
 */
public record LevelSql(
    String expression,
    List<String> joinClauses)
{
    public LevelSql {
        joinClauses = Collections.unmodifiableList(joinClauses);
    }

    public LevelSql(String expression) {
        this(expression, Collections.emptyList());
    }
}
