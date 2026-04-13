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
 * Outcome of the source-plan resolution step in NativeQueryEngine's
 * aggregate-table support (Phase 2A).
 *
 * <p>Two closed states:
 * <ul>
 *   <li>{@link UnresolvedSourcePlan} — no agg table matched; carries a
 *       human-readable reason string for diagnostics.</li>
 *   <li>{@link SingleSourcePlan} — exactly one {@link ResolvedTable} was
 *       selected; the NQE SQL generator uses it directly.</li>
 * </ul>
 *
 * <p>The resolver never throws; it always returns one of these two states so
 * the generator can handle the miss path without try/catch.
 */
public sealed interface ResolvedSourcePlan
    permits ResolvedSourcePlan.UnresolvedSourcePlan,
            ResolvedSourcePlan.SingleSourcePlan
{
    /**
     * No agg table could be matched for the requested query shape.
     *
     * @param reason human-readable diagnostic (never {@code null})
     */
    record UnresolvedSourcePlan(String reason)
        implements ResolvedSourcePlan {}

    /**
     * A single agg table (or the fact table itself) was selected.
     *
     * @param table the resolved table descriptor
     */
    record SingleSourcePlan(ResolvedTable table)
        implements ResolvedSourcePlan {}
}
