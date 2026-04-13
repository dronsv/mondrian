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
 * Stable key that identifies a measure within an NQE resolution request.
 *
 * <p>Carries the three pieces of information the resolver needs to locate an
 * aggregate column for the measure:
 * <ul>
 *   <li>{@code uniqueName} — fully-qualified MDX name, e.g.
 *       {@code [Measures].[Unit Sales]}</li>
 *   <li>{@code cubeName} — owning cube, used to scope the agg-table search</li>
 *   <li>{@code bitPosition} — position in the cube's {@link RolapStar}
 *       measure bit-key; used for {@link AggStar} lookups</li>
 * </ul>
 */
public record MeasureRef(
    String uniqueName,
    String cubeName,
    int bitPosition) {}
