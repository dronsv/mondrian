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
 * SQL rendering result for a single measure column resolved against a
 * {@link ResolvedTable}.
 *
 * <p>{@code expression} is the complete SQL fragment to place in the SELECT
 * clause, already qualified with the table alias supplied to
 * {@link ResolvedTable#resolveMeasure}.  Examples:
 * <ul>
 *   <li>fact table aggregate: {@code SUM("t0"."unit_sales")}</li>
 *   <li>pre-aggregated column: {@code SUM("a0"."unit_sales_sum")}</li>
 *   <li>HLL merge aggregate: {@code uniqCombinedMerge("a0"."akb_state")}</li>
 * </ul>
 */
public record MeasureSql(String expression) {}
