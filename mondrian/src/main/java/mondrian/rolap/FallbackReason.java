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
 * Reasons why NativeQueryEngine falls back to legacy evaluation.
 * Logged at INFO level for rollout monitoring.
 */
public enum FallbackReason {
    DISABLED_BY_CONFIG,
    UNSUPPORTED_MEASURE_PATTERN,
    FORMULA_NORMALIZATION_FAILED,
    DEPENDENCY_LOWERING_FAILED,
    POSTPROCESS_CANDIDATE_DEGRADED,
    INCOMPATIBLE_COORDINATE_CLASSES,
    RESULT_CONTEXT_TOO_LARGE,
    SQL_GENERATION_FAILED,
    SQL_EXECUTION_FAILED
}
