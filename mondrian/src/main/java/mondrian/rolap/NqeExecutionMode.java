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
 * Execution mode for a NativeQueryEngine query, derived from the set of
 * measure candidates.
 *
 * <ul>
 * <li>{@link #FULL_RESULT} — NQE owns all measures; result is complete.</li>
 * <li>{@link #PREFETCH_ONLY} — NQE pre-populates the segment cache for stored
 *     measures so that non-ownable measures (e.g. DIRECT_PUSH_NATIVE) can be
 *     resolved by their own evaluator against already-cached data.</li>
 * <li>{@link #BYPASS} — no stored measures; NQE cannot help and must yield
 *     entirely to the legacy evaluator.</li>
 * </ul>
 */
public enum NqeExecutionMode {
    FULL_RESULT,
    PREFETCH_ONLY,
    BYPASS
}

// End NqeExecutionMode.java
