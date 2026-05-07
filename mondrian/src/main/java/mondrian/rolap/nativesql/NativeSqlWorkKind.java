/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.nativesql;

/**
 * Result shape classifier for {@link NativeSqlWork}.
 *
 * <p>Per Contract 1 of the design spec, a cached pending-plane result is
 * uniquely identified by {@code (fingerprint, NativeSqlWorkKind)}. (Pending
 * plane only — the one-shot plane added in Phase 8a uses an internal bucket
 * discriminator.)  Contract 5 forbids mixing kinds under the same fingerprint
 * within one registry.
 */
public enum NativeSqlWorkKind {
    /** One SQL → one scalar.  Materialization is shape-agnostic. */
    SCALAR,
    /** One SQL → a coordinate-keyed map of values. */
    BATCH
}
