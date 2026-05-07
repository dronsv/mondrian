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
 * Shared classification-policy hooks for native SQL work units.
 *
 * <p>Implemented by both {@link NativeSqlWork} (pending plane) and
 * {@link NativeSqlOneShotWork} (one-shot plane) so the FALLBACK /
 * PROPAGATE classification rules are expressed once and applied
 * uniformly by the registry's drain loop and by the static one-shot
 * entry point.
 *
 * <p>Default behaviour:
 * <ul>
 *   <li>{@link #policyAdjust} — accept the classifier verdict unchanged.</li>
 *   <li>{@link #allowsPropagateDowngrade} — reject PROPAGATE → FALLBACK
 *       downgrades. Implementations override only with explicit, documented
 *       rationale.</li>
 * </ul>
 */
public interface NativeSqlPolicy {

    /**
     * Consumer-side classification override hook. Default: accept
     * {@code base} unchanged.
     */
    default NativeSqlError.Classification policyAdjust(
        Throwable t,
        NativeSqlError.Classification base)
    {
        return base;
    }

    /**
     * Opt-in flag for PROPAGATE → FALLBACK downgrades in
     * {@link #policyAdjust}. Default {@code false} — the registry rejects
     * downgrades and emits an unauthorized-downgrade telemetry warning
     * unless this returns {@code true}.
     */
    default boolean allowsPropagateDowngrade() {
        return false;
    }
}
