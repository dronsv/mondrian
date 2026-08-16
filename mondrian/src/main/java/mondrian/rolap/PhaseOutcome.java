/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap;

import mondrian.rolap.agg.CellRequestKey;

import java.util.Collections;
import java.util.Set;

/**
 * Immutable summary of one batch-drain phase (emondrian-clickhouse#84):
 * the structural keys of the cell requests that were pending when the
 * phase started, the raw pending count (unkeyable requests included),
 * and whether the native-SQL registry advanced during the phase.
 *
 * <p>Consumed by {@link PhaseProgressGuard} to distinguish monotonic
 * progress from a repeated no-progress state.
 */
public final class PhaseOutcome {

    private final Set<CellRequestKey> pendingKeys;
    private final int pendingRequestCount;
    private final boolean registryAdvanced;

    public PhaseOutcome(
        Set<CellRequestKey> pendingKeys,
        int pendingRequestCount,
        boolean registryAdvanced)
    {
        this.pendingKeys = pendingKeys == null
            ? Collections.<CellRequestKey>emptySet()
            : pendingKeys;
        this.pendingRequestCount = pendingRequestCount;
        this.registryAdvanced = registryAdvanced;
    }

    /** Keys of requests pending at phase start; never null. */
    public Set<CellRequestKey> getPendingKeys() {
        return pendingKeys;
    }

    /** Raw pending request count, including unkeyable requests. */
    public int getPendingRequestCount() {
        return pendingRequestCount;
    }

    /** Whether the native-SQL work registry completed work this phase. */
    public boolean isRegistryAdvanced() {
        return registryAdvanced;
    }
}
