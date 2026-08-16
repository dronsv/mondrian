/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.rolap.agg.CellRequestKey;

import java.util.HashSet;
import java.util.Set;

/**
 * Two-tier fixed-point progress guard for the batch-drain phase loops
 * in {@link RolapResult} (emondrian-clickhouse#84).
 *
 * <p>Replaces the fixed {@code MaxEvalDepth} pass limit, which counted
 * successful finite phases as if they were recursion and rejected
 * legitimate multi-phase loads as false cycles.
 *
 * <p><b>Tier 1</b> — below {@code activationThreshold} phases nothing
 * is tracked but a counter: zero overhead and zero memory for the
 * overwhelming majority of queries.
 *
 * <p><b>Tier 2</b> — past the threshold the guard computes
 * {@code delta = pendingKeys − seenKeys} for each phase. Progress is a
 * non-empty delta, a native-registry advance, or an empty pending set.
 * A phase with an empty delta while requests remain unresolved and no
 * asynchronous work is outstanding (the batch loader waits out its SQL
 * futures before the phase ends) is a repeated state; after
 * {@code noProgressBudget} consecutive repeated states the guard fails
 * with full diagnostics. The small budget absorbs transient repetition
 * such as mid-query segment eviction.
 *
 * <p><b>Second fuse</b> — {@code maxTrackedUniqueKeys} bounds an
 * expression that keeps generating unique requests forever (tracked
 * keys only, so ordinary large queries below the activation threshold
 * are unaffected). The query timeout/cancellation check remains the
 * caller's responsibility per phase.
 */
public final class PhaseProgressGuard {

    private final int activationThreshold;
    private final int noProgressBudget;
    private final int maxTrackedUniqueKeys;

    private final Set<CellRequestKey> seen = new HashSet<CellRequestKey>();
    private int phaseCount;
    private int trackedPhaseCount;
    private int consecutiveNoProgress;

    public PhaseProgressGuard(
        int activationThreshold,
        int noProgressBudget,
        int maxTrackedUniqueKeys)
    {
        this.activationThreshold = activationThreshold;
        this.noProgressBudget = noProgressBudget;
        this.maxTrackedUniqueKeys = maxTrackedUniqueKeys;
    }

    /**
     * Records a phase that reported more work to do. Throws when the
     * loop provably stopped making progress, or when the unique-work
     * cap is exceeded.
     */
    public void onPhaseAdvanced(PhaseOutcome outcome) {
        phaseCount++;
        if (phaseCount <= activationThreshold) {
            return;
        }
        trackedPhaseCount++;

        int newKeys = 0;
        for (CellRequestKey key : outcome.getPendingKeys()) {
            if (seen.add(key)) {
                newKeys++;
            }
        }
        if (seen.size() > maxTrackedUniqueKeys) {
            throw Util.newInternal(
                "Aggregation loading exceeded the unique-work cap: "
                + seen.size() + " unique cell requests tracked past phase "
                + activationThreshold + " (total phases: " + phaseCount
                + "). The query keeps generating new cell requests; "
                + "raise the query timeout or narrow the query.");
        }

        final boolean progress =
            newKeys > 0
            || outcome.isRegistryAdvanced()
            || outcome.getPendingKeys().isEmpty();
        if (progress) {
            consecutiveNoProgress = 0;
            return;
        }

        consecutiveNoProgress++;
        if (consecutiveNoProgress > noProgressBudget) {
            throw Util.newInternal(buildNoProgressMessage(outcome));
        }
    }

    private String buildNoProgressMessage(PhaseOutcome outcome) {
        final StringBuilder sb = new StringBuilder(256);
        sb.append("Aggregation loading stopped making progress: ")
            .append(consecutiveNoProgress)
            .append(" consecutive phases produced no new cell requests ")
            .append("while ")
            .append(outcome.getPendingRequestCount())
            .append(" request(s) stayed unresolved and no asynchronous ")
            .append("work was outstanding (repeated state, not finite ")
            .append("progress). Total phases: ").append(phaseCount)
            .append(", tracked phases: ").append(trackedPhaseCount)
            .append(", unique requests tracked: ").append(seen.size())
            .append(", no-progress budget: ").append(noProgressBudget)
            .append('.');
        int rendered = 0;
        for (CellRequestKey key : outcome.getPendingKeys()) {
            if (rendered == 3) {
                sb.append(" …");
                break;
            }
            sb.append(" Repeated request: ").append(key.describe()).append('.');
            rendered++;
        }
        return sb.toString();
    }

    /**
     * Whether the next phase's pending keys will actually be consumed.
     * Callers skip building {@link CellRequestKey}s below the
     * activation threshold — tier 1 stays zero-cost.
     */
    public boolean keysNeededForNextPhase() {
        return phaseCount + 1 > activationThreshold;
    }

    public int getPhaseCount() {
        return phaseCount;
    }

    public int getConsecutiveNoProgress() {
        return consecutiveNoProgress;
    }
}
