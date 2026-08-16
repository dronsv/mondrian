package mondrian.rolap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import mondrian.rolap.agg.CellRequest;
import mondrian.rolap.agg.CellRequestKey;
import mondrian.rolap.agg.ValueColumnPredicate;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * emondrian-clickhouse#84 — two-tier phase-progress guard for the
 * batch-drain loops in RolapResult.
 *
 * <p>Tier 1 (below the activation threshold): counts phases only —
 * zero overhead, no key tracking. Tier 2 (past the threshold):
 * delta = pendingKeys − seenKeys per phase; progress is a non-empty
 * delta, a native-registry advance, or an empty pending set. A small
 * consecutive-no-progress budget separates a real repeat cycle from
 * transient repetition (e.g. mid-query segment eviction); a unique-key
 * cap guards against an unbounded stream of new requests.
 */
class PhaseProgressGuardTest {

    private final RolapStar star = mockStar();
    private final RolapStar.Measure measure = mockMeasure(star);

    @Test
    void tier1_underThreshold_neverThrows_evenWhenRepeating() {
        PhaseProgressGuard guard = new PhaseProgressGuard(10, 1, 1000);
        for (int i = 0; i < 10; i++) {
            guard.onPhaseAdvanced(outcome(keys(key(1, "A")), false));
        }
        assertEquals(10, guard.getPhaseCount());
    }

    @Test
    void lemanaShape_manyDistinctProgressPhases_neverThrows() {
        // 12 finite phases, each introducing a new structural key —
        // the exact live shape that the old MaxEvalDepth pass limit
        // rejected as a false cycle.
        PhaseProgressGuard guard = new PhaseProgressGuard(5, 3, 1000);
        for (int i = 0; i < 12; i++) {
            guard.onPhaseAdvanced(
                outcome(keys(key(1, "member-" + i)), false));
        }
        assertEquals(12, guard.getPhaseCount());
    }

    @Test
    void repeatedKey_pastActivation_burnsBudget_thenThrows() {
        PhaseProgressGuard guard = new PhaseProgressGuard(2, 3, 1000);
        CellRequestKey a = key(1, "A");

        guard.onPhaseAdvanced(outcome(keys(a), false)); // tier 1
        guard.onPhaseAdvanced(outcome(keys(a), false)); // tier 1
        guard.onPhaseAdvanced(outcome(keys(a), false)); // first tracked: new
        guard.onPhaseAdvanced(outcome(keys(a), false)); // no progress 1
        guard.onPhaseAdvanced(outcome(keys(a), false)); // no progress 2
        guard.onPhaseAdvanced(outcome(keys(a), false)); // no progress 3

        RuntimeException e = assertThrows(
            RuntimeException.class,
            () -> guard.onPhaseAdvanced(outcome(keys(a), false)));
        assertTrue(
            e.getMessage().contains("no new cell requests"),
            "diagnostic must state the no-progress condition: "
                + e.getMessage());
        assertTrue(
            e.getMessage().contains("measure="),
            "diagnostic must render a repeated request: " + e.getMessage());
    }

    @Test
    void alternatingCycle_aba_detected() {
        PhaseProgressGuard guard = new PhaseProgressGuard(0, 2, 1000);
        CellRequestKey a = key(1, "A");
        CellRequestKey b = key(2, "B");

        guard.onPhaseAdvanced(outcome(keys(a), false)); // new
        guard.onPhaseAdvanced(outcome(keys(b), false)); // new
        guard.onPhaseAdvanced(outcome(keys(a), false)); // repeat 1
        guard.onPhaseAdvanced(outcome(keys(b), false)); // repeat 2

        assertThrows(
            RuntimeException.class,
            () -> guard.onPhaseAdvanced(outcome(keys(a), false)));
    }

    @Test
    void partialRepetition_newKeyAmongRepeats_isProgress() {
        PhaseProgressGuard guard = new PhaseProgressGuard(0, 1, 1000);
        CellRequestKey a = key(1, "A");

        guard.onPhaseAdvanced(outcome(keys(a, key(2, "B")), false));
        // {a, c}: a repeats but c is new — monotonic progress.
        guard.onPhaseAdvanced(outcome(keys(a, key(3, "C")), false));
        assertEquals(0, guard.getConsecutiveNoProgress());
    }

    @Test
    void registryAdvance_withEmptyDelta_isProgress() {
        PhaseProgressGuard guard = new PhaseProgressGuard(0, 1, 1000);
        CellRequestKey a = key(1, "A");

        guard.onPhaseAdvanced(outcome(keys(a), false));
        guard.onPhaseAdvanced(outcome(keys(a), true)); // registry advanced
        assertEquals(0, guard.getConsecutiveNoProgress());
    }

    @Test
    void emptyPendingSet_isProgress() {
        PhaseProgressGuard guard = new PhaseProgressGuard(0, 1, 1000);
        guard.onPhaseAdvanced(
            outcome(Collections.<CellRequestKey>emptySet(), false));
        guard.onPhaseAdvanced(
            outcome(Collections.<CellRequestKey>emptySet(), false));
        assertEquals(0, guard.getConsecutiveNoProgress());
    }

    @Test
    void budgetResets_afterProgress() {
        PhaseProgressGuard guard = new PhaseProgressGuard(0, 2, 1000);
        CellRequestKey a = key(1, "A");

        guard.onPhaseAdvanced(outcome(keys(a), false)); // new
        guard.onPhaseAdvanced(outcome(keys(a), false)); // repeat 1
        guard.onPhaseAdvanced(outcome(keys(key(2, "B")), false)); // progress
        assertEquals(0, guard.getConsecutiveNoProgress());
        guard.onPhaseAdvanced(outcome(keys(a), false)); // repeat 1 again
        guard.onPhaseAdvanced(outcome(keys(a), false)); // repeat 2
        // Budget (2) not yet exceeded — no throw so far.
        assertEquals(2, guard.getConsecutiveNoProgress());
    }

    @Test
    void unboundedUniqueStream_hitsUniqueKeyCap() {
        PhaseProgressGuard guard = new PhaseProgressGuard(0, 3, 5);
        for (int i = 0; i < 5; i++) {
            guard.onPhaseAdvanced(outcome(keys(key(1, "m" + i)), false));
        }
        RuntimeException e = assertThrows(
            RuntimeException.class,
            () -> guard.onPhaseAdvanced(outcome(keys(key(1, "m99")), false)));
        assertTrue(
            e.getMessage().contains("unique"),
            "diagnostic must state the unique-work cap: " + e.getMessage());
    }

    /**
     * Tier-1 laziness contract: callers skip building CellRequestKeys
     * entirely while the next phase is still below the activation
     * threshold — the guard must announce when keys become needed.
     */
    @Test
    void keysNeeded_falseUnderThreshold_trueOnce_activationReached() {
        PhaseProgressGuard guard = new PhaseProgressGuard(2, 1, 1000);
        assertFalse(guard.keysNeededForNextPhase());
        guard.onPhaseAdvanced(
            outcome(Collections.<CellRequestKey>emptySet(), false));
        assertFalse(guard.keysNeededForNextPhase());
        guard.onPhaseAdvanced(
            outcome(Collections.<CellRequestKey>emptySet(), false));
        assertTrue(
            guard.keysNeededForNextPhase(),
            "phase 3 exceeds threshold 2 — keys must be requested");
    }

    // ----- fixtures --------------------------------------------------------

    private PhaseOutcome outcome(
        Set<CellRequestKey> pending, boolean registryAdvanced)
    {
        return new PhaseOutcome(pending, pending.size(), registryAdvanced);
    }

    private static Set<CellRequestKey> keys(CellRequestKey... keys) {
        Set<CellRequestKey> set = new LinkedHashSet<CellRequestKey>();
        Collections.addAll(set, keys);
        return set;
    }

    private CellRequestKey key(int bitPos, Object value) {
        RolapStar.Column column = mock(RolapStar.Column.class);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(bitPos);
        when(column.getTable()).thenReturn(mock(RolapStar.Table.class));
        CellRequest request = new CellRequest(measure, false, false);
        request.addConstrainedColumn(
            column, new ValueColumnPredicate(column, value));
        return CellRequestKey.of(request);
    }

    private static RolapStar mockStar() {
        RolapStar star = mock(RolapStar.class);
        when(star.getColumnCount()).thenReturn(8);
        return star;
    }

    private static RolapStar.Measure mockMeasure(RolapStar star) {
        RolapStar.Measure measure = mock(RolapStar.Measure.class);
        when(measure.getStar()).thenReturn(star);
        when(measure.getName()).thenReturn("m");
        when(measure.getCubeName()).thenReturn("c");
        return measure;
    }
}
