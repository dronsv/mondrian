package mondrian.rolap;

/**
 * Outcome of physical source resolution for an NQE coordinate class plan.
 *
 * <p>{@code UnresolvedSourcePlan} means NQE logical eligibility succeeded
 * but no supported single-source physical plan could be constructed.
 * The caller ({@link NativeQueryEngine}) decides fallback policy.
 *
 * <p>Phase 2A accepts only {@code SingleSourcePlan}.
 */
public sealed interface ResolvedSourcePlan
    permits ResolvedSourcePlan.UnresolvedSourcePlan,
            ResolvedSourcePlan.SingleSourcePlan
{
    record UnresolvedSourcePlan(String reason)
        implements ResolvedSourcePlan {}

    record SingleSourcePlan(ResolvedTable table)
        implements ResolvedSourcePlan {}
}
