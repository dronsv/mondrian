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

import mondrian.mdx.MemberExpr;
import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.*;

/**
 * Phase B of NativeQueryEngine: resolves Phase A candidates into
 * physical value requests.
 *
 * <p>For each {@link MeasureClassifier.Candidate} produced by Phase A,
 * DependencyResolver produces one or more
 * {@link PhysicalValueRequest} objects that describe the physical SQL
 * columns and aggregations needed. For PostProcess candidates, it also
 * resolves leaf measure references in the formula and builds a
 * {@link PostProcessPlan}.
 *
 * <p>If any leaf resolution fails (e.g. a leaf is an unresolvable
 * calculated measure), the entire resolution returns {@code null},
 * signalling that the query must fall back to the standard MDX
 * evaluator.
 */
public class DependencyResolver {

    private static final Logger LOGGER =
        LogManager.getLogger(DependencyResolver.class);

    private DependencyResolver() {}

    // -----------------------------------------------------------------------
    // Public data types
    // -----------------------------------------------------------------------

    /**
     * Holds the resolved output for one PostProcess measure.
     */
    public static class PostProcessPlan {
        /** The calculated measure this plan covers. */
        public final Member measure;
        /** The normalized formula from Phase A. */
        public final FormulaAnalyzer.Result normalizedFormula;
        /**
         * Leaf index to PhysicalValueRequest. Index matches the order
         * in {@link FormulaAnalyzer.Result#leafRefs}.
         */
        public final Map<Integer, PhysicalValueRequest> leafBindings;

        PostProcessPlan(
            Member measure,
            FormulaAnalyzer.Result normalizedFormula,
            Map<Integer, PhysicalValueRequest> leafBindings)
        {
            this.measure = measure;
            this.normalizedFormula = normalizedFormula;
            this.leafBindings = Collections.unmodifiableMap(
                new LinkedHashMap<Integer, PhysicalValueRequest>(leafBindings));
        }
    }

    /**
     * Complete resolution output for the query.
     */
    public static class ResolvedPlan {
        /** All unique physical value requests needed by the query. */
        public final List<PhysicalValueRequest> allRequests;
        /**
         * Post-process plans keyed by the original calculated measure
         * member. Empty when no PostProcess candidates exist.
         */
        public final Map<Member, PostProcessPlan> postProcessPlans;

        ResolvedPlan(
            List<PhysicalValueRequest> allRequests,
            Map<Member, PostProcessPlan> postProcessPlans)
        {
            this.allRequests = Collections.unmodifiableList(
                new ArrayList<PhysicalValueRequest>(allRequests));
            this.postProcessPlans = Collections.unmodifiableMap(
                new LinkedHashMap<Member, PostProcessPlan>(postProcessPlans));
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Resolves all Phase A candidates into physical value requests.
     *
     * <p>Returns {@code null} if resolution fails and the query should
     * fall back to the standard MDX evaluator.
     *
     * @param candidates       from {@link MeasureClassifier#classifyAll}
     * @param queryHierarchies all hierarchies present on query axes
     * @return a {@link ResolvedPlan}, or {@code null} on failure
     */
    public static ResolvedPlan resolve(
        List<MeasureClassifier.Candidate> candidates,
        Set<Hierarchy> queryHierarchies)
    {
        // Dedup map: measureId -> PhysicalValueRequest
        Map<String, PhysicalValueRequest> requestsByMeasureId =
            new LinkedHashMap<String, PhysicalValueRequest>();
        Map<Member, PostProcessPlan> postProcessPlans =
            new LinkedHashMap<Member, PostProcessPlan>();

        for (MeasureClassifier.Candidate candidate : candidates) {
            switch (candidate.candidateClass) {
            case DIRECT_PUSH_STORED:
                PhysicalValueRequest storedReq =
                    resolveStoredMeasure(candidate.measure, queryHierarchies);
                if (storedReq == null) {
                    return null;
                }
                requestsByMeasureId.put(
                    storedReq.getPhysicalMeasureId(), storedReq);
                break;

            case DIRECT_PUSH_NATIVE:
                PhysicalValueRequest nativeReq =
                    resolveNativeMeasure(candidate.measure, queryHierarchies);
                if (nativeReq == null) {
                    return null;
                }
                requestsByMeasureId.put(
                    nativeReq.getPhysicalMeasureId(), nativeReq);
                break;

            case POST_PROCESS_CANDIDATE:
                PostProcessPlan plan = resolvePostProcess(
                    candidate, queryHierarchies, requestsByMeasureId);
                if (plan == null) {
                    return null;
                }
                postProcessPlans.put(candidate.measure, plan);
                break;

            default:
                // EVALUATOR should never reach here (classifyAll filters
                // them out), but if it does, fallback.
                return null;
            }
        }

        return new ResolvedPlan(
            new ArrayList<PhysicalValueRequest>(requestsByMeasureId.values()),
            postProcessPlans);
    }

    // -----------------------------------------------------------------------
    // Member unwrapping
    // -----------------------------------------------------------------------

    /**
     * Unwraps delegating / cube member wrappers to reach the underlying
     * member.  In a VirtualCube the query sees {@code RolapCubeMember}
     * wrapping a {@code RolapVirtualCubeMeasure} (which implements
     * {@code RolapStoredMeasure}).  Without unwrapping, {@code instanceof}
     * checks fail.
     *
     * <p>The loop guards against chains of wrappers (max 10 hops).
     */
    static Member unwrapMember(Member member) {
        int hops = 0;
        while (member instanceof DelegatingRolapMember && hops++ < 10) {
            member = ((DelegatingRolapMember) member).member;
        }
        return member;
    }

    // -----------------------------------------------------------------------
    // Per-candidate resolution
    // -----------------------------------------------------------------------

    /**
     * Resolves a stored (non-calculated) measure to a PhysicalValueRequest.
     *
     * <p>Records the source cube name on the request so that cross-cube
     * measures (e.g. ОКБ base from "География" when the primary cube is
     * "Продажи") are placed into separate CoordinateClassPlans and
     * executed against the correct star/fact table.
     */
    private static PhysicalValueRequest resolveStoredMeasure(
        Member measure, Set<Hierarchy> queryHierarchies)
    {
        Member unwrapped = unwrapMember(measure);
        if (!(unwrapped instanceof RolapStoredMeasure)) {
            // Not a stored measure after unwrapping — try inlining
            // through ValidMeasure/null-guard formulas to reach the
            // underlying stored measure (e.g. ОКБ → ValidMeasure(ОКБ base))
            PhysicalValueRequest inlined =
                tryInlineCalcMeasure(unwrapped, queryHierarchies);
            if (inlined != null) {
                return inlined;
            }
            LOGGER.warn(
                "resolveStoredMeasure: measure {} is {}, not RolapStoredMeasure",
                measure.getUniqueName(),
                unwrapped.getClass().getSimpleName());
            return null;
        }
        RolapStoredMeasure stored = (RolapStoredMeasure) unwrapped;
        String measureId = stored.getUniqueName();

        RolapAggregator agg = stored.getAggregator();
        PhysicalValueRequest.AggregationKind aggKind = mapAggregator(agg);

        // Distinct-count measures use HLL state aggregation, not plain column
        PhysicalValueRequest.ExpressionProviderKind providerKind =
            agg.isDistinct()
                ? PhysicalValueRequest.ExpressionProviderKind.STATE_AGGREGATE
                : PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN;

        // Record the source cube so cross-cube measures get separate SQL
        RolapCube sourceCube = stored.getCube();
        String sourceCubeName =
            sourceCube != null ? sourceCube.getName() : null;

        return new PhysicalValueRequest(
            measureId,
            queryHierarchies,
            Collections.<Hierarchy>emptySet(),
            aggKind,
            providerKind,
            null,
            null,
            sourceCubeName);
    }

    /**
     * Resolves a native SQL measure to a PhysicalValueRequest.
     */
    private static PhysicalValueRequest resolveNativeMeasure(
        Member measure, Set<Hierarchy> queryHierarchies)
    {
        Member unwrapped = unwrapMember(measure);
        if (!(unwrapped instanceof RolapMember)) {
            LOGGER.warn(
                "resolveNativeMeasure: measure {} is {}, not RolapMember",
                measure.getUniqueName(),
                unwrapped.getClass().getSimpleName());
            return null;
        }
        RolapCalculatedMember nativeMember =
            NativeSqlConfig.findNativeSqlMember((RolapMember) unwrapped);
        if (nativeMember == null) {
            return null;
        }
        NativeSqlConfig.NativeSqlDef def = NativeSqlConfig.fromAnnotations(
            nativeMember.getName(), nativeMember.getAnnotationMap());
        if (def == null) {
            return null;
        }

        String measureId = measure.getUniqueName();
        String template = def.getTemplate();

        return new PhysicalValueRequest(
            measureId,
            queryHierarchies,
            null,
            PhysicalValueRequest.AggregationKind.NATIVE_EXPRESSION,
            PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE,
            template,
            def.getVariables());
    }

    /**
     * Resolves a PostProcess candidate: walks its leaf refs, resolves
     * each to a PhysicalValueRequest, and builds a PostProcessPlan.
     */
    private static PostProcessPlan resolvePostProcess(
        MeasureClassifier.Candidate candidate,
        Set<Hierarchy> queryHierarchies,
        Map<String, PhysicalValueRequest> requestsByMeasureId)
    {
        FormulaAnalyzer.Result nf = candidate.normalizedFormula;
        if (nf == null) {
            LOGGER.warn(
                "resolvePostProcess: normalizedFormula is null for {}",
                candidate.measure.getUniqueName());
            return null;
        }

        Map<Integer, PhysicalValueRequest> leafBindings =
            new LinkedHashMap<Integer, PhysicalValueRequest>();

        for (int i = 0; i < nf.leafRefs.size(); i++) {
            Exp leafRef = nf.leafRefs.get(i);

            Member leafMember = extractMember(leafRef);
            if (leafMember == null) {
                LOGGER.warn(
                    "resolvePostProcess: cannot extract member from leaf[{}]"
                    + " expression {} for measure {}",
                    i, leafRef, candidate.measure.getUniqueName());
                return null; // can't resolve (e.g. Id reference)
            }

            PhysicalValueRequest req =
                resolveLeafMember(leafMember, queryHierarchies);
            if (req == null) {
                LOGGER.warn(
                    "resolvePostProcess: leaf[{}] {} ({}) cannot be resolved"
                    + " for measure {}",
                    i, leafMember.getUniqueName(),
                    leafMember.getClass().getSimpleName(),
                    candidate.measure.getUniqueName());
                return null; // leaf can't be resolved
            }

            // Dedup: merge into the global map
            String id = req.getPhysicalMeasureId();
            if (requestsByMeasureId.containsKey(id)) {
                req = requestsByMeasureId.get(id);
            } else {
                requestsByMeasureId.put(id, req);
            }
            leafBindings.put(i, req);
        }

        return new PostProcessPlan(candidate.measure, nf, leafBindings);
    }

    // -----------------------------------------------------------------------
    // Leaf resolution helpers
    // -----------------------------------------------------------------------

    /**
     * Resolves a leaf member (from a PostProcess formula) to a
     * PhysicalValueRequest. Handles stored measures and native SQL
     * measures. Returns {@code null} for calculated measures that
     * are not themselves native — those can't be pushed down.
     *
     * <p>Unwraps delegating members (e.g. {@code RolapCubeMember})
     * before checking member kind so that VirtualCube measures
     * resolve correctly.
     */
    private static PhysicalValueRequest resolveLeafMember(
        Member member, Set<Hierarchy> queryHierarchies)
    {
        Member unwrapped = unwrapMember(member);

        if (!unwrapped.isMeasure()) {
            LOGGER.warn(
                "resolveLeafMember: {} ({}) is not a measure",
                member.getUniqueName(),
                unwrapped.getClass().getSimpleName());
            return null;
        }

        // Stored (non-calculated) measure
        if (!unwrapped.isCalculated()) {
            return resolveStoredMeasure(member, queryHierarchies);
        }

        // Calculated member — check for native SQL annotations
        if (unwrapped instanceof RolapMember) {
            RolapCalculatedMember nativeMember =
                NativeSqlConfig.findNativeSqlMember(
                    (RolapMember) unwrapped);
            if (nativeMember != null) {
                return resolveNativeMeasure(member, queryHierarchies);
            }
        }

        // Try to inline through the calc measure's formula
        PhysicalValueRequest inlined =
            tryInlineCalcMeasure(member, queryHierarchies);
        if (inlined != null) {
            return inlined;
        }

        // Calculated measure with no native SQL and can't inline
        LOGGER.warn(
            "resolveLeafMember: {} ({}) is calculated but has no native SQL"
            + " and cannot be inlined",
            member.getUniqueName(),
            unwrapped.getClass().getSimpleName());
        return null;
    }

    /**
     * Extracts a Member from a leaf expression. Currently handles
     * {@link MemberExpr} only.
     */
    private static Member extractMember(Exp exp) {
        if (exp instanceof MemberExpr) {
            return ((MemberExpr) exp).getMember();
        }
        // Id or other expression types — can't resolve
        return null;
    }

    // -----------------------------------------------------------------------
    // Calc measure inlining
    // -----------------------------------------------------------------------

    /**
     * Tries to inline a calculated measure by walking its formula
     * to find an underlying stored measure.
     *
     * <p>Handles patterns like:
     * <ul>
     *   <li>{@code IIF(IsEmpty(x), NULL, ValidMeasure([StoredMeasure]))}</li>
     *   <li>{@code ValidMeasure([StoredMeasure])}</li>
     *   <li>{@code [StoredMeasure]} (simple alias)</li>
     * </ul>
     *
     * @return a resolved request, or {@code null} if inlining fails
     */
    private static PhysicalValueRequest tryInlineCalcMeasure(
        Member calcMember, Set<Hierarchy> queryHierarchies)
    {
        Exp formula = calcMember.getExpression();
        if (formula == null) {
            return null;
        }

        // Step 1: Normalize (strips IIF null guards)
        FormulaAnalyzer.Result nf = FormulaAnalyzer.analyze(formula);
        Exp inner = nf.normalizedExp;
        if (inner == null) {
            return null;
        }

        // Step 2: Unwrap ValidMeasure() if present
        inner = unwrapValidMeasure(inner);

        // Step 3: If we now have a simple member reference, resolve it
        Member innerMember = extractMember(inner);
        if (innerMember != null) {
            // Recursively try to resolve (with depth limit to prevent
            // infinite loops)
            return resolveLeafMemberRecursive(
                innerMember, queryHierarchies, 3);
        }

        return null;
    }

    /**
     * Unwraps {@code ValidMeasure(expr)} to {@code expr}.
     *
     * <p>{@code ValidMeasure} is a VirtualCube transparency wrapper
     * that is a no-op for NativeQueryEngine (our SQL already targets
     * the base cube).
     */
    private static Exp unwrapValidMeasure(Exp exp) {
        if (exp instanceof FunCall) {
            FunCall fc = (FunCall) exp;
            if ("ValidMeasure".equalsIgnoreCase(fc.getFunName())
                && fc.getArgCount() == 1)
            {
                return fc.getArg(0);
            }
        }
        return exp;
    }

    /**
     * Recursive version of {@link #resolveLeafMember} with a depth limit
     * to prevent infinite loops on circular calc measure definitions.
     */
    private static PhysicalValueRequest resolveLeafMemberRecursive(
        Member member, Set<Hierarchy> queryHierarchies, int maxDepth)
    {
        if (maxDepth <= 0) {
            return null;
        }
        if (!member.isMeasure()) {
            return null;
        }

        // Unwrap delegating wrappers
        Member unwrapped = unwrapMember(member);

        // Stored measure (by interface — RolapVirtualCubeMeasure
        // implements RolapStoredMeasure)
        if (unwrapped instanceof RolapStoredMeasure) {
            return resolveStoredMeasure(unwrapped, queryHierarchies);
        }

        // Non-calculated that is not RolapStoredMeasure — unlikely but
        // try stored resolution anyway
        if (!unwrapped.isCalculated()) {
            return resolveStoredMeasure(unwrapped, queryHierarchies);
        }

        // Native SQL
        if (unwrapped instanceof RolapMember) {
            RolapCalculatedMember nativeMember =
                NativeSqlConfig.findNativeSqlMember(
                    (RolapMember) unwrapped);
            if (nativeMember != null) {
                return resolveNativeMeasure(unwrapped, queryHierarchies);
            }
        }

        // Try inlining this calc measure too
        Exp formula = unwrapped.getExpression();
        if (formula == null) {
            return null;
        }
        FormulaAnalyzer.Result nf = FormulaAnalyzer.analyze(formula);
        Exp inner = nf.normalizedExp;
        if (inner == null) {
            return null;
        }
        inner = unwrapValidMeasure(inner);
        Member innerMember = extractMember(inner);
        if (innerMember != null) {
            return resolveLeafMemberRecursive(
                innerMember, queryHierarchies, maxDepth - 1);
        }

        return null;
    }

    // -----------------------------------------------------------------------
    // Aggregator mapping
    // -----------------------------------------------------------------------

    /**
     * Maps a {@link RolapAggregator} to the corresponding
     * {@link PhysicalValueRequest.AggregationKind}.
     */
    static PhysicalValueRequest.AggregationKind mapAggregator(
        RolapAggregator agg)
    {
        if (agg.isDistinct()) {
            return PhysicalValueRequest.AggregationKind.DISTINCT_MERGE;
        }
        String name = agg.getName();
        if ("sum".equalsIgnoreCase(name)) {
            return PhysicalValueRequest.AggregationKind.SUM;
        }
        if ("count".equalsIgnoreCase(name)) {
            return PhysicalValueRequest.AggregationKind.COUNT;
        }
        if ("min".equalsIgnoreCase(name)) {
            return PhysicalValueRequest.AggregationKind.MIN;
        }
        if ("max".equalsIgnoreCase(name)) {
            return PhysicalValueRequest.AggregationKind.MAX;
        }
        // Fallback — treat unknown aggregators as SUM
        LOGGER.warn(
            "mapAggregator: unknown aggregator '{}', falling back to SUM",
            agg.getName());
        return PhysicalValueRequest.AggregationKind.SUM;
    }
}

// End DependencyResolver.java
