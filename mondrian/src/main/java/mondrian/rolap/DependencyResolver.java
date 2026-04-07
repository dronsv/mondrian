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
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;

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
        public final FormulaNormalizer.Result normalizedFormula;
        /**
         * Leaf index to PhysicalValueRequest. Index matches the order
         * in {@link FormulaNormalizer.Result#leafRefs}.
         */
        public final Map<Integer, PhysicalValueRequest> leafBindings;

        PostProcessPlan(
            Member measure,
            FormulaNormalizer.Result normalizedFormula,
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
    // Per-candidate resolution
    // -----------------------------------------------------------------------

    /**
     * Resolves a stored (non-calculated) measure to a PhysicalValueRequest.
     */
    private static PhysicalValueRequest resolveStoredMeasure(
        Member measure, Set<Hierarchy> queryHierarchies)
    {
        if (!(measure instanceof RolapStoredMeasure)) {
            return null;
        }
        RolapStoredMeasure stored = (RolapStoredMeasure) measure;
        String measureId = stored.getUniqueName();

        RolapAggregator agg = stored.getAggregator();
        PhysicalValueRequest.AggregationKind aggKind = mapAggregator(agg);

        return new PhysicalValueRequest(
            measureId,
            queryHierarchies,
            Collections.<Hierarchy>emptySet(),
            aggKind,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);
    }

    /**
     * Resolves a native SQL measure to a PhysicalValueRequest.
     */
    private static PhysicalValueRequest resolveNativeMeasure(
        Member measure, Set<Hierarchy> queryHierarchies)
    {
        if (!(measure instanceof RolapMember)) {
            return null;
        }
        RolapCalculatedMember nativeMember =
            NativeSqlConfig.findNativeSqlMember((RolapMember) measure);
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
            template);
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
        FormulaNormalizer.Result nf = candidate.normalizedFormula;
        if (nf == null) {
            return null;
        }

        Map<Integer, PhysicalValueRequest> leafBindings =
            new LinkedHashMap<Integer, PhysicalValueRequest>();

        for (int i = 0; i < nf.leafRefs.size(); i++) {
            Exp leafRef = nf.leafRefs.get(i);

            Member leafMember = extractMember(leafRef);
            if (leafMember == null) {
                return null; // can't resolve (e.g. Id reference)
            }

            PhysicalValueRequest req =
                resolveLeafMember(leafMember, queryHierarchies);
            if (req == null) {
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
     */
    private static PhysicalValueRequest resolveLeafMember(
        Member member, Set<Hierarchy> queryHierarchies)
    {
        if (!member.isMeasure()) {
            return null;
        }

        // Stored (non-calculated) measure
        if (!member.isCalculated()) {
            return resolveStoredMeasure(member, queryHierarchies);
        }

        // Calculated member — check for native SQL annotations
        if (member instanceof RolapMember) {
            RolapCalculatedMember nativeMember =
                NativeSqlConfig.findNativeSqlMember((RolapMember) member);
            if (nativeMember != null) {
                return resolveNativeMeasure(member, queryHierarchies);
            }
        }

        // Calculated measure with no native SQL — can't resolve
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
        return PhysicalValueRequest.AggregationKind.SUM;
    }
}

// End DependencyResolver.java
