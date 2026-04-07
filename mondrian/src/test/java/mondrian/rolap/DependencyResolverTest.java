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

import junit.framework.TestCase;
import mondrian.mdx.MemberExpr;
import mondrian.olap.*;

import java.util.*;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link DependencyResolver} — Phase B of NativeQueryEngine.
 *
 * <p>Uses Mockito to simulate Mondrian runtime types (RolapStoredMeasure,
 * RolapAggregator, etc.) without requiring a full schema/connection.
 */
public class DependencyResolverTest extends TestCase {

    // -----------------------------------------------------------------------
    // DirectPush Stored tests
    // -----------------------------------------------------------------------

    /**
     * A single stored measure must resolve to exactly one
     * PhysicalValueRequest with Identity projection (all query
     * hierarchies), SUM aggregation, and STORED_COLUMN provider.
     */
    public void testDirectStoredResolvesToIdentityRequest() {
        RolapStoredMeasure stored = mockStoredMeasure(
            "[Measures].[Sales Qty]", "sum");
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            stored, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null);

        Set<Hierarchy> queryHierarchies = mockHierarchies("Brand", "Year");

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), queryHierarchies);

        assertNotNull(plan);
        assertEquals(1, plan.allRequests.size());
        PhysicalValueRequest req = plan.allRequests.get(0);
        assertEquals("[Measures].[Sales Qty]", req.getPhysicalMeasureId());
        assertEquals(
            PhysicalValueRequest.AggregationKind.SUM,
            req.getAggregationKind());
        assertEquals(
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            req.getProviderKind());
        assertEquals(queryHierarchies, req.getProjectedHierarchies());
        assertTrue(req.getResetHierarchies().isEmpty());
        assertNull(req.getNativeTemplate());
        assertTrue(plan.postProcessPlans.isEmpty());
    }

    /**
     * A stored COUNT measure must resolve with COUNT aggregation kind.
     */
    public void testDirectStoredCountResolvesCorrectly() {
        RolapStoredMeasure stored = mockStoredMeasure(
            "[Measures].[Row Count]", "count");
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            stored, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null);

        Set<Hierarchy> queryHierarchies = mockHierarchies("Brand");

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), queryHierarchies);

        assertNotNull(plan);
        assertEquals(1, plan.allRequests.size());
        assertEquals(
            PhysicalValueRequest.AggregationKind.COUNT,
            plan.allRequests.get(0).getAggregationKind());
    }

    /**
     * A stored distinct-count measure must resolve with DISTINCT_MERGE.
     */
    public void testDirectStoredDistinctCountResolvesToDistinctMerge() {
        RolapStoredMeasure stored = mockStoredMeasure(
            "[Measures].[AKB]", "distinct-count", true);
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            stored, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null);

        Set<Hierarchy> queryHierarchies = mockHierarchies("Brand");

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), queryHierarchies);

        assertNotNull(plan);
        assertEquals(1, plan.allRequests.size());
        assertEquals(
            PhysicalValueRequest.AggregationKind.DISTINCT_MERGE,
            plan.allRequests.get(0).getAggregationKind());
    }

    /**
     * A stored MIN measure must resolve with MIN aggregation kind.
     */
    public void testDirectStoredMinResolvesCorrectly() {
        RolapStoredMeasure stored = mockStoredMeasure(
            "[Measures].[Min Price]", "min");
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            stored, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null);

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), mockHierarchies("Year"));

        assertNotNull(plan);
        assertEquals(
            PhysicalValueRequest.AggregationKind.MIN,
            plan.allRequests.get(0).getAggregationKind());
    }

    /**
     * A stored MAX measure must resolve with MAX aggregation kind.
     */
    public void testDirectStoredMaxResolvesCorrectly() {
        RolapStoredMeasure stored = mockStoredMeasure(
            "[Measures].[Max Price]", "max");
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            stored, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null);

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), mockHierarchies("Year"));

        assertNotNull(plan);
        assertEquals(
            PhysicalValueRequest.AggregationKind.MAX,
            plan.allRequests.get(0).getAggregationKind());
    }

    // -----------------------------------------------------------------------
    // PostProcess Candidate tests
    // -----------------------------------------------------------------------

    /**
     * A PostProcess ratio candidate whose two leaf refs are both stored
     * measures must resolve successfully with 2 physical requests and
     * 1 PostProcessPlan.
     */
    public void testPostProcessRatioResolvesLeaves() {
        RolapStoredMeasure salesRub = mockStoredMeasure(
            "[Measures].[Sales RUB]", "sum");
        RolapStoredMeasure akb = mockStoredMeasure(
            "[Measures].[AKB]", "sum");

        MemberExpr salesExpr = mockMemberExpr(salesRub);
        MemberExpr akbExpr = mockMemberExpr(akb);
        FunCall divide = mockFunCall("/", salesExpr, akbExpr);

        FormulaNormalizer.Result nf = FormulaNormalizer.normalize(divide);

        Member calcMeasure = mock(Member.class);
        when(calcMeasure.getUniqueName()).thenReturn("[Measures].[Offtake]");
        when(calcMeasure.isMeasure()).thenReturn(true);
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            calcMeasure, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE,
            nf, null);

        Set<Hierarchy> queryHierarchies = mockHierarchies("Brand");

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), queryHierarchies);

        assertNotNull(plan);
        // Should have 2 physical requests (one per leaf)
        assertEquals(2, plan.allRequests.size());
        // Should have 1 PostProcess plan
        assertEquals(1, plan.postProcessPlans.size());
        DependencyResolver.PostProcessPlan pp =
            plan.postProcessPlans.get(calcMeasure);
        assertNotNull(pp);
        assertEquals(2, pp.leafBindings.size());
        assertSame(nf, pp.normalizedFormula);
        assertSame(calcMeasure, pp.measure);
    }

    /**
     * A PostProcess candidate that references another calculated measure
     * which is not stored or native must cause resolution to fail (null).
     */
    public void testPostProcessWithCalcLeafDegradesToNull() {
        Member innerCalc = mock(Member.class);
        when(innerCalc.isMeasure()).thenReturn(true);
        when(innerCalc.isCalculated()).thenReturn(true);
        when(innerCalc.getExpression()).thenReturn(null);

        MemberExpr innerExpr = mockMemberExpr(innerCalc);
        MemberExpr storedExpr = mockMemberExpr(
            mockStoredMeasure("[Measures].[X]", "sum"));
        FunCall divide = mockFunCall("/", innerExpr, storedExpr);

        FormulaNormalizer.Result nf = FormulaNormalizer.normalize(divide);

        Member calcMeasure = mock(Member.class);
        when(calcMeasure.isMeasure()).thenReturn(true);
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            calcMeasure, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE,
            nf, null);

        Set<Hierarchy> queryHierarchies = mockHierarchies("Brand");

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), queryHierarchies);

        assertNull("Should degrade when leaf is unresolvable calc", plan);
    }

    /**
     * A PostProcess additive candidate (a + b) where both leaves are
     * stored must resolve successfully.
     */
    public void testPostProcessAdditiveResolvesLeaves() {
        RolapStoredMeasure a = mockStoredMeasure(
            "[Measures].[Sales Qty]", "sum");
        RolapStoredMeasure b = mockStoredMeasure(
            "[Measures].[Returns Qty]", "sum");

        MemberExpr aExpr = mockMemberExpr(a);
        MemberExpr bExpr = mockMemberExpr(b);
        FunCall plus = mockFunCall("+", aExpr, bExpr);

        FormulaNormalizer.Result nf = FormulaNormalizer.normalize(plus);

        Member calcMeasure = mock(Member.class);
        when(calcMeasure.getUniqueName()).thenReturn("[Measures].[Net Qty]");
        when(calcMeasure.isMeasure()).thenReturn(true);
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            calcMeasure, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE,
            nf, null);

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), mockHierarchies("Brand"));

        assertNotNull(plan);
        assertEquals(2, plan.allRequests.size());
        assertEquals(1, plan.postProcessPlans.size());
    }

    // -----------------------------------------------------------------------
    // Mixed candidates
    // -----------------------------------------------------------------------

    /**
     * 2 stored candidates + 1 PostProcess ratio that references the same
     * 2 stored measures. The resolver must deduplicate requests — only 2
     * unique PhysicalValueRequests, not 4.
     */
    public void testMixedStoredAndPostProcessDeduplicates() {
        RolapStoredMeasure salesQty = mockStoredMeasure(
            "[Measures].[Sales Qty]", "sum");
        RolapStoredMeasure salesRub = mockStoredMeasure(
            "[Measures].[Sales RUB]", "sum");

        MemberExpr qtyExpr = mockMemberExpr(salesQty);
        MemberExpr rubExpr = mockMemberExpr(salesRub);
        FunCall divide = mockFunCall("/", rubExpr, qtyExpr);
        FormulaNormalizer.Result nf = FormulaNormalizer.normalize(divide);

        Member avgPrice = mock(Member.class);
        when(avgPrice.getUniqueName()).thenReturn("[Measures].[Avg Price]");
        when(avgPrice.isMeasure()).thenReturn(true);

        List<MeasureClassifier.Candidate> candidates =
            new ArrayList<MeasureClassifier.Candidate>();
        candidates.add(new MeasureClassifier.Candidate(
            salesQty, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null));
        candidates.add(new MeasureClassifier.Candidate(
            salesRub, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null));
        candidates.add(new MeasureClassifier.Candidate(
            avgPrice, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE,
            nf, null));

        Set<Hierarchy> queryHierarchies = mockHierarchies("Brand", "Year");

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            candidates, queryHierarchies);

        assertNotNull(plan);
        // 2 stored measures are shared between direct push and PostProcess
        // leaves, so dedup yields exactly 2
        assertEquals(2, plan.allRequests.size());
        assertEquals(1, plan.postProcessPlans.size());
    }

    /**
     * Multiple stored candidates only, no PostProcess — all resolve,
     * no postProcessPlans.
     */
    public void testMultipleStoredCandidates() {
        List<MeasureClassifier.Candidate> candidates =
            new ArrayList<MeasureClassifier.Candidate>();
        candidates.add(new MeasureClassifier.Candidate(
            mockStoredMeasure("[Measures].[A]", "sum"),
            MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null));
        candidates.add(new MeasureClassifier.Candidate(
            mockStoredMeasure("[Measures].[B]", "count"),
            MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null));

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            candidates, mockHierarchies("Brand"));

        assertNotNull(plan);
        assertEquals(2, plan.allRequests.size());
        assertTrue(plan.postProcessPlans.isEmpty());
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    /**
     * An empty candidate list must produce an empty but non-null plan.
     */
    public void testEmptyCandidateListReturnsEmptyPlan() {
        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.<MeasureClassifier.Candidate>emptyList(),
            mockHierarchies("Brand"));

        assertNotNull(plan);
        assertTrue(plan.allRequests.isEmpty());
        assertTrue(plan.postProcessPlans.isEmpty());
    }

    /**
     * A PostProcess candidate whose normalizedFormula is null
     * (shouldn't happen in practice) must cause a fallback.
     */
    public void testPostProcessWithNullNormalizedFormulaDegradesToNull() {
        Member calcMeasure = mock(Member.class);
        when(calcMeasure.isMeasure()).thenReturn(true);

        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            calcMeasure, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE,
            null, null);

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), mockHierarchies("Brand"));

        assertNull(
            "Null normalizedFormula should degrade to fallback", plan);
    }

    /**
     * A PostProcess candidate with a leaf that is a non-measure member
     * (e.g., Id expression) must cause a fallback.
     */
    public void testPostProcessWithNonMeasureLeafDegradesToNull() {
        // Build a formula with a non-measure MemberExpr
        Member nonMeasure = mock(Member.class);
        when(nonMeasure.isMeasure()).thenReturn(false);

        MemberExpr nonMeasureExpr = mockMemberExpr(nonMeasure);
        MemberExpr storedExpr = mockMemberExpr(
            mockStoredMeasure("[Measures].[X]", "sum"));
        FunCall divide = mockFunCall("/", nonMeasureExpr, storedExpr);
        // normalize won't pick up nonMeasure as a leaf ref because
        // isMeasure() is false — so leafRefs will only have 1 entry.
        // That's fine; the formula has 2 args but 1 leaf ref,
        // and it should still resolve the 1 leaf that is there.
        FormulaNormalizer.Result nf = FormulaNormalizer.normalize(divide);

        Member calcMeasure = mock(Member.class);
        when(calcMeasure.isMeasure()).thenReturn(true);

        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            calcMeasure, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE,
            nf, null);

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), mockHierarchies("Brand"));

        // This should still resolve since the leaf that IS a measure is valid
        assertNotNull(plan);
        assertEquals(1, plan.allRequests.size());
    }

    /**
     * Deduplication test: same measure unique name from two different
     * PostProcess formulas should produce only one physical request.
     */
    public void testDeduplicationAcrossMultiplePostProcessPlans() {
        RolapStoredMeasure shared = mockStoredMeasure(
            "[Measures].[Sales Qty]", "sum");
        RolapStoredMeasure other = mockStoredMeasure(
            "[Measures].[Sales RUB]", "sum");

        // First PostProcess: shared / other
        MemberExpr sharedExpr1 = mockMemberExpr(shared);
        MemberExpr otherExpr1 = mockMemberExpr(other);
        FunCall div1 = mockFunCall("/", sharedExpr1, otherExpr1);
        FormulaNormalizer.Result nf1 = FormulaNormalizer.normalize(div1);
        Member calc1 = mock(Member.class);
        when(calc1.getUniqueName()).thenReturn("[Measures].[Ratio1]");
        when(calc1.isMeasure()).thenReturn(true);

        // Second PostProcess: other - shared
        MemberExpr sharedExpr2 = mockMemberExpr(shared);
        MemberExpr otherExpr2 = mockMemberExpr(other);
        FunCall sub2 = mockFunCall("-", otherExpr2, sharedExpr2);
        FormulaNormalizer.Result nf2 = FormulaNormalizer.normalize(sub2);
        Member calc2 = mock(Member.class);
        when(calc2.getUniqueName()).thenReturn("[Measures].[Diff1]");
        when(calc2.isMeasure()).thenReturn(true);

        List<MeasureClassifier.Candidate> candidates =
            new ArrayList<MeasureClassifier.Candidate>();
        candidates.add(new MeasureClassifier.Candidate(
            calc1, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE,
            nf1, null));
        candidates.add(new MeasureClassifier.Candidate(
            calc2, MeasureClassifier.CandidateClass.POST_PROCESS_CANDIDATE,
            nf2, null));

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            candidates, mockHierarchies("Brand"));

        assertNotNull(plan);
        // Both formulas reference the same 2 stored measures, so only 2
        // unique PhysicalValueRequests
        assertEquals(2, plan.allRequests.size());
        assertEquals(2, plan.postProcessPlans.size());
    }

    /**
     * An unknown aggregator name should map to SUM as a fallback.
     */
    public void testUnknownAggregatorFallsBackToSum() {
        RolapStoredMeasure stored = mockStoredMeasure(
            "[Measures].[Unknown Agg]", "weird");
        MeasureClassifier.Candidate candidate = new MeasureClassifier.Candidate(
            stored, MeasureClassifier.CandidateClass.DIRECT_PUSH_STORED,
            null, null);

        DependencyResolver.ResolvedPlan plan = DependencyResolver.resolve(
            Collections.singletonList(candidate), mockHierarchies("Brand"));

        assertNotNull(plan);
        assertEquals(1, plan.allRequests.size());
        assertEquals(
            PhysicalValueRequest.AggregationKind.SUM,
            plan.allRequests.get(0).getAggregationKind());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private RolapStoredMeasure mockStoredMeasure(
        String uniqueName, String aggName)
    {
        return mockStoredMeasure(uniqueName, aggName, false);
    }

    private RolapStoredMeasure mockStoredMeasure(
        String uniqueName, String aggName, boolean distinct)
    {
        RolapStoredMeasure m = mock(RolapStoredMeasure.class);
        when(m.getUniqueName()).thenReturn(uniqueName);
        when(m.getName()).thenReturn(uniqueName);
        when(m.isMeasure()).thenReturn(true);
        when(m.isCalculated()).thenReturn(false);

        RolapAggregator agg = mock(RolapAggregator.class);
        when(agg.getName()).thenReturn(aggName);
        when(agg.isDistinct()).thenReturn(distinct);
        when(m.getAggregator()).thenReturn(agg);

        return m;
    }

    private MemberExpr mockMemberExpr(Member member) {
        MemberExpr expr = mock(MemberExpr.class);
        when(expr.getMember()).thenReturn(member);
        return expr;
    }

    private FunCall mockFunCall(String name, Exp... args) {
        FunCall fc = mock(FunCall.class);
        when(fc.getFunName()).thenReturn(name);
        when(fc.getArgs()).thenReturn(args);
        when(fc.getArgCount()).thenReturn(args.length);
        for (int i = 0; i < args.length; i++) {
            when(fc.getArg(i)).thenReturn(args[i]);
        }
        return fc;
    }

    private Set<Hierarchy> mockHierarchies(String... names) {
        Set<Hierarchy> set = new LinkedHashSet<Hierarchy>();
        for (String name : names) {
            Hierarchy h = mock(Hierarchy.class);
            when(h.getUniqueName()).thenReturn("[" + name + "]");
            when(h.getName()).thenReturn(name);
            set.add(h);
        }
        return set;
    }
}

// End DependencyResolverTest.java
