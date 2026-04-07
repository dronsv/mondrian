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
import mondrian.olap.Hierarchy;

import java.util.*;

import static org.mockito.Mockito.*;

public class CoordinateClassMergerTest extends TestCase {

    public void testSingleRequestProducesSinglePlan() {
        PhysicalValueRequest req = makeStoredRequest(
            "m1", mockHierarchies("Brand", "Year"), null);

        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(
            Collections.singletonList(req));

        assertEquals(1, plans.size());
        assertEquals(1, plans.get(0).getRequests().size());
        assertTrue(plans.get(0).getClassId().startsWith("Identity"));
    }

    public void testCompatibleRequestsMergeIntoOnePlan() {
        Set<Hierarchy> proj = mockHierarchies("Brand", "Year");
        PhysicalValueRequest r1 = makeStoredRequest("m1", proj, null);
        PhysicalValueRequest r2 = makeStoredRequest("m2", proj, null);

        List<PhysicalValueRequest> requests = Arrays.asList(r1, r2);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        assertEquals(1, plans.size());
        assertEquals(2, plans.get(0).getRequests().size());
    }

    public void testIncompatibleProjectionsSplitIntoTwoPlans() {
        PhysicalValueRequest r1 = makeStoredRequest(
            "m1", mockHierarchies("Brand", "Year"), null);
        PhysicalValueRequest r2 = makeStoredRequest(
            "m2", mockHierarchies("Category", "Year"), null);

        List<PhysicalValueRequest> requests = Arrays.asList(r1, r2);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        assertEquals(2, plans.size());
        assertEquals(1, plans.get(0).getRequests().size());
        assertEquals(1, plans.get(1).getRequests().size());
    }

    public void testResetHierarchiesCauseSplit() {
        Set<Hierarchy> proj = mockHierarchies("Brand", "Year");
        Set<Hierarchy> noReset = Collections.<Hierarchy>emptySet();
        Set<Hierarchy> resetBrand = mockHierarchies("Brand");

        PhysicalValueRequest r1 = makeStoredRequest("m1", proj, noReset);
        PhysicalValueRequest r2 = makeStoredRequest("m2", proj, resetBrand);

        List<PhysicalValueRequest> requests = Arrays.asList(r1, r2);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        assertEquals(2, plans.size());
    }

    public void testNativeTemplateOnlyCompatibleWithSameTemplate() {
        Set<Hierarchy> proj = mockHierarchies("Brand");
        PhysicalValueRequest r1 = makeNativeRequest("m1", proj, "SELECT 1");
        PhysicalValueRequest r2 = makeNativeRequest("m2", proj, "SELECT 2");
        PhysicalValueRequest r3 = makeNativeRequest("m3", proj, "SELECT 1");

        List<PhysicalValueRequest> requests = Arrays.asList(r1, r2, r3);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        assertEquals(2, plans.size());
        // r1 and r3 should merge (same template), r2 separate
        assertEquals(2, plans.get(0).getRequests().size()); // r1 + r3
        assertEquals(1, plans.get(1).getRequests().size()); // r2
    }

    public void testStoredAndStateAggregateAreCompatible() {
        Set<Hierarchy> proj = mockHierarchies("Brand");
        PhysicalValueRequest stored = makeStoredRequest("m1", proj, null);
        PhysicalValueRequest state = new PhysicalValueRequest(
            "m2", proj, null,
            PhysicalValueRequest.AggregationKind.DISTINCT_MERGE,
            PhysicalValueRequest.ExpressionProviderKind.STATE_AGGREGATE,
            null);

        List<PhysicalValueRequest> requests = Arrays.asList(stored, state);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        assertEquals(1, plans.size());
        assertEquals(2, plans.get(0).getRequests().size());
    }

    public void testEmptyInputReturnsEmptyList() {
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(
            Collections.<PhysicalValueRequest>emptyList());
        assertNotNull(plans);
        assertTrue(plans.isEmpty());
    }

    public void testNullInputReturnsEmptyList() {
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(null);
        assertNotNull(plans);
        assertTrue(plans.isEmpty());
    }

    public void testClassIdContainsResetHierarchyName() {
        Set<Hierarchy> proj = mockHierarchies("Brand", "Year");
        Set<Hierarchy> resetBrand = mockHierarchies("Brand");
        PhysicalValueRequest req = makeStoredRequest("m1", proj, resetBrand);

        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(
            Collections.singletonList(req));

        assertEquals(1, plans.size());
        assertTrue(plans.get(0).getClassId().contains("Reset"));
        assertTrue(plans.get(0).getClassId().contains("Brand"));
    }

    public void testGetMeasureIdsReturnsAllMeasuresInClass() {
        Set<Hierarchy> proj = mockHierarchies("Brand");
        PhysicalValueRequest r1 = makeStoredRequest("m1", proj, null);
        PhysicalValueRequest r2 = makeStoredRequest("m2", proj, null);

        List<PhysicalValueRequest> requests = Arrays.asList(r1, r2);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        Set<String> ids = plans.get(0).getMeasureIds();
        assertEquals(2, ids.size());
        assertTrue(ids.contains("m1"));
        assertTrue(ids.contains("m2"));
    }

    // ---- Cross-cube tests ----

    public void testDifferentSourceCubesSplitIntoSeparatePlans() {
        Set<Hierarchy> proj = mockHierarchies("Brand");
        PhysicalValueRequest r1 = new PhysicalValueRequest(
            "m1", proj, null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null, "CubeA");
        PhysicalValueRequest r2 = new PhysicalValueRequest(
            "m2", proj, null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null, "CubeB");

        List<PhysicalValueRequest> requests = Arrays.asList(r1, r2);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        assertEquals(2, plans.size());
        assertEquals(1, plans.get(0).getRequests().size());
        assertEquals(1, plans.get(1).getRequests().size());
    }

    public void testSameSourceCubeMergesIntoOnePlan() {
        Set<Hierarchy> proj = mockHierarchies("Brand");
        PhysicalValueRequest r1 = new PhysicalValueRequest(
            "m1", proj, null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null, "CubeA");
        PhysicalValueRequest r2 = new PhysicalValueRequest(
            "m2", proj, null,
            PhysicalValueRequest.AggregationKind.COUNT,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null, "CubeA");

        List<PhysicalValueRequest> requests = Arrays.asList(r1, r2);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        assertEquals(1, plans.size());
        assertEquals(2, plans.get(0).getRequests().size());
    }

    public void testNullAndNonNullSourceCubeSplit() {
        Set<Hierarchy> proj = mockHierarchies("Brand");
        PhysicalValueRequest r1 = makeStoredRequest("m1", proj, null);
        PhysicalValueRequest r2 = new PhysicalValueRequest(
            "m2", proj, null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null, "CubeB");

        List<PhysicalValueRequest> requests = Arrays.asList(r1, r2);
        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(requests);

        assertEquals(2, plans.size());
    }

    public void testCrossCubeClassIdContainsCubeName() {
        Set<Hierarchy> proj = mockHierarchies("Brand");
        PhysicalValueRequest req = new PhysicalValueRequest(
            "m1", proj, null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null, "География");

        List<CoordinateClassPlan> plans = CoordinateClassMerger.merge(
            Collections.singletonList(req));

        assertEquals(1, plans.size());
        assertTrue(
            "ClassId should contain '@География'",
            plans.get(0).getClassId().contains("@География"));
    }

    // ---- Helpers ----

    private PhysicalValueRequest makeStoredRequest(
        String measureId,
        Set<Hierarchy> projected,
        Set<Hierarchy> reset)
    {
        return new PhysicalValueRequest(
            measureId, projected, reset,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);
    }

    private PhysicalValueRequest makeNativeRequest(
        String measureId,
        Set<Hierarchy> projected,
        String template)
    {
        return new PhysicalValueRequest(
            measureId, projected, null,
            PhysicalValueRequest.AggregationKind.NATIVE_EXPRESSION,
            PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE,
            template);
    }

    private Set<Hierarchy> mockHierarchies(String... names) {
        Set<Hierarchy> set = new LinkedHashSet<Hierarchy>();
        for (String name : names) {
            Hierarchy h = mock(Hierarchy.class);
            when(h.getName()).thenReturn(name);
            when(h.getUniqueName()).thenReturn("[" + name + "]");
            set.add(h);
        }
        return set;
    }
}
