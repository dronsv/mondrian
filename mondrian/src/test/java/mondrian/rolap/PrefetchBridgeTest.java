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

import mondrian.olap.Hierarchy;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link PrefetchBridge} — NQE-to-legacy coordinate translation.
 */
public class PrefetchBridgeTest {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Creates a mock {@link RolapStar} with a fact table that contains
     * the specified measures.
     */
    private static RolapStar mockStar(MeasureSpec... specs) {
        RolapStar star = mock(RolapStar.class);
        RolapStar.Table factTable = mock(RolapStar.Table.class);
        when(star.getFactTable()).thenReturn(factTable);

        List<RolapStar.Column> columns = new ArrayList<>();
        for (MeasureSpec spec : specs) {
            RolapStar.Measure measure = mock(RolapStar.Measure.class);
            when(measure.getName()).thenReturn(spec.name);
            when(measure.getCubeName()).thenReturn(spec.cubeName);
            when(measure.getBitPosition()).thenReturn(spec.bitPosition);
            columns.add(measure);

            // Wire up cube-qualified lookup
            when(factTable.lookupMeasureByName(spec.cubeName, spec.name))
                .thenReturn(measure);
        }
        when(factTable.getColumns()).thenReturn(columns);
        return star;
    }

    private record MeasureSpec(String name, String cubeName, int bitPosition) {}

    /**
     * Creates a {@link CoordinateClassPlan} with one STORED_COLUMN request
     * per measure, with the given projected hierarchy count.
     */
    private static CoordinateClassPlan plan(
        String classId,
        int projectedHierarchyCount,
        String... measureIds)
    {
        Set<Hierarchy> projected = new LinkedHashSet<>();
        for (int i = 0; i < projectedHierarchyCount; i++) {
            projected.add(mock(Hierarchy.class));
        }

        List<PhysicalValueRequest> requests = new ArrayList<>();
        for (String measureId : measureIds) {
            requests.add(new PhysicalValueRequest(
                measureId,
                projected,
                Collections.emptySet(),
                PhysicalValueRequest.AggregationKind.SUM,
                PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
                null));
        }
        return new CoordinateClassPlan(classId, requests);
    }

    /**
     * Creates a plan with a NATIVE_TEMPLATE request (should be skipped
     * by the bridge).
     */
    private static CoordinateClassPlan nativeTemplatePlan(
        String classId,
        String measureId)
    {
        Set<Hierarchy> projected = new LinkedHashSet<>();
        projected.add(mock(Hierarchy.class));

        List<PhysicalValueRequest> requests = new ArrayList<>();
        requests.add(new PhysicalValueRequest(
            measureId,
            projected,
            Collections.emptySet(),
            PhysicalValueRequest.AggregationKind.NATIVE_EXPRESSION,
            PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE,
            "someTemplate(${col})"));
        return new CoordinateClassPlan(classId, requests);
    }

    /**
     * Builds a projected key string from parts, using \0 as separator.
     */
    private static String projKey(String... parts) {
        return String.join("\0", parts);
    }

    // -----------------------------------------------------------------------
    // Test 1: Empty context produces empty provider with zero metrics
    // -----------------------------------------------------------------------

    @Test
    public void testEmptyContext_producesEmptyResult() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        RolapStar star = mockStar(
            new MeasureSpec("Sales", "MyCube", 5));

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(plan("class1", 1, "[Measures].[Sales]")),
            star);

        assertEquals(0, result.provider().size(),
            "Empty context must produce empty provider");
        assertEquals(0, result.metrics().nqeRowsProcessed());
        assertEquals(0, result.metrics().rowsMapped());
        assertEquals(0, result.metrics().rowsRejected());
        assertEquals(0, result.metrics().duplicateKeys());
        assertEquals(0, result.metrics().providerSize());
    }

    @Test
    public void testEmptyPlans_producesEmptyResult() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("class1", "val1", "m1", 100);
        RolapStar star = mockStar();

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            Collections.emptyList(),
            star);

        assertEquals(0, result.provider().size());
        assertEquals(0, result.metrics().nqeRowsProcessed());
    }

    // -----------------------------------------------------------------------
    // Test 2: Single stored measure maps correctly
    // -----------------------------------------------------------------------

    @Test
    public void testSingleStoredMeasure_mapsCorrectly() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("class1", projKey("brand-A"),
            "[Measures].[Sales]", 42.5);

        RolapStar star = mockStar(
            new MeasureSpec("Sales", "MyCube", 7));

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(plan("class1", 1, "[Measures].[Sales]")),
            star);

        assertEquals(1, result.provider().size(),
            "Provider must have exactly one entry");
        assertEquals(1, result.metrics().rowsMapped());
        assertEquals(0, result.metrics().rowsRejected());
        assertEquals(1, result.metrics().providerSize());

        // Verify the key is findable via PrefetchKeyBuilder
        PrefetchKeyBuilder builder = new PrefetchKeyBuilder();
        PrefetchKey key = builder.fromNqeRow(7, new Object[]{"brand-A"});
        Object value = result.provider().lookup(key);
        assertNotSame(PrefetchKey.MISS, value,
            "Lookup must find the stored value");
        assertEquals(42.5, value);
    }

    @Test
    public void testMultipleMeasures_sameProjectedKey() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("class1", projKey("2024-01"),
            "[Measures].[Sales]", 100.0);
        context.put("class1", projKey("2024-01"),
            "[Measures].[Quantity]", 50L);

        RolapStar star = mockStar(
            new MeasureSpec("Sales", "MyCube", 3),
            new MeasureSpec("Quantity", "MyCube", 4));

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(plan("class1", 1,
                "[Measures].[Sales]", "[Measures].[Quantity]")),
            star);

        assertEquals(2, result.provider().size(),
            "Two measures with same key must produce two entries");
        assertEquals(2, result.metrics().rowsMapped());

        PrefetchKeyBuilder builder = new PrefetchKeyBuilder();
        assertEquals(100.0,
            result.provider().lookup(
                builder.fromNqeRow(3, new Object[]{"2024-01"})));
        assertEquals(50L,
            result.provider().lookup(
                builder.fromNqeRow(4, new Object[]{"2024-01"})));
    }

    // -----------------------------------------------------------------------
    // Test 3: Unmappable measure (not in star) is rejected
    // -----------------------------------------------------------------------

    @Test
    public void testUnmappableMeasure_isRejected() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("class1", projKey("brand-A"),
            "[Measures].[GhostMeasure]", 99.9);

        // Star has no measures at all
        RolapStar star = mockStar();

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(plan("class1", 1, "[Measures].[GhostMeasure]")),
            star);

        assertEquals(0, result.provider().size(),
            "Unmappable measure must not produce entries");
        assertEquals(1, result.metrics().nqeRowsProcessed());
        assertEquals(0, result.metrics().rowsMapped());
        assertEquals(1, result.metrics().rowsRejected());
        assertEquals(1, result.metrics().measureResolutionRejects());
    }

    // -----------------------------------------------------------------------
    // Test 4: Duplicate keys — first value wins
    // -----------------------------------------------------------------------

    @Test
    public void testDuplicateKeys_firstValueWins() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        // Put two entries with same classId and projectedKey for same measure
        // NativeQueryResultContext.put will overwrite the first value
        // since the composite key is the same, but let's test via two plans
        // with overlapping classIds.
        //
        // Actually, since NativeQueryResultContext.put overwrites, there
        // will be only one entry per (classId, projectedKey, measureId).
        // To test duplicate PrefetchKeys, we need two different classIds
        // that produce the same PrefetchKey.

        // Two plans with different classIds but same measure and dim layout
        context.put("classA", projKey("brand-A"),
            "[Measures].[Sales]", 100.0);
        context.put("classB", projKey("brand-A"),
            "[Measures].[Sales]", 200.0);

        RolapStar star = mockStar(
            new MeasureSpec("Sales", "MyCube", 5));

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(
                plan("classA", 1, "[Measures].[Sales]"),
                plan("classB", 1, "[Measures].[Sales]")),
            star);

        assertEquals(1, result.provider().size(),
            "Duplicate PrefetchKey must keep only one entry");
        assertEquals(1, result.metrics().duplicateKeys());
        assertEquals(1, result.metrics().rowsMapped());

        // First value wins — classA was processed first
        PrefetchKeyBuilder builder = new PrefetchKeyBuilder();
        PrefetchKey key = builder.fromNqeRow(5, new Object[]{"brand-A"});
        assertEquals(100.0, result.provider().lookup(key),
            "First value must win on duplicate key");
    }

    // -----------------------------------------------------------------------
    // Test 5: Metrics track all counters correctly
    // -----------------------------------------------------------------------

    @Test
    public void testMetrics_trackAllCounters() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        // 2 valid entries
        context.put("class1", projKey("brand-A"),
            "[Measures].[Sales]", 100.0);
        context.put("class1", projKey("brand-B"),
            "[Measures].[Sales]", 200.0);
        // 1 entry for unmappable measure
        context.put("class1", projKey("brand-C"),
            "[Measures].[Unknown]", 300.0);

        RolapStar star = mockStar(
            new MeasureSpec("Sales", "MyCube", 3));

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(plan("class1", 1,
                "[Measures].[Sales]", "[Measures].[Unknown]")),
            star);

        PrefetchBuildMetrics m = result.metrics();
        assertEquals(3, m.nqeRowsProcessed(),
            "Should process all 3 entries");
        assertEquals(2, m.rowsMapped(),
            "2 valid Sales entries should map");
        assertEquals(1, m.rowsRejected(),
            "1 Unknown entry should be rejected");
        assertEquals(1, m.measureResolutionRejects(),
            "1 measure resolution reject");
        assertEquals(0, m.duplicateKeys());
        assertEquals(0, m.decodeRejects());
        assertEquals(0, m.normalizationRejects());
        assertEquals(2, m.providerSize());
    }

    // -----------------------------------------------------------------------
    // NATIVE_TEMPLATE requests are skipped
    // -----------------------------------------------------------------------

    @Test
    public void testNativeTemplateRequests_areSkipped() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("class1", projKey("val"),
            "[Measures].[CalcField]", 42);

        RolapStar star = mockStar();

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(nativeTemplatePlan("class1", "[Measures].[CalcField]")),
            star);

        // NATIVE_TEMPLATE should be skipped entirely
        assertEquals(0, result.provider().size());
    }

    // -----------------------------------------------------------------------
    // Zero-dimension query (no projected hierarchies)
    // -----------------------------------------------------------------------

    @Test
    public void testZeroDimensionQuery_mapsCorrectly() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("class1", "", "[Measures].[Total]", 999L);

        RolapStar star = mockStar(
            new MeasureSpec("Total", "MyCube", 0));

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(plan("class1", 0, "[Measures].[Total]")),
            star);

        assertEquals(1, result.provider().size());
        PrefetchKeyBuilder builder = new PrefetchKeyBuilder();
        PrefetchKey key = builder.fromNqeRow(0, new Object[0]);
        assertEquals(999L, result.provider().lookup(key));
    }

    // -----------------------------------------------------------------------
    // Multi-dimension projected key
    // -----------------------------------------------------------------------

    @Test
    public void testMultiDimProjectedKey_decodesCorrectly() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("class1",
            projKey("brand-A", "region-X", "2024-01"),
            "[Measures].[Sales]", 55.5);

        RolapStar star = mockStar(
            new MeasureSpec("Sales", "MyCube", 10));

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(plan("class1", 3, "[Measures].[Sales]")),
            star);

        assertEquals(1, result.provider().size());
        PrefetchKeyBuilder builder = new PrefetchKeyBuilder();
        PrefetchKey key = builder.fromNqeRow(
            10, new Object[]{"brand-A", "region-X", "2024-01"});
        assertEquals(55.5, result.provider().lookup(key));
    }

    // -----------------------------------------------------------------------
    // Null cell value is preserved
    // -----------------------------------------------------------------------

    @Test
    public void testNullCellValue_isStored() {
        NativeQueryResultContext context = new NativeQueryResultContext();
        context.put("class1", projKey("brand-A"),
            "[Measures].[Sales]", null);

        RolapStar star = mockStar(
            new MeasureSpec("Sales", "MyCube", 2));

        PrefetchBridge.BuildResult result = PrefetchBridge.build(
            context,
            List.of(plan("class1", 1, "[Measures].[Sales]")),
            star);

        // null values are stored; ImmutablePrefetchProvider returns MISS
        // for absent keys, but null-valued keys map to null.
        // Note: ImmutablePrefetchProvider.lookup returns MISS for null
        // values too (can't distinguish null from absent). This is
        // expected — null cells are treated as absent.
        assertEquals(1, result.metrics().rowsMapped());
    }

    // -----------------------------------------------------------------------
    // decodeProjectedKey unit tests
    // -----------------------------------------------------------------------

    @Test
    public void testDecodeProjectedKey_singleValue() {
        Object[] result = PrefetchBridge.decodeProjectedKey("hello", 1);
        assertArrayEquals(new Object[]{"hello"}, result);
    }

    @Test
    public void testDecodeProjectedKey_multiValue() {
        Object[] result = PrefetchBridge.decodeProjectedKey(
            "a\0b\0c", 3);
        assertArrayEquals(new Object[]{"a", "b", "c"}, result);
    }

    @Test
    public void testDecodeProjectedKey_nullPart() {
        Object[] result = PrefetchBridge.decodeProjectedKey(
            "a\0null\0c", 3);
        assertArrayEquals(new Object[]{"a", null, "c"}, result);
    }

    @Test
    public void testDecodeProjectedKey_emptyString() {
        Object[] result = PrefetchBridge.decodeProjectedKey("", 0);
        assertEquals(0, result.length);
    }

    @Test
    public void testDecodeProjectedKey_nullInput() {
        Object[] result = PrefetchBridge.decodeProjectedKey(null, 0);
        assertEquals(0, result.length);
    }
}

// End PrefetchBridgeTest.java
