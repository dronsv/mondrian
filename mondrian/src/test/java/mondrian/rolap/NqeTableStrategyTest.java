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
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef;
import mondrian.rolap.aggmatcher.AggStar;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NqeTableStrategy}.
 *
 * <p>Uses Mockito to simulate Mondrian runtime types without a full
 * schema/connection.
 */
public class NqeTableStrategyTest {

    // -----------------------------------------------------------------------
    // Shared fixtures
    // -----------------------------------------------------------------------

    private static final int COLUMN_COUNT = 64;

    private NqeTableStrategy strategy;
    private RolapCube cube;
    private RolapStar star;
    private RolapStar.Table starFactTable;
    private RolapEvaluator evaluator;

    @BeforeEach
    public void setUp() {
        strategy = new NqeTableStrategy();
        cube = mock(RolapCube.class);
        star = mock(RolapStar.class);
        starFactTable = mock(RolapStar.Table.class);
        evaluator = mock(RolapEvaluator.class);

        when(cube.getStar()).thenReturn(star);
        when(cube.getName()).thenReturn("TestCube");
        when(star.getColumnCount()).thenReturn(COLUMN_COUNT);
        when(star.getFactTable()).thenReturn(starFactTable);
        // Default: empty non-all members and no agg stars
        when(evaluator.getNonAllMembers()).thenReturn(new Member[0]);
        when(star.getAggStars()).thenReturn(Collections.<AggStar>emptyList());
        // Default: no columns in fact table
        when(starFactTable.getColumns())
            .thenReturn(Collections.<RolapStar.Column>emptyList());
    }

    // -----------------------------------------------------------------------
    // Test 1: resolve_noStar_returnsUnresolved
    // -----------------------------------------------------------------------

    @Test
    public void resolve_noStar_returnsUnresolved() {
        when(cube.getStar()).thenReturn(null);

        CoordinateClassPlan plan = makePlan("class1");
        NqeTableStrategy.ResolvedSourcePlan result =
            strategy.resolve(cube, plan, evaluator);

        assertFalse(result.isResolved());
        assertNull(result.getTable());
        assertInstanceOf(NqeTableStrategy.Unresolved.class, result);
        assertTrue(
            ((NqeTableStrategy.Unresolved) result).getReason()
                .contains("no star"));
    }

    // -----------------------------------------------------------------------
    // Test 2: resolve_noAggStars_returnsFactTable
    // -----------------------------------------------------------------------

    @Test
    public void resolve_noAggStars_returnsFactTable() {
        when(star.getAggStars()).thenReturn(Collections.<AggStar>emptyList());

        CoordinateClassPlan plan = makePlan("class1",
            makeStoredColumnRequest("[Measures].[Sales]"));
        NqeTableStrategy.ResolvedSourcePlan result =
            strategy.resolve(cube, plan, evaluator);

        assertTrue(result.isResolved());
        assertInstanceOf(NqeTableStrategy.SingleSourcePlan.class, result);
        assertNotNull(result.getTable());
        assertInstanceOf(FactResolvedTable.class, result.getTable());
        assertFalse(result.getTable().isAggregate());
    }

    // -----------------------------------------------------------------------
    // Test 3: resolve_coveringAggStar_returnsAggTable
    // -----------------------------------------------------------------------

    @Test
    public void resolve_coveringAggStar_returnsAggTable() {
        // Set up a measure in the star
        RolapStar.Measure starMeasure = mock(RolapStar.Measure.class);
        when(starMeasure.getBitPosition()).thenReturn(5);
        when(starMeasure.getName()).thenReturn("Sales");
        when(starFactTable.lookupMeasureByName("TestCube", "Sales"))
            .thenReturn(starMeasure);

        // Build agg star that covers bit 5
        AggStar aggStar = mockCoveringAggStar(100, "agg_table_1",
            BitKey.Factory.makeBitKey(COLUMN_COUNT),  // no level bits
            makeBitKeyWithBit(5));                      // measure bit 5

        when(star.getAggStars())
            .thenReturn(Collections.singletonList(aggStar));

        CoordinateClassPlan plan = makePlan("class1",
            makeStoredColumnRequest("[Measures].[Sales]"));

        NqeTableStrategy.ResolvedSourcePlan result =
            strategy.resolve(cube, plan, evaluator);

        assertTrue(result.isResolved());
        assertInstanceOf(AggResolvedTable.class, result.getTable());
        assertTrue(result.getTable().isAggregate());
        assertEquals("agg_table_1", result.getTable().tableName());
    }

    // -----------------------------------------------------------------------
    // Test 4: resolve_aggStarMissesLevel_fallsToFact
    // -----------------------------------------------------------------------

    @Test
    public void resolve_aggStarMissesLevel_fallsToFact() {
        // Set up a hierarchy projected in the plan
        RolapHierarchy hier = mockHierarchy("brand_table", "brand_id", 2);

        // Set up a measure in the star
        RolapStar.Measure starMeasure = mock(RolapStar.Measure.class);
        when(starMeasure.getBitPosition()).thenReturn(5);
        when(starMeasure.getName()).thenReturn("Sales");
        when(starFactTable.lookupMeasureByName("TestCube", "Sales"))
            .thenReturn(starMeasure);

        // Agg star does NOT cover level bit 2 (superSetMatch returns false)
        AggStar aggStar = mock(AggStar.class);
        AggStar.FactTable aggFactTable = mock(AggStar.FactTable.class);
        when(aggStar.getFactTable()).thenReturn(aggFactTable);
        when(aggFactTable.getName()).thenReturn("agg_small");
        when(aggStar.superSetMatch(any(BitKey.class))).thenReturn(false);
        when(aggStar.getDistinctMeasureBitKey())
            .thenReturn(BitKey.Factory.makeBitKey(COLUMN_COUNT));

        when(star.getAggStars())
            .thenReturn(Collections.singletonList(aggStar));

        Set<Hierarchy> projected = new LinkedHashSet<Hierarchy>();
        projected.add(hier);
        PhysicalValueRequest req = new PhysicalValueRequest(
            "[Measures].[Sales]", projected, null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);

        CoordinateClassPlan plan = makePlan("class1", req);

        NqeTableStrategy.ResolvedSourcePlan result =
            strategy.resolve(cube, plan, evaluator);

        assertTrue(result.isResolved());
        assertInstanceOf(FactResolvedTable.class, result.getTable());
        assertFalse(result.getTable().isAggregate());
    }

    // -----------------------------------------------------------------------
    // Test 5: resolve_selectsSmallestCoveringAggStar
    // -----------------------------------------------------------------------

    @Test
    public void resolve_selectsSmallestCoveringAggStar() {
        // Set up a measure in the star
        RolapStar.Measure starMeasure = mock(RolapStar.Measure.class);
        when(starMeasure.getBitPosition()).thenReturn(5);
        when(starMeasure.getName()).thenReturn("Sales");
        when(starFactTable.lookupMeasureByName("TestCube", "Sales"))
            .thenReturn(starMeasure);

        // Two covering agg stars — smallest first (100 rows), then 1000 rows
        AggStar small = mockCoveringAggStar(100, "agg_small",
            BitKey.Factory.makeBitKey(COLUMN_COUNT),
            makeBitKeyWithBit(5));
        AggStar large = mockCoveringAggStar(1000, "agg_large",
            BitKey.Factory.makeBitKey(COLUMN_COUNT),
            makeBitKeyWithBit(5));

        // AggStars are ordered smallest first by convention
        List<AggStar> aggStars = new ArrayList<AggStar>();
        aggStars.add(small);
        aggStars.add(large);
        when(star.getAggStars()).thenReturn(aggStars);

        CoordinateClassPlan plan = makePlan("class1",
            makeStoredColumnRequest("[Measures].[Sales]"));

        NqeTableStrategy.ResolvedSourcePlan result =
            strategy.resolve(cube, plan, evaluator);

        assertTrue(result.isResolved());
        assertInstanceOf(AggResolvedTable.class, result.getTable());
        assertEquals("agg_small", result.getTable().tableName());
    }

    // -----------------------------------------------------------------------
    // Test 6: resolve_coverageValidation_missingMeasure_skipsAgg
    // -----------------------------------------------------------------------

    @Test
    public void resolve_coverageValidation_missingMeasure_skipsAgg() {
        // Star has measure at bitPos 5
        RolapStar.Measure starMeasure = mock(RolapStar.Measure.class);
        when(starMeasure.getBitPosition()).thenReturn(5);
        when(starMeasure.getName()).thenReturn("Sales");
        when(starFactTable.lookupMeasureByName("TestCube", "Sales"))
            .thenReturn(starMeasure);

        // Agg star passes superSetMatch but lookupColumn(5) returns a
        // non-Measure column (Level), so measure coverage fails
        AggStar aggStar = mock(AggStar.class);
        AggStar.FactTable aggFactTable = mock(AggStar.FactTable.class);
        when(aggStar.getFactTable()).thenReturn(aggFactTable);
        when(aggFactTable.getName()).thenReturn("agg_bad_coverage");
        when(aggStar.superSetMatch(any(BitKey.class))).thenReturn(true);
        when(aggStar.getDistinctMeasureBitKey())
            .thenReturn(BitKey.Factory.makeBitKey(COLUMN_COUNT));
        when(aggStar.hasIgnoredColumns()).thenReturn(false);
        when(aggStar.isFullyCollapsed()).thenReturn(true);
        when(aggStar.getLevelBitKey())
            .thenReturn(BitKey.Factory.makeBitKey(COLUMN_COUNT));
        when(aggStar.getSize()).thenReturn(50L);

        // Return a Level (not Measure) at bit 5 — coverage validation fails
        AggStar.Table.Level notAMeasure = mock(AggStar.Table.Level.class);
        when(aggStar.lookupColumn(5)).thenReturn(notAMeasure);

        when(star.getAggStars())
            .thenReturn(Collections.singletonList(aggStar));

        CoordinateClassPlan plan = makePlan("class1",
            makeStoredColumnRequest("[Measures].[Sales]"));

        NqeTableStrategy.ResolvedSourcePlan result =
            strategy.resolve(cube, plan, evaluator);

        // Should fall through to fact table since agg fails coverage
        assertTrue(result.isResolved());
        assertInstanceOf(FactResolvedTable.class, result.getTable());
    }

    // -----------------------------------------------------------------------
    // Helper: create a PhysicalValueRequest for a STORED_COLUMN measure
    // -----------------------------------------------------------------------

    private PhysicalValueRequest makeStoredColumnRequest(String measureId) {
        return new PhysicalValueRequest(
            measureId,
            Collections.<Hierarchy>emptySet(),
            null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);
    }

    // -----------------------------------------------------------------------
    // Helper: create a CoordinateClassPlan
    // -----------------------------------------------------------------------

    private CoordinateClassPlan makePlan(
        String classId, PhysicalValueRequest... requests)
    {
        return new CoordinateClassPlan(
            classId,
            requests.length == 0
                ? Collections.singletonList(
                    makeStoredColumnRequest("[Measures].[Dummy]"))
                : Arrays.asList(requests));
    }

    // -----------------------------------------------------------------------
    // Helper: create a BitKey with a single bit set
    // -----------------------------------------------------------------------

    private BitKey makeBitKeyWithBit(int bit) {
        BitKey bk = BitKey.Factory.makeBitKey(COLUMN_COUNT);
        bk.set(bit);
        return bk;
    }

    // -----------------------------------------------------------------------
    // Helper: mock a covering AggStar
    // -----------------------------------------------------------------------

    /**
     * Creates an {@link AggStar} mock that passes superSetMatch for any
     * key that is a subset of its combined level + measure bits, has proper
     * lookupColumn behaviour, and is fully collapsed.
     */
    private AggStar mockCoveringAggStar(
        long size,
        String name,
        BitKey levelBitKey,
        BitKey measureBitKey)
    {
        AggStar aggStar = mock(AggStar.class);
        AggStar.FactTable aggFactTable = mock(AggStar.FactTable.class);
        when(aggStar.getFactTable()).thenReturn(aggFactTable);
        when(aggFactTable.getName()).thenReturn(name);
        when(aggStar.getSize()).thenReturn(size);

        // Combined bitkey
        BitKey combined = levelBitKey.or(measureBitKey);
        when(aggStar.getBitKey()).thenReturn(combined);
        when(aggStar.getLevelBitKey()).thenReturn(levelBitKey);
        when(aggStar.getMeasureBitKey()).thenReturn(measureBitKey);
        when(aggStar.getDistinctMeasureBitKey())
            .thenReturn(BitKey.Factory.makeBitKey(COLUMN_COUNT));

        // superSetMatch: combined must be superset of query key
        when(aggStar.superSetMatch(any(BitKey.class))).thenAnswer(inv -> {
            BitKey queryKey = inv.getArgument(0);
            return combined.isSuperSetOf(queryKey);
        });

        // Coverage: lookupColumn returns a Measure mock for each measure bit
        for (int bit = measureBitKey.nextSetBit(0); bit >= 0;
             bit = measureBitKey.nextSetBit(bit + 1))
        {
            AggStar.FactTable.Measure aggMeasure =
                mock(AggStar.FactTable.Measure.class);
            when(aggStar.lookupColumn(bit)).thenReturn(aggMeasure);
        }

        // Coverage: lookupColumn returns a Column mock for each level bit
        for (int bit = levelBitKey.nextSetBit(0); bit >= 0;
             bit = levelBitKey.nextSetBit(bit + 1))
        {
            AggStar.Table.Column aggCol = mock(AggStar.Table.Column.class);
            when(aggStar.lookupColumn(bit)).thenReturn(aggCol);
        }

        when(aggStar.isFullyCollapsed()).thenReturn(true);
        when(aggStar.hasIgnoredColumns()).thenReturn(false);

        return aggStar;
    }

    // -----------------------------------------------------------------------
    // Helper: mock a RolapHierarchy with a leaf level
    // -----------------------------------------------------------------------

    /**
     * Creates a mock hierarchy whose leaf level has a
     * {@link MondrianDef.Column} key expression and whose star column is
     * at the given bit position.
     */
    private RolapHierarchy mockHierarchy(
        String tableAlias,
        String columnName,
        int bitPosition)
    {
        RolapHierarchy hier = mock(RolapHierarchy.class);
        RolapLevel leafLevel = mock(RolapLevel.class);
        when(hier.getLevels()).thenReturn(new Level[]{leafLevel});
        when(leafLevel.isAll()).thenReturn(false);

        MondrianDef.Column keyCol = new MondrianDef.Column();
        keyCol.table = tableAlias;
        keyCol.name = columnName;
        when(leafLevel.getKeyExp()).thenReturn(keyCol);

        RolapStar.Column starCol = mock(RolapStar.Column.class);
        when(star.lookupColumn(tableAlias, columnName)).thenReturn(starCol);
        when(starCol.getBitPosition()).thenReturn(bitPosition);

        return hier;
    }
}
