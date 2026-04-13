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
import mondrian.rolap.aggmatcher.AggStar;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Sentinel tests for Phase 2A invariants: agg-table selection, fact-table
 * fallback, and the basic contracts of {@link FactResolvedTable},
 * {@link AggResolvedTable}, and the inner result types of
 * {@link NqeTableStrategy}.
 *
 * <p>These tests document stable behavioural contracts that must hold across
 * refactors and should never be silently broken.  They are intentionally
 * lightweight (no real schema, no SQL execution) and rely only on Mockito
 * mocks for Mondrian runtime types.
 */
public class NqeAggIntegrationTest {

    private static final int COLUMN_COUNT = 64;

    private NqeTableStrategy strategy;

    @BeforeEach
    public void setUp() {
        strategy = new NqeTableStrategy();
    }

    // -----------------------------------------------------------------------
    // Test 1: No-agg cube returns a SingleSourcePlan backed by FactResolvedTable
    // -----------------------------------------------------------------------

    /**
     * When a cube has no agg stars, {@link NqeTableStrategy#resolve} must
     * return a {@link NqeTableStrategy.SingleSourcePlan} whose table is a
     * {@link FactResolvedTable} (isAggregate = false).
     */
    @Test
    public void noAggCube_returnsFactTable() {
        RolapCube cube = mockCubeWithStar("NoAggCube", "fact_table");
        when(cube.getStar().getAggStars())
            .thenReturn(Collections.<AggStar>emptyList());

        CoordinateClassPlan plan = makeStoredPlan("c1", "[Measures].[Sales]");

        NqeTableStrategy.ResolvedSourcePlan result =
            strategy.resolve(cube, plan, null);

        assertTrue(result.isResolved());
        assertInstanceOf(NqeTableStrategy.SingleSourcePlan.class, result);

        ResolvedTable table = result.getTable();
        assertNotNull(table);
        assertFalse(table.isAggregate());
        assertEquals("fact_table", table.tableName());
    }

    // -----------------------------------------------------------------------
    // Test 2: Cube with no star returns Unresolved sentinel
    // -----------------------------------------------------------------------

    /**
     * When {@code cube.getStar()} returns {@code null}, the strategy cannot
     * build bit-keys and must return an {@link NqeTableStrategy.Unresolved}
     * with {@code isResolved() == false} and {@code getTable() == null}.
     */
    @Test
    public void unresolvedPlan_returnsUnresolved() {
        RolapCube cube = mock(RolapCube.class);
        when(cube.getStar()).thenReturn(null);
        when(cube.getName()).thenReturn("BrokenCube");

        CoordinateClassPlan plan = makeStoredPlan("c1", "[Measures].[Sales]");

        NqeTableStrategy.ResolvedSourcePlan result =
            strategy.resolve(cube, plan, null);

        assertFalse(result.isResolved());
        assertNull(result.getTable());
        assertInstanceOf(NqeTableStrategy.Unresolved.class, result);
        assertNotNull(((NqeTableStrategy.Unresolved) result).getReason());
        assertFalse(
            ((NqeTableStrategy.Unresolved) result).getReason().isBlank());
    }

    // -----------------------------------------------------------------------
    // Test 3: Exhaustive switch over both result-type variants
    // -----------------------------------------------------------------------

    /**
     * Both {@link NqeTableStrategy.Unresolved} and
     * {@link NqeTableStrategy.SingleSourcePlan} must be reachable from the
     * same switch-style dispatch (simulating sealed-type exhaustiveness).
     * This test documents the complete variant set so a third variant can
     * never be added silently.
     */
    @Test
    public void resultTypeVariants_exhaustiveDispatch() {
        CoordinateClassPlan plan = makeStoredPlan("c1", "[Measures].[X]");

        // Build an Unresolved
        RolapCube brokenCube = mock(RolapCube.class);
        when(brokenCube.getStar()).thenReturn(null);
        when(brokenCube.getName()).thenReturn("Broken");
        NqeTableStrategy.ResolvedSourcePlan unresolved =
            strategy.resolve(brokenCube, plan, null);

        // Build a SingleSourcePlan (no-agg cube)
        RolapCube goodCube = mockCubeWithStar("Good", "fact_good");
        when(goodCube.getStar().getAggStars())
            .thenReturn(Collections.<AggStar>emptyList());
        NqeTableStrategy.ResolvedSourcePlan single =
            strategy.resolve(goodCube, plan, null);

        String kindUnresolved = dispatchResultPlan(unresolved);
        String kindSingle     = dispatchResultPlan(single);

        assertEquals("unresolved", kindUnresolved);
        assertEquals("single",     kindSingle);
    }

    /** Dispatches over the two known concrete types. */
    private static String dispatchResultPlan(
        NqeTableStrategy.ResolvedSourcePlan plan)
    {
        if (plan instanceof NqeTableStrategy.Unresolved) {
            return "unresolved";
        } else if (plan instanceof NqeTableStrategy.SingleSourcePlan) {
            return "single";
        }
        return "unknown";
    }

    // -----------------------------------------------------------------------
    // Test 4: FactResolvedTable basic contract
    // -----------------------------------------------------------------------

    /**
     * {@link FactResolvedTable} must advertise itself as non-aggregate,
     * non-rollup, and expose the fact-table name from its star.
     */
    @Test
    public void factResolvedTable_basicContract() {
        RolapStar star       = mock(RolapStar.class);
        RolapStar.Table ft   = mock(RolapStar.Table.class);
        RolapCube cube       = mock(RolapCube.class);

        when(star.getFactTable()).thenReturn(ft);
        when(ft.getTableName()).thenReturn("mart_konfet_flat");

        FactResolvedTable frt = new FactResolvedTable(star, cube);

        assertEquals("mart_konfet_flat", frt.tableName());
        assertFalse(frt.isAggregate());
        assertFalse(frt.needsRollup());
    }

    // -----------------------------------------------------------------------
    // Test 5: AggResolvedTable basic contract
    // -----------------------------------------------------------------------

    /**
     * {@link AggResolvedTable} must advertise itself as aggregate, expose the
     * agg-table name from its {@link AggStar}, reflect the rollup flag passed
     * at construction, and return the same {@code AggStar} from
     * {@link AggResolvedTable#getAggStar()}.
     */
    @Test
    public void aggResolvedTable_basicContract() {
        AggStar aggStar            = mock(AggStar.class);
        AggStar.FactTable aggFact  = mock(AggStar.FactTable.class);

        when(aggStar.getFactTable()).thenReturn(aggFact);
        when(aggFact.getName()).thenReturn("mart_konfet_agg_brand");

        AggResolvedTable art = new AggResolvedTable(aggStar, /*rollup=*/true);

        assertEquals("mart_konfet_agg_brand", art.tableName());
        assertTrue(art.isAggregate());
        assertTrue(art.needsRollup());
        assertSame(aggStar, art.getAggStar());
    }

    /**
     * Rollup flag of {@code false} must propagate through {@code needsRollup}.
     */
    @Test
    public void aggResolvedTable_noRollupFlag() {
        AggStar aggStar           = mock(AggStar.class);
        AggStar.FactTable aggFact = mock(AggStar.FactTable.class);
        when(aggStar.getFactTable()).thenReturn(aggFact);
        when(aggFact.getName()).thenReturn("mart_konfet_agg_mfr");

        AggResolvedTable art = new AggResolvedTable(aggStar, /*rollup=*/false);

        assertTrue(art.isAggregate());
        assertFalse(art.needsRollup());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Creates a minimal {@link RolapCube} mock wired to a {@link RolapStar}
     * whose fact table reports the given name.  The star's column count is set
     * to {@value #COLUMN_COUNT} and its fact-table column list is empty.
     */
    private RolapCube mockCubeWithStar(String cubeName, String factTableName) {
        RolapCube cube         = mock(RolapCube.class);
        RolapStar star         = mock(RolapStar.class);
        RolapStar.Table ft     = mock(RolapStar.Table.class);

        when(cube.getName()).thenReturn(cubeName);
        when(cube.getStar()).thenReturn(star);
        when(star.getColumnCount()).thenReturn(COLUMN_COUNT);
        when(star.getFactTable()).thenReturn(ft);
        when(ft.getTableName()).thenReturn(factTableName);
        when(ft.getColumns())
            .thenReturn(Collections.<RolapStar.Column>emptyList());
        return cube;
    }

    /**
     * Creates a {@link CoordinateClassPlan} with a single STORED_COLUMN
     * request for the given measure unique name.
     */
    private static CoordinateClassPlan makeStoredPlan(
        String classId,
        String measureId)
    {
        PhysicalValueRequest req = new PhysicalValueRequest(
            measureId,
            Collections.<Hierarchy>emptySet(),
            /*resetHierarchies=*/null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            /*nativeTemplate=*/null);
        return new CoordinateClassPlan(classId, List.of(req));
    }
}
