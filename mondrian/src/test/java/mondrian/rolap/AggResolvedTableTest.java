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
import mondrian.olap.MondrianDef;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AggResolvedTable}.
 *
 * <p>Uses Mockito to simulate Mondrian runtime types without a full
 * schema/connection.
 */
public class AggResolvedTableTest {

    // -----------------------------------------------------------------------
    // Shared mocks
    // -----------------------------------------------------------------------

    private AggStar aggStar;
    private AggStar.FactTable aggFactTable;
    private RolapStar star;
    private SqlQuery sqlQuery;

    @BeforeEach
    public void setUp() {
        aggStar      = mock(AggStar.class);
        aggFactTable = mock(AggStar.FactTable.class);
        star         = mock(RolapStar.class);
        sqlQuery     = mock(SqlQuery.class);

        when(aggStar.getFactTable()).thenReturn(aggFactTable);
        when(aggFactTable.getName()).thenReturn("mart_konfet_agg_brand");
        when(aggStar.getStar()).thenReturn(star);
        when(star.getSqlQuery()).thenReturn(sqlQuery);
    }

    // -----------------------------------------------------------------------
    // Basic identity tests
    // -----------------------------------------------------------------------

    @Test
    public void testTableNameDelegatesToAggStar() {
        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        assertEquals("mart_konfet_agg_brand", table.tableName());
    }

    @Test
    public void testIsAggregateReturnsTrue() {
        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        assertTrue(table.isAggregate());
    }

    @Test
    public void testNeedsRollupReflectsConstructorFlagFalse() {
        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        assertFalse(table.needsRollup());
    }

    @Test
    public void testNeedsRollupReflectsConstructorFlagTrue() {
        AggResolvedTable table = new AggResolvedTable(aggStar, true);
        assertTrue(table.needsRollup());
    }

    @Test
    public void testGetAggStarReturnsWrappedAggStar() {
        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        assertSame(aggStar, table.getAggStar());
    }

    // -----------------------------------------------------------------------
    // resolveMeasure — rollup path
    // -----------------------------------------------------------------------

    @Test
    public void testResolveMeasureWithRollupCallsGenerateRollupString() {
        AggStar.FactTable.Measure aggMeasure =
            mock(AggStar.FactTable.Measure.class);
        when(aggMeasure.generateRollupString(sqlQuery))
            .thenReturn("uniqCombinedMerge(f.akb_state)");
        when(aggStar.lookupColumn(5)).thenReturn(aggMeasure);

        AggResolvedTable table = new AggResolvedTable(aggStar, true);
        MeasureRef ref = new MeasureRef("[Measures].[AKB]", "Candy", 5);
        MeasureSql result = table.resolveMeasure(ref, "a0");

        assertNotNull(result);
        assertEquals("uniqCombinedMerge(f.akb_state)", result.expression());
        verify(aggMeasure).generateRollupString(sqlQuery);
        verify(aggMeasure, never()).generateExprString(any());
    }

    // -----------------------------------------------------------------------
    // resolveMeasure — no-rollup path
    // -----------------------------------------------------------------------

    @Test
    public void testResolveMeasureWithoutRollupCallsGenerateExprString() {
        AggStar.FactTable.Measure aggMeasure =
            mock(AggStar.FactTable.Measure.class);
        when(aggMeasure.generateExprString(sqlQuery))
            .thenReturn("uniqCombinedMerge(a0.akb_state)");
        when(aggStar.lookupColumn(5)).thenReturn(aggMeasure);

        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        MeasureRef ref = new MeasureRef("[Measures].[AKB]", "Candy", 5);
        MeasureSql result = table.resolveMeasure(ref, "a0");

        assertNotNull(result);
        assertEquals("uniqCombinedMerge(a0.akb_state)", result.expression());
        verify(aggMeasure).generateExprString(sqlQuery);
        verify(aggMeasure, never()).generateRollupString(any());
    }

    // -----------------------------------------------------------------------
    // resolveMeasure — null / not-a-Measure cases
    // -----------------------------------------------------------------------

    @Test
    public void testResolveMeasureUnknownBitPosReturnsNull() {
        when(aggStar.lookupColumn(99)).thenReturn(null);

        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        MeasureRef ref = new MeasureRef("[Measures].[NoSuch]", "Candy", 99);
        MeasureSql result = table.resolveMeasure(ref, "a0");

        assertNull(result);
    }

    @Test
    public void testResolveMeasureColumnIsNotMeasureReturnsNull() {
        // Return a plain Column (Level), not a Measure
        AggStar.Table.Column nonMeasure = mock(AggStar.Table.Level.class);
        when(aggStar.lookupColumn(7)).thenReturn(nonMeasure);

        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        MeasureRef ref = new MeasureRef("[Measures].[Something]", "Candy", 7);
        MeasureSql result = table.resolveMeasure(ref, "a0");

        assertNull(result);
    }

    // -----------------------------------------------------------------------
    // resolveMeasure — name-based fallback (bitPosition < 0)
    // -----------------------------------------------------------------------

    @Test
    public void testResolveMeasureWithNegativeBitPositionUsesNameLookup() {
        // Star fact table lookup returns a star measure at bitPos=3
        RolapStar.Table starFactTable = mock(RolapStar.Table.class);
        RolapStar.Measure starMeasure  = mock(RolapStar.Measure.class);
        AggStar.FactTable.Measure aggMeasure =
            mock(AggStar.FactTable.Measure.class);

        when(star.getFactTable()).thenReturn(starFactTable);
        when(starFactTable.lookupMeasureByName("Candy", "UnitSales"))
            .thenReturn(starMeasure);
        when(starMeasure.getBitPosition()).thenReturn(3);
        when(aggStar.lookupColumn(3)).thenReturn(aggMeasure);
        when(aggMeasure.generateExprString(sqlQuery))
            .thenReturn("SUM(a0.unit_sales)");

        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        // bitPosition = -1 → must fall back to name lookup
        MeasureRef ref =
            new MeasureRef("[Measures].[UnitSales]", "Candy", -1);
        MeasureSql result = table.resolveMeasure(ref, "a0");

        assertNotNull(result);
        assertEquals("SUM(a0.unit_sales)", result.expression());
    }

    // -----------------------------------------------------------------------
    // resolveLevel — collapsed column (no join)
    // -----------------------------------------------------------------------

    @Test
    public void testResolveLevelCollapsedReturnsAliasedColumn() {
        // Set up hierarchy with one non-All level
        RolapHierarchy hier   = mock(RolapHierarchy.class);
        RolapLevel     leaf   = mock(RolapLevel.class);

        when(hier.getLevels()).thenReturn(new Level[]{leaf});
        when(leaf.isAll()).thenReturn(false);

        MondrianDef.Column keyCol = new MondrianDef.Column();
        keyCol.table = "mart_konfet_agg_brand";
        keyCol.name  = "brand_id";
        when(leaf.getKeyExp()).thenReturn(keyCol);

        // Star resolves the column to bitPos=2
        RolapStar.Column starCol = mock(RolapStar.Column.class);
        when(star.lookupColumn("mart_konfet_agg_brand", "brand_id"))
            .thenReturn(starCol);
        when(starCol.getBitPosition()).thenReturn(2);

        // Agg column lives in the fact table (collapsed)
        AggStar.Table.Column aggCol = mock(AggStar.Table.Column.class);
        when(aggStar.lookupColumn(2)).thenReturn(aggCol);
        when(aggCol.getName()).thenReturn("brand_id");
        when(aggCol.getTable()).thenReturn(aggFactTable);

        LevelRef levelRef = new LevelRef(hier, star);
        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        LevelSql result = table.resolveLevel(levelRef, "a0");

        assertNotNull(result);
        assertEquals("a0.brand_id", result.expression());
        assertTrue(result.joinClauses().isEmpty(),
            "No JOIN expected for a collapsed level");
    }

    // -----------------------------------------------------------------------
    // resolveLevel — non-RolapHierarchy returns null
    // -----------------------------------------------------------------------

    @Test
    public void testResolveLevelNonRolapHierarchyReturnsNull() {
        Hierarchy nonRolap = mock(Hierarchy.class);
        LevelRef levelRef  = new LevelRef(nonRolap, star);

        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        LevelSql result = table.resolveLevel(levelRef, "a0");

        assertNull(result);
    }

    // -----------------------------------------------------------------------
    // resolveLevel — column not in agg star returns null
    // -----------------------------------------------------------------------

    @Test
    public void testResolveLevelMissingAggColumnReturnsNull() {
        RolapHierarchy hier = mock(RolapHierarchy.class);
        RolapLevel     leaf = mock(RolapLevel.class);

        when(hier.getLevels()).thenReturn(new Level[]{leaf});
        when(leaf.isAll()).thenReturn(false);

        MondrianDef.Column keyCol = new MondrianDef.Column();
        keyCol.table = "fact_table";
        keyCol.name  = "brand_id";
        when(leaf.getKeyExp()).thenReturn(keyCol);

        RolapStar.Column starCol = mock(RolapStar.Column.class);
        when(star.lookupColumn("fact_table", "brand_id")).thenReturn(starCol);
        when(starCol.getBitPosition()).thenReturn(4);
        when(aggStar.lookupColumn(4)).thenReturn(null); // not in this agg star

        LevelRef levelRef = new LevelRef(hier, star);
        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        LevelSql result = table.resolveLevel(levelRef, "a0");

        assertNull(result);
    }
}

// End AggResolvedTableTest.java
