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
        // Physical column expression — resolveLevel uses getExpression()
        MondrianDef.Column aggExpr = new MondrianDef.Column();
        aggExpr.table = "mart_konfet_agg_brand";
        aggExpr.name = "brand_id";
        when(aggCol.getExpression()).thenReturn(aggExpr);

        StarLevelRef levelRef = new StarLevelRef(hier, star);
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
        StarLevelRef levelRef  = new StarLevelRef(nonRolap, star);

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

        StarLevelRef levelRef = new StarLevelRef(hier, star);
        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        LevelSql result = table.resolveLevel(levelRef, "a0");

        assertNull(result);
    }

    // -----------------------------------------------------------------------
    // resolvePredicateColumn — collapsed (denormalized) column
    //
    // Hardens the JOIN-skip optimization landed in af34dc3d7: when the agg
    // already carries the column inline on its fact row, the resolver must
    // emit "<alias>.<col>" with an empty join list (no spurious dim JOIN).
    // -----------------------------------------------------------------------

    @Test
    public void testResolvePredicateColumnCollapsedReturnsInlineAlias() {
        RolapStar.Column starCol = mock(RolapStar.Column.class);
        when(starCol.getBitPosition()).thenReturn(11);

        // Agg has the column denormalized into its own fact table.
        AggStar.Table.Column aggCol = mock(AggStar.Table.Column.class);
        when(aggStar.lookupColumn(11)).thenReturn(aggCol);
        when(aggCol.getTable()).thenReturn(aggFactTable);
        when(aggCol.getName()).thenReturn("brand_id");
        MondrianDef.Column aggExpr = new MondrianDef.Column();
        aggExpr.table = "mart_konfet_agg_brand";
        aggExpr.name  = "brand_id";
        when(aggCol.getExpression()).thenReturn(aggExpr);

        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        PredicateSql result = table.resolvePredicateColumn(starCol, "f");

        assertNotNull(result);
        assertEquals("f.brand_id", result.qualifiedColumn());
        assertTrue(result.joinClauses().isEmpty(),
            "Collapsed column must not emit a JOIN");
    }

    // -----------------------------------------------------------------------
    // resolvePredicateColumn — falls through to AggStar JOIN condition
    //
    // When the column lives in a child DimTable (non-collapsed), the resolver
    // must fall through to the agg's own JoinCondition: the qualified column
    // gets the dim alias, and a JOIN clause is registered.
    // -----------------------------------------------------------------------

    @Test
    public void testResolvePredicateColumnFallsThroughToAggJoinCondition() {
        RolapStar.Column starCol = mock(RolapStar.Column.class);
        when(starCol.getBitPosition()).thenReturn(13);

        // Agg column is on a DimTable (not the agg fact table).
        AggStar.DimTable dimTable = mock(AggStar.DimTable.class);
        when(dimTable.getName()).thenReturn("dim_konfet_product");
        when(dimTable.hasJoinCondition()).thenReturn(true);
        when(dimTable.hasParent()).thenReturn(true);
        // Parent is the agg fact table → triggers FK-validation branch.
        when(dimTable.getParent()).thenReturn(aggFactTable);

        AggStar.Table.JoinCondition jc =
            mock(AggStar.Table.JoinCondition.class);
        MondrianDef.Column left  = new MondrianDef.Column(null, "sku_id");
        MondrianDef.Column right = new MondrianDef.Column(null, "sku_id");
        when(jc.getLeft()).thenReturn(left);
        when(jc.getRight()).thenReturn(right);
        when(dimTable.getJoinCondition()).thenReturn(jc);

        AggStar.Table.Column aggCol = mock(AggStar.Table.Column.class);
        when(aggStar.lookupColumn(13)).thenReturn(aggCol);
        when(aggCol.getTable()).thenReturn(dimTable);
        when(aggCol.getName()).thenReturn("brand_name");
        MondrianDef.Column aggExpr =
            new MondrianDef.Column("dim_konfet_product", "brand_name");
        when(aggCol.getExpression()).thenReturn(aggExpr);

        // FK-validation loop: scan star.getColumnCount() for an agg-fact
        // column whose name matches the JOIN's left side ("sku_id").
        AggStar.Table.Column fkCol = mock(AggStar.Table.Column.class);
        when(fkCol.getTable()).thenReturn(aggFactTable);
        when(fkCol.getName()).thenReturn("sku_id");
        when(star.getColumnCount()).thenReturn(1);
        when(aggStar.lookupColumn(0)).thenReturn(fkCol);

        AggResolvedTable table = new AggResolvedTable(aggStar, false);
        PredicateSql result = table.resolvePredicateColumn(starCol, "f");

        assertNotNull(result);
        assertEquals("dim_konfet_product.brand_name",
            result.qualifiedColumn());
        assertFalse(result.joinClauses().isEmpty(),
            "Non-collapsed column must register the agg's JOIN clause");
        assertEquals(1, result.joinClauses().size());
        assertEquals(
            "JOIN dim_konfet_product dim_konfet_product"
            + " ON f.sku_id = dim_konfet_product.sku_id",
            result.joinClauses().get(0));
    }
}

// End AggResolvedTableTest.java
