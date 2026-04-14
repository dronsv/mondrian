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

import mondrian.olap.Level;
import mondrian.olap.MondrianDef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link FactResolvedTable}.
 *
 * <p>Uses Mockito to simulate Mondrian runtime types without a full
 * schema/connection.
 */
public class FactResolvedTableTest {

    private RolapStar star;
    private RolapStar.Table factTable;
    private RolapCube cube;
    private FactResolvedTable table;

    @BeforeEach
    public void setUp() {
        star = mock(RolapStar.class);
        factTable = mock(RolapStar.Table.class);
        cube = mock(RolapCube.class);

        when(star.getFactTable()).thenReturn(factTable);
        when(factTable.getTableName()).thenReturn("fact_sales");
        when(cube.getName()).thenReturn("Sales");

        table = new FactResolvedTable(star, cube);
    }

    // -----------------------------------------------------------------------
    // Basic identity tests
    // -----------------------------------------------------------------------

    @Test
    public void testTableNameDelegatesToStar() {
        assertEquals("fact_sales", table.tableName());
    }

    @Test
    public void testIsAggregateReturnsFalse() {
        assertFalse(table.isAggregate());
    }

    @Test
    public void testNeedsRollupReturnsFalse() {
        assertFalse(table.needsRollup());
    }

    // -----------------------------------------------------------------------
    // resolveMeasure tests
    // -----------------------------------------------------------------------

    @Test
    public void testResolveMeasureForSumReturnsCorrectExpression() {
        RolapStar.Measure m = mock(RolapStar.Measure.class);
        when(factTable.lookupMeasureByName("Sales", "Unit Sales")).thenReturn(m);

        MondrianDef.Column colExpr = new MondrianDef.Column();
        colExpr.name = "unit_sales";
        when(m.getExpression()).thenReturn(colExpr);

        RolapAggregator agg = mock(RolapAggregator.class);
        when(agg.getExpression("f.unit_sales")).thenReturn("SUM(f.unit_sales)");
        when(m.getAggregator()).thenReturn(agg);

        MeasureRef ref = new MeasureRef(
            "[Measures].[Unit Sales]", "Sales", 0);
        MeasureSql result = table.resolveMeasure(ref, "f");

        assertNotNull(result);
        assertEquals("SUM(f.unit_sales)", result.expression());
    }

    @Test
    public void testResolveMeasureForUnknownMeasureReturnsNull() {
        when(factTable.lookupMeasureByName(anyString(), anyString()))
            .thenReturn(null);
        when(factTable.getColumns()).thenReturn(new java.util.ArrayList<>());

        MeasureRef ref = new MeasureRef("[Measures].[NoSuch]", "Sales", 0);
        MeasureSql result = table.resolveMeasure(ref, "f");

        assertNull(result);
    }

    @Test
    public void testResolveMeasureIsStableForRepeatedCalls() {
        RolapStar.Measure m = mock(RolapStar.Measure.class);
        when(factTable.lookupMeasureByName("Sales", "Revenue"))
            .thenReturn(m);

        MondrianDef.Column colExpr = new MondrianDef.Column();
        colExpr.name = "revenue";
        when(m.getExpression()).thenReturn(colExpr);

        RolapAggregator agg = mock(RolapAggregator.class);
        when(agg.getExpression("t0.revenue")).thenReturn("SUM(t0.revenue)");
        when(m.getAggregator()).thenReturn(agg);

        MeasureRef ref = new MeasureRef("[Measures].[Revenue]", "Sales", 1);

        MeasureSql first = table.resolveMeasure(ref, "t0");
        MeasureSql second = table.resolveMeasure(ref, "t0");

        assertNotNull(first);
        assertNotNull(second);
        assertEquals(first.expression(), second.expression());
    }

    // -----------------------------------------------------------------------
    // resolveLevel tests — via helper resolveHierarchyColumn
    // -----------------------------------------------------------------------

    /**
     * When NativeSqlCalc.resolveLevelColumnSql returns a resolved column
     * directly from the star (fact table column), no JOIN clauses are added.
     */
    @Test
    public void testResolveLevelForFactColumnNoJoin() {
        RolapHierarchy hier = mock(RolapHierarchy.class);
        RolapLevel leafLevel = mock(RolapLevel.class);

        // Hierarchy returns one non-all level
        when(hier.getLevels()).thenReturn(new Level[]{leafLevel});
        when(leafLevel.isAll()).thenReturn(false);

        // Key expression is a Column pointing at the fact table
        MondrianDef.Column keyCol = new MondrianDef.Column();
        keyCol.table = "fact_sales";
        keyCol.name = "brand_id";
        when(leafLevel.getKeyExp()).thenReturn(keyCol);

        // star.lookupColumn returns a column whose table IS the fact table
        RolapStar.Column starCol = mock(RolapStar.Column.class);
        when(star.lookupColumn("fact_sales", "brand_id")).thenReturn(starCol);
        when(starCol.getExpression()).thenReturn(keyCol);
        when(starCol.getTable()).thenReturn(factTable);  // same as fact table

        List<String> joins = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        String col = table.resolveHierarchyColumn(hier, "f", joins, seen);

        assertNotNull(col, "Should resolve fact-table column");
        assertEquals("f.brand_id", col);
        assertTrue(joins.isEmpty(), "No JOIN should be added for a fact column");
    }

    /**
     * When the hierarchy lives in a dimension table (not in the star directly),
     * resolveLevel should add a JOIN clause and return a dim-qualified expression.
     */
    @Test
    public void testResolveLevelForDimensionColumnAddsJoin() {
        RolapHierarchy hier = mock(RolapHierarchy.class);
        RolapLevel leafLevel = mock(RolapLevel.class);

        // Hierarchy returns one non-all level
        when(hier.getLevels()).thenReturn(new Level[]{leafLevel});
        when(leafLevel.isAll()).thenReturn(false);

        // Key expression references dim table column
        MondrianDef.Column keyCol = new MondrianDef.Column();
        keyCol.table = "dim_product";
        keyCol.name = "brand_name";
        when(leafLevel.getKeyExp()).thenReturn(keyCol);

        // star does not know this column → falls back to dim join
        when(star.lookupColumn("dim_product", "brand_name")).thenReturn(null);
        when(hier.getUniqueName()).thenReturn("[Product]");
        when(hier.getName()).thenReturn("Product");

        // Dimension table relation
        MondrianDef.Table dimRelation = new MondrianDef.Table();
        dimRelation.name = "dim_product";
        // no explicit alias → uses table name as alias
        when(hier.getRelation()).thenReturn(dimRelation);

        // XML hierarchy for primary key
        MondrianDef.Hierarchy xmlHier = new MondrianDef.Hierarchy();
        xmlHier.primaryKey = "product_id";
        when(hier.getXmlHierarchy()).thenReturn(xmlHier);

        // Cube usages → foreign key
        HierarchyUsage usage = mock(HierarchyUsage.class);
        when(usage.getForeignKey()).thenReturn("product_fk");
        when(cube.getUsages(hier)).thenReturn(new HierarchyUsage[]{usage});

        List<String> joins = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        String col = table.resolveHierarchyColumn(hier, "f", joins, seen);

        assertNotNull(col, "Should resolve dimension column");
        assertEquals("dim_product.brand_name", col);
        assertEquals(1, joins.size());
        assertTrue(
            joins.get(0).contains("JOIN dim_product"),
            "JOIN clause should reference dim_product");
        assertTrue(
            joins.get(0).contains("f.product_fk"),
            "JOIN should use foreign key from cube usages");
        assertTrue(
            joins.get(0).contains("dim_product.product_id"),
            "JOIN should use primary key from XML hierarchy");
    }

    // -----------------------------------------------------------------------
    // extractSimpleName helper tests
    // -----------------------------------------------------------------------

    @Test
    public void testExtractSimpleNameFromFullyQualifiedName() {
        assertEquals("Sales", FactResolvedTable.extractSimpleName("[Measures].[Sales]"));
    }

    @Test
    public void testExtractSimpleNameFromBareNamePassesThrough() {
        assertEquals("Sales", FactResolvedTable.extractSimpleName("Sales"));
    }

    @Test
    public void testExtractSimpleNameFromNullReturnsNull() {
        assertNull(FactResolvedTable.extractSimpleName(null));
    }
}

// End FactResolvedTableTest.java
