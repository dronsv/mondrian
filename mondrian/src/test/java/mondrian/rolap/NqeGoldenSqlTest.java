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
import mondrian.olap.Member;
import mondrian.olap.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Golden SQL tests for {@link NativeQuerySqlGenerator} with
 * {@link ResolvedTable}.
 *
 * <p>Verifies that the generator is source-agnostic: it uses the
 * table name and expressions from the ResolvedTable, not from
 * hard-coded star/fact-table references.
 */
public class NqeGoldenSqlTest {

    private ResolvedTable resolvedTable;
    private RolapEvaluator evaluator;
    private RolapCube baseCube;
    private RolapStar star;
    private Query query;

    @BeforeEach
    public void setUp() {
        resolvedTable = mock(ResolvedTable.class);
        evaluator = mock(RolapEvaluator.class);
        baseCube = mock(RolapCube.class);
        star = mock(RolapStar.class);
        query = mock(Query.class);

        when(baseCube.getStar()).thenReturn(star);
        when(baseCube.getName()).thenReturn("TestCube");
        when(evaluator.getMembers()).thenReturn(new Member[0]);
        when(evaluator.getQuery()).thenReturn(query);
        when(query.getSubcubePredicates(baseCube)).thenReturn(null);
    }

    // ------------------------------------------------------------------
    // Test: generator uses ResolvedTable.tableName() for FROM clause
    // ------------------------------------------------------------------

    @Test
    public void testGeneratorUsesResolvedTableName() {
        when(resolvedTable.tableName()).thenReturn("mart_konfet_agg");

        // Measure resolution
        MeasureSql measureSql = new MeasureSql("SUM(f.sales_qty)");
        when(resolvedTable.resolveMeasure(any(MeasureRef.class), eq("f")))
            .thenReturn(measureSql);

        // Build a plan with no projected hierarchies (scalar query)
        Set<Hierarchy> projected = new LinkedHashSet<>();
        PhysicalValueRequest req = new PhysicalValueRequest(
            "[Measures].[Sales Qty]",
            projected,
            null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);

        CoordinateClassPlan plan = new CoordinateClassPlan(
            "class1", Collections.singletonList(req));

        NativeQuerySqlGenerator gen = new NativeQuerySqlGenerator(
            resolvedTable, evaluator, baseCube);
        String sql = gen.generateSql(plan);

        assertNotNull(sql, "SQL should be generated for valid plan");
        assertTrue(sql.contains("FROM mart_konfet_agg f"),
            "SQL should use table name from ResolvedTable: " + sql);
        assertTrue(sql.contains("SUM(f.sales_qty)"),
            "SQL should use measure expression from ResolvedTable: " + sql);
    }

    // ------------------------------------------------------------------
    // Test: generator uses ResolvedTable.resolveLevel() for GROUP BY
    // ------------------------------------------------------------------

    @Test
    public void testGeneratorUsesResolvedTableForGroupBy() {
        when(resolvedTable.tableName()).thenReturn("mart_konfet_agg_brand");

        // One hierarchy projected
        Hierarchy brandHier = mock(Hierarchy.class);
        Set<Hierarchy> projected = new LinkedHashSet<>();
        projected.add(brandHier);

        LevelSql levelSql = new LevelSql("f.brand_name");
        when(resolvedTable.resolveLevel(any(LevelRef.class), eq("f")))
            .thenReturn(levelSql);

        MeasureSql measureSql = new MeasureSql("SUM(f.sales_qty)");
        when(resolvedTable.resolveMeasure(any(MeasureRef.class), eq("f")))
            .thenReturn(measureSql);

        PhysicalValueRequest req = new PhysicalValueRequest(
            "[Measures].[Sales Qty]",
            projected,
            null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);

        CoordinateClassPlan plan = new CoordinateClassPlan(
            "class2", Collections.singletonList(req));

        NativeQuerySqlGenerator gen = new NativeQuerySqlGenerator(
            resolvedTable, evaluator, baseCube);
        String sql = gen.generateSql(plan);

        assertNotNull(sql);
        assertTrue(sql.contains("FROM mart_konfet_agg_brand f"),
            "SQL should use agg table name: " + sql);
        assertTrue(sql.contains("f.brand_name"),
            "SQL should use level expression from ResolvedTable: " + sql);
        assertTrue(sql.contains("GROUP BY f.brand_name"),
            "SQL should group by the resolved level expression: " + sql);
    }

    // ------------------------------------------------------------------
    // Test: generator accumulates join clauses from ResolvedTable
    // ------------------------------------------------------------------

    @Test
    public void testGeneratorIncludesJoinClausesFromResolvedTable() {
        when(resolvedTable.tableName()).thenReturn("fact_sales");

        Hierarchy storeHier = mock(Hierarchy.class);
        Set<Hierarchy> projected = new LinkedHashSet<>();
        projected.add(storeHier);

        // Level resolution returns a join
        List<String> joins = Collections.singletonList(
            "JOIN dim_store d ON f.store_key = d.store_key");
        LevelSql levelSql = new LevelSql("d.store_name", joins);
        when(resolvedTable.resolveLevel(any(LevelRef.class), eq("f")))
            .thenReturn(levelSql);

        MeasureSql measureSql = new MeasureSql("SUM(f.amount)");
        when(resolvedTable.resolveMeasure(any(MeasureRef.class), eq("f")))
            .thenReturn(measureSql);

        PhysicalValueRequest req = new PhysicalValueRequest(
            "[Measures].[Amount]",
            projected,
            null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);

        CoordinateClassPlan plan = new CoordinateClassPlan(
            "class3", Collections.singletonList(req));

        NativeQuerySqlGenerator gen = new NativeQuerySqlGenerator(
            resolvedTable, evaluator, baseCube);
        String sql = gen.generateSql(plan);

        assertNotNull(sql);
        assertTrue(sql.contains(
            "JOIN dim_store d ON f.store_key = d.store_key"),
            "SQL should include join clauses from ResolvedTable: " + sql);
    }

    // ------------------------------------------------------------------
    // Test: null measure resolution skips the request
    // ------------------------------------------------------------------

    @Test
    public void testNullMeasureResolutionSkipsRequest() {
        when(resolvedTable.tableName()).thenReturn("fact_sales");

        // Measure cannot be resolved
        when(resolvedTable.resolveMeasure(any(MeasureRef.class), eq("f")))
            .thenReturn(null);

        Set<Hierarchy> projected = new LinkedHashSet<>();
        PhysicalValueRequest req = new PhysicalValueRequest(
            "[Measures].[Unknown]",
            projected,
            null,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN,
            null);

        CoordinateClassPlan plan = new CoordinateClassPlan(
            "class4", Collections.singletonList(req));

        NativeQuerySqlGenerator gen = new NativeQuerySqlGenerator(
            resolvedTable, evaluator, baseCube);
        String sql = gen.generateSql(plan);

        // All measures unresolvable => null SQL
        assertNull(sql,
            "SQL should be null when no measures can be resolved");
    }

    // ------------------------------------------------------------------
    // Test: agg table with merge expression
    // ------------------------------------------------------------------

    @Test
    public void testAggTableMergeExpression() {
        when(resolvedTable.tableName()).thenReturn("mart_konfet_agg_brand");
        when(resolvedTable.isAggregate()).thenReturn(true);
        when(resolvedTable.needsRollup()).thenReturn(true);

        // Merge function expression from AggResolvedTable
        MeasureSql measureSql =
            new MeasureSql("uniqCombinedMerge(akb_state)");
        when(resolvedTable.resolveMeasure(any(MeasureRef.class), eq("f")))
            .thenReturn(measureSql);

        // One hierarchy
        Hierarchy brandHier = mock(Hierarchy.class);
        Set<Hierarchy> projected = new LinkedHashSet<>();
        projected.add(brandHier);

        LevelSql levelSql = new LevelSql("f.brand_name");
        when(resolvedTable.resolveLevel(any(LevelRef.class), eq("f")))
            .thenReturn(levelSql);

        PhysicalValueRequest req = new PhysicalValueRequest(
            "[Measures].[AKB]",
            projected,
            null,
            PhysicalValueRequest.AggregationKind.DISTINCT_MERGE,
            PhysicalValueRequest.ExpressionProviderKind.STATE_AGGREGATE,
            null);

        CoordinateClassPlan plan = new CoordinateClassPlan(
            "class5", Collections.singletonList(req));

        NativeQuerySqlGenerator gen = new NativeQuerySqlGenerator(
            resolvedTable, evaluator, baseCube);
        String sql = gen.generateSql(plan);

        assertNotNull(sql);
        assertTrue(sql.contains("uniqCombinedMerge(akb_state)"),
            "SQL should use merge expression from ResolvedTable: " + sql);
        assertTrue(sql.contains("FROM mart_konfet_agg_brand f"),
            "SQL should use agg table name: " + sql);
    }
}
