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
import mondrian.rolap.agg.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.DriverManager;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Predicate failures must decline NQE before any unrestricted SQL executes. */
class NqePredicateSafetyTest {
    private ResolvedTable table;
    private RolapEvaluator evaluator;
    private RolapCube cube;
    private RolapStar star;
    private RolapStar.Column column;
    private Hierarchy reset;
    private NativeQuerySqlGenerator generator;

    enum Path { OUTER, INNER, TEMPLATE }

    @BeforeEach void setUp() {
        table = mock(ResolvedTable.class);
        evaluator = mock(RolapEvaluator.class);
        cube = mock(RolapCube.class);
        star = mock(RolapStar.class);
        RolapStar.Table fact = mock(RolapStar.Table.class);
        column = mock(RolapStar.Column.class);
        reset = mock(Hierarchy.class);
        when(cube.getStar()).thenReturn(star);
        when(cube.getName()).thenReturn("PredicateSafety");
        when(star.getFactTable()).thenReturn(fact);
        when(fact.getTableName()).thenReturn("fact");
        when(star.getColumnCount()).thenReturn(8);
        when(column.getTable()).thenReturn(fact);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(2);
        when(evaluator.getMembers()).thenReturn(new Member[0]);
        when(table.tableName()).thenReturn("fact");
        when(table.resolveMeasure(any(MeasureRef.class), anyString()))
            .thenAnswer(inv -> new MeasureSql(
                "SUM(" + inv.getArgument(1) + ".qty)"));
        when(table.resolvePredicateColumn(eq(column), anyString()))
            .thenAnswer(inv -> new PredicateSql(
                inv.getArgument(1) + ".key_col"));
        generator = new NativeQuerySqlGenerator(table, evaluator, cube);
    }

    @ParameterizedTest @EnumSource(Path.class)
    void unsupportedAtomDeclinesEveryBooleanContext(Path path) {
        StarPredicate unsupported = mock(StarPredicate.class);
        when(unsupported.getConstrainedColumnList())
            .thenReturn(Collections.emptyList());
        assertEveryBooleanContextDeclines(path, unsupported);
    }

    @ParameterizedTest @EnumSource(Path.class)
    void unresolvedAtomDeclinesEveryBooleanContext(Path path) {
        when(table.resolvePredicateColumn(eq(column), anyString()))
            .thenReturn(null);
        assertEveryBooleanContextDeclines(
            path, new ValueColumnPredicate(column, 1));
    }

    @ParameterizedTest @EnumSource(Path.class)
    void emptyResolvedColumnDeclinesPlan(Path path) {
        when(table.resolvePredicateColumn(eq(column), anyString()))
            .thenReturn(new PredicateSql(""));
        when(evaluator.getSubcubePredicate())
            .thenReturn(new ValueColumnPredicate(column, 1));
        assertNull(generator.generateSql(plan(path)));
    }

    @ParameterizedTest @EnumSource(Path.class)
    void unresolvedSubqueryAtomDeclinesPlan(Path path) {
        when(table.resolvePredicateColumn(eq(column), anyString()))
            .thenReturn(null);
        when(evaluator.getSubcubePredicate()).thenReturn(
            new SqlInSubqueryPredicate(column, "SELECT 1"));
        assertNull(generator.generateSql(plan(path)));
    }

    @ParameterizedTest @EnumSource(Path.class)
    void unresolvedContextMemberDeclinesPlan(Path path) {
        RolapMember member = mock(RolapMember.class);
        RolapLevel level = mock(RolapLevel.class);
        when(member.getLevel()).thenReturn(level);
        when(member.getHierarchy()).thenReturn(mock(RolapHierarchy.class));
        when(member.getKey()).thenReturn("selected");
        when(evaluator.getMembers()).thenReturn(new Member[] {member});
        assertNull(generator.generateSql(plan(path)));
    }

    @ParameterizedTest @EnumSource(value = Path.class, names = {"OUTER", "INNER"})
    void booleanIdentitiesPreserveSqlTruthValues(Path path) throws Exception {
        StarPredicate one = new ValueColumnPredicate(column, 1);
        List<StarPredicate> predicates = Arrays.asList(
            new OrPredicate(Arrays.asList(LiteralStarPredicate.TRUE, one)),
            new NotPredicate(new OrPredicate(
                Arrays.asList(LiteralStarPredicate.TRUE, one))),
            new AndPredicate(Collections.emptyList()),
            new OrPredicate(Collections.emptyList()),
            new NotPredicate(new OrPredicate(Collections.emptyList())),
            new ListColumnPredicate(column, Collections.emptyList()),
            new AndPredicate(Arrays.asList(LiteralStarPredicate.FALSE, one)),
            new OrPredicate(Arrays.asList(LiteralStarPredicate.FALSE, one)));
        List<Integer> expected = Arrays.asList(30, null, 30, null, 30, null, null, 10);
        try (java.sql.Connection db = DriverManager.getConnection("jdbc:h2:mem:");
             java.sql.Statement sql = db.createStatement())
        {
            sql.execute("CREATE TABLE fact (key_col INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (1,10),(2,20)");
            for (int i = 0; i < predicates.size(); i++) {
                when(evaluator.getSubcubePredicate()).thenReturn(predicates.get(i));
                String query = generator.generateSql(plan(path));
                assertNotNull(query, "supported Boolean predicate " + i);
                try (java.sql.ResultSet rows = sql.executeQuery(query)) {
                    assertTrue(rows.next());
                    Object value = rows.getObject(path == Path.INNER ? 2 : 1);
                    assertEquals(expected.get(i),
                        value == null ? null : ((Number) value).intValue(), query);
                }
            }
        }
    }

    @Test void templateExclusionReplacesOrAtomsWithTrue() throws Exception {
        RolapStar.Column other = mock(RolapStar.Column.class);
        when(other.getStar()).thenReturn(star);
        RolapStar.Table fact = column.getTable();
        when(other.getTable()).thenReturn(fact);
        when(other.getBitPosition()).thenReturn(3);
        when(table.resolvePredicateColumn(eq(other), anyString()))
            .thenAnswer(inv -> new PredicateSql(inv.getArgument(1) + ".other_key"));
        StarPredicate x1 = memberPredicate(column, "X", 1);
        StarPredicate x2 = memberPredicate(column, "X", 2);
        StarPredicate y1 = memberPredicate(other, "Y", 1);
        StarPredicate y2 = memberPredicate(other, "Y", 2);
        List<StarPredicate> predicates = List.of(
            new OrPredicate(List.of(x1, y1)),
            new AndPredicate(List.of(x1, y1)),
            new OrPredicate(List.of(
                new AndPredicate(List.of(x1, y1)),
                new AndPredicate(List.of(x2, y2)))),
            new AndPredicate(List.of(new OrPredicate(List.of(x1, y1)), y2)),
            new OrPredicate(List.of(x1, LiteralStarPredicate.FALSE)),
            new AndPredicate(List.of(x1, LiteralStarPredicate.FALSE)),
            new OrPredicate(Collections.emptyList()),
            new AndPredicate(Collections.emptyList()));
        List<Integer> expected = Arrays.asList(100, 40, 100, 60, 100, null, null, 100);
        CoordinateClassPlan plan = new CoordinateClassPlan("excluded", List.of(
            new PhysicalValueRequest("[Measures].[Quantity]", Collections.emptySet(),
                null, PhysicalValueRequest.AggregationKind.NATIVE_EXPRESSION,
                PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE,
                "SELECT SUM(f.qty) FROM ${factTable} f WHERE ${whereClauseExcept:X}")));
        try (java.sql.Connection db = DriverManager.getConnection("jdbc:h2:mem:");
             java.sql.Statement sql = db.createStatement())
        {
            sql.execute("CREATE TABLE fact (key_col INT, other_key INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40)");
            for (int i = 0; i < predicates.size(); i++) {
                when(evaluator.getSubcubePredicate()).thenReturn(predicates.get(i));
                String query = generator.generateSql(plan);
                assertNotNull(query, "Supported exclusion must generate SQL");
                try (java.sql.ResultSet rows = sql.executeQuery(query)) {
                    assertTrue(rows.next());
                    Object value = rows.getObject(1);
                    assertEquals(expected.get(i),
                        value == null ? null : ((Number) value).intValue(), query);
                }
            }
        }
    }

    private static StarPredicate memberPredicate(
        RolapStar.Column column, String name, int key)
    {
        RolapMember member = mock(RolapMember.class);
        RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        mondrian.olap.Dimension dimension = mock(mondrian.olap.Dimension.class);
        when(dimension.getName()).thenReturn(name);
        when(hierarchy.getDimension()).thenReturn(dimension);
        when(hierarchy.getName()).thenReturn(name);
        when(member.getHierarchy()).thenReturn(hierarchy);
        when(member.getKey()).thenReturn(key);
        return new MemberColumnPredicate(column, member);
    }

    @Test void unresolvedInnerOnlyAtomCannotBeHiddenByValidOuterResolution() {
        when(table.resolvePredicateColumn(column, "f_inner")).thenReturn(null);
        when(evaluator.getSubcubePredicate())
            .thenReturn(new ValueColumnPredicate(column, 1));
        assertNull(generator.generateSql(plan(Path.INNER)));
    }

    @Test void unsupportedReducedProjectionReturnsFallbackWithoutExecutingSql() {
        StarPredicate unsupported = mock(StarPredicate.class);
        when(evaluator.getSubcubePredicate()).thenReturn(unsupported);
        assertFalse(generator.executePlanWithProjection(
            plan(Path.OUTER), Collections.emptySet(), "reduced",
            new NativeQueryResultContext()));
        verify(evaluator, never()).getSchemaReader();
    }

    @Test void nonEmptyFilterDeclinesUnsupportedPredicateWithoutPruning() {
        when(evaluator.getSubcubePredicate()).thenReturn(mock(StarPredicate.class));
        assertNull(NativeNonEmptyFilter.buildNonEmptySql(
            Collections.emptySet(),
            Collections.singletonMap("qty", NativeNonEmptyFilter.AggKind.SUM),
            cube, evaluator));
    }

    @Test void templateDeclinesPredicateKindsWithoutExclusionSemantics() {
        ValueColumnPredicate value = new ValueColumnPredicate(column, 1);
        List<StarPredicate> predicates = Arrays.asList(
            new NotPredicate(value),
            new ListColumnPredicate(column, Collections.singletonList(value)),
            new SqlInSubqueryPredicate(column, "SELECT 1"));
        assertAll(predicates.stream().map(predicate -> () -> {
            StarPredicate disjunction = new OrPredicate(
                Arrays.asList(LiteralStarPredicate.FALSE, predicate));
            when(evaluator.getSubcubePredicate()).thenReturn(disjunction);
            assertNull(generator.generateSql(plan(Path.TEMPLATE)));
        }));
    }

    private void assertEveryBooleanContextDeclines(Path path, StarPredicate atom) {
        List<StarPredicate> predicates = Arrays.asList(
            atom,
            new AndPredicate(Arrays.asList(LiteralStarPredicate.TRUE, atom)),
            new OrPredicate(Arrays.asList(LiteralStarPredicate.FALSE, atom)),
            new OrPredicate(Arrays.asList(LiteralStarPredicate.TRUE, atom)),
            new NotPredicate(atom),
            new NotPredicate(new OrPredicate(
                Arrays.asList(LiteralStarPredicate.FALSE, atom))));
        assertAll(predicates.stream().map(predicate -> () -> {
            when(evaluator.getSubcubePredicate()).thenReturn(predicate);
            assertNull(generator.generateSql(plan(path)),
                path + " must reject incomplete " + predicate.getClass().getSimpleName());
        }));
    }

    private CoordinateClassPlan plan(Path path) {
        if (path == Path.TEMPLATE) {
            return new CoordinateClassPlan("template", Collections.singletonList(
                new PhysicalValueRequest("[Measures].[Quantity]",
                    Collections.emptySet(), null,
                    PhysicalValueRequest.AggregationKind.NATIVE_EXPRESSION,
                    PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE,
                    "SELECT SUM(f.qty) FROM ${factTable} f WHERE ${whereClause}")));
        }
        PhysicalValueRequest plain = stored(Collections.emptySet());
        return new CoordinateClassPlan("stored", path == Path.INNER
            ? Arrays.asList(plain, stored(Collections.singleton(reset)))
            : Collections.singletonList(plain));
    }

    private PhysicalValueRequest stored(Set<Hierarchy> resets) {
        return new PhysicalValueRequest("[Measures].[Quantity]",
            Collections.emptySet(), resets,
            PhysicalValueRequest.AggregationKind.SUM,
            PhysicalValueRequest.ExpressionProviderKind.STORED_COLUMN, null);
    }
}
