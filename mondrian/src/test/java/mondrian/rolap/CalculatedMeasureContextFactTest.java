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

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #98: only a context measure that no stored measure backs is fact-less.
 * A coordinate-preserving calculation over stored measures still enumerates
 * native sets through the fact; shifted calculations keep fallback candidates.
 *
 * <p>Fact rows exist for (A, 2026) and (B, 2026) only; Product x Year is
 * nine tuples.
 */
public class CalculatedMeasureContextFactTest {
    private static final String CROSS_JOIN =
        "CrossJoin([Product].[Name].Members, [Calendar].[Year].Members)";
    private static final String NON_EMPTY_CROSS_JOIN = "NonEmpty" + CROSS_JOIN;
    private static final String TWICE =
        "WITH MEMBER [Measures].[Twice] AS [Measures].[Quantity] * 2 ";

    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private final List<String> statements = new ArrayList<>();
    private int previousPreCache;
    private String previousNativeQueryEngine;
    private boolean previousNativeSql;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:calc_context_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE calendar (id INT, year INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025),(2,2026),(3,2027)");
            sql.execute("CREATE TABLE product (id INT, name VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'A'),(2,'B'),(3,'C')");
            sql.execute("CREATE TABLE fact (calendar_id INT, product_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (2,1,10),(2,2,20)");
            // an independent fact: C is stocked but was never sold
            sql.execute("CREATE TABLE stock (calendar_id INT, product_id INT, qty INT)");
            sql.execute("INSERT INTO stock VALUES (3,3,300)");
        }
        MondrianProperties props = MondrianProperties.instance();
        previousPreCache = props.LevelPreCacheThreshold.get();
        props.LevelPreCacheThreshold.set(0);
        previousNativeQueryEngine =
            props.getProperty("mondrian.native.queryEngine.enable");
        props.setProperty("mondrian.native.queryEngine.enable", "false");
        previousNativeSql = props.NativeSqlEnable.get();
        props.NativeSqlEnable.set(true);
        Util.PropertyList connect =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        connect.put("JdbcUser", "sa");
        connect.put("JdbcDrivers", "org.h2.Driver");
        connect.put("Jdbc", jdbc);
        connect.put("CatalogContent", """
            <Schema name="CalculatedContext">
              <Dimension name="Calendar">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="calendar"/>
                  <Level name="Year" column="year" type="Integer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="fact"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                <CalculatedMember name="TwiceSchema" dimension="Measures">
                  <Formula>[Measures].[Quantity] * 2</Formula>
                </CalculatedMember>
                <CalculatedMember name="DenseSchema" dimension="Measures">
                  <Formula>Iif([Calendar].CurrentMember.Level.Ordinal = 1, 1, [Measures].[Quantity])</Formula>
                </CalculatedMember>
                <CalculatedMember name="SchemaPrev" dimension="Measures">
                  <Formula>([Measures].[Quantity], [Calendar].CurrentMember.PrevMember)</Formula>
                </CalculatedMember>
                <CalculatedMember name="SchemaPick" dimension="Measures">
                  <Formula>IIf([Calendar].CurrentMember.Level.Ordinal = 1, [Measures].[SchemaPrev], [Measures].[Quantity])</Formula>
                </CalculatedMember>
                <CalculatedMember name="SchemaCase" dimension="Measures">
                  <Formula>CASE WHEN [Calendar].CurrentMember.Level.Ordinal = 1 THEN [Measures].[SchemaPrev] ELSE [Measures].[Quantity] END</Formula>
                </CalculatedMember>
                <CalculatedMember name="SchemaItem" dimension="Measures">
                  <Formula>{[Measures].[SchemaPrev]}.Item(0)</Formula>
                </CalculatedMember>
                <CalculatedMember name="Stock" dimension="Measures">
                  <Annotations>
                    <Annotation name="nativeSql.enabled">true</Annotation>
                    <Annotation name="nativeSql.template">
                      SELECT ${axisExpr1} AS k0, ${axisExpr2} AS k1, SUM(f.qty) AS val
                      FROM (SELECT p.name AS name, c.year AS year, s.qty AS qty
                            FROM stock s
                            JOIN product p ON p.id = s.product_id
                            JOIN calendar c ON c.id = s.calendar_id) f
                      GROUP BY ${axisExpr1}, ${axisExpr2}
                    </Annotation>
                  </Annotations>
                  <Formula>NULL</Formula>
                </CalculatedMember>
                <CalculatedMember name="StockScalar" dimension="Measures">
                  <Annotations>
                    <Annotation name="nativeSql.enabled">true</Annotation>
                    <Annotation name="nativeSql.scalar">true</Annotation>
                    <Annotation name="nativeSql.template">SELECT SUM(qty) AS val FROM stock</Annotation>
                  </Annotations>
                  <Formula>NULL</Formula>
                </CalculatedMember>
                <CalculatedMember name="QuantityNative" dimension="Measures">
                  <Annotations>
                    <Annotation name="nativeSql.enabled">true</Annotation>
                    <Annotation name="nativeSql.template">SELECT SUM(qty) AS val FROM fact</Annotation>
                  </Annotations>
                  <Formula>[Measures].[Quantity]</Formula>
                </CalculatedMember>
              </Cube>
              <Cube name="StockCube"><Table name="stock"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="StockQuantity" column="qty" aggregator="sum"/>
              </Cube>
              <VirtualCube name="Combined">
                <VirtualCubeDimension name="Calendar"/>
                <VirtualCubeDimension name="Product"/>
                <VirtualCubeMeasure cubeName="Sales" name="[Measures].[Quantity]"/>
                <VirtualCubeMeasure cubeName="StockCube" name="[Measures].[StockQuantity]"/>
                <CalculatedMember name="Both" dimension="Measures">
                  <Formula>[Measures].[Quantity] + [Measures].[StockQuantity]</Formula>
                </CalculatedMember>
              </VirtualCube>
            </Schema>
            """);
        connection = mondrian.olap.DriverManager.getConnection(connect, null);
        RolapUtil.setHook(statements::add);
    }

    @AfterEach void close() throws Exception {
        RolapUtil.setHook(null);
        MondrianProperties props = MondrianProperties.instance();
        props.LevelPreCacheThreshold.set(previousPreCache);
        props.NativeSqlEnable.set(previousNativeSql);
        if (previousNativeQueryEngine == null) {
            props.remove("mondrian.native.queryEngine.enable");
        } else {
            props.setProperty(
                "mondrian.native.queryEngine.enable", previousNativeQueryEngine);
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } finally {
            if (database != null) {
                database.close();
            }
        }
    }

    @Test void nonEmptyCrossJoinUnderACalculationIsBoundedByTheFact() {
        assertEquals(
            List.of("A,2026", "B,2026"),
            columns(TWICE + "SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
                + " FROM [Sales] WHERE [Measures].[Twice]"));
        assertTrue(tupleSql().contains("\"fact\""), tupleSql());
    }

    @Test void topCountOverNonEmptyCrossJoinUnderACalculation() {
        assertEquals(
            List.of("A,2026", "B,2026"),
            columns(TWICE + "SELECT TopCount(" + NON_EMPTY_CROSS_JOIN
                + ", 50) ON 0 FROM [Sales] WHERE [Measures].[Twice]"));
    }

    @Test void nonEmptyAxisUnderACalculationEnumeratesThroughTheFact() {
        assertNonEmptyAxisEnumeratesThroughTheFact(
            "[Measures].[Quantity] * 2");
    }

    @Test void nonEmptyAxisUnderANestedCalculationEnumeratesThroughTheFact() {
        // reaches the stored measure through a schema calculation
        assertNonEmptyAxisEnumeratesThroughTheFact(
            "[Measures].[Quantity] / [Measures].[TwiceSchema]");
    }

    private void assertNonEmptyAxisEnumeratesThroughTheFact(String measure) {
        assertEquals(
            List.of("A,2026", "B,2026"),
            columns("WITH MEMBER [Measures].[M] AS " + measure
                + " SELECT NON EMPTY " + CROSS_JOIN + " ON 0"
                + " FROM [Sales] WHERE [Measures].[M]"));
        assertTrue(tupleSql().contains("\"fact\""), tupleSql());
        assertTrue(
            statements.stream().anyMatch(sql -> sql.contains("sum(")
                && sql.contains("\"calendar\".\"year\" = 2026")),
            statements.toString());
    }

    @Test void memberListUnderACalculationEnumeratesThroughTheFact() {
        Result result = connection.execute(connection.parseQuery(
            TWICE + "SELECT NON EMPTY [Product].[Name].Members ON 0"
            + " FROM [Sales] WHERE ([Measures].[Twice], [Calendar].[2026])"));
        assertEquals(2, result.getAxes()[0].getPositions().size());
        assertTrue(
            statements.stream().anyMatch(sql ->
                sql.contains("\"product\".\"name\"")
                && sql.contains("\"fact\"") && !sql.contains("sum(")),
            statements.toString());
    }

    @Test void constantMeasureHasNoFactToEnumerateThrough() {
        assertEquals(
            9,
            columns("WITH MEMBER [Measures].[One] AS 1 SELECT NON EMPTY "
                + CROSS_JOIN + " ON 0 FROM [Sales] WHERE [Measures].[One]")
                .size());
        assertFalse(tupleSql().contains("\"fact\""), tupleSql());
    }

    @Test void calculationOverAnIndependentFactStaysFactless() {
        // C has stock and no sales: the sales fact must not decide which
        // tuples a measure that also reads the stock fact can have.
        assertEquals(
            List.of("A,2026", "B,2026", "C,2027"),
            columns("WITH MEMBER [Measures].[M] AS"
                + " [Measures].[Quantity] + [Measures].[Stock]"
                + " SELECT NON EMPTY " + CROSS_JOIN + " ON 0"
                + " FROM [Sales] WHERE [Measures].[M]"));
        assertFalse(tupleSql().contains("\"fact\""), tupleSql());
    }

    @Test void nativeSqlAnnotationsAreInertWhenNativeSqlIsOff() {
        // the formula runs then, and it reads the cube's own fact
        MondrianProperties.instance().NativeSqlEnable.set(false);
        assertEquals(
            List.of("A,2026", "B,2026"),
            columns("SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
                + " FROM [Sales] WHERE [Measures].[QuantityNative]"));
        assertTrue(tupleSql().contains("\"fact\""), tupleSql());
    }

    @Test void virtualCubeCalculationIsBoundedByTheFactsOfBothBaseCubes() {
        // stored measures of two base cubes: no single carrier, still facts
        assertEquals(
            List.of("A,2026", "B,2026", "C,2027"),
            columns("SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
                + " FROM [Combined] WHERE [Measures].[Both]"));
    }

    @Test void formulaThatNamesNoMeasureReadsTheDefaultOne() {
        // Evaluated with the default measure current, each of these reads
        // the cube's fact without a stored measure in its text.
        final List<String> formulas = List.of(
            "Aggregate({[Calendar].CurrentMember})",
            "Sum({[Calendar].CurrentMember})",
            "Iif(Count({[Calendar].CurrentMember}, EXCLUDEEMPTY) = 0, NULL, 1)",
            "([Product].CurrentMember)",
            "StrToMember(\"[Measures].[Quantity]\") * 2",
            "[Measures].DefaultMember * 2",
            "Parameter(\"P\", [Measures], [Measures].[Quantity], \"d\") * 2");
        for (int i = 0; i < formulas.size(); i++) {
            // a name of its own: the tuple list is cached by measure name
            final String measure = "[Measures].[M" + i + "]";
            statements.clear();
            assertEquals(
                List.of("A,2026", "B,2026"),
                columns("WITH MEMBER " + measure + " AS " + formulas.get(i)
                    + " SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
                    + " FROM [Sales] WHERE " + measure),
                formulas.get(i));
            Result measureResult = connection.execute(connection.parseQuery(
                "WITH MEMBER " + measure + " AS " + formulas.get(i)
                    + " SELECT {" + measure + "} ON 0 FROM [Sales]"));
            assertFalse(SqlConstraintUtils.isFactlessMeasure(
                measureResult.getAxes()[0].getPositions().get(0).get(0)),
                formulas.get(i));
        }
    }

    @Test void calculationOverAConstantMeasureStaysFactless() {
        assertEquals(
            9,
            columns("WITH MEMBER [Measures].[One] AS 1"
                + " MEMBER [Measures].[Two] AS [Measures].[One] * 2"
                + " SELECT NON EMPTY " + CROSS_JOIN + " ON 0"
                + " FROM [Sales] WHERE [Measures].[Two]").size());
        assertFalse(tupleSql().contains("\"fact\""), tupleSql());
    }

    @Test void compoundSlicerOfConstantMeasuresStaysFactless() {
        // the placeholder of WHERE {M, N} reads what M and N read
        assertEquals(
            9,
            columns("WITH MEMBER [Measures].[M] AS 1 MEMBER [Measures].[N] AS 2"
                + " SELECT NON EMPTY " + CROSS_JOIN + " ON 0 FROM [Sales]"
                + " WHERE {[Measures].[M], [Measures].[N]}").size());
        assertFalse(tupleSql().contains("\"fact\""), tupleSql());
    }

    @Test void topCountByALiteralUnderACalculationTakesTheJavaPath() {
        // no stored measure to rank by, and SQL cannot be restricted to a
        // calculation: natively this was an internal error before #93
        Result result = connection.execute(connection.parseQuery(
            TWICE + "SELECT NON EMPTY TopCount([Product].[Name].Members, 2, 1)"
            + " ON 0 FROM [Sales]"
            + " WHERE ([Measures].[Twice], [Calendar].[2026])"));
        assertEquals(
            List.of("A", "B"),
            result.getAxes()[0].getPositions().stream()
                .map(position -> position.get(0).getName()).toList());
    }

    @Test void previousPeriodKeepsCoordinatesWithoutCurrentFacts() {
        assertShiftedAxis(
            "([Measures].[Quantity], [Calendar].CurrentMember.PrevMember)",
            List.of("A,2027=10.0", "B,2027=20.0"));
    }

    @Test void parallelPeriodKeepsCoordinatesWithoutCurrentFacts() {
        assertShiftedAxis(
            "([Measures].[Quantity],"
                + " ParallelPeriod([Calendar].[Year], 1, [Calendar].CurrentMember))",
            List.of("A,2027=10.0", "B,2027=20.0"));
    }

    @Test void literalResetKeepsCoordinatesWithoutCurrentFacts() {
        assertShiftedAxis(
            "([Measures].[Quantity], [Calendar].[2026])",
            List.of("A,2025=10.0", "A,2026=10.0", "A,2027=10.0",
                "B,2025=20.0", "B,2026=20.0", "B,2027=20.0"));
    }

    @Test void lagKeepsCoordinatesWithoutCurrentFacts() {
        assertShiftedAxis(
            "([Measures].[Quantity], [Calendar].CurrentMember.Lag(1))",
            List.of("A,2027=10.0", "B,2027=20.0"));
    }

    @Test void dynamicResetKeepsCoordinatesWithoutCurrentFacts() {
        assertShiftedAxis(
            "([Measures].[Quantity], StrToMember(\"[Calendar].[2026]\"))",
            List.of("A,2025=10.0", "A,2026=10.0", "A,2027=10.0",
                "B,2025=20.0", "B,2026=20.0", "B,2027=20.0"));
    }

    @Test void dynamicNameKeepsCandidateContextUntilCellEvaluation() {
        assertShiftedAxis(
            "StrToMember(Iif([Calendar].CurrentMember.Name = \"2027\","
                + " \"[Calendar].[2026]\", \"[Calendar].[2025]\"))",
            List.of("A,2027=10.0", "B,2027=20.0"));
    }

    @Test void unusedInvalidDynamicNameDoesNotFailCandidateAnalysis() {
        assertShiftedAxis(
            "Iif(1 = 1, [Measures].[Quantity], StrToMember(\"[Calendar].[missing]\"))",
            List.of("A,2026=10.0", "B,2026=20.0"));
    }

    @Test void nestedCalculationKeepsShiftedCoordinates() {
        assertShiftedAxis(
            "[Measures].[Inner] * 2",
            "MEMBER [Measures].[Inner] AS"
                + " ([Measures].[Quantity], [Calendar].CurrentMember.PrevMember) ",
            List.of("A,2027=20.0", "B,2027=40.0"));
    }

    @Test void nonEmptyCrossJoinFiltersFinalCellsForShiftedMeasures() {
        String mdx = "WITH MEMBER [Measures].[Prev] AS"
            + " ([Measures].[Quantity], [Calendar].CurrentMember.PrevMember)"
            + " SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
            + " FROM [Sales] WHERE [Measures].[Prev]";
        List<String> expected = List.of(
            "A,2027=10.0", "B,2027=20.0");
        assertEquals(expected, axisValues(mdx, 0, false));
        assertEquals(expected, axisValues(mdx, 0, true));
    }

    @Test void nativePreFilterCannotRemoveShiftedFallbackCandidates() throws Exception {
        List<String> expected = new ArrayList<>(List.of(
            "A,2027=10.0", "B,2027=20.0"));
        // 42 fact-backed products x three calendar years crosses the
        // pre-filter's 100-candidate threshold. Observed zero is non-empty.
        try (Statement sql = database.createStatement()) {
            for (int i = 0; i < 40; i++) {
                String name = String.format("D%03d", i);
                sql.execute("INSERT INTO product VALUES ("
                    + (i + 4) + ",'" + name + "')");
                sql.execute("INSERT INTO fact VALUES (2," + (i + 4) + ",0)");
                expected.add(name + ",2027=0.0");
            }
        }
        MondrianProperties props = MondrianProperties.instance();
        boolean previousFilter = props.NativeNonEmptyFilterEnable.get();
        try {
            String mdx = "WITH MEMBER [Measures].[Prev] AS"
                + " ([Measures].[Quantity], [Calendar].CurrentMember.PrevMember)"
                + " SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
                + " FROM [Sales] WHERE [Measures].[Prev]";
            props.NativeNonEmptyFilterEnable.set(false);
            assertEquals(expected, axisValues(mdx, 0, false));
            props.NativeNonEmptyFilterEnable.set(true);
            assertEquals(expected, axisValues(mdx, 0, true));
        } finally {
            props.NativeNonEmptyFilterEnable.set(previousFilter);
        }
    }

    @Test void computedNameNonEmptyCrossJoinFiltersItsFinalResult() {
        String mdx = "WITH MEMBER [Measures].[M] AS"
            + " StrToMember(Iif([Calendar].CurrentMember.Name = \"2027\","
            + " \"[Calendar].[2026]\", \"[Calendar].[2025]\"))"
            + " MEMBER [Measures].[Helper] AS [Measures].[Quantity]"
            + " SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
            + " FROM [Sales] WHERE [Measures].[M]";
        List<String> expected = List.of("A,2027=10.0", "B,2027=20.0");
        assertEquals(expected, axisValues(mdx, 0, false));
        assertEquals(expected, axisValues(mdx, 0, true));
    }

    @Test void otherQueryMeasuresCannotKeepEmptyFinalCrossings() {
        String mdx = "WITH MEMBER [Measures].[Dynamic] AS"
            + " StrToMember(Iif([Calendar].CurrentMember.Name = \"2027\","
            + " \"[Calendar].[2026]\", \"[Calendar].[2025]\"))"
            + " MEMBER [Measures].[CrossingCount] AS Count(" + NON_EMPTY_CROSS_JOIN + ")"
            + " SELECT {[Measures].[Quantity], [Measures].[Dynamic],"
            + " [Measures].[CrossingCount]} ON 0 FROM [Sales]";
        for (boolean nativeEnabled : new boolean[] {false, true}) {
            RolapNativeRegistry registry = ((RolapSchema) connection.getSchema()).getNativeRegistry();
            boolean previous = registry.isEnabled();
            try {
                registry.setEnabled(nativeEnabled);
                registry.flushAllNativeSetCache();
                Result result = connection.execute(connection.parseQuery(mdx));
                assertEquals(30d, ((Number) result.getCell(new int[] {0}).getValue()).doubleValue());
                assertTrue(result.getCell(new int[] {1}).isNull());
                assertEquals(2d, ((Number) result.getCell(new int[] {2}).getValue()).doubleValue());
            } finally {
                registry.setEnabled(previous);
            }
        }
    }

    @Test void metadataBranchCanBeNonEmptyWithoutStoredFacts() {
        assertShiftedAxis(
            "Iif([Calendar].CurrentMember.Level.Ordinal = 1, 1, [Measures].[Quantity])",
            List.of("A,2025=1.0", "A,2026=1.0", "A,2027=1.0",
                "B,2025=1.0", "B,2026=1.0", "B,2027=1.0",
                "C,2025=1.0", "C,2026=1.0", "C,2027=1.0"));
    }

    @Test void zeroCountIsNonEmptyEvenWithoutStoredFacts() {
        assertShiftedAxis(
            "Count({[Calendar].CurrentMember}, EXCLUDEEMPTY)",
            List.of("A,2025=0.0", "A,2026=1.0", "A,2027=0.0",
                "B,2025=0.0", "B,2026=1.0", "B,2027=0.0",
                "C,2025=0.0", "C,2026=0.0", "C,2027=0.0"));
    }

    @Test void nestedMetadataBranchKeepsNonFactCoordinates() {
        assertShiftedAxis("[Measures].[Inner] * 2",
            "MEMBER [Measures].[Inner] AS"
                + " Iif([Calendar].CurrentMember.Level.Ordinal = 1, 1, [Measures].[Quantity]) ",
            List.of("A,2025=2.0", "A,2026=2.0", "A,2027=2.0",
                "B,2025=2.0", "B,2026=2.0", "B,2027=2.0",
                "C,2025=2.0", "C,2026=2.0", "C,2027=2.0"));
    }

    @Test void schemaMeasureSelectedThroughANamedSetKeepsNonFactCoordinates() {
        String prefix = "WITH SET [Displayed] AS {[Measures].[DenseSchema]} ";
        List<String> expected = List.of(
            "A,2025=1.0", "A,2026=1.0", "A,2027=1.0",
            "B,2025=1.0", "B,2026=1.0", "B,2027=1.0",
            "C,2025=1.0", "C,2026=1.0", "C,2027=1.0");
        for (boolean nativeEnabled : new boolean[] {false, true}) {
            assertEquals(expected, axisValues(prefix
                + "SELECT [Displayed] ON 0, NON EMPTY " + CROSS_JOIN
                + " ON 1 FROM [Sales]", 1, nativeEnabled));
            assertEquals(expected, axisValues(prefix
                + "SELECT NON EMPTY " + CROSS_JOIN
                + " ON 0 FROM [Sales] WHERE [Displayed]", 0, nativeEnabled));
        }
    }

    @Test void nonAxisSubselectCannotPruneAnAllPinnedMeasure() {
        String from = " FROM (SELECT {[Calendar].[2027]} ON 0 FROM [Sales])";
        String prefix = "WITH MEMBER [Measures].[M] AS"
            + " ([Measures].[Quantity], [Calendar].[All Calendars]) ";
        Result scalar = connection.execute(connection.parseQuery(prefix
            + "SELECT {[Measures].[M]} ON 0" + from));
        assertEquals(30d, ((Number) scalar.getCell(new int[] {0}).getValue()).doubleValue());
        RolapNativeRegistry registry = ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previous = registry.isEnabled();
        try {
            for (boolean nativeEnabled : new boolean[] {false, true}) {
                registry.setEnabled(nativeEnabled);
                registry.flushAllNativeSetCache();
                Result result = connection.execute(connection.parseQuery(prefix
                    + "SELECT NON EMPTY CrossJoin([Product].[Name].Members,"
                    + " {[Measures].[M]}) ON 0" + from));
                assertEquals(List.of("A", "B"), result.getAxes()[0].getPositions().stream()
                    .map(position -> position.get(0).getName()).toList());
                assertEquals(10d, ((Number) result.getCell(new int[] {0}).getValue()).doubleValue());
                assertEquals(20d, ((Number) result.getCell(new int[] {1}).getValue()).doubleValue());
            }
        } finally {
            registry.setEnabled(previous);
        }
    }

    @Test void independentFactInEitherNonEmptyCrossJoinOperandIsDetected() {
        for (String set : List.of(
            "NonEmptyCrossJoin([Product].[Name].Members, {[Measures].[StockScalar]})",
            "NonEmptyCrossJoin({[Measures].[StockScalar]}, [Product].[Name].Members)"))
        {
            Result result = connection.execute(connection.parseQuery(
                "WITH MEMBER [Measures].[M] AS Count(" + set + ")"
                    + " SELECT {[Measures].[M]} ON 0 FROM [Sales]"));
            assertTrue(SqlConstraintUtils.isFactlessMeasure(
                result.getAxes()[0].getPositions().get(0).get(0)), set);
            assertEquals(3d, ((Number) result.getCell(new int[] {0}).getValue()).doubleValue(), set);
        }
    }

    @Test void independentFactInANamedNonEmptyCrossJoinOperandIsDetected() {
        for (String set : List.of(
            "NonEmptyCrossJoin([Product].[Name].Members, [StockSet])",
            "NonEmptyCrossJoin([StockSet], [Product].[Name].Members)"))
        {
            Result result = connection.execute(connection.parseQuery(
                "WITH SET [StockSet] AS {[Measures].[StockScalar]}"
                    + " MEMBER [Measures].[M] AS Count(" + set + ")"
                    + " SELECT {[Measures].[M]} ON 0 FROM [Sales]"));
            assertTrue(SqlConstraintUtils.isFactlessMeasure(
                result.getAxes()[0].getPositions().get(0).get(0)), set);
            assertEquals(3d, ((Number) result.getCell(new int[] {0}).getValue()).doubleValue(), set);
        }
    }

    @Test void metadataUsedOnlyToFilterOrOrderDoesNotBecomeAnOutputMeasure() {
        for (String set : List.of(
            "Filter([Product].[Name].Members, [Measures].[__XLRelated] = 1)",
            "Order([Product].[Name].Members, [Measures].[__XLRelated], BASC)"))
        {
            mondrian.olap.Query query = connection.parseQuery(
                "WITH MEMBER [Measures].[__XLRelated] AS"
                    + " Iif([Product].CurrentMember.Level.Ordinal = 1, 1, 0)"
                    + " SELECT {[Measures].[Quantity]} ON 0, NON EMPTY " + set
                    + " ON 1 FROM [Sales]");
            Result result = connection.execute(query);
            assertEquals(List.of("A", "B"), result.getAxes()[1].getPositions().stream()
                .map(position -> position.get(0).getName()).toList());
            assertEquals(10d, ((Number) result.getCell(new int[] {0, 0}).getValue()).doubleValue());
            assertEquals(20d, ((Number) result.getCell(new int[] {0, 1}).getValue()).doubleValue());
            RolapEvaluator evaluator = new RolapEvaluator(new RolapEvaluatorRoot(query.getStatement()));
            assertFalse(SqlConstraintUtils.hasUnboundedNonEmptyMeasure(evaluator), set);
            Member quantity = evaluator.getMembers()[0];
            Result products = connection.execute(connection.parseQuery(
                "SELECT [Product].[Name].Members ON 0 FROM [Sales]"));
            mondrian.calc.TupleList candidates = mondrian.calc.TupleCollections.createList(1);
            for (int i = 0; i < 40; i++) {
                candidates.addAll(products.getAxes()[0].getPositions());
            }
            boolean previous = MondrianProperties.instance().NativeNonEmptyFilterEnable.get();
            try {
                MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
                mondrian.calc.TupleList pruned = mondrian.server.Locus.execute(
                    new mondrian.server.Execution(query.getStatement(), 0), "metadata predicate prune",
                    () -> NativeNonEmptyFilter.tryPrune(evaluator, candidates, java.util.Set.of(quantity)));
                org.junit.jupiter.api.Assertions.assertNotNull(pruned, set);
                assertEquals(80, pruned.size(), set);
                assertTrue(pruned.stream().allMatch(tuple ->
                    tuple.get(0).getName().equals("A") || tuple.get(0).getName().equals("B")), set);
            } finally {
                MondrianProperties.instance().NativeNonEmptyFilterEnable.set(previous);
            }
        }
    }

    @Test void additionCanProduceNonEmptyValuesWithoutFacts() {
        assertShiftedAxis("[Measures].[Quantity] + 1",
            List.of("A,2025=1.0", "A,2026=11.0", "A,2027=1.0",
                "B,2025=1.0", "B,2026=21.0", "B,2027=1.0",
                "C,2025=1.0", "C,2026=1.0", "C,2027=1.0"));
    }

    @Test void coalesceCanProduceNonEmptyValuesWithoutFacts() {
        assertShiftedAxis("CoalesceEmpty([Measures].[Quantity], 1)",
            List.of("A,2025=1.0", "A,2026=10.0", "A,2027=1.0",
                "B,2025=1.0", "B,2026=20.0", "B,2027=1.0",
                "C,2025=1.0", "C,2026=1.0", "C,2027=1.0"));
    }

    @Test void nullDenominatorDoesNotBoundDivisionSupport() {
        boolean previous = MondrianProperties.instance().NullDenominatorProducesNull.get();
        try {
            MondrianProperties.instance().NullDenominatorProducesNull.set(false);
            assertShiftedAxis("2 / [Measures].[Quantity]",
                List.of("A,2025=Infinity", "A,2026=0.2", "A,2027=Infinity",
                    "B,2025=Infinity", "B,2026=0.1", "B,2027=Infinity",
                    "C,2025=Infinity", "C,2026=Infinity", "C,2027=Infinity"));
        } finally {
            MondrianProperties.instance().NullDenominatorProducesNull.set(previous);
        }
    }

    @Test void nullNumeratorStillBoundsDivisionSupport() {
        assertEquals(List.of("A,2026=5.0", "B,2026=10.0"), axisValues(
            "WITH MEMBER [Measures].[M] AS [Measures].[Quantity] / 2"
                + " SELECT NON EMPTY " + CROSS_JOIN
                + " ON 0 FROM [Sales] WHERE [Measures].[M]", 0, true));
        assertTrue(tupleSql().contains("\"fact\""), tupleSql());
    }

    @Test void metadataFormulasRemainFactless() {
        for (String formula : List.of(
            "Count(Descendants([Calendar].[All Calendars], [Calendar].[Year]))",
            "Iif([Calendar].CurrentMember.Level.Ordinal = 1, 1, NULL)",
            "Iif([Product].CurrentMember.Properties(\"MEMBER_CAPTION\") = \"C\", 1, 1)"))
        {
            String prefix = "WITH MEMBER [Measures].[Metadata] AS " + formula;
            Result result = connection.execute(connection.parseQuery(prefix
                + " SELECT {[Measures].[Metadata]} ON 0 FROM [Sales]"));
            assertTrue(SqlConstraintUtils.isFactlessMeasure(
                result.getAxes()[0].getPositions().get(0).get(0)), formula);
            statements.clear();
            ((RolapSchema) connection.getSchema()).getNativeRegistry().flushAllNativeSetCache();
            assertEquals(9, columns(prefix + " SELECT NON EMPTY " + CROSS_JOIN
                + " ON 0 FROM [Sales] WHERE [Measures].[Metadata]").size(), formula);
            assertFalse(statements.stream().anyMatch(sql -> sql.contains("\"fact\"")),
                statements.toString());
        }
    }

    @Test void metadataInStoredFormulasKeepsNativeEnumeration() {
        for (String formula : List.of(
            "[Measures].[Quantity] * Iif([Calendar].CurrentMember.Level.Ordinal = 1, 1, 1)",
            "[Measures].[Quantity] * Iif([Product].CurrentMember.Properties(\"MEMBER_CAPTION\") = \"C\", 1, 1)"))
        {
            statements.clear();
            ((RolapSchema) connection.getSchema()).getNativeRegistry().flushAllNativeSetCache();
            assertEquals(List.of("A,2026=10.0", "B,2026=20.0"),
                axisValues("WITH MEMBER [Measures].[M] AS " + formula
                    + " SELECT NON EMPTY " + CROSS_JOIN
                    + " ON 0 FROM [Sales] WHERE [Measures].[M]", 0, true));
            assertTrue(tupleSql().contains("\"fact\""), tupleSql());
        }
    }

    @Test void excelMetadataHelpersDoNotDisableNativeStoredEnumeration() {
        assertEquals(List.of("A,2026=10.0", "B,2026=20.0"), axisValues(
            "WITH MEMBER [Measures].[__XLRelated] AS"
                + " Iif([Product].CurrentMember.Level.Ordinal = 1, 1, 1)"
                + " MEMBER [Measures].[__XLPath] AS"
                + " [Product].CurrentMember.Properties(\"MEMBER_CAPTION\")"
                + " MEMBER [Measures].[M] AS [Measures].[Quantity]"
                + " * Iif([Measures].[__XLPath] = \"C\", [Measures].[__XLRelated], 1)"
                + " SELECT NON EMPTY " + CROSS_JOIN
                + " ON 0 FROM [Sales] WHERE [Measures].[M]", 0, true));
        assertTrue(tupleSql().contains("\"fact\""), tupleSql());
    }

    @Test void dynamicTupleKeepsCandidateContextUntilCellEvaluation() {
        assertShiftedAxis(
            "StrToTuple(Iif([Calendar].CurrentMember.Name = \"2027\","
                + " \"[Calendar].[2026]\", \"[Calendar].[2025]\"), [Calendar])",
            List.of("A,2027=10.0", "B,2027=20.0"));
    }

    @Test void dynamicSetKeepsCandidateContextUntilCellEvaluation() {
        assertShiftedAxis(
            "Sum(StrToSet(Iif([Calendar].CurrentMember.Name = \"2027\","
                + " \"{[Calendar].[2026]}\", \"{[Calendar].[2025]}\"), [Calendar]),"
                + " [Measures].[Quantity])",
            List.of("A,2027=10.0", "B,2027=20.0"));
    }

    @Test void nativePrunerIgnoresAnUnconstrainedNonCandidateHierarchy() {
        mondrian.olap.Query query = connection.parseQuery(
            "WITH MEMBER [Measures].[M] AS"
                + " ([Measures].[Quantity], [Calendar].[All Calendars])"
                + " SELECT {[Measures].[M]} ON 0 FROM [Sales]");
        RolapEvaluator evaluator = new RolapEvaluator(new RolapEvaluatorRoot(query.getStatement()));
        Member measure = query.getMeasuresMembers().stream()
            .filter(m -> m.getName().equals("M")).findFirst().orElseThrow();
        Member quantity = query.getMeasuresMembers().stream()
            .filter(m -> m.getName().equals("Quantity")).findFirst().orElseThrow();
        evaluator.setContext(measure);
        Result products = connection.execute(connection.parseQuery(
            "SELECT [Product].[Name].Members ON 0 FROM [Sales]"));
        mondrian.calc.TupleList candidates = mondrian.calc.TupleCollections.createList(1);
        for (int i = 0; i < 40; i++) {
            candidates.addAll(products.getAxes()[0].getPositions());
        }
        boolean previous = MondrianProperties.instance().NativeNonEmptyFilterEnable.get();
        try {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
            mondrian.calc.TupleList pruned = mondrian.server.Locus.execute(
                new mondrian.server.Execution(query.getStatement(), 0), "native prune regression",
                () -> NativeNonEmptyFilter.tryPrune(evaluator, candidates, java.util.Set.of(quantity)));
            org.junit.jupiter.api.Assertions.assertNotNull(pruned,
                "An All pin outside Product must retain the native pruner");
            assertEquals(80, pruned.size());
            assertTrue(pruned.stream().allMatch(tuple ->
                tuple.get(0).getName().equals("A") || tuple.get(0).getName().equals("B")));
            evaluator.setContext(connection.execute(connection.parseQuery(
                "SELECT {[Calendar].[2027]} ON 0 FROM [Sales]"))
                .getAxes()[0].getPositions().get(0).get(0));
            assertNull(NativeNonEmptyFilter.tryPrune(
                evaluator, candidates, java.util.Set.of(quantity)),
                "Resetting a constrained non-axis year must reject fact pruning");
        } finally {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(previous);
        }
    }

    @Test void contextShiftAnalysisReusesFormulaWalksButRechecksHierarchies() {
        mondrian.olap.Query query = connection.parseQuery(
            "WITH MEMBER [Measures].[M] AS"
                + " ([Measures].[Quantity], [Calendar].CurrentMember.PrevMember)"
                + " SELECT {[Measures].[M]} ON 0 FROM [Sales]");
        RolapCalculatedMember measure = (RolapCalculatedMember) query.getMeasuresMembers().stream()
            .filter(m -> m.getName().equals("M")).findFirst().orElseThrow();
        CountingExp expression = new CountingExp(measure.getExpression());
        measure.getFormula().setExpression(expression);
        RolapEvaluator evaluator = new RolapEvaluator(new RolapEvaluatorRoot(query.getStatement()));
        evaluator.setContext(measure);
        mondrian.olap.Level product = java.util.Arrays.stream(query.getCube().getDimensions())
            .filter(d -> d.getName().equals("Product")).findFirst().orElseThrow()
            .getHierarchy().getLevels()[1];
        mondrian.olap.Level calendar = java.util.Arrays.stream(query.getCube().getDimensions())
            .filter(d -> d.getName().equals("Calendar")).findFirst().orElseThrow()
            .getHierarchy().getLevels()[1];
        for (int i = 0; i < 4; i++) {
            assertFalse(SqlConstraintUtils.measuresMayShiftContext(evaluator, new mondrian.olap.Level[] {product}));
            assertTrue(SqlConstraintUtils.measuresMayShiftContext(evaluator, new mondrian.olap.Level[] {calendar}));
        }
        assertEquals(1, expression.visits, "Repeated eligibility checks must reuse the formula analysis");
    }

    private static final class CountingExp extends mondrian.olap.ExpBase {
        private final mondrian.olap.Exp delegate;
        private int visits;
        CountingExp(mondrian.olap.Exp delegate) { this.delegate = delegate; }
        @Override public int getCategory() { return delegate.getCategory(); }
        @Override public mondrian.olap.type.Type getType() { return delegate.getType(); }
        @Override public void unparse(java.io.PrintWriter writer) { delegate.unparse(writer); }
        @Override public CountingExp clone() { return new CountingExp(delegate.clone()); }
        @Override public mondrian.olap.Exp accept(mondrian.olap.Validator validator) {
            return delegate.accept(validator);
        }
        @Override public mondrian.calc.Calc accept(mondrian.calc.ExpCompiler compiler) {
            return delegate.accept(compiler);
        }
        @Override public Object accept(mondrian.mdx.MdxVisitor visitor) {
            visits++;
            return delegate.accept(visitor);
        }
    }

    @Test void shiftedMeasureOnColumnsKeepsAStandaloneLevelAndChildren() {
        for (String set : List.of(
            "[Calendar].[Year].Members", "[Calendar].[All Calendars].Children"))
        {
            Result result = connection.execute(connection.parseQuery(
                "WITH MEMBER [Measures].[Prev] AS"
                    + " ([Measures].[Quantity], [Calendar].CurrentMember.PrevMember)"
                    + " SELECT {[Measures].[Prev]} ON 0, NON EMPTY "
                    + set + " ON 1 FROM [Sales]"));
            assertEquals(List.of("[Calendar].[2027]"),
                result.getAxes()[1].getPositions().stream()
                    .map(position -> position.get(0).getUniqueName()).toList());
            assertEquals(30d,
                ((Number) result.getCell(new int[] {0, 0}).getValue()).doubleValue());
        }
    }

    @Test void shiftedFallbackStillEnforcesTheCandidateResultLimit() {
        int previous = MondrianProperties.instance().ResultLimit.get();
        try {
            MondrianProperties.instance().ResultLimit.set(4);
            String mdx = "WITH MEMBER [Measures].[Prev] AS"
                + " ([Measures].[Quantity], [Calendar].CurrentMember.PrevMember)"
                + " SELECT NON EMPTY " + CROSS_JOIN + " ON 0"
                + " FROM [Sales] WHERE [Measures].[Prev]";
            for (boolean nativeEnabled : new boolean[] {false, true}) {
                RuntimeException error = assertThrows(
                    RuntimeException.class,
                    () -> axisValues(mdx, 0, nativeEnabled));
                Throwable cause = error;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                assertTrue(cause instanceof mondrian.olap.ResourceLimitExceededException,
                    error.toString());
            }
        } finally {
            MondrianProperties.instance().ResultLimit.set(previous);
        }
    }

    // A member or tuple in value position is a cell read at its coordinate.

    @Test void tupleBranchOfIifCarriesItsShiftedCoordinate() {
        assertShiftedAxis("IIf([Calendar].CurrentMember.Level.Ordinal = 1,"
            + " ([Measures].[Quantity], [Calendar].CurrentMember.PrevMember),"
            + " [Measures].[Quantity])",
            List.of("A,2027=10.0", "B,2027=20.0"));
    }

    @Test void measureChosenByAMemberExpressionCarriesItsShift() {
        // SchemaPrev is named only by schema formulas, never by the query
        for (String measure : List.of("SchemaPick", "SchemaCase", "SchemaItem")) {
            assertShiftedAxis("[Measures].[" + measure + "]",
                List.of("A,2027=10.0", "B,2027=20.0"));
        }
    }

    // A calculated coordinate is evaluated by its own formula.

    @Test void denseCalculatedCoordinateIsNotBoundedByTheFact() {
        List<String> dense = List.of(
            "A,2025=1.0", "A,2026=1.0", "A,2027=1.0",
            "B,2025=1.0", "B,2026=1.0", "B,2027=1.0",
            "C,2025=1.0", "C,2026=1.0", "C,2027=1.0");
        for (String formula : List.of(
            "([Measures].[Quantity], [Calendar].[One])",
            "([Calendar].[One])",
            "Sum({[Calendar].[One]}, [Measures].[Quantity])"))
        {
            assertShiftedAxis(formula, "MEMBER [Calendar].[One] AS 1 ", dense);
        }
    }

    @Test void calculatedCoordinateOverStoredCellsKeepsItsSupport() {
        assertShiftedAxis("([Measures].[Quantity], [Calendar].[Early])",
            "MEMBER [Calendar].[Early] AS [Calendar].[2025] + [Calendar].[2026] ",
            List.of("A,2025=10.0", "A,2026=10.0", "A,2027=10.0",
                "B,2025=20.0", "B,2026=20.0", "B,2027=20.0"));
    }

    @Test void shareOfTotalKeepsNativeEnumeration() {
        // The denominator reads every product, but only the numerator can
        // make the share non-empty: the fact at the candidate bounds it.
        String prefix = "WITH MEMBER [Measures].[Share] AS [Measures].[Quantity]"
            + " / ([Measures].[Quantity], [Product].[All Products]) SELECT ";
        List<String> expected = List.of("A,2026=" + 10d / 30, "B,2026=" + 20d / 30);
        for (boolean onColumns : new boolean[] {false, true}) {
            String mdx = prefix + (onColumns
                ? "{[Measures].[Share]} ON 0, NON EMPTY " + CROSS_JOIN + " ON 1 FROM [Sales]"
                : "NON EMPTY " + CROSS_JOIN + " ON 0 FROM [Sales] WHERE [Measures].[Share]");
            int axis = onColumns ? 1 : 0;
            assertEquals(expected, axisValues(mdx, axis, false), mdx);
            statements.clear();
            assertEquals(expected, axisValues(mdx, axis, true), mdx);
            assertTrue(tupleSql().contains("\"fact\""), tupleSql());
        }
    }

    // The interpreter's pruning resets whatever the formulas may shift.

    private static final List<String> EVERY_YEAR_OF_SOLD_PRODUCTS = List.of(
        "A,2025=10.0", "A,2026=10.0", "A,2027=10.0",
        "B,2025=20.0", "B,2026=20.0", "B,2027=20.0");

    @Test void unlistedNavigationKeepsShiftedCoordinates() {
        for (String formula : List.of(
            "Sum([Calendar].CurrentMember.Siblings, [Measures].[Quantity])",
            "Sum([Calendar].[Year].Members, [Measures].[Quantity])",
            "([Measures].[Quantity], [Calendar].CurrentMember.Siblings.Item(1))",
            "Sum([Calendar].[2025]:[Calendar].[2027], [Measures].[Quantity])",
            "Sum(Descendants([Calendar].[All Calendars], [Calendar].[Year]),"
                + " [Measures].[Quantity])",
            "Sum(Head([Calendar].[Year].Members, 2), [Measures].[Quantity])"))
        {
            assertShiftedAxis(formula, EVERY_YEAR_OF_SOLD_PRODUCTS);
        }
    }

    @Test void nonEmptyCrossJoinKeepsCandidatesOfUnlistedNavigation() {
        String mdx = "WITH MEMBER [Measures].[S] AS"
            + " Sum([Calendar].CurrentMember.Siblings, [Measures].[Quantity])"
            + " SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
            + " FROM [Sales] WHERE [Measures].[S]";
        for (boolean nativeEnabled : new boolean[] {false, true}) {
            assertEquals(EVERY_YEAR_OF_SOLD_PRODUCTS, axisValues(mdx, 0, nativeEnabled));
        }
    }

    @Test void factAnalysisReusesTheFormulaWalk() {
        mondrian.olap.Query query = connection.parseQuery(
            "WITH MEMBER [Measures].[M] AS [Measures].[Quantity] * 2"
                + " SELECT {[Measures].[M]} ON 0 FROM [Sales]");
        RolapCalculatedMember measure = (RolapCalculatedMember) query.getMeasuresMembers().stream()
            .filter(m -> m.getName().equals("M")).findFirst().orElseThrow();
        CountingExp expression = new CountingExp(measure.getExpression());
        measure.getFormula().setExpression(expression);
        RolapEvaluator evaluator = new RolapEvaluator(new RolapEvaluatorRoot(query.getStatement()));
        evaluator.setContext(measure);
        for (int i = 0; i < 4; i++) {
            assertFalse(SqlConstraintUtils.isFactlessContext(evaluator));
            assertFalse(SqlConstraintUtils.hasUnboundedNonEmptyMeasure(evaluator));
        }
        assertEquals(1, expression.visits, "Every enumeration must reuse the formula summary");
    }

    private void assertShiftedAxis(String formula, List<String> expected) {
        assertShiftedAxis(formula, "", expected);
    }

    private void assertShiftedAxis(
        String formula, String nested, List<String> expected)
    {
        String prefix = "WITH " + nested + " MEMBER [Measures].[Shifted] AS "
            + formula + " SELECT ";
        org.junit.jupiter.api.Assertions.assertAll(
            java.util.stream.Stream.of(false, true).map(onColumns -> () -> {
                String mdx = prefix + (onColumns
                    ? "{[Measures].[Shifted]} ON 0, NON EMPTY " + CROSS_JOIN
                        + " ON 1 FROM [Sales]"
                    : "NON EMPTY " + CROSS_JOIN
                        + " ON 0 FROM [Sales] WHERE [Measures].[Shifted]");
                int axis = onColumns ? 1 : 0;
                assertEquals(expected, axisValues(mdx, axis, false), mdx);
                statements.clear();
                assertEquals(expected, axisValues(mdx, axis, true), mdx);
                assertFalse(statements.stream().anyMatch(sql ->
                    sql.contains("\"fact\"") && sql.contains("\"product\".\"name\" as")
                        && sql.contains("\"calendar\".\"year\" as") && !sql.contains("sum(")),
                    statements.toString());
            }));
    }

    private List<String> axisValues(String mdx, int axis, boolean nativeEnabled) {
        Result result = execute(mdx, nativeEnabled);
        List<String> values = new ArrayList<>();
        for (int i = 0; i < result.getAxes()[axis].getPositions().size(); i++) {
            var position = result.getAxes()[axis].getPositions().get(i);
            var cell = result.getCell(axis == 0 ? new int[] {i} : new int[] {0, i});
            values.add(position.get(0).getName() + "," + position.get(1).getName()
                + "=" + cellValue(cell));
        }
        return values;
    }

    private static String cellValue(mondrian.olap.Cell cell) {
        return cell.isNull() ? "NULL"
            : String.valueOf(((Number) cell.getValue()).doubleValue());
    }

    private Result execute(String mdx, boolean nativeEnabled) {
        MondrianProperties props = MondrianProperties.instance();
        boolean previous = props.EnableNativeNonEmpty.get();
        RolapNativeRegistry registry =
            ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previousRegistry = registry.isEnabled();
        try {
            props.EnableNativeNonEmpty.set(nativeEnabled);
            registry.setEnabled(nativeEnabled);
            registry.flushAllNativeSetCache();
            return connection.execute(connection.parseQuery(mdx));
        } finally {
            props.EnableNativeNonEmpty.set(previous);
            SqlConstraintFactory.setNativeNonEmptyValue();
            registry.setEnabled(previousRegistry);
        }
    }

    @Test void contextShiftingCalculationIsNotConstrainedByTheCurrentSlicer() {
        // A carrier resolved for the calculation would push the current
        // Calendar slicer into the member SQL and drop A and B, which have
        // rows in 2026 only. Nothing else protects this shape: Lag is not
        // among the functions MemberExtractingVisitor resets to All.
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Prev] AS"
            + " ([Measures].[Quantity], [Calendar].CurrentMember.Lag(1))"
            + " SELECT NON EMPTY [Product].[Name].Members ON 0"
            + " FROM [Sales] WHERE ([Measures].[Prev], [Calendar].[2027])"));
        assertEquals(
            List.of("A", "B"),
            result.getAxes()[0].getPositions().stream()
                .map(position -> position.get(0).getName()).toList());
    }

    @Test void sameNamedMeasuresDoNotShareAnEnumeration() {
        // Members are equal by unique name, so the cached tuple list of one
        // [Measures].[M] must not answer for another one over a different fact.
        final String query = " SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
            + " FROM [Sales] WHERE [Measures].[M]";
        final String constant = "WITH MEMBER [Measures].[M] AS 1";
        final String stored =
            "WITH MEMBER [Measures].[M] AS [Measures].[Quantity] * 2";

        assertEquals(9, columns(constant + query).size());
        assertEquals(List.of("A,2026", "B,2026"), columns(stored + query));
        assertEquals(9, columns(constant + query).size());
    }

    @Test void onlyAMeasureWithoutAStoredMeasureBehindItIsFactless() {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Twice] AS [Measures].[Quantity] * 2"
            + " MEMBER [Measures].[Ratio] AS"
            + "  [Measures].[Quantity] / [Measures].[TwiceSchema]"
            + " MEMBER [Measures].[One] AS 1"
            + " MEMBER [Measures].[Mixed] AS"
            + "  [Measures].[Quantity] + [Measures].[Stock]"
            + " SELECT {[Measures].[Quantity], [Measures].[Twice],"
            + " [Measures].[TwiceSchema], [Measures].[Ratio],"
            + " [Measures].[One], [Measures].[Stock], [Measures].[Mixed]}"
            + " ON 0 FROM [Sales]"));
        List<String> factless = new ArrayList<>();
        for (var position : result.getAxes()[0].getPositions()) {
            Member measure = position.get(0);
            if (SqlConstraintUtils.isFactlessMeasure(measure)) {
                factless.add(measure.getName());
            }
            if (measure instanceof RolapCalculatedMember) {
                // deliberate: a carrier would bring the slicer into the SQL
                // of context-shifting formulas, see the Lag test above
                assertNull(
                    SqlConstraintUtils.resolveStoredMeasureCarrier(measure),
                    measure.getName());
            }
        }
        assertEquals(List.of("One", "Stock", "Mixed"), factless);
    }

    private List<String> columns(String mdx) {
        Result result = connection.execute(connection.parseQuery(mdx));
        return result.getAxes()[0].getPositions().stream()
            .map(position -> position.get(0).getName()
                + "," + position.get(1).getName())
            .toList();
    }

    /** The statement that enumerated both axis levels together. */
    private String tupleSql() {
        return statements.stream()
            .filter(sql -> sql.contains("\"product\".\"name\"")
                && sql.contains("\"calendar\".\"year\"")
                && !sql.contains("sum("))
            .findFirst()
            .orElseThrow(() -> new AssertionError(statements.toString()));
    }
}
