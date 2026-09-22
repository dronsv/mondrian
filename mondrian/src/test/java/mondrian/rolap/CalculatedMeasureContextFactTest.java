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

    @Test void nonEmptyCrossJoinUsesItsInterpreterSemanticsForShiftedMeasures() {
        // Unlike an axis NON EMPTY, this function's interpreter probes the
        // stored measures reached through the calculation with time reset.
        // Keep its six tuples, including the four empty calculated cells.
        String mdx = "WITH MEMBER [Measures].[Prev] AS"
            + " ([Measures].[Quantity], [Calendar].CurrentMember.PrevMember)"
            + " SELECT " + NON_EMPTY_CROSS_JOIN + " ON 0"
            + " FROM [Sales] WHERE [Measures].[Prev]";
        List<String> expected = List.of(
            "A,2025=NULL", "A,2026=NULL", "A,2027=10.0",
            "B,2025=NULL", "B,2026=NULL", "B,2027=20.0");
        assertEquals(expected, axisValues(mdx, 0, false));
        assertEquals(expected, axisValues(mdx, 0, true));
    }

    @Test void nativePreFilterCannotRemoveShiftedFallbackCandidates() throws Exception {
        List<String> expected = new ArrayList<>(List.of(
            "A,2025=NULL", "A,2026=NULL", "A,2027=10.0",
            "B,2025=NULL", "B,2026=NULL", "B,2027=20.0"));
        // 42 fact-backed products x three calendar years crosses the
        // pre-filter's 100-candidate threshold. Observed zero is non-empty.
        try (Statement sql = database.createStatement()) {
            for (int i = 0; i < 40; i++) {
                String name = String.format("D%03d", i);
                sql.execute("INSERT INTO product VALUES ("
                    + (i + 4) + ",'" + name + "')");
                sql.execute("INSERT INTO fact VALUES (2," + (i + 4) + ",0)");
                expected.add(name + ",2025=NULL");
                expected.add(name + ",2026=NULL");
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
        MondrianProperties props = MondrianProperties.instance();
        boolean previous = props.EnableNativeNonEmpty.get();
        RolapNativeRegistry registry =
            ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previousRegistry = registry.isEnabled();
        try {
            props.EnableNativeNonEmpty.set(nativeEnabled);
            registry.setEnabled(nativeEnabled);
            registry.flushAllNativeSetCache();
            Result result = connection.execute(connection.parseQuery(mdx));
            List<String> values = new ArrayList<>();
            for (int i = 0; i < result.getAxes()[axis].getPositions().size(); i++) {
                var position = result.getAxes()[axis].getPositions().get(i);
                var cell = result.getCell(axis == 0 ? new int[] {i} : new int[] {0, i});
                values.add(position.get(0).getName() + "," + position.get(1).getName()
                    + "=" + (cell.isNull() ? "NULL" :
                        ((Number) cell.getValue()).doubleValue()));
            }
            return values;
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
