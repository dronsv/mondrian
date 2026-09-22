/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/** Real SQL/cell regressions for emondrian-clickhouse#103. */
class NqePrefetchContextTest {
    private static final String NQE = "mondrian.native.queryEngine.enable";
    private static final String PRODUCTS = "[Product].[Name].Members";
    private static final String MONTHS =
        "{[Calendar].[2026].[2], [Calendar].[2026].[8]}";

    @Test void sumCannotReuseAllStoresAtEachStore() throws Exception {
        assertCells("Sum([Store].[Name].Members, [Measures].[Quantity])",
            PRODUCTS, "", false, List.of("P1=77781", "P2=1002"));
    }

    @Test void literalTupleCannotReuseAllStores() throws Exception {
        assertCells("([Measures].[Quantity], [Store].[S1])",
            PRODUCTS, "", false, List.of("P1=4", "P2=null"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "[Calendar].[2026].[8].[36]",
        "[Calendar].[2026].[8].LastChild",
        "[Calendar].[2026].[8].[35].NextMember",
        "ClosingPeriod([Calendar].[Week], [Calendar].[2026].[8])"
    })
    void calendarNavigationCannotReuseAllWeeks(String coordinate)
        throws Exception
    {
        assertCells("([Measures].[Quantity], " + coordinate + ")",
            PRODUCTS, "", false, List.of("P1=null", "P2=2"));
    }

    @Test void currentMemberParentUsesMonthContext() throws Exception {
        assertCells("([Measures].[Quantity], [Calendar].CurrentMember.Parent)",
            PRODUCTS, "WHERE [Calendar].[2026].[8].[36]", false,
            List.of("P1=4", "P2=1002"));
    }

    @Test void defaultMemberCanResetANonAxisSlicer() throws Exception {
        assertCells("([Measures].[Quantity], [Calendar].DefaultMember)",
            PRODUCTS, "WHERE [Calendar].[2026].[8].[36]", false,
            List.of("P1=77781", "P2=1002"));
    }

    @Test void projectedGrainCannotCollideWithALeafKey() throws Exception {
        // Month 8 has quantity 1006; unrelated week 8 has quantity 77777.
        QueryRun result = assertCells(
            "Sum({[Store].[All Stores]}, [Measures].[Quantity])",
            MONTHS, "", false, List.of("2=77777", "8=1006"));
        assertTrue(result.prefetchHits() > 0, result.logs.toString());
    }

    @Test void evaluatorChangingProjectedLevelMustMiss() throws Exception {
        assertCells("Sum([Calendar].CurrentMember.Children, [Measures].[Quantity])",
            MONTHS, "", false, List.of("2=77777", "8=1006"));
    }

    @Test void repeatedMonthKeysCannotMergeDifferentYears() throws Exception {
        assertCells("[Measures].[Quantity]",
            "{[Calendar].[2025].[8], [Calendar].[2026].[8]}", "", true,
            List.of("8=5000", "8=1006"));
    }

    @Test void nonUniqueSlicerMustRetainAncestorKeys() throws Exception {
        assertCells("[Measures].[Quantity]", PRODUCTS,
            "WHERE [Calendar].[2026].[8]", true,
            List.of("P1=4", "P2=1002"));
    }

    @Test void scalarStoredFormulaStillUsesFullResult() throws Exception {
        QueryRun result = assertCells("[Measures].[Quantity] * 2",
            PRODUCTS, "", false, List.of("P1=155562", "P2=2004"));
        assertTrue(result.logs.stream().anyMatch(
            s -> s.contains("NQE: mode=FULL_RESULT")), result.logs.toString());
    }

    @Test void contextIndependentEvaluatorKeepsPrefetchHits() throws Exception {
        QueryRun result = assertCells(
            "Sum({[Store].[All Stores]}, [Measures].[Quantity])",
            PRODUCTS, "", false, List.of("P1=77781", "P2=1002"));
        assertTrue(result.prefetchHits() > 0, result.logs.toString());
    }

    @Test void setFunctionOutsideOldBlockListMustUseEvaluator() throws Exception {
        assertCells("Stddevp({[Calendar].[2026].[8].[35], [Calendar].[2026].[8].[36]},"
                + " [Measures].[Quantity])",
            "{[Product].[P2]}", "", false, List.of("P2=499"));
    }

    @Test void nullGuardCannotHideCoordinateChanges() throws Exception {
        assertCells("IIF(IsEmpty(([Measures].[Quantity], [Store].[S1])),"
                + " NULL, [Measures].[Quantity])", PRODUCTS, "", false,
            List.of("P1=77781", "P2=null"));
    }

    @Test void aggregateContextMustUseSegmentPredicates() throws Exception {
        assertCells("Aggregate([Store].[Name].Members, [Measures].[Quantity])",
            PRODUCTS, "", false, List.of("P1=77781", "P2=1002"));
    }

    @Test void resettingASlicerMustNotReuseItsRestrictedValues() throws Exception {
        assertCells("Sum({[Store].[All Stores]},"
                + " ([Measures].[Quantity], [Product].[All Products]))",
            "{[Calendar].[2026].[8]}", "WHERE [Product].[P1]", false,
            List.of("8=1006"));
    }

    @Test void nonEmptyPruningUsesTheChangedContext() throws Exception {
        assertCells("([Measures].[Quantity], [Calendar].[2026].[8].[36])",
            "NON EMPTY " + PRODUCTS, "", false, List.of("P2=2"));
    }

    @Test void literalAllResetsANonProjectedSlicer() throws Exception {
        assertCells("([Measures].[Quantity], [Product].[All Products])",
            "{[Calendar].[2026].[8]}", "WHERE [Product].[P1]", false,
            List.of("8=1006"));
    }

    @Test void scalarGuardAroundAllPinMustRemainEvaluated() throws Exception {
        assertCells("IIF(IsEmpty([Measures].[Quantity]), NULL,"
                + " ([Measures].[Quantity], [Calendar].[All Calendars]))",
            "{[Calendar].[2026].[9]}", "", false, List.of("9=null"));
    }

    @Test void compoundSlicerWithContextIndependentEvaluator() throws Exception {
        assertCells("Sum({[Store].[All Stores]}, [Measures].[Quantity])",
            PRODUCTS, "WHERE {[Calendar].[2026].[2], [Calendar].[2026].[8]}", false,
            List.of("P1=77781", "P2=1002"));
    }

    // Allowing native leaves into POST_PROCESS makes these schema-only
    // consumers return NULL at both products: the template evaluator has
    // no per-cell axis context, and its cell reader only has native values.
    @ParameterizedTest
    @ValueSource(strings = {"NativeAlias", "NativeScaled"})
    void schemaFormulaOverNativeMeasureKeepsEachCellCoordinate(String measure)
        throws Exception
    {
        String mdx = "SELECT {[Measures].[" + measure + "]} ON COLUMNS, "
            + PRODUCTS + " ON ROWS FROM [Sales]";
        List<String> expected = measure.equals("NativeAlias")
            ? List.of("P1=77781", "P2=1002")
            : List.of("P1=155562", "P2=2004");
        assertEquals(expected, run(mdx, false, false).cells);
        QueryRun actual = run(mdx, true, false);
        assertEquals(expected, actual.cells, actual.logs.toString());
    }

    @Test void nativeConsumersAlongsideStoredMeasureKeepPrefetch() throws Exception {
        String mdx = "SELECT {[Measures].[Quantity], [Measures].[NativeAlias],"
            + " [Measures].[NativeScaled]} ON COLUMNS, " + PRODUCTS
            + " ON ROWS FROM [Sales]";
        List<String> expected = List.of("P1=77781", "P1=77781", "P1=155562",
            "P2=1002", "P2=1002", "P2=2004");
        assertEquals(expected, run(mdx, false, false).cells);
        QueryRun actual = run(mdx, true, false);
        assertEquals(expected, actual.cells, actual.logs.toString());
        assertTrue(actual.hasMode("PREFETCH_ONLY"), actual.logs.toString());
        assertTrue(actual.prefetchHits() > 0, actual.logs.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "Sum({[Store].[All Stores]}, ([Measures].[Quantity], [Store].[All Stores]))",
        "([Measures].[Quantity], [Store].[All Stores])"
    })
    void explicitAllCanChangeTheSubcubeWithoutChangingMemberKeys(String formula)
        throws Exception
    {
        String mdx = "WITH MEMBER [Measures].[M] AS " + formula
            + " SELECT {[Measures].[M]} ON COLUMNS, " + PRODUCTS
            + " ON ROWS FROM (SELECT {[Store].[S1]} ON COLUMNS FROM [Sales])";
        List<String> expected = List.of("P1=77781", "P2=1002");
        assertEquals(expected, run(mdx, false, false).cells);
        assertEquals(expected, run(mdx, true, false).cells);
    }

    @Test void unchangedSubcubeStillAllowsPrefetchHits() throws Exception {
        String mdx = "WITH MEMBER [Measures].[M] AS"
            + " Sum({[Measures].[Quantity]}, [Measures].[Quantity])"
            + " SELECT {[Measures].[M]} ON COLUMNS, " + PRODUCTS
            + " ON ROWS FROM (SELECT {[Store].[S1]} ON COLUMNS FROM [Sales])";
        List<String> expected = List.of("P1=4", "P2=null");
        assertEquals(expected, run(mdx, false, false).cells);
        QueryRun actual = run(mdx, true, false);
        assertEquals(expected, actual.cells);
        assertTrue(actual.prefetchHits() > 0, actual.logs.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "[Measures].[Quantity] * 2",
        "Sum({[Store].[All Stores]}, [Measures].[Quantity])"
    })
    void firstNonAllLevelNeedsNoAncestorKey(String formula) throws Exception {
        String mdx = "WITH MEMBER [Measures].[M] AS " + formula
            + " SELECT {[Measures].[M]} ON COLUMNS, " + PRODUCTS
            + " ON ROWS FROM [Sales]";
        List<String> expected = formula.endsWith("* 2")
            ? List.of("P1=155562", "P2=2004")
            : List.of("P1=77781", "P2=1002");
        assertEquals(expected, run(mdx, false, false, false).cells);
        QueryRun actual = run(mdx, true, false, false);
        assertEquals(expected, actual.cells);
        if (formula.endsWith("* 2")) {
            assertTrue(actual.hasMode("FULL_RESULT"), actual.logs.toString());
        } else {
            assertTrue(actual.hasMode("PREFETCH_ONLY"), actual.logs.toString());
            assertTrue(actual.prefetchHits() > 0, actual.logs.toString());
        }
    }

    @Test void firstNonAllSlicerNeedsNoAncestorKey() throws Exception {
        String mdx = "SELECT {[Measures].[Quantity]} ON COLUMNS, " + PRODUCTS
            + " ON ROWS FROM [Sales] WHERE [Store].[S1]";
        List<String> expected = List.of("P1=4", "P2=null");
        assertEquals(expected, run(mdx, false, false, false).cells);
        QueryRun actual = run(mdx, true, false, false);
        assertEquals(expected, actual.cells);
        assertTrue(actual.hasMode("FULL_RESULT"), actual.logs.toString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void mixedLevelsKeepSafePrefetchRegardlessOfAxisOrder(boolean reversed)
        throws Exception
    {
        String month = "[Calendar].[2026].[8]";
        // Week 8 collides with the selected month key, but has another value.
        String week = "[Calendar].[2026].[2].[8]";
        String rows = reversed ? "{" + week + "," + month + "}"
            : "{" + month + "," + week + "}";
        List<String> expected = reversed
            ? List.of("8=77777", "8=1006") : List.of("8=1006", "8=77777");
        QueryRun actual = assertCells(
            "Sum({[Store].[All Stores]}, [Measures].[Quantity])",
            rows, "", false, expected);
        assertTrue(actual.hasMode("PREFETCH_ONLY"), actual.logs.toString());
        assertTrue(actual.prefetchHits() > 0, actual.logs.toString());
    }

    @Test void mixedLevelsCapFullResultButKeepSafePrefetch() throws Exception {
        QueryRun actual = assertCells("[Measures].[Quantity] * 2",
            "{[Calendar].[2026].[8], [Calendar].[2026].[2].[8]}", "", false,
            List.of("8=2012", "8=155554"));
        assertTrue(actual.hasMode("PREFETCH_ONLY"), actual.logs.toString());
        assertTrue(actual.prefetchHits() > 0, actual.logs.toString());
    }

    @Test void resetPlanWithSubcubeMasksThePinnedHierarchy() throws Exception {
        String mdx = "WITH MEMBER [Measures].[AllStores] AS"
            + " ([Measures].[Quantity], [Store].[All Stores])"
            + " SELECT {[Measures].[Quantity], [Measures].[AllStores]} ON COLUMNS, "
            + PRODUCTS + " ON ROWS FROM"
            + " (SELECT {[Store].[S1]} ON COLUMNS FROM [Sales])";
        List<String> expected = List.of("P1=4", "P1=77781", "P2=null", "P2=1002");
        assertEquals(expected, run(mdx, false, false).cells);
        QueryRun actual = run(mdx, true, false);
        assertEquals(expected, actual.cells);
        // The reset SQL drops the Store subselect, as the evaluator's
        // explicit-All mask does, so every cell comes from NQE.
        assertTrue(actual.hasMode("FULL_RESULT"), actual.logs.toString());
    }

    @Test void shareWithOffAxisResetKeepsSubcubePrefetch() throws Exception {
        String mdx = "WITH MEMBER [Measures].[AllStores] AS"
            + " ([Measures].[Quantity], [Store].[All Stores])"
            + " MEMBER [Measures].[M] AS [Measures].[Quantity] / [Measures].[AllStores]"
            + " SELECT {[Measures].[M]} ON COLUMNS, " + PRODUCTS
            + " ON ROWS FROM (SELECT {[Store].[S1]} ON COLUMNS FROM [Sales])";
        // P1 has 4 in the subcube and 4 + 77777 when Store is reset.
        List<String> expected = List.of("P1=" + BigDecimal.valueOf(4d / 77781d)
            .stripTrailingZeros().toPlainString(), "P2=null");
        assertEquals(expected, run(mdx, false, false).cells);
        QueryRun actual = run(mdx, true, false);
        assertEquals(expected, actual.cells);
        assertTrue(actual.hasMode("PREFETCH_ONLY"), actual.logs.toString());
        assertTrue(actual.prefetchHits() > 0, actual.logs.toString());
    }

    // A parent-child member's cell rolls up its descendants, through the
    // closure table or $AggregateChildren; its key column holds only the
    // member's own facts. uniqueMembers="true" must not hide that.
    @ParameterizedTest
    @ValueSource(strings = {"Emp", "EmpClosure"})
    void parentChildAxisMembersRollUpDescendants(String dimension)
        throws Exception
    {
        String rows = "[" + dimension + "].[Employee].Members";
        List<String> expected = List.of("Boss=1110", "Mid=1100", "Leaf=1000");
        assertQuery("SELECT {[Measures].[Quantity]} ON COLUMNS, " + rows
            + " ON ROWS FROM [PC]", false, expected);
        assertQuery("WITH MEMBER [Measures].[M] AS"
            + " Sum({[Store].[All Stores]}, [Measures].[Quantity])"
            + " SELECT {[Measures].[M]} ON COLUMNS, " + rows
            + " ON ROWS FROM [PC]", false, expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Emp", "EmpClosure"})
    void parentChildSlicerRollsUpDescendants(String dimension) throws Exception {
        assertQuery("SELECT {[Measures].[Quantity]} ON COLUMNS,"
            + " [Store].[Name].Members ON ROWS FROM [PC]"
            + " WHERE [" + dimension + "].[Boss].[Mid]", false,
            List.of("S1=100", "S2=1000"));
    }

    @Test void parentChildHierarchyAtAllKeepsFullResult() throws Exception {
        QueryRun actual = assertQuery("SELECT {[Measures].[Quantity]} ON COLUMNS,"
            + " [Store].[Name].Members ON ROWS FROM [PC]", false,
            List.of("S1=110", "S2=1000"));
        assertTrue(actual.hasMode("FULL_RESULT"), actual.logs.toString());
    }

    // A calculated axis member is a formula, not a stored key: only the
    // evaluator can compute it, from prefetched stored inputs.
    @ParameterizedTest
    @ValueSource(strings = {
        "Aggregate({[Store].[S1], [Store].[S2]})",
        "Sum({[Store].[S2]}) - Sum({[Store].[S1]})"
    })
    void calculatedAxisMemberIsEvaluatedFromPrefetchedInputs(String formula)
        throws Exception
    {
        QueryRun actual = assertQuery("WITH MEMBER [Store].[Calc] AS " + formula
            + " SELECT {[Measures].[Quantity]} ON COLUMNS,"
            + " {[Store].[S1], [Store].[Calc]} ON ROWS FROM [Sales]", false,
            List.of("S1=4", formula.startsWith("Aggregate")
                ? "Calc=78783" : "Calc=78775"));
        assertTrue(actual.hasMode("PREFETCH_ONLY"), actual.logs.toString());
        assertTrue(actual.prefetchHits() > 0, actual.logs.toString());
    }

    @Test void calculatedAxisMemberAlongsideEvaluatorMeasure() throws Exception {
        QueryRun actual = assertQuery("WITH MEMBER [Calendar].[2026].[Calc] AS"
            + " Aggregate({[Calendar].[2026].[2], [Calendar].[2026].[8]})"
            + " MEMBER [Measures].[M] AS"
            + " Sum({[Store].[All Stores]}, [Measures].[Quantity])"
            + " SELECT {[Measures].[M]} ON COLUMNS,"
            + " {[Calendar].[2026].[8], [Calendar].[2026].[Calc]} ON ROWS"
            + " FROM [Sales]", false, List.of("8=1006", "Calc=78783"));
        assertTrue(actual.hasMode("PREFETCH_ONLY"), actual.logs.toString());
        assertTrue(actual.prefetchHits() > 0, actual.logs.toString());
    }

    // An All pin masks the subselect of its own hierarchy only; a subselect
    // on another hierarchy still applies and needs no evaluator fallback.
    @Test void allPinWithSubselectOnAnotherHierarchyKeepsFullResult()
        throws Exception
    {
        QueryRun actual = assertQuery("WITH MEMBER [Measures].[AllStores] AS"
            + " ([Measures].[Quantity], [Store].[All Stores])"
            + " SELECT {[Measures].[Quantity], [Measures].[AllStores]} ON COLUMNS, "
            + PRODUCTS + " ON ROWS FROM"
            + " (SELECT {[Calendar].[2026].[8]} ON COLUMNS FROM [Sales])", false,
            List.of("P1=4", "P1=4", "P2=1002", "P2=1002"));
        assertTrue(actual.hasMode("FULL_RESULT"), actual.logs.toString());
    }

    @Test void allPinMasksOnlyItsOwnHierarchyOfTheSubselect() throws Exception {
        // Store S2 and month 8: P1 has no such fact; with Store masked, P1
        // keeps only month 8 (4), not its month 2 fact (77777).
        QueryRun actual = assertQuery("WITH MEMBER [Measures].[AllStores] AS"
            + " ([Measures].[Quantity], [Store].[All Stores])"
            + " SELECT {[Measures].[Quantity], [Measures].[AllStores]} ON COLUMNS, "
            + PRODUCTS + " ON ROWS FROM (SELECT {[Store].[S2]} ON COLUMNS,"
            + " {[Calendar].[2026].[8]} ON ROWS FROM [Sales])", false,
            List.of("P1=null", "P1=4", "P2=1002", "P2=1002"));
        assertTrue(actual.hasMode("FULL_RESULT"), actual.logs.toString());
    }

    private static QueryRun assertCells(
        String formula, String rows, String slicer, boolean repeatedMonth,
        List<String> expected) throws Exception
    {
        return assertQuery("WITH MEMBER [Measures].[M] AS " + formula
            + " SELECT {[Measures].[M]} ON COLUMNS, " + rows
            + " ON ROWS FROM [Sales] " + slicer, repeatedMonth, expected);
    }

    /** Asserts literal values, then that NQE returns the same cells. */
    private static QueryRun assertQuery(
        String mdx, boolean repeatedMonth, List<String> expected) throws Exception
    {
        assertEquals(expected, run(mdx, false, repeatedMonth).cells,
            "independent interpreter oracle");
        QueryRun actual = run(mdx, true, repeatedMonth);
        assertEquals(expected, actual.cells, "NQE: " + mdx);
        return actual;
    }

    private record QueryRun(List<String> cells, List<String> logs) {
        boolean hasMode(String mode) {
            return logs.stream().anyMatch(s -> s.equals("NQE: mode=" + mode));
        }

        int prefetchHits() {
            return logs.stream().filter(s -> s.startsWith("NQE prefetch: hits="))
                .mapToInt(s -> Integer.parseInt(s.split("hits=")[1].split(" ")[0]))
                .sum();
        }

        long nqeSqlCount() {
            return logs.stream().filter(
                s -> s.startsWith("NativeQuerySqlGenerator: executing SQL")).count();
        }
    }

    private static QueryRun run(String mdx, boolean nqe, boolean repeatedMonth)
        throws Exception
    {
        return run(mdx, nqe, repeatedMonth, true);
    }

    private static QueryRun run(
        String mdx, boolean nqe, boolean repeatedMonth, boolean uniqueFirstLevels)
        throws Exception
    {
        MondrianProperties properties = MondrianProperties.instance();
        String previous = properties.getProperty(NQE);
        properties.setProperty(NQE, Boolean.toString(nqe));
        try (LogCapture capture = new LogCapture()) {
            mondrian.olap.Connection connection = open(repeatedMonth, uniqueFirstLevels);
            try {
                Result result = connection.execute(connection.parseQuery(mdx));
                List<String> cells = new ArrayList<>();
                List<Position> rows = result.getAxes()[1].getPositions();
                for (int r = 0; r < rows.size(); r++) {
                    for (int c = 0; c < result.getAxes()[0].getPositions().size(); c++) {
                        Object value = result.getCell(new int[] {c, r}).getValue();
                        String text = value instanceof Number
                            ? new BigDecimal(value.toString()).stripTrailingZeros().toPlainString()
                            : String.valueOf(value);
                        cells.add(rows.get(r).get(0).getName() + "=" + text);
                    }
                }
                return new QueryRun(cells, List.copyOf(capture.messages));
            } finally {
                connection.close();
            }
        } finally {
            if (previous == null) {
                properties.remove(NQE);
            } else {
                properties.setProperty(NQE, previous);
            }
        }
    }

    private static mondrian.olap.Connection open(
        boolean repeatedMonth, boolean uniqueFirstLevels)
        throws Exception
    {
        String jdbc = "jdbc:h2:mem:nqe_context_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR,MONTH,WEEK";
        // Mondrian opens its pooled connection before the fixture connection closes.
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "");
             Statement sql = db.createStatement())
        {
            sql.execute("CREATE TABLE product (id INT, name VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'P1'),(2,'P2')");
            sql.execute("CREATE TABLE store (id INT, name VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'S1'),(2,'S2')");
            sql.execute("CREATE TABLE calendar (id INT, year INT, month INT, week INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025,12,52),"
                + "(2,2026,8,35),(3,2026,8,36),(4,2026,2,8),(5,2026,9,37)");
            sql.execute("CREATE TABLE fact (product_id INT, store_id INT, calendar_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (1,1,2,4),(2,2,3,2),"
                + "(2,2,2,1000),(1,2,4,77777)");
            if (repeatedMonth) {
                sql.execute("INSERT INTO calendar VALUES (6,2025,8,33)");
                sql.execute("INSERT INTO fact VALUES (1,1,6,5000)");
            }
            sql.execute("CREATE TABLE wide AS SELECT f.*, p.name, c.year, c.month, c.week"
                + " FROM fact f JOIN product p ON p.id=f.product_id"
                + " JOIN calendar c ON c.id=f.calendar_id");
            // Boss > Mid > Leaf, each with facts of its own.
            sql.execute("CREATE TABLE employee (id INT, parent_id INT, name VARCHAR)");
            sql.execute("INSERT INTO employee VALUES (1,NULL,'Boss'),(2,1,'Mid'),(3,2,'Leaf')");
            sql.execute("CREATE TABLE employee_closure (parent_id INT, child_id INT, distance INT)");
            sql.execute("INSERT INTO employee_closure VALUES (1,1,0),(1,2,1),(1,3,2),"
                + "(2,2,0),(2,3,1),(3,3,0)");
            sql.execute("CREATE TABLE pcfact (emp_id INT, store_id INT, qty INT)");
            sql.execute("INSERT INTO pcfact VALUES (1,1,10),(2,1,100),(3,2,1000)");
            Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
            props.put("JdbcUser", "sa");
            props.put("JdbcDrivers", "org.h2.Driver");
            props.put("Jdbc", jdbc);
            props.put("CatalogContent", """
                <Schema name="NqeContext">
                  <Dimension name="Product">
                    <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                      <Level name="Name" column="name" uniqueMembers="%s"/>
                    </Hierarchy>
                  </Dimension>
                  <Dimension name="Store">
                    <Hierarchy hasAll="true" primaryKey="id"><Table name="store"/>
                      <Level name="Name" column="name" uniqueMembers="%s"/>
                    </Hierarchy>
                  </Dimension>
                  <Dimension name="Calendar" type="TimeDimension">
                    <Hierarchy hasAll="true" primaryKey="id"><Table name="calendar"/>
                      <Level name="Year" column="year" type="Integer" levelType="TimeYears" uniqueMembers="%s"/>
                      <Level name="Month" column="month" type="Integer" levelType="TimeMonths" uniqueMembers="%s"/>
                      <Level name="Week" column="week" type="Integer" levelType="TimeWeeks" uniqueMembers="false"/>
                    </Hierarchy>
                  </Dimension>
                  <Dimension name="Emp">
                    <Hierarchy hasAll="true" primaryKey="id"><Table name="employee"/>
                      <Level name="Employee" column="id" nameColumn="name" type="Integer"
                             uniqueMembers="true" parentColumn="parent_id"/>
                    </Hierarchy>
                  </Dimension>
                  <Dimension name="EmpClosure">
                    <Hierarchy hasAll="true" primaryKey="id"><Table name="employee"/>
                      <Level name="Employee" column="id" nameColumn="name" type="Integer"
                             uniqueMembers="true" parentColumn="parent_id">
                        <Closure parentColumn="parent_id" childColumn="child_id">
                          <Table name="employee_closure"/>
                        </Closure>
                      </Level>
                    </Hierarchy>
                  </Dimension>
                  <Cube name="PC"><Table name="pcfact"/>
                    <DimensionUsage name="Emp" source="Emp" foreignKey="emp_id"/>
                    <DimensionUsage name="EmpClosure" source="EmpClosure" foreignKey="emp_id"/>
                    <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                    <Measure name="Quantity" column="qty" aggregator="sum"/>
                  </Cube>
                  <Cube name="Sales"><Table name="fact"/>
                    <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                    <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                    <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                    <Measure name="Quantity" column="qty" aggregator="sum"/>
                    <CalculatedMember name="NativeQuantity" dimension="Measures">
                      <Annotations>
                        <Annotation name="nativeSql.enabled">true</Annotation>
                        <Annotation name="nativeSql.template"><![CDATA[
                          SELECT ${axisResultSelectList} SUM(pr.q) AS val FROM (
                            SELECT f.qty AS q, 1 AS g ${axisPresenceSelectList}
                            FROM wide f ${joinClauses} WHERE ${whereClause}
                          ) pr GROUP BY ${axisGroupByList} pr.g
                        ]]></Annotation>
                      </Annotations>
                      <Formula>[Measures].[Quantity]</Formula>
                    </CalculatedMember>
                    <CalculatedMember name="NativeAlias" dimension="Measures">
                      <Formula>[Measures].[NativeQuantity]</Formula>
                    </CalculatedMember>
                    <CalculatedMember name="NativeScaled" dimension="Measures">
                      <Formula>[Measures].[NativeQuantity] * 2</Formula>
                    </CalculatedMember>
                  </Cube>
                </Schema>
                """.formatted(uniqueFirstLevels, uniqueFirstLevels,
                    uniqueFirstLevels, !repeatedMonth));
            return mondrian.olap.DriverManager.getConnection(props, null);
        }
    }

    private static final class LogCapture implements AutoCloseable {
        private final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        private final Map<String, LoggerConfig> previous = new LinkedHashMap<>();
        private final List<String> messages = new ArrayList<>();
        private final AbstractAppender appender = new AbstractAppender(
            "nqeContextCapture", null, null, true, Property.EMPTY_ARRAY)
        {
            @Override public void append(LogEvent event) {
                messages.add(event.getMessage().getFormattedMessage());
            }
        };

        LogCapture() {
            appender.start();
            for (String name : List.of("mondrian.rolap.NativeQueryEngine",
                "mondrian.rolap.NativeQuerySqlGenerator", "mondrian.olap.ResultBase"))
            {
                previous.put(name, context.getConfiguration().getLoggers().get(name));
                LoggerConfig logger = new LoggerConfig(name,
                    org.apache.logging.log4j.Level.INFO, false);
                logger.addAppender(appender, org.apache.logging.log4j.Level.INFO, null);
                context.getConfiguration().removeLogger(name);
                context.getConfiguration().addLogger(name, logger);
            }
            context.updateLoggers();
        }

        @Override public void close() {
            previous.forEach((name, logger) -> {
                context.getConfiguration().removeLogger(name);
                if (logger != null) {
                    context.getConfiguration().addLogger(name, logger);
                }
            });
            context.updateLoggers();
            appender.stop();
        }
    }
}
