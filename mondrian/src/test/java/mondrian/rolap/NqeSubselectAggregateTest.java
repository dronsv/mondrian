/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;

/** Real subselect values with an aggregate missing the selected columns (#100). */
class NqeSubselectAggregateTest {
    private record Case(String name, String selection, String slicer,
                        Double s1, Double s2, Double s3) { }

    static Stream<Object[]> selections() {
        List<Case> cases = Arrays.asList(
            new Case("plain member", "{[Product.Manufacturer].[Red]}", "", 48d, 16d, null),
            new Case("except", "Except([Product].[Name].Members, {[Product].[P016]})", "", 32d, 16d, 1028d),
            new Case("unary not", "-{[Product].[P016]}", "", 32d, 16d, 1028d),
            new Case("explicit or", "{[Product].[P002], [Product].[P004]}", "", 2d, 4d, null),
            new Case("sql in", "Filter([Product].[Name].AllMembers, InStr([Product].CurrentMember.Member_Caption, \"01\") > 0)", "", 40d, 12d, 14d),
            new Case("not sql in", "Filter([Product].[Name].AllMembers, InStr([Product].CurrentMember.Member_Caption, \"01\") = 0)", "", 8d, 4d, 1014d),
            new Case("empty", "Filter([Product].[Name].Members, 1 = 0)", "", null, null, null),
            new Case("cross hierarchy or", "{([Product.Manufacturer].[Red], [Store].[All Stores]), ([Product.Manufacturer].[All Mfr], [Store].[S3])}", "", 48d, 16d, 1028d),
            new Case("and inside or", "{([Product.Manufacturer].[Blue], [Store].[S1]), ([Product.Manufacturer].[Red], [Store].[S3])}", "", null, null, null),
            new Case("parent keys", "{[Calendar].[2026].[8]}", "", 48d, 16d, 28d),
            new Case("covered slicer", "Except([Product].[Name].Members, {[Product].[P016]})", " WHERE [Calendar.FlatWeek].[202635]", 0d, 16d, 28d));
        return cases.stream().flatMap(c -> Stream.of(
            new Object[] {c, false}, new Object[] {c, true}));
    }

    @ParameterizedTest(name = "{0}, prefetch={1}")
    @MethodSource("selections")
    void subselectValuesMatchLegacyWithNarrowAggregate(Case c, boolean prefetch)
        throws Exception
    {
        String mdx = "SELECT {[Measures].[Quantity]"
            + (prefetch ? ", [Measures].[NativeQty]" : "")
            + "} ON COLUMNS, [Store].[Name].Members ON ROWS "
            + "FROM (SELECT " + c.selection + " ON COLUMNS FROM [Navigation])"
            + c.slicer;
        List<Double> expected = new ArrayList<>();
        for (Double value : Arrays.asList(c.s1, c.s2, c.s3)) {
            expected.add(value);
            if (prefetch) {
                expected.add(value);
            }
        }
        assertEquals(expected, execute(mdx, false, prefetch, false));
        assertEquals(expected, execute(mdx, true, prefetch, false));
    }

    static Stream<Object[]> exclusions() {
        List<Case> cases = Arrays.asList(
            new Case("excluded OR child",
                "{([Product.Manufacturer].[Blue], [Store].[All Stores]),"
                    + " ([Product.Manufacturer].[All Mfr], [Store].[S1])}",
                "", 48d, 16d, 1028d),
            new Case("excluded OR child with retained slicer",
                "{([Product.Manufacturer].[Blue], [Store].[All Stores]),"
                    + " ([Product.Manufacturer].[All Mfr], [Store].[S1])}",
                " WHERE [Calendar.FlatWeek].[202635]", 16d, 16d, 28d),
            new Case("retained constraints inside OR",
                "{([Product.Manufacturer].[Blue], [Store].[S1]),"
                    + " ([Product.Manufacturer].[Red], [Store].[S3])}",
                "", 48d, null, 1028d),
            new Case("retained AND child",
                "{([Product.Manufacturer].[Blue], [Store].[S3])}",
                "", null, null, 1028d));
        return cases.stream().flatMap(c -> Stream.of(
            new Object[] {c, false}, new Object[] {c, true}));
    }

    @ParameterizedTest(name = "{0}, NQE={1}")
    @MethodSource("exclusions")
    void whereClauseExceptWidensExcludedAtomsWithinBooleanTrees(
        Case c, boolean nativeEnabled) throws Exception
    {
        String mdx = "SELECT {[Measures].[NativeQty]} ON COLUMNS,"
            + " [Store].[Name].Members ON ROWS FROM (SELECT "
            + c.selection + " ON COLUMNS FROM [Navigation])" + c.slicer;
        assertEquals(Arrays.asList(c.s1, c.s2, c.s3),
            execute(mdx, nativeEnabled, false, false,
                "${whereClauseExcept:Product.Manufacturer}"));
    }

    /**
     * {@code ${whereClauseExcept:H}} is the SQL counterpart of resetting H
     * to All in a tuple. Legacy evaluates such a tuple with H masked out of
     * every subselect axis and the slicer overridden, i.e. each H atom
     * becomes TRUE within its own axis; it does not project the
     * intersection of all axes. Oracle: QtyAllMfr =
     * ([Measures].[Quantity], [Product.Manufacturer].[All Mfr]) with native
     * evaluation off.
     */
    static Stream<Object[]> exceptOracleSelections() {
        List<Object[]> cases = Arrays.asList(
            new Object[] {"member",
                "(SELECT {[Product.Manufacturer].[Red]} ON COLUMNS FROM [Navigation])",
                Arrays.asList(48d, 16d, 1028d)},
            new Object[] {"excluded OR child",
                "(SELECT {([Product.Manufacturer].[Blue], [Store].[All Stores]),"
                    + " ([Product.Manufacturer].[All Mfr], [Store].[S1])}"
                    + " ON COLUMNS FROM [Navigation])",
                Arrays.asList(48d, 16d, 1028d)},
            new Object[] {"correlated tuples",
                "(SELECT {([Product.Manufacturer].[Blue], [Store].[S1]),"
                    + " ([Product.Manufacturer].[Red], [Store].[S3])}"
                    + " ON COLUMNS FROM [Navigation])",
                Arrays.asList(48d, null, 1028d)},
            // The two axes intersect to nothing, yet each keeps S1 and S2
            // once Manufacturer is masked.
            new Object[] {"nested correlated subselects",
                "(SELECT {([Product.Manufacturer].[Red], [Store].[S1]),"
                    + " ([Product.Manufacturer].[Blue], [Store].[S2])} ON COLUMNS FROM"
                    + " (SELECT {([Product.Manufacturer].[Red], [Store].[S2]),"
                    + " ([Product.Manufacturer].[Blue], [Store].[S1])}"
                    + " ON COLUMNS FROM [Navigation]))",
                Arrays.asList(48d, 16d, null)},
            // The reset overrides the Blue slicer; the subselect keeps S1, S3.
            new Object[] {"slicer on the excluded hierarchy",
                "(SELECT {([Product.Manufacturer].[Red], [Store].[S1]),"
                    + " ([Product.Manufacturer].[Blue], [Store].[S3])}"
                    + " ON COLUMNS FROM [Navigation])"
                    + " WHERE [Product.Manufacturer].[Blue]",
                Arrays.asList(48d, null, 1028d)});
        return cases.stream().flatMap(c -> Stream.of(
            new Object[] {c[0], c[1], c[2], false},
            new Object[] {c[0], c[1], c[2], true}));
    }

    @ParameterizedTest(name = "{0}, NQE={3}")
    @MethodSource("exceptOracleSelections")
    void whereClauseExceptMatchesLegacyTupleReset(String name, String from,
        List<Double> expected, boolean nativeEnabled) throws Exception
    {
        String axes = " ON COLUMNS, [Store].[Name].Members ON ROWS FROM ";
        assertEquals(expected, execute(
            "SELECT {[Measures].[QtyAllMfr]}" + axes + from, false, false, false));
        assertEquals(expected, execute(
            "SELECT {[Measures].[NativeQty]}" + axes + from, nativeEnabled, false,
            false, "${whereClauseExcept:Product.Manufacturer}"));
    }

    /**
     * One evaluator serves every plan of a virtual-cube query. Its current
     * measure (the default, Visits) comes from Other, which has no Product
     * dimension, so a subselect built for that cube restricts nothing. Each
     * plan must see the subselect as legacy cells of its own measures do:
     * Product members restrict Quantity (Navigation) and not Visits.
     */
    static Stream<Object[]> virtualCubeSelections() {
        List<Double> visitsAndRedQuantity =
            Arrays.asList(100d, 48d, 200d, 16d, 300d, null);
        return Stream.of(
            new Object[] {"stored measure of the other cube",
                "[Measures].[Quantity]", "{[Product.Manufacturer].[Red]}",
                Arrays.asList(48d, 16d, null)},
            new Object[] {"measures of both cubes",
                "[Measures].[Visits], [Measures].[Quantity]",
                "{[Product.Manufacturer].[Red]}", visitsAndRedQuantity},
            new Object[] {"unary not",
                "[Measures].[Visits], [Measures].[Quantity]",
                "-{[Product.Manufacturer].[Blue]}", visitsAndRedQuantity},
            // Other joins only Store: its restriction keeps S1 and S2.
            new Object[] {"tuples over a shared and a cube-only hierarchy",
                "[Measures].[Visits], [Measures].[Quantity]",
                "{([Product.Manufacturer].[Red], [Store].[S1]),"
                    + " ([Product.Manufacturer].[Blue], [Store].[S2])}",
                Arrays.asList(100d, 48d, 200d, null, null, null)});
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("virtualCubeSelections")
    void subselectIsScopedToEachPlanCube(String name, String measures,
        String selection, List<Double> expected) throws Exception
    {
        String mdx = "SELECT {" + measures + "} ON COLUMNS,"
            + " [Store].[Name].Members ON ROWS FROM (SELECT " + selection
            + " ON COLUMNS FROM [Both])";
        assertEquals(expected, run(mdx, false, null).cells());
        Run nqe = run(mdx, true, null);
        assertEquals(expected, nqe.cells(), nqe.log().toString());
        nqe.assertNqeFullResult();
    }

    /**
     * A coordinate-pin tuple resets Manufacturer. Legacy masks a reset
     * hierarchy out of the subselect (ExplicitTupleSubcubeMaskSupport), so
     * the pinned value is the store total over every manufacturer; the
     * subselect's other hierarchies still restrict it. Cells are
     * (Quantity, QtyAllMfr) per (store, manufacturer) row.
     */
    static Stream<Object[]> pinnedSelections() {
        return Stream.of(
            new Object[] {"member", "{[Product.Manufacturer].[Red]}",
                Arrays.asList(null, 48d, 48d, 48d, null, 16d, 16d, 16d,
                    null, 1028d, null, 1028d)},
            // Neither tuple has facts; masking Manufacturer leaves S1 or S3.
            new Object[] {"tuples",
                "{([Product.Manufacturer].[Blue], [Store].[S1]),"
                    + " ([Product.Manufacturer].[Red], [Store].[S3])}",
                Arrays.asList(null, 48d, null, 48d, null, null, null, null,
                    null, 1028d, null, 1028d)});
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pinnedSelections")
    void pinnedResetHierarchyIsMaskedOutOfSubselect(String name,
        String selection, List<Double> expected) throws Exception
    {
        String mdx = "SELECT {[Measures].[Quantity], [Measures].[QtyAllMfr]}"
            + " ON COLUMNS, CrossJoin([Store].[Name].Members,"
            + " [Product.Manufacturer].[Name].Members) ON ROWS FROM (SELECT "
            + selection + " ON COLUMNS FROM [Navigation])";
        List<String> rows = Arrays.asList(
            "S1,Blue", "S1,Red", "S2,Blue", "S2,Red", "S3,Blue", "S3,Red");
        Run legacy = run(mdx, false, null);
        assertEquals(rows, legacy.rows());
        assertEquals(expected, legacy.cells());
        Run nqe = run(mdx, true, null);
        assertEquals(rows, nqe.rows());
        assertEquals(expected, nqe.cells(), nqe.log().toString());
        nqe.assertNqeFullResult();
    }

    /**
     * A coordinate pin on a hierarchy that is on no axis: Manufacturer is
     * only restricted by the subselect (or slicer). The pin must keep its
     * reset signature although Manufacturer is not projected (#35/#42), and
     * the reset must mask Manufacturer out of the subselect built for the
     * plan's cube (#44). Alone, #44 returned the Manufacturer-restricted
     * pinned value (48/16/null for the member case); #42 got the pinned
     * cells right only because the store-level aggregate dropped the whole
     * subselect, leaving plain Quantity unrestricted (#100). Together NQE
     * owns the result and matches legacy in both columns. Oracle: native
     * evaluation off. Cells are (Quantity, pinned total) per store.
     */
    static Stream<Object[]> offAxisPinnedSelections() {
        List<Object[]> selections = Arrays.asList(
            new Object[] {"member",
                "(SELECT {[Product.Manufacturer].[Red]} ON COLUMNS FROM [Navigation])",
                Arrays.asList(48d, 48d, 16d, 16d, null, 1028d)},
            new Object[] {"slicer on the pinned hierarchy",
                "(SELECT {([Product.Manufacturer].[Red], [Store].[S1]),"
                    + " ([Product.Manufacturer].[Blue], [Store].[S3])}"
                    + " ON COLUMNS FROM [Navigation])"
                    + " WHERE [Product.Manufacturer].[Blue]",
                Arrays.asList(null, 48d, null, null, 1028d, 1028d)},
            new Object[] {"correlated tuples",
                "(SELECT {([Product.Manufacturer].[Blue], [Store].[S1]),"
                    + " ([Product.Manufacturer].[Red], [Store].[S3])}"
                    + " ON COLUMNS FROM [Navigation])",
                Arrays.asList(null, 48d, null, null, null, 1028d)});
        return selections.stream().flatMap(s -> Stream.of(
            new Object[] {s[0] + ", schema member", "", "[Measures].[QtyAllMfr]", s[1], s[2]},
            new Object[] {s[0] + ", query member",
                "WITH MEMBER [Measures].[PinAllMfr] AS"
                    + " ([Measures].[Quantity], [Product.Manufacturer].[All Mfr]) ",
                "[Measures].[PinAllMfr]", s[1], s[2]}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("offAxisPinnedSelections")
    void offAxisPinMasksItsHierarchyOutOfTheSubselect(String name, String with,
        String pin, String from, List<Double> expected) throws Exception
    {
        String mdx = with + "SELECT {[Measures].[Quantity], " + pin + "} ON COLUMNS,"
            + " [Store].[Name].Members ON ROWS FROM " + from;
        Run legacy = run(mdx, false, null);
        assertEquals(Arrays.asList("S1", "S2", "S3"), legacy.rows());
        assertEquals(expected, legacy.cells());
        Run nqe = run(mdx, true, null);
        assertEquals(legacy.rows(), nqe.rows());
        assertEquals(expected, nqe.cells(), nqe.log().toString());
        nqe.assertNqeFullResult();
    }

    /**
     * PREFETCH_ONLY on a virtual cube. Each stored plan's SQL applies the
     * subselect built for its own cube (#44), so a prefetched value may
     * only serve a cell read with that same restriction (#42's guard).
     * PinAllProd resets Product to All, which masks the Product subselect
     * for its Quantity read. That read's restriction (none) equals the one
     * built for the evaluator's current measure, Visits, whose cube has no
     * Product. Comparing against that one served the subselect-restricted
     * Quantity (2 / 4) instead of the store totals (48 / 16).
     * M (a Sum) keeps the query in PREFETCH_ONLY. Displayed stored
     * measures must still be served from the prefetch.
     */
    static Stream<Object[]> virtualCubePrefetchPins() {
        return Stream.of(
            new Object[] {"pin only", "[Measures].[PinAllProd], [Measures].[M]",
                Arrays.asList(48d, 6d, 16d, 6d, 1028d, 6d), false},
            new Object[] {"pin next to its stored measure",
                "[Measures].[Quantity], [Measures].[PinAllProd], [Measures].[M]",
                Arrays.asList(2d, 48d, 6d, 4d, 16d, 6d, null, 1028d, 6d), true},
            new Object[] {"measures of both cubes",
                "[Measures].[Visits], [Measures].[Quantity],"
                    + " [Measures].[PinAllProd], [Measures].[M]",
                Arrays.asList(100d, 2d, 48d, 6d, 200d, 4d, 16d, 6d,
                    300d, null, 1028d, 6d), true});
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("virtualCubePrefetchPins")
    void prefetchServesOnlyReadsUnderItsPlanCubeSubselect(String name,
        String measures, List<Double> expected, boolean servesStoredCells)
        throws Exception
    {
        String mdx = "WITH MEMBER [Measures].[PinAllProd] AS"
            + " ([Measures].[Quantity], [Product].[All Products])"
            + " MEMBER [Measures].[M] AS"
            + " Sum({[Store].[All Stores]}, [Measures].[Quantity])"
            + " SELECT {" + measures + "} ON COLUMNS,"
            + " [Store].[Name].Members ON ROWS FROM (SELECT"
            + " {[Product].[P002], [Product].[P004]} ON COLUMNS FROM [Both])";
        assertEquals(expected, run(mdx, false, null).cells());
        Run nqe = run(mdx, true, null);
        assertEquals(expected, nqe.cells(), nqe.log().toString());
        assertTrue(nqe.log().contains("NQE: mode=PREFETCH_ONLY"), nqe.log().toString());
        assertTrue(nqe.log().stream().anyMatch(line ->
            line.startsWith("NQE PREFETCH_ONLY: context attached")), nqe.log().toString());
        if (servesStoredCells) {
            assertTrue(nqe.log().stream().anyMatch(line ->
                line.matches("NQE prefetch: hits=[1-9].*")), nqe.log().toString());
        }
    }

    static Stream<Boolean> modes() { return Stream.of(false, true); }

    @ParameterizedTest @MethodSource("modes")
    void coveredAggregateRemainsNative(boolean prefetch) throws Exception {
        String mdx = "SELECT {[Measures].[Quantity]"
            + (prefetch ? ", [Measures].[NativeQty]" : "")
            + "} ON COLUMNS, [Store].[Name].Members ON ROWS FROM [Navigation]";
        List<Double> expected = prefetch
            ? Arrays.asList(48d, 48d, 16d, 16d, 1028d, 1028d)
            : Arrays.asList(48d, 16d, 1028d);
        assertEquals(expected, execute(mdx, true, prefetch, true));
    }

    private List<Double> execute(String mdx, boolean nativeEnabled,
        boolean prefetch, boolean expectAggregate) throws Exception
    {
        return execute(mdx, nativeEnabled, prefetch, expectAggregate, null);
    }

    private List<Double> execute(String mdx, boolean nativeEnabled,
        boolean prefetch, boolean expectAggregate, String nativeWhere)
        throws Exception
    {
        Run run = run(mdx, nativeEnabled, nativeWhere);
        assertEquals(Arrays.asList("S1", "S2", "S3"), run.rows());
        if (nativeEnabled && nativeWhere != null) {
            // Native-only queries intentionally use NSC directly.
            assertTrue(run.log().stream().anyMatch(line ->
                line.contains("NQE: mode=BYPASS")), run.log().toString());
        }
        if (nativeEnabled && nativeWhere == null) {
            assertTrue(run.log().stream().anyMatch(line -> line.contains(
                "NQE: mode=" + (prefetch ? "PREFETCH_ONLY" : "FULL_RESULT"))),
                run.log().toString());
            assertTrue(run.log().stream().anyMatch(line -> line.contains("executing SQL")),
                "Regression must exercise NQE execution: " + run.log());
            if (expectAggregate) {
                assertTrue(run.log().stream().anyMatch(line -> line.contains("FROM f f")),
                    "Covered aggregate must remain usable: " + run.log());
            }
        }
        return run.cells();
    }

    /** Row captions, cells in row-major order and the captured NQE log. */
    private record Run(List<String> rows, List<Double> cells, List<String> log) {
        /** NQE owned the whole result: no plan fell back to legacy. */
        void assertNqeFullResult() {
            assertTrue(log.contains("NQE: mode=FULL_RESULT"), log.toString());
            assertTrue(log.stream().anyMatch(line -> line.contains("executing SQL")),
                "Regression must exercise NQE execution: " + log);
            assertTrue(log.stream().noneMatch(line -> line.contains("fall")),
                "NQE must not fall back: " + log);
        }
    }

    private Run run(String mdx, boolean nativeEnabled, String nativeWhere)
        throws Exception
    {
        MondrianProperties p = MondrianProperties.instance();
        Map<String, String> saved = new LinkedHashMap<>();
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("mondrian.native.queryEngine.enable", "" + nativeEnabled);
        settings.put("mondrian.native.sql.enable", "" + (nativeWhere != null));
        settings.put("mondrian.rolap.aggregates.Use", "true");
        settings.put("mondrian.rolap.aggregates.Read", "false");
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            saved.put(entry.getKey(), p.getProperty(entry.getKey()));
            p.setProperty(entry.getKey(), entry.getValue());
        }
        int previousCache = p.LevelPreCacheThreshold.get();
        p.LevelPreCacheThreshold.set(0);
        try (Capture capture = new Capture()) {
            mondrian.olap.Connection connection = open(16, nativeWhere);
            try {
                Result result = connection.execute(connection.parseQuery(mdx));
                List<Position> rows = result.getAxes()[1].getPositions();
                List<Double> values = new ArrayList<>();
                for (int r = 0; r < rows.size(); r++) {
                    for (int c = 0; c < result.getAxes()[0].getPositions().size(); c++) {
                        Cell cell = result.getCell(new int[] {c, r});
                        values.add(cell.isNull() ? null : ((Number) cell.getValue()).doubleValue());
                    }
                }
                return new Run(
                    rows.stream().map(row -> row.stream().map(Member::getName)
                        .collect(java.util.stream.Collectors.joining(","))).toList(),
                    values, capture.lines);
            } finally {
                connection.close();
            }
        } finally {
            p.LevelPreCacheThreshold.set(previousCache);
            for (Map.Entry<String, String> entry : saved.entrySet()) {
                if (entry.getValue() == null) {
                    p.remove(entry.getKey());
                } else {
                    p.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private static String name(int product) {
        return String.format("P%03d", product);
    }

    private static final String TEMPLATE =
        "SELECT ${axisResultSelectList} SUM(pr.q) AS val FROM ("
            + "SELECT f.qty AS q, 1 AS g ${axisPresenceSelectList} "
            + "FROM fact f ${joinClauses} WHERE %s) pr "
            + "GROUP BY ${axisGroupByList} pr.g";

    private static String nativeMeasure(String name, String where) {
        String template = TEMPLATE.formatted(where);
        if (where.contains("whereClauseExcept:")) {
            // NativeSqlCalc binds ordinary columns directly on its source.
            template = template.replace("FROM fact f ${joinClauses}", "FROM wide f");
        }
        return """
            <CalculatedMember name="%s" dimension="Measures">
              <Annotations>
                <Annotation name="nativeSql.enabled">true</Annotation>
                <Annotation name="nativeSql.fallbackMdx">false</Annotation>
                <Annotation name="nativeSql.template"><![CDATA[%s]]></Annotation>
              </Annotations>
              <Formula>[Measures].[Quantity]</Formula>
            </CalculatedMember>
            """.formatted(name, template);
    }

    private static mondrian.olap.Connection open(int products, String nativeWhere)
        throws Exception
    {
        String jdbc = "jdbc:h2:mem:nqe_subselect_" + UUID.randomUUID().toString().replace("-", "")
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR,MONTH,WEEK";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "")) {
            Statement sql = db.createStatement();
            sql.execute("CREATE TABLE calendar (id INT, year INT, month INT, week INT, flat_week INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025,12,52,202552),(2,2026,8,35,202635),"
                + "(3,2026,8,36,202636),(4,2026,9,37,202637),(5,2027,1,1,202701),(6,2025,8,34,202534)");
            sql.execute("CREATE TABLE product (id INT, product_name VARCHAR, manufacturer VARCHAR)");
            sql.execute("CREATE TABLE store (id INT, store_name VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'S1'),(2,'S2'),(3,'S3')");
            sql.execute("CREATE TABLE fact (calendar_id INT, product_id INT, store_id INT, qty INT)");
            for (int i = 1; i <= products; i++) {
                sql.execute("INSERT INTO product VALUES (" + i + ",'" + name(i) + "','"
                    + (i % 2 == 0 ? "Red" : "Blue") + "')");
                switch (i % 4) {
                case 0 -> sql.execute("INSERT INTO fact VALUES (2," + i + "," + (1 + (i / 4) % 2) + ","
                    + (i % 16 == 8 ? 0 : i) + ")");
                case 1 -> sql.execute("INSERT INTO fact VALUES (2," + i + ",3," + i + ")");
                case 2 -> sql.execute("INSERT INTO fact VALUES (3," + i + ",1," + i + ")");
                default -> { }
                }
            }
            // August 2025 (same month key 8 as August 2026): P003, S3, 1000
            sql.execute("INSERT INTO fact VALUES (6,3,3,1000)");
            // Aggregate that LACKS product and calendar: store only.
            // Table is NAMED "f" on purpose: NQE renders agg measures as
            // sum("<aggTable>"."qty") while aliasing the table as f. ClickHouse
            // accepts the original table name next to an alias, H2 does not.
            sql.execute("CREATE TABLE \"f\" (store_name VARCHAR, flat_week INT, qty INT, fact_count INT)");
            sql.execute("INSERT INTO \"f\" SELECT s.store_name, c.flat_week, SUM(x.qty), COUNT(*) "
                + "FROM fact x JOIN store s ON s.id = x.store_id JOIN calendar c ON c.id = x.calendar_id "
                + "GROUP BY s.store_name, c.flat_week");
            sql.execute("CREATE TABLE fact2 (store_id INT, visits INT)");
            sql.execute("INSERT INTO fact2 VALUES (1,100),(2,200),(3,300)");
            sql.execute("CREATE TABLE wide AS SELECT x.calendar_id, x.product_id, x.store_id, x.qty, "
                + "s.store_name, p.product_name, p.manufacturer, c.flat_week, c.year, c.month, c.week "
                + "FROM fact x JOIN store s ON s.id = x.store_id JOIN product p ON p.id = x.product_id "
                + "JOIN calendar c ON c.id = x.calendar_id");
        }
        Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="NqeSubselect">
              <Dimension name="Calendar" type="TimeDimension">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="calendar"/>
                  <Level name="Year" column="year" type="Integer" levelType="TimeYears" uniqueMembers="true"/>
                  <Level name="Month" column="month" type="Integer" levelType="TimeMonths" uniqueMembers="false"/>
                  <Level name="Week" column="week" type="Integer" levelType="TimeWeeks" uniqueMembers="false"/>
                </Hierarchy>
                <Hierarchy name="FlatWeek" hasAll="true" primaryKey="id"><Table name="calendar"/>
                  <Level name="Week" column="flat_week" type="Integer" levelType="TimeWeeks" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="product_name" uniqueMembers="true"/>
                </Hierarchy>
                <Hierarchy name="Manufacturer" hasAll="true" allMemberName="All Mfr" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="manufacturer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Store">
                <Hierarchy hasAll="true" allMemberName="All Stores" primaryKey="id"><Table name="store"/>
                  <Level name="Name" column="store_name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Navigation">
                <Table name="fact">
                  <AggName name="f">
                    <AggFactCount column="fact_count"/>
                    <AggMeasure name="[Measures].[Quantity]" column="qty"/>
                    <AggLevel name="[Store].[Name]" column="store_name"/>
                    <AggLevel name="[Calendar.FlatWeek].[Week]" column="flat_week"/>
                  </AggName>
                </Table>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                <CalculatedMember name="QtyAllMfr" dimension="Measures">
                  <Formula>([Measures].[Quantity], [Product.Manufacturer].[All Mfr])</Formula>
                </CalculatedMember>
                %s
              </Cube>
              <Cube name="Other">
                <Table name="fact2"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="Visits" column="visits" aggregator="sum"/>
              </Cube>
              <VirtualCube name="Both" defaultMeasure="Visits">
                <VirtualCubeDimension name="Store"/>
                <VirtualCubeDimension cubeName="Navigation" name="Product"/>
                <VirtualCubeMeasure cubeName="Other" name="[Measures].[Visits]"/>
                <VirtualCubeMeasure cubeName="Navigation" name="[Measures].[Quantity]"/>
              </VirtualCube>
            </Schema>
            """.formatted(nativeMeasure("NativeQty",
                nativeWhere == null ? "${whereClause}" : nativeWhere)));
        return mondrian.olap.DriverManager.getConnection(props, null);
    }

    private static final class Capture extends AbstractAppender implements AutoCloseable {
        private static final String[] LOGGERS = {
            "mondrian.rolap.NativeQuerySqlGenerator", "mondrian.rolap.NativeQueryEngine",
            // RolapResult logs the prefetch hit counts through ResultBase's logger.
            "mondrian.olap.ResultBase"
        };
        private final Map<String, LoggerConfig> saved = new LinkedHashMap<>();
        final List<String> lines = new ArrayList<>();
        Capture() {
            super("nqe-subselect", null, null, true, Property.EMPTY_ARRAY);
            start();
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration config = context.getConfiguration();
            for (String name : LOGGERS) {
                saved.put(name, config.getLoggers().get(name));
                LoggerConfig logger = new LoggerConfig(name, Level.INFO, false);
                logger.addAppender(this, Level.INFO, null);
                config.addLogger(name, logger);
            }
            context.updateLoggers();
        }
        @Override public void append(LogEvent event) {
            lines.add(event.getMessage().getFormattedMessage());
        }
        @Override public void close() {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration config = context.getConfiguration();
            for (String name : LOGGERS) {
                config.removeLogger(name);
                if (saved.get(name) != null) {
                    config.addLogger(name, saved.get(name));
                }
            }
            context.updateLoggers();
            stop();
        }
    }
}
