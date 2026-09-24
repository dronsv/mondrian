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

import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.rolap.agg.PredicateCanonicalizer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A subselect restriction, and the canonical string cell caching keys on, are
 * a function of the query and the cube — not of the cell. Reading a cell
 * rebuilt both, so a wide subselect cost one predicate-tree walk and one
 * multi-kilobyte string per cell read.
 *
 * <p>Two things are pinned here: the cost, as counters that must follow the
 * query's distinct scopes rather than its axis, and the identity, as cells
 * that must keep differing whenever the restriction differs — between
 * subselects, between the base cubes of a virtual cube, between parameter
 * bindings, and between the cells of a subselect evaluated per cell.</p>
 */
public class SubcubeRestrictionMemoTest {
    private static final int SMALL = 8;
    private static final int LARGE = 80;
    private static final int STORES = 3;
    /** Headroom for scopes that do not depend on the axis; a per-cell cost exceeds it many times over. */
    private static final int SLACK = 8;

    private static final String NATIVE_QUERY_ENGINE =
        "mondrian.native.queryEngine.enable";

    private final List<mondrian.olap.Connection> connections =
        new ArrayList<>();
    private String previousNativeQueryEngine;
    private int previousPreCache;

    @BeforeEach void saveProperties() {
        previousPreCache =
            MondrianProperties.instance().LevelPreCacheThreshold.get();
        MondrianProperties.instance().LevelPreCacheThreshold.set(0);
        previousNativeQueryEngine =
            MondrianProperties.instance().getProperty(NATIVE_QUERY_ENGINE);
    }

    @AfterEach void close() {
        MondrianProperties.instance().LevelPreCacheThreshold
            .set(previousPreCache);
        if (previousNativeQueryEngine == null) {
            MondrianProperties.instance().remove(NATIVE_QUERY_ENGINE);
        } else {
            MondrianProperties.instance()
                .setProperty(NATIVE_QUERY_ENGINE, previousNativeQueryEngine);
        }
        connections.forEach(mondrian.olap.Connection::close);
    }

    private static String name(int product) {
        return String.format("P%03d", product);
    }

    /** The one store a product sells in, in the Sparse cube. */
    private static int sparseStore(int product) {
        return (product - 1) % STORES + 1;
    }

    /**
     * Even products are Red, odd ones Blue. Every product sells {@code id} of
     * itself in every store, and holds {@code 10 * id} in stock in store 1
     * only — so a restriction that reaches one base cube of the virtual cube
     * has a visibly different effect on the other.
     */
    private mondrian.olap.Connection open(int products) throws Exception {
        String jdbc = "jdbc:h2:mem:subcubememo_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
        try (java.sql.Connection db =
                 DriverManager.getConnection(jdbc, "sa", ""))
        {
            Statement sql = db.createStatement();
            sql.execute(
                "CREATE TABLE product (id INT, name VARCHAR,"
                + " manufacturer VARCHAR)");
            sql.execute("CREATE TABLE store (id INT, name VARCHAR)");
            for (int s = 1; s <= STORES; s++) {
                sql.execute("INSERT INTO store VALUES (" + s + ",'S" + s + "')");
            }
            sql.execute(
                "CREATE TABLE fact (product_id INT, store_id INT, qty INT)");
            sql.execute(
                "CREATE TABLE stock (product_id INT, store_id INT, qty INT)");
            sql.execute(
                "CREATE TABLE sparse (product_id INT, store_id INT, qty INT)");
            for (int i = 1; i <= products; i++) {
                sql.execute(
                    "INSERT INTO product VALUES (" + i + ",'" + name(i)
                    + "','" + (i % 2 == 0 ? "Red" : "Blue") + "')");
                for (int s = 1; s <= STORES; s++) {
                    sql.execute(
                        "INSERT INTO fact VALUES (" + i + "," + s + "," + i
                        + ")");
                }
                sql.execute(
                    "INSERT INTO stock VALUES (" + i + ",1," + (10 * i) + ")");
                sql.execute(
                    "INSERT INTO sparse VALUES (" + i + "," + sparseStore(i)
                    + "," + i + ")");
            }
        }
        Util.PropertyList props =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="SubcubeMemo">
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
                <Hierarchy name="Manufacturer" hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="manufacturer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Store">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="store"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="fact"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
              <Cube name="Sparse"><Table name="sparse"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
              <Cube name="Stock"><Table name="stock"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="StockQuantity" column="qty" aggregator="sum"/>
              </Cube>
              <VirtualCube name="Combined">
                <VirtualCubeDimension name="Product"/>
                <VirtualCubeDimension name="Store"/>
                <VirtualCubeMeasure cubeName="Sales" name="[Measures].[Quantity]"/>
                <VirtualCubeMeasure cubeName="Stock" name="[Measures].[StockQuantity]"/>
              </VirtualCube>
            </Schema>
            """);
        mondrian.olap.Connection connection =
            mondrian.olap.DriverManager.getConnection(props, null);
        connections.add(connection);
        return connection;
    }

    private record Run(
        List<String> cells, long canonicalizations, long builds, long kept) { }

    private Run run(mondrian.olap.Connection connection, String mdx) {
        Query query = connection.parseQuery(mdx);
        long before = PredicateCanonicalizer.canonicalizationCount();
        Result result = connection.execute(query);
        List<String> cells = cells(result);
        return new Run(
            cells,
            PredicateCanonicalizer.canonicalizationCount() - before,
            query.getSubcubePredicateBuilds(),
            query.getSubcubeRestrictionsKept());
    }

    private Run run(int products, String mdx) throws Exception {
        return run(open(products), mdx);
    }

    private static String names(Position position) {
        return String.join(
            ",", position.stream().map(m -> m.getUniqueName()).toList());
    }

    /** One entry per cell, rows first: {@code "row / column=value"}. */
    private static List<String> cells(Result result) {
        List<Position> columns = result.getAxes()[0].getPositions();
        List<String> cells = new ArrayList<>();
        if (result.getAxes().length == 1) {
            for (int c = 0; c < columns.size(); c++) {
                cells.add(
                    names(columns.get(c)) + "="
                    + result.getCell(new int[] {c}).getValue());
            }
            return cells;
        }
        List<Position> rows = result.getAxes()[1].getPositions();
        for (int r = 0; r < rows.size(); r++) {
            for (int c = 0; c < columns.size(); c++) {
                cells.add(
                    names(rows.get(r)) + " / " + names(columns.get(c)) + "="
                    + result.getCell(new int[] {c, r}).getValue());
            }
        }
        return cells;
    }

    // -----------------------------------------------------------------
    // Cost
    // -----------------------------------------------------------------

    /** A subselect naming every even product, read over a store x product axis. */
    private static String wideSubselect(int products) {
        StringBuilder set = new StringBuilder();
        for (int i = 2; i <= products; i += 2) {
            if (set.length() > 0) {
                set.append(",");
            }
            set.append("[Product].[").append(name(i)).append("]");
        }
        return "SELECT {[Measures].[Quantity]} ON COLUMNS, "
            + "NON EMPTY Crossjoin([Store].[Name].Members,"
            + " [Product].[Name].Members) ON ROWS "
            + "FROM (SELECT {" + set + "} ON COLUMNS FROM [Sales])";
    }

    /** Every even product, once per store. */
    private static List<String> evenProducts(int products) {
        List<String> cells = new ArrayList<>();
        for (int store = 1; store <= STORES; store++) {
            for (int i = 2; i <= products; i += 2) {
                cells.add(
                    "[Store].[S" + store + "],[Product].[" + name(i)
                    + "] / [Measures].[Quantity]=" + i + ".0");
            }
        }
        return cells;
    }

    /**
     * The bound: both counters follow the query's distinct scopes, never the
     * size of its axis. A counter stuck at zero proves nothing.
     */
    private static void assertDoesNotGrowWithTheAxis(
        String counter, long small, long large)
    {
        assertTrue(
            small >= 1 && large <= small + SLACK
                && large < STORES * LARGE / 2,
            counter + " grew with the axis: " + small + " for "
            + STORES * SMALL / 2 + " cells, " + large + " for "
            + STORES * LARGE / 2);
    }

    @Test void canonicalizationsDoNotGrowWithTheAxis() throws Exception {
        Run small = run(SMALL, wideSubselect(SMALL));
        Run large = run(LARGE, wideSubselect(LARGE));
        assertEquals(evenProducts(SMALL), numeric(small.cells()));
        assertEquals(evenProducts(LARGE), numeric(large.cells()));
        assertDoesNotGrowWithTheAxis(
            "canonicalizations", small.canonicalizations(),
            large.canonicalizations());
        assertDoesNotGrowWithTheAxis(
            "subcube predicate builds", small.builds(), large.builds());
        assertDoesNotGrowWithTheAxis(
            "kept restrictions", small.kept(), large.kept());
    }

    /** The native query engine is on by default: the bound must hold there too. */
    @Test void canonicalizationsDoNotGrowWithTheAxisNatively()
        throws Exception
    {
        MondrianProperties.instance()
            .setProperty(NATIVE_QUERY_ENGINE, "true");
        Run small = run(SMALL, wideSubselect(SMALL));
        Run large = run(LARGE, wideSubselect(LARGE));
        assertEquals(evenProducts(SMALL), numeric(small.cells()));
        assertEquals(evenProducts(LARGE), numeric(large.cells()));
        assertDoesNotGrowWithTheAxis(
            "canonicalizations", small.canonicalizations(),
            large.canonicalizations());
        assertDoesNotGrowWithTheAxis(
            "subcube predicate builds", small.builds(), large.builds());
    }

    /**
     * Each cell with its value as a double: a read served from the engine's
     * prefetch keeps the stored integer, one it does not cover comes back as a
     * double, and which is which is the engine's choice, not this test's.
     */
    private static List<String> numeric(List<String> cells) {
        return cells.stream().map(cell -> {
            int start = cell.lastIndexOf('=') + 1;
            String value = cell.substring(start);
            return value.equals("null")
                ? cell
                : cell.substring(0, start) + Double.parseDouble(value);
        }).toList();
    }

    // -----------------------------------------------------------------
    // Identity: what a shared restriction must never do
    // -----------------------------------------------------------------

    /**
     * The same query on the same data, read once with the memo keeping nothing
     * — every build private to its cell, as before the memo — and once with it
     * on. A restriction shared where it should not be shows up as a cell that
     * differs between the two.
     */
    private void assertMemoChangesNoCell(String what, int products, String... mdx)
        throws Exception
    {
        List<List<String>> eager = new ArrayList<>();
        int capacity = SubcubeRestriction.memoCapacity;
        SubcubeRestriction.memoCapacity = 0;
        try {
            mondrian.olap.Connection connection = open(products);
            for (String one : mdx) {
                eager.add(numeric(run(connection, one).cells()));
            }
        } finally {
            SubcubeRestriction.memoCapacity = capacity;
        }
        mondrian.olap.Connection connection = open(products);
        for (int i = 0; i < mdx.length; i++) {
            assertEquals(
                eager.get(i), numeric(run(connection, mdx[i]).cells()),
                what + ": query " + (i + 1) + " read different cells with the"
                + " memo on");
        }
    }

    private static String manufacturerSubselect(String manufacturer) {
        return "SELECT {[Measures].[Quantity]} ON COLUMNS, "
            + "{[Store].[S1]} ON ROWS "
            + "FROM (SELECT {[Product.Manufacturer].[" + manufacturer
            + "]} ON COLUMNS FROM [Sales])";
    }

    /**
     * Two queries differing only in their subselect, on one connection and so
     * against one warm cell cache: the second must not be served the first
     * one's cells. Red is the even products (2+4+6+8), Blue the odd ones
     * (1+3+5+7).
     */
    @Test void subselectsDoNotShareCells() throws Exception {
        mondrian.olap.Connection connection = open(SMALL);
        assertEquals(
            List.of("[Store].[S1] / [Measures].[Quantity]=20.0"),
            numeric(run(connection, manufacturerSubselect("Red")).cells()));
        assertEquals(
            List.of("[Store].[S1] / [Measures].[Quantity]=16.0"),
            numeric(run(connection, manufacturerSubselect("Blue")).cells()));
        // ... and back, so neither order hides a stale entry.
        assertEquals(
            List.of("[Store].[S1] / [Measures].[Quantity]=20.0"),
            numeric(run(connection, manufacturerSubselect("Red")).cells()));
    }

    /**
     * Two measures of a virtual cube, read in one execution and so against one
     * memo: each is restricted in its own base cube. Stock exists in store 1
     * only, so a restriction scoped to the wrong cube would show.
     */
    private static final String VIRTUAL_CUBE_SUBSELECT =
        "SELECT {[Measures].[Quantity],[Measures].[StockQuantity]}"
        + " ON COLUMNS, {[Store].[S1],[Store].[S2]} ON ROWS "
        + "FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS"
        + " FROM [Combined])";

    @Test void virtualCubeMeasuresDoNotShareARestriction() throws Exception {
        // Red products are 2,4,6,8: 20 sold per store, 200 in stock in S1.
        Run run = run(open(SMALL), VIRTUAL_CUBE_SUBSELECT);
        assertEquals(
            List.of(
                "[Store].[S1] / [Measures].[Quantity]=20.0",
                "[Store].[S1] / [Measures].[StockQuantity]=200.0",
                "[Store].[S2] / [Measures].[Quantity]=20.0",
                "[Store].[S2] / [Measures].[StockQuantity]=null"),
            numeric(run.cells()));
        assertTrue(
            run.kept() >= 2,
            "the two base cubes shared one restriction: kept " + run.kept());
        assertMemoChangesNoCell(
            "virtual cube", SMALL, VIRTUAL_CUBE_SUBSELECT,
            VIRTUAL_CUBE_SUBSELECT.replace("[Red]", "[Blue]"),
            "SELECT {[Measures].[Quantity],[Measures].[StockQuantity]}"
            + " ON COLUMNS, {[Store].[S1],[Store].[S2]} ON ROWS"
            + " FROM [Combined]");
    }

    /**
     * An explicit All member in a formula escapes the subselect of its
     * hierarchy, so the cell it reads is restricted by a different mask than
     * the cell beside it. Both are read in one execution, from one base cube:
     * only the mask tells their restrictions apart.
     */
    private static final String ESCAPING_FORMULA =
        "WITH MEMBER [Measures].[AllManufacturers] AS"
        + " ([Measures].[Quantity], [Product.Manufacturer].DefaultMember) "
        + "SELECT {[Measures].[Quantity],[Measures].[AllManufacturers]}"
        + " ON COLUMNS, {[Store].[S1]} ON ROWS "
        + "FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS"
        + " FROM [Sales])";

    @Test void maskedAndUnmaskedCellsDoNotShareARestriction()
        throws Exception
    {
        // Red is 2+4+6+8; escaping the subselect reaches all of 1..8.
        Run run = run(open(SMALL), ESCAPING_FORMULA);
        assertEquals(
            List.of(
                "[Store].[S1] / [Measures].[Quantity]=20.0",
                "[Store].[S1] / [Measures].[AllManufacturers]=36.0"),
            numeric(run.cells()));
        assertTrue(
            run.kept() >= 2,
            "the masked and unmasked cells shared one restriction: kept "
            + run.kept());
        assertMemoChangesNoCell("subcube mask", SMALL, ESCAPING_FORMULA);
    }

    /**
     * One parsed query, two parameter bindings. The binding is read while the
     * subselect set is evaluated, which is a context-dependent step and so
     * never memoized — this pins the cells that would break if it ever were.
     */
    @Test void parameterBindingsDoNotShareCells() throws Exception {
        mondrian.olap.Connection connection = open(SMALL);
        Query query = connection.parseQuery(
            "SELECT {[Measures].[Quantity]} ON COLUMNS, {[Store].[S1]} ON ROWS"
            + " FROM (SELECT Filter([Product.Manufacturer].[Name].Members,"
            + " [Product.Manufacturer].CurrentMember.Name ="
            + " Parameter(\"M\", STRING, \"Red\")) ON COLUMNS FROM [Sales])");
        assertEquals(
            List.of("[Store].[S1] / [Measures].[Quantity]=20.0"),
            numeric(cells(connection.execute(query))));
        query.setParameter("M", "Blue");
        assertEquals(
            List.of("[Store].[S1] / [Measures].[Quantity]=16.0"),
            numeric(cells(connection.execute(query))));
        query.setParameter("M", "Red");
        assertEquals(
            List.of("[Store].[S1] / [Measures].[Quantity]=20.0"),
            numeric(cells(connection.execute(query))));
    }

    /**
     * A subselect set evaluated while a cell is read restricts each cell in
     * its own context, so no build of it may be kept. In the Sparse cube each
     * product sells in one store only, so the set the filter selects — and
     * therefore the cell — differs per row: a restriction shared between the
     * rows would collapse them onto whichever row built it first.
     */
    private static final String PER_CELL_SUBSELECT =
        "SELECT {[Measures].[Quantity]} ON COLUMNS,"
        + " [Store].[Name].Members ON ROWS "
        + "FROM (SELECT Filter([Product].[Name].Members,"
        + " [Measures].[Quantity] > 4) ON COLUMNS FROM [Sparse])";

    @Test void perCellSubselectIsNeverShared() throws Exception {
        // Products 7 (S1), 5 and 8 (S2), 6 (S3) sell more than 4.
        List<String> perRow = List.of(
            "[Store].[S1] / [Measures].[Quantity]=7.0",
            "[Store].[S2] / [Measures].[Quantity]=13.0",
            "[Store].[S3] / [Measures].[Quantity]=6.0");
        Run run = run(open(SMALL), PER_CELL_SUBSELECT);
        assertEquals(perRow, numeric(run.cells()));
        assertTrue(run.builds() >= 1, "nothing was built at all");
        assertEquals(
            0, run.kept(),
            "a restriction whose build read the evaluator's own context was"
            + " kept for reuse");
        assertMemoChangesNoCell(
            "per-cell subselect", SMALL, PER_CELL_SUBSELECT);
    }

    /** Same, with the native query engine explicitly on. */
    @Test void perCellSubselectIsNeverSharedNatively() throws Exception {
        MondrianProperties.instance()
            .setProperty(NATIVE_QUERY_ENGINE, "true");
        Run run = run(open(SMALL), PER_CELL_SUBSELECT);
        assertTrue(run.builds() >= 1, "nothing was built at all");
        assertEquals(0, run.kept(), "a per-cell restriction was kept");
        assertMemoChangesNoCell(
            "per-cell subselect", SMALL, PER_CELL_SUBSELECT);
    }

    /**
     * The memo starts over rather than growing without bound, and a query that
     * outruns either bound still reads the same cells.
     */
    @Test void aFullMemoStartsOverWithoutChangingCells() throws Exception {
        List<String> eager;
        int capacity = SubcubeRestriction.memoCapacity;
        int weight = SubcubeRestriction.memoWeightCapacity;
        SubcubeRestriction.memoCapacity = 0;
        try {
            eager = numeric(run(SMALL, wideSubselect(SMALL)).cells());
        } finally {
            SubcubeRestriction.memoCapacity = capacity;
        }
        assertEquals(evenProducts(SMALL), eager);
        SubcubeRestriction.memoCapacity = 1;
        try {
            assertEquals(
                eager, numeric(run(SMALL, wideSubselect(SMALL)).cells()));
        } finally {
            SubcubeRestriction.memoCapacity = capacity;
        }
        SubcubeRestriction.memoWeightCapacity = 1;
        try {
            Run oversized = run(SMALL, wideSubselect(SMALL));
            assertEquals(eager, numeric(oversized.cells()));
            assertTrue(oversized.builds() > 0, "the restriction must be built");
            assertEquals(0, oversized.kept(),
                "an entry larger than the whole weight budget must not be retained");
        } finally {
            SubcubeRestriction.memoWeightCapacity = weight;
        }
    }
}

// End SubcubeRestrictionMemoTest.java
