package mondrian.rolap;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mondrian.olap.Evaluator;
import mondrian.olap.MondrianException;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Query;
import mondrian.olap.QueryCanceledException;
import mondrian.olap.Result;
import mondrian.olap.ResultBase;
import mondrian.olap.Syntax;
import mondrian.olap.Util;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #97: navigation in dimension context must build one constraint per distinct
 * context, not one per evaluated tuple. Expected cells were captured from the
 * engine before the reuse existed; the reuse must not change any of them.
 */
public class DimensionContextConstraintReuseTest {
    private static final int SMALL = 16;
    private static final int LARGE = 160;
    private static final int STORES = 3;
    /** Headroom for builds that do not depend on the axis; a per-tuple cost exceeds it many times over. */
    private static final int SLACK = 8;

    private static final String CLOSING_QTY = "WITH MEMBER [Measures].[ClosingQty] AS "
        + "IIF(([Measures].[Quantity], ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember)) = 0, NULL, "
        + "([Measures].[Quantity], ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember))) ";
    private static final String BOUNDARY_MEMBER = "MEMBER [Measures].[Boundary] AS "
        + "ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember).UniqueName ";
    private static final String BOUNDARY = "WITH " + BOUNDARY_MEMBER;
    /** A calculated sibling member over a fixed set: weeks 35 and 36 close on week 36. */
    private static final String PICK_MEMBER = "MEMBER [Calendar.FlatWeek].[Pick] AS "
        + "Aggregate({[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]}) ";
    private static final String STORE_BY_PRODUCT =
        "NON EMPTY CrossJoin([Store].[Name].Members, [Product].[Name].Members)";
    private static final String RED_NAVIGATION = redSubselect("Navigation");
    private static final String TRACE_ENABLED = "mondrian.rolap.traceSlowQuery.enabled";
    private static final String TRACE_THRESHOLD_MS = "mondrian.rolap.traceSlowQuery.thresholdMs";
    private static final String NATIVE_QUERY_ENGINE = "mondrian.native.queryEngine.enable";

    private final List<mondrian.olap.Connection> connections = new ArrayList<>();
    private String previousNativeQueryEngine;
    private int previousPreCache;

    @BeforeEach void saveProperties() {
        previousPreCache = MondrianProperties.instance().LevelPreCacheThreshold.get();
        MondrianProperties.instance().LevelPreCacheThreshold.set(0);
        previousNativeQueryEngine = MondrianProperties.instance().getProperty(NATIVE_QUERY_ENGINE);
        MondrianProperties.instance().setProperty(NATIVE_QUERY_ENGINE, "false");
    }

    @AfterEach void close() {
        MondrianProperties.instance().LevelPreCacheThreshold.set(previousPreCache);
        restoreProperty(NATIVE_QUERY_ENGINE, previousNativeQueryEngine);
        connections.forEach(mondrian.olap.Connection::close);
    }

    private static void restoreProperty(String name, String previous) {
        if (previous == null) {
            MondrianProperties.instance().remove(name);
        } else {
            MondrianProperties.instance().setProperty(name, previous);
        }
    }

    private static String name(int product) {
        return String.format("P%03d", product);
    }

    /** Store holding the week-35 fact of a product divisible by four. */
    private static int week35Store(int product) {
        return 1 + (product / 4) % 2;
    }

    /** Week-35 quantity of a product divisible by four; every other one in S1 is a stored zero. */
    private static int week35Quantity(int product) {
        return product % 16 == 8 ? 0 : product;
    }

    /**
     * Even products are Red, odd ones Blue. By {@code product % 4}: 0 sells in
     * week 35 (S1 or S2), 2 sells in week 36 (S1), 1 sells in week 35 (S3,
     * Blue, so outside a Red subselect), 3 never sells.
     */
    private mondrian.olap.Connection open(int products) throws Exception {
        String jdbc = "jdbc:h2:mem:reuse_" + UUID.randomUUID().toString().replace("-", "")
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR,MONTH,WEEK";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "")) {
            Statement sql = db.createStatement();
            sql.execute("CREATE TABLE calendar (id INT, year INT, month INT, week INT, flat_week INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025,12,52,202552),(2,2026,8,35,202635),"
                + "(3,2026,8,36,202636),(4,2026,9,37,202637),(5,2027,1,1,202701)");
            sql.execute("CREATE TABLE product (id INT, name VARCHAR, manufacturer VARCHAR)");
            sql.execute("CREATE TABLE store (id INT, name VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'S1'),(2,'S2'),(3,'S3')");
            sql.execute("CREATE TABLE fact (calendar_id INT, product_id INT, store_id INT, qty INT)");
            sql.execute("CREATE TABLE stock (calendar_id INT, product_id INT, store_id INT, qty INT)");
            sql.execute("INSERT INTO stock VALUES (2,4,1,100),(3,2,1,300)");
            for (int i = 1; i <= products; i++) {
                sql.execute("INSERT INTO product VALUES (" + i + ",'" + name(i) + "','"
                    + (i % 2 == 0 ? "Red" : "Blue") + "')");
                switch (i % 4) {
                case 0 -> sql.execute("INSERT INTO fact VALUES (2," + i + "," + week35Store(i) + ","
                    + week35Quantity(i) + ")");
                case 1 -> sql.execute("INSERT INTO fact VALUES (2," + i + ",3," + i + ")");
                case 2 -> sql.execute("INSERT INTO fact VALUES (3," + i + ",1," + i + ")");
                default -> { }
                }
            }
        }
        Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="ConstraintReuse">
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
              <Cube name="Navigation"><Table name="fact"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
              <Cube name="Stock"><Table name="stock"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="StockQuantity" column="qty" aggregator="sum"/>
              </Cube>
              <VirtualCube name="Combined">
                <VirtualCubeDimension name="Calendar"/>
                <VirtualCubeDimension name="Product"/>
                <VirtualCubeDimension name="Store"/>
                <VirtualCubeMeasure cubeName="Navigation" name="[Measures].[Quantity]"/>
                <VirtualCubeMeasure cubeName="Stock" name="[Measures].[StockQuantity]"/>
              </VirtualCube>
              <UserDefinedFunction name="CancelProbe"
                  className="mondrian.rolap.DimensionContextConstraintReuseTest$CancelProbe"/>
            </Schema>
            """);
        mondrian.olap.Connection connection = mondrian.olap.DriverManager.getConnection(props, null);
        connections.add(connection);
        return connection;
    }

    private record Run(List<String> cells, long builds, long idResolutions) { }

    /** Runs on a fresh fixture, so no member cache or schema is shared between sizes. */
    private Run run(int products, String mdx) throws Exception {
        mondrian.olap.Connection connection = open(products);
        Query query = connection.parseQuery(mdx);
        Result result = connection.execute(query);
        return new Run(cells(result),
            ((ResultBase) result).getExecution().getDimensionContextConstraintBuilds(),
            query.getSubcubeIdResolutions());
    }

    private static String names(Position position) {
        return String.join(",", position.stream().map(member -> member.getUniqueName()).toList());
    }

    /** One entry per cell, rows first: {@code "row / column=value"}, or {@code "column=value"} without rows. */
    private static List<String> cells(Result result) {
        List<Position> columns = result.getAxes()[0].getPositions();
        List<String> cells = new ArrayList<>();
        if (result.getAxes().length == 1) {
            for (int c = 0; c < columns.size(); c++) {
                cells.add(names(columns.get(c)) + "=" + result.getCell(new int[] {c}).getValue());
            }
            return cells;
        }
        List<Position> rows = result.getAxes()[1].getPositions();
        for (int r = 0; r < rows.size(); r++) {
            for (int c = 0; c < columns.size(); c++) {
                cells.add(names(rows.get(r)) + " / " + names(columns.get(c)) + "="
                    + result.getCell(new int[] {c, r}).getValue());
            }
        }
        return cells;
    }

    private void assertDoesNotGrowWithTheAxis(String counter, ToLongFunction<Run> value, String mdx)
        throws Exception
    {
        assertDoesNotGrowWithTheAxis(counter, value.applyAsLong(run(SMALL, mdx)), value.applyAsLong(run(LARGE, mdx)));
    }

    /**
     * The #97 bound: the counter follows the distinct contexts of the query,
     * never the size of its axis. A counter stuck at zero proves nothing.
     */
    private static void assertDoesNotGrowWithTheAxis(String counter, long small, long large) {
        assertTrue(small >= 1 && large <= small + SLACK && large < STORES * LARGE,
            counter + " grew with the axis: " + small + " for " + STORES * SMALL + " axis tuples, "
                + large + " for " + STORES * LARGE);
    }

    private static String redSubselect(String cube) {
        return "FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS FROM [" + cube + "]) ";
    }

    private static String productionShape(String cube) {
        return CLOSING_QTY + "SELECT " + STORE_BY_PRODUCT + " ON COLUMNS " + redSubselect(cube)
            + "WHERE ([Calendar.FlatWeek].[202635], [Measures].[ClosingQty])";
    }

    /** Red products with a non-zero week-35 fact, under their store. */
    private static List<String> closingWeek35(int products) {
        List<String> cells = new ArrayList<>();
        for (int store = 1; store <= STORES; store++) {
            for (int i = 4; i <= products; i += 4) {
                if (week35Store(i) == store && week35Quantity(i) != 0) {
                    cells.add("[Store].[S" + store + "],[Product].[" + name(i) + "]=" + i + ".0");
                }
            }
        }
        return cells;
    }

    private void assertProductionShapeResult(String cube) throws Exception {
        assertEquals(List.of("[Store].[S1],[Product].[P016]=16.0", "[Store].[S2],[Product].[P004]=4.0",
            "[Store].[S2],[Product].[P012]=12.0"), closingWeek35(SMALL));
        assertEquals(closingWeek35(SMALL), run(SMALL, productionShape(cube)).cells());
        assertEquals(closingWeek35(LARGE), run(LARGE, productionShape(cube)).cells());
    }

    @Test void productionShapeKeepsItsResult() throws Exception {
        assertProductionShapeResult("Navigation");
        // eMondrian rewrites the NON EMPTY axis into NonEmpty(set, {measures}): the per-tuple loop of #97.
        StringWriter plan = new StringWriter();
        open(SMALL).parseQuery(productionShape("Navigation")).explain(new PrintWriter(plan));
        assertTrue(plan.toString().contains("NonEmptyFunDef$NonEmptyListCalcImpl"), plan.toString());
    }

    @Test void productionShapeBuildsOneConstraintPerContextNotPerTuple() throws Exception {
        assertDoesNotGrowWithTheAxis("constraint builds", Run::builds, productionShape("Navigation"));
    }

    /** The native query engine is on by default: the bound and the cells must hold there too. */
    @Test void productionShapeWithTheNativeQueryEngine() throws Exception {
        MondrianProperties.instance().setProperty(NATIVE_QUERY_ENGINE, "true");
        Run small = run(SMALL, productionShape("Navigation"));
        Run large = run(LARGE, productionShape("Navigation"));
        assertEquals(closingWeek35(SMALL), numeric(small.cells()));
        assertEquals(closingWeek35(LARGE), numeric(large.cells()));
        assertDoesNotGrowWithTheAxis("constraint builds", small.builds(), large.builds());
    }

    /**
     * Each cell with its value as a double. A read served from the engine's
     * prefetch keeps the stored integer, one it does not cover comes back as
     * a double: which reads it covers is the engine's choice, not this test's.
     */
    private static List<String> numeric(List<String> cells) {
        return cells.stream().map(cell -> {
            int start = cell.lastIndexOf('=') + 1;
            String value = cell.substring(start);
            return value.equals("null") ? cell : cell.substring(0, start) + Double.parseDouble(value);
        }).toList();
    }

    @Test void productionShapeResolvesSubselectIdsPerExecutionNotPerTuple() throws Exception {
        assertDoesNotGrowWithTheAxis("subselect Id resolutions", Run::idResolutions,
            productionShape("Navigation"));
    }

    /**
     * A calculated context member rebuilds the constraint for every tuple, and
     * each rebuild asks for the subselect predicate: the member behind the
     * static {@code Id} must still be looked up once.
     */
    @Test void rebuiltConstraintsResolveSubselectIdsOnce() throws Exception {
        String mdx = CLOSING_QTY + PICK_MEMBER + "SELECT " + STORE_BY_PRODUCT + " ON COLUMNS " + RED_NAVIGATION
            + "WHERE ([Calendar.FlatWeek].[Pick], [Measures].[ClosingQty])";
        Run small = run(SMALL, mdx);
        Run large = run(LARGE, mdx);
        assertEquals(closingWeek36(SMALL, ""), small.cells());
        assertEquals(closingWeek36(LARGE, ""), large.cells());
        assertTrue(large.builds() > small.builds() + SLACK,
            "constraints were reused: " + small.builds() + " builds, then " + large.builds());
        assertDoesNotGrowWithTheAxis("subselect Id resolutions", small.idResolutions(), large.idResolutions());
    }

    private static final String ROWS_VARYING_CALENDAR = CLOSING_QTY + "SELECT " + STORE_BY_PRODUCT
        + " ON COLUMNS, {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]} ON ROWS "
        + RED_NAVIGATION + "WHERE [Measures].[ClosingQty]";

    /** Each row closes on its own week: a product sold in week 35 is empty on the week-36 row and vice versa. */
    private static List<String> closingPerWeekRow(int products) {
        List<String> cells = new ArrayList<>();
        for (int week = 35; week <= 36; week++) {
            for (int store = 1; store <= STORES; store++) {
                for (int i = 2; i <= products; i += 2) {
                    boolean week35 = i % 4 == 0 && week35Store(i) == store && week35Quantity(i) != 0;
                    boolean week36 = i % 4 == 2 && store == 1;
                    if (week35 || week36) {
                        cells.add("[Calendar.FlatWeek].[2026" + week + "] / [Store].[S" + store + "],[Product].["
                            + name(i) + "]=" + ((week == 35 ? week35 : week36) ? i + ".0" : "null"));
                    }
                }
            }
        }
        return cells;
    }

    @Test void rowsVaryingCalendarContextKeepsThePerRowBoundary() throws Exception {
        assertEquals(List.of(
            "[Calendar.FlatWeek].[202635] / [Store].[S1],[Product].[P002]=null",
            "[Calendar.FlatWeek].[202635] / [Store].[S1],[Product].[P006]=null",
            "[Calendar.FlatWeek].[202635] / [Store].[S1],[Product].[P010]=null",
            "[Calendar.FlatWeek].[202635] / [Store].[S1],[Product].[P014]=null",
            "[Calendar.FlatWeek].[202635] / [Store].[S1],[Product].[P016]=16.0",
            "[Calendar.FlatWeek].[202635] / [Store].[S2],[Product].[P004]=4.0",
            "[Calendar.FlatWeek].[202635] / [Store].[S2],[Product].[P012]=12.0",
            "[Calendar.FlatWeek].[202636] / [Store].[S1],[Product].[P002]=2.0",
            "[Calendar.FlatWeek].[202636] / [Store].[S1],[Product].[P006]=6.0",
            "[Calendar.FlatWeek].[202636] / [Store].[S1],[Product].[P010]=10.0",
            "[Calendar.FlatWeek].[202636] / [Store].[S1],[Product].[P014]=14.0",
            "[Calendar.FlatWeek].[202636] / [Store].[S1],[Product].[P016]=null",
            "[Calendar.FlatWeek].[202636] / [Store].[S2],[Product].[P004]=null",
            "[Calendar.FlatWeek].[202636] / [Store].[S2],[Product].[P012]=null"), closingPerWeekRow(SMALL));
        assertEquals(closingPerWeekRow(SMALL), run(SMALL, ROWS_VARYING_CALENDAR).cells());
        assertEquals(closingPerWeekRow(LARGE), run(LARGE, ROWS_VARYING_CALENDAR).cells());
    }

    @Test void rowsVaryingCalendarContextBuildsPerDistinctContextNotPerTuple() throws Exception {
        assertDoesNotGrowWithTheAxis("constraint builds", Run::builds, ROWS_VARYING_CALENDAR);
    }

    /**
     * Navigation replaces the position on the navigated hierarchy itself, so
     * the months of an axis share one constraint, and it still carries the
     * sibling's week: only August holds week 35.
     */
    @Test void navigatedHierarchyOnAnAxisSharesOneConstraint() throws Exception {
        String select = BOUNDARY + "SELECT {[Measures].[Boundary]} ON COLUMNS, ";
        String from = " ON ROWS FROM [Navigation] WHERE [Calendar.FlatWeek].[202635]";
        Run oneMonth = run(SMALL, select + "{[Calendar].[2026].[8]}" + from);
        Run allMonths = run(SMALL, select + "[Calendar].[Month].Members" + from);
        assertEquals(List.of(
            "[Calendar].[2025].[12] / [Measures].[Boundary]=[Calendar].[#null]",
            "[Calendar].[2026].[8] / [Measures].[Boundary]=[Calendar].[2026].[8].[35]",
            "[Calendar].[2026].[9] / [Measures].[Boundary]=[Calendar].[#null]",
            "[Calendar].[2027].[1] / [Measures].[Boundary]=[Calendar].[#null]"), allMonths.cells());
        assertTrue(oneMonth.builds() >= 1 && allMonths.builds() == oneMonth.builds(),
            "builds grew with the months on the axis: " + oneMonth.builds() + " for one month, "
                + allMonths.builds() + " for all");
    }

    /** Nor does a calculated member there force a rebuild: a build never reads it. */
    @Test void calculatedMemberOfTheNavigatedHierarchySharesOneConstraint() throws Exception {
        String select = "WITH MEMBER [Calendar].[First] AS [Calendar].[2026] "
            + "MEMBER [Calendar].[Second] AS [Calendar].[2027] "
            + "MEMBER [Measures].[AugustClose] AS ClosingPeriod([Calendar].[Week],[Calendar].[2026].[8]).UniqueName "
            + "SELECT {[Measures].[AugustClose]} ON COLUMNS, ";
        Run oneRow = run(SMALL, select + "{[Calendar].[First]} ON ROWS FROM [Navigation]");
        Run twoRows = run(SMALL, select + "{[Calendar].[First],[Calendar].[Second]} ON ROWS FROM [Navigation]");
        assertEquals(List.of(
            "[Calendar].[First] / [Measures].[AugustClose]=[Calendar].[2026].[8].[36]",
            "[Calendar].[Second] / [Measures].[AugustClose]=[Calendar].[2026].[8].[36]"), twoRows.cells());
        assertTrue(oneRow.builds() >= 1 && twoRows.builds() == oneRow.builds(),
            "calculated rows rebuilt the constraint: " + oneRow.builds() + " builds for one row, "
                + twoRows.builds() + " for two");
    }

    /** Same members, one execution: Existing applies the Calendar position, PrevMember.LastChild must not. */
    @Test void anchoredAndUnanchoredNavigationDoNotShareAConstraint() throws Exception {
        assertEquals(List.of(
            "[Calendar].[2026] / [Measures].[Weeks]=3",
            "[Calendar].[2026] / [Measures].[PrevLast]=[Calendar].[2025].[12]"),
            run(SMALL, "WITH MEMBER [Measures].[Weeks] AS Count(Existing [Calendar].[Week].Members) "
                + "MEMBER [Measures].[PrevLast] AS [Calendar].CurrentMember.PrevMember.LastChild.UniqueName "
                + "SELECT {[Measures].[Weeks],[Measures].[PrevLast]} ON COLUMNS, {[Calendar].[2026]} ON ROWS "
                + "FROM [Navigation]").cells());
    }

    /** The subselect set is evaluated in each cell's context: equal Calendar members, different constraints. */
    @Test void dynamicSubselectOnTheNavigatedDimensionDiffersPerRow() throws Exception {
        assertEquals(List.of(
            "[Product].[P004] / [Measures].[Boundary]=[Calendar].[2026].[8].[35]",
            "[Product].[P002] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]"),
            run(SMALL, BOUNDARY + "SELECT {[Measures].[Boundary]} ON COLUMNS, "
                + "{[Product].[P004],[Product].[P002]} ON ROWS "
                + "FROM (SELECT Filter([Calendar.FlatWeek].[Week].Members,[Measures].[Quantity]>0) ON COLUMNS "
                + "FROM [Navigation])").cells());
    }

    /**
     * The filter measure navigates while the subselect set is still being
     * evaluated, where that set restricts nothing yet: such a build must never
     * serve the rows. Week 37 is outside the set.
     */
    @Test void navigationInsideTheDynamicSubselectIsNeverReused() throws Exception {
        String select = CLOSING_QTY + BOUNDARY_MEMBER + "SELECT {[Measures].[Boundary]} ON COLUMNS, ";
        String from = " ON ROWS FROM (SELECT Filter([Calendar.FlatWeek].[Week].Members,[Measures].[ClosingQty]>0) "
            + "ON COLUMNS FROM [Navigation])";
        Run oneRow = run(SMALL, select + "{[Calendar.FlatWeek].[202635]}" + from);
        Run twoRows = run(SMALL, select + "{[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202637]}" + from);
        assertEquals(List.of(
            "[Calendar.FlatWeek].[202635] / [Measures].[Boundary]=[Calendar].[2026].[8].[35]",
            "[Calendar.FlatWeek].[202637] / [Measures].[Boundary]=[Calendar].[#null]"), twoRows.cells());
        assertTrue(twoRows.builds() > oneRow.builds(),
            "rows reused a constraint built inside the subselect: " + oneRow.builds() + " builds for one row, "
                + twoRows.builds() + " for two");
    }

    /** An empty dynamic set empties the subcube even on an ignored hierarchy: S2 sells nothing in week 36. */
    @Test void dynamicSubselectOnAnIgnoredHierarchyDiffersPerRow() throws Exception {
        String rows = "SELECT {[Measures].[Boundary]} ON COLUMNS, {[Store].[S1],[Store].[S2]} ON ROWS ";
        assertEquals(List.of(
            "[Store].[S1] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]",
            "[Store].[S2] / [Measures].[Boundary]=[Calendar].[#null]"),
            run(SMALL, BOUNDARY + rows
                + "FROM (SELECT Filter([Product].[Name].Members,[Measures].[Quantity]>0) ON COLUMNS "
                + "FROM [Navigation]) WHERE [Calendar.FlatWeek].[202636]").cells());
        assertEquals(List.of(
            "[Store].[S1] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]",
            "[Store].[S2] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]"),
            run(SMALL, BOUNDARY + rows + "FROM [Navigation] WHERE [Calendar.FlatWeek].[202636]").cells());
    }

    /** The aggregated set depends on the Product context; the engine evaluates it once, in the slicer context. */
    @Test void calculatedAggregateSiblingMemberKeepsItsResult() throws Exception {
        String pick = "WITH MEMBER [Calendar.FlatWeek].[Pick] AS "
            + "Aggregate(Filter([Calendar.FlatWeek].[Week].Members,[Measures].[Quantity]>0)) "
            + BOUNDARY_MEMBER + "SELECT {[Measures].[Boundary]} ON COLUMNS";
        assertEquals(List.of(
            "[Product].[P004] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]",
            "[Product].[P002] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]"),
            run(SMALL, pick + ", {[Product].[P004],[Product].[P002]} ON ROWS FROM [Navigation] "
                + "WHERE [Calendar.FlatWeek].[Pick]").cells());
        assertEquals(List.of("[Measures].[Boundary]=[Calendar].[2026].[8].[35]"),
            run(SMALL, pick + " FROM [Navigation] WHERE ([Calendar.FlatWeek].[Pick],[Product].[P004])").cells());
        assertEquals(List.of("[Measures].[Boundary]=[Calendar].[2026].[8].[36]"),
            run(SMALL, pick + " FROM [Navigation] WHERE ([Calendar.FlatWeek].[Pick],[Product].[P002])").cells());
    }

    private static final String COMPOUND_SLICER = CLOSING_QTY + "SELECT {[Measures].[ClosingQty]} ON COLUMNS, "
        + STORE_BY_PRODUCT + " ON ROWS " + RED_NAVIGATION
        + "WHERE {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]}";

    private static final String CLOSING_QTY_COLUMN = " / [Measures].[ClosingQty]";

    /** Both weeks selected close on week 36, so only the week-36 sellers keep a value. */
    private static List<String> closingWeek36(int products, String column) {
        List<String> cells = new ArrayList<>();
        for (int i = 2; i <= products; i += 4) {
            cells.add("[Store].[S1],[Product].[" + name(i) + "]" + column + "=" + i + ".0");
        }
        return cells;
    }

    @Test void compoundSlicerKeepsItsResult() throws Exception {
        assertEquals(List.of(
            "[Store].[S1],[Product].[P002] / [Measures].[ClosingQty]=2.0",
            "[Store].[S1],[Product].[P006] / [Measures].[ClosingQty]=6.0",
            "[Store].[S1],[Product].[P010] / [Measures].[ClosingQty]=10.0",
            "[Store].[S1],[Product].[P014] / [Measures].[ClosingQty]=14.0"),
            closingWeek36(SMALL, CLOSING_QTY_COLUMN));
        assertEquals(closingWeek36(SMALL, CLOSING_QTY_COLUMN), run(SMALL, COMPOUND_SLICER).cells());
        assertEquals(closingWeek36(LARGE, CLOSING_QTY_COLUMN), run(LARGE, COMPOUND_SLICER).cells());
    }

    @Test void compoundSlicerBuildsOneConstraintPerContextNotPerTuple() throws Exception {
        assertDoesNotGrowWithTheAxis("constraint builds", Run::builds, COMPOUND_SLICER);
    }

    /**
     * A memo of one entry, or of one character of predicate text, keeps
     * starting over as the rows alternate: more builds, the same cells.
     */
    @Test void tinyMemoCapacityCostsBuildsNotResults() throws Exception {
        Run roomy = run(SMALL, ROWS_VARYING_CALENDAR);
        int previousCapacity = SqlDimensionContextConstraint.memoCapacity;
        int previousWeightCapacity = SqlDimensionContextConstraint.memoWeightCapacity;
        try {
            SqlDimensionContextConstraint.memoCapacity = 1;
            assertTinyMemoCostsBuildsNotResults("capacity 1", roomy);
            SqlDimensionContextConstraint.memoCapacity = previousCapacity;
            SqlDimensionContextConstraint.memoWeightCapacity = 1;
            assertTinyMemoCostsBuildsNotResults("weight capacity 1", roomy);
        } finally {
            SqlDimensionContextConstraint.memoCapacity = previousCapacity;
            SqlDimensionContextConstraint.memoWeightCapacity = previousWeightCapacity;
        }
    }

    private void assertTinyMemoCostsBuildsNotResults(String memo, Run roomy) throws Exception {
        Run tiny = run(SMALL, ROWS_VARYING_CALENDAR);
        assertEquals(closingPerWeekRow(SMALL), tiny.cells(), memo);
        assertEquals(closingPerWeekRow(LARGE), run(LARGE, ROWS_VARYING_CALENDAR).cells(), memo);
        assertTrue(tiny.builds() > roomy.builds(),
            memo + " must evict: " + tiny.builds() + " builds against " + roomy.builds());
    }

    /** A result outlives its execution (result cache, an open CellSet); the memo serves the execution only. */
    @Test void memoIsReleasedWhenTheExecutionEnds() throws Exception {
        mondrian.olap.Connection connection = open(SMALL);
        Result result = connection.execute(connection.parseQuery(BOUNDARY
            + "SELECT {[Measures].[Boundary]} ON COLUMNS FROM [Navigation] WHERE [Calendar.FlatWeek].[202635]"));
        assertEquals(List.of("[Measures].[Boundary]=[Calendar].[2026].[8].[35]"), cells(result));
        assertEquals(1, ((ResultBase) result).getExecution().getDimensionContextConstraintBuilds());
        RolapEvaluatorRoot root = ((RolapEvaluator) ((RolapResult) result).getRootEvaluator()).root;
        assertTrue(root.dimensionContextConstraints.isEmpty(),
            root.dimensionContextConstraints.size() + " constraints outlived the execution");
    }

    /**
     * Inside a compound slicer the current member is only the slicer's
     * placeholder, yet the calculated member is still expanded through the
     * evaluator: every row must build its own constraint.
     */
    @Test void calculatedMemberInsideACompoundSlicerIsNeverReused() throws Exception {
        String select = "WITH " + PICK_MEMBER + BOUNDARY_MEMBER + "SELECT {[Measures].[Boundary]} ON COLUMNS, ";
        String slicer = " ON ROWS FROM [Navigation] WHERE {[Calendar.FlatWeek].[Pick],[Calendar.FlatWeek].[202552]}";
        Run oneRow = run(SMALL, select + "{[Product].[P004]}" + slicer);
        Run threeRows = run(SMALL, select + "{[Product].[P004],[Product].[P002],[Product].[P001]}" + slicer);
        assertEquals(List.of(
            "[Product].[P004] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]",
            "[Product].[P002] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]",
            "[Product].[P001] / [Measures].[Boundary]=[Calendar].[2026].[8].[36]"), threeRows.cells());
        assertTrue(threeRows.builds() > oneRow.builds(),
            "rows shared a constraint: " + oneRow.builds() + " builds for one row, " + threeRows.builds()
                + " for three");
    }

    private static final String CLOSE_2026 = "MEMBER [Measures].[Close2026] AS "
        + "ClosingPeriod([Calendar].[Week],[Calendar].[2026]).UniqueName ";
    private static final String CALENDAR_SLICER =
        "WHERE {ClosingPeriod([Calendar].[Week],[Calendar].[2026].[8]),[Calendar].[2025].[12].[52]}";

    /**
     * The slicer set navigates before there are slicer tuples, with every
     * current member the cells will have: only the tuple list tells the two
     * apart. The cells must see the slicer: of 2026 it keeps week 36 alone,
     * while the unrestricted year closes on week 37.
     */
    @Test void navigationInsideTheSlicerSetDoesNotServeTheCells() throws Exception {
        assertEquals(List.of("[Measures].[Close2026]=[Calendar].[2026].[8].[36]"),
            run(SMALL, "WITH " + CLOSE_2026 + "SELECT {[Measures].[Close2026]} ON COLUMNS FROM [Navigation] "
                + CALENDAR_SLICER).cells());
        assertEquals(List.of(
            "[Product].[P004] / [Measures].[Close2026]=[Calendar].[2026].[8].[36]",
            "[Product].[P002] / [Measures].[Close2026]=[Calendar].[2026].[8].[36]"),
            run(SMALL, "WITH " + CLOSE_2026 + "SELECT {[Measures].[Close2026]} ON COLUMNS, "
                + "{[Product].[P004],[Product].[P002]} ON ROWS FROM [Navigation] " + CALENDAR_SLICER).cells());
    }

    /**
     * Same current members, axis and cell: the axis navigates outside any
     * calculated member, where the compound slicer still restricts the week
     * 37 context to nothing; the cell's calculated member overrides the slicer
     * with week 37.
     */
    @Test void overriddenSlicerPositionKeepsCellAndAxisNavigationApart() throws Exception {
        assertEquals(List.of("[Store].[S1] / [Measures].[Close2026At37]=[Calendar].[2026].[9].[37]"),
            run(SMALL, "WITH " + CLOSE_2026
                + "MEMBER [Measures].[Close2026At37] AS ([Measures].[Close2026], [Calendar.FlatWeek].[202637]) "
                + "SELECT {[Measures].[Close2026At37]} ON COLUMNS, "
                + "Filter({[Store].[S1]}, Count(Generate({[Calendar.FlatWeek].[202637]}, "
                + "{ClosingPeriod([Calendar].[Week],[Calendar].[2026])})) >= 0) ON ROWS "
                + "FROM [Navigation] WHERE {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]}").cells());
    }

    @Test void virtualCubeKeepsItsResult() throws Exception {
        assertProductionShapeResult("Combined");
    }

    @Test void virtualCubeBuildsOneConstraintPerContextNotPerTuple() throws Exception {
        assertDoesNotGrowWithTheAxis("constraint builds", Run::builds, productionShape("Combined"));
    }

    /**
     * A strict parse rejects the member before any execution; only with
     * IgnoreInvalidMembersDuringQuery does the run-time resolution fail, and
     * that failure must never be remembered.
     */
    @Test void missingSubselectMemberFailsOnEveryExecution() throws Exception {
        boolean previous = MondrianProperties.instance().IgnoreInvalidMembersDuringQuery.get();
        MondrianProperties.instance().IgnoreInvalidMembersDuringQuery.set(true);
        try {
            mondrian.olap.Connection connection = open(SMALL);
            Query query = null;
            for (int execution = 1; execution <= 3; execution++) {
                // The third execution reuses the second query, as a prepared statement would.
                if (execution < 3) {
                    query = connection.parseQuery(productionShape("Navigation").replace("[Red]", "[NoSuch]"));
                }
                Query executed = query;
                long resolvedBefore = executed.getSubcubeIdResolutions();
                Throwable failure = rootCause(
                    assertThrows(MondrianException.class, () -> connection.execute(executed)));
                assertTrue(failure.getMessage().contains("Member '[Product.Manufacturer].[NoSuch]' not found"),
                    "execution " + execution + ": " + failure.getMessage());
                assertTrue(executed.getSubcubeIdResolutions() > resolvedBefore,
                    "execution " + execution + " must resolve the member again");
            }
        } finally {
            MondrianProperties.instance().IgnoreInvalidMembersDuringQuery.set(previous);
        }
    }

    private static Throwable rootCause(Throwable failure) {
        while (failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    /** Cancels its execution on the first call; the count tells how far the caller's loop ran on. */
    public static class CancelProbe implements UserDefinedFunction {
        static final AtomicInteger CALLS = new AtomicInteger();

        @Override public String getName() {
            return "CancelProbe";
        }

        @Override public String getDescription() {
            return getName();
        }

        @Override public Syntax getSyntax() {
            return Syntax.Function;
        }

        @Override public Type[] getParameterTypes() {
            return new Type[0];
        }

        @Override public Type getReturnType(Type[] parameterTypes) {
            return new NumericType();
        }

        @Override public Object execute(Evaluator evaluator, Argument[] arguments) {
            if (CALLS.getAndIncrement() == 0) {
                try {
                    // The slow-query trace skips executions shorter than its threshold of at least 1 ms.
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                evaluator.getQuery().getStatement().getCurrentExecution().cancel();
            }
            return 1;
        }

        @Override public String[] getReservedWords() {
            return null;
        }
    }

    /** A cancelled (or timed-out) query must leave the NonEmpty loop at the next check, not walk the whole set. */
    @Test void nonEmptyLoopStopsAtTheNextCancelCheck() throws Exception {
        int interval = 10;
        int previous = MondrianProperties.instance().CheckCancelOrTimeoutInterval.get();
        MondrianProperties.instance().CheckCancelOrTimeoutInterval.set(interval);
        try {
            String probe = "WITH MEMBER [Measures].[Probe] AS CancelProbe() SELECT ";
            String set = "CrossJoin([Store].[Name].Members, [Product].[Name].Members)";
            for (String mdx : List.of(
                probe + "NonEmpty(" + set + ") ON COLUMNS FROM [Navigation] WHERE [Measures].[Probe]",
                probe + "NonEmpty(" + set + ", {[Measures].[Probe]}) ON COLUMNS FROM [Navigation]"))
            {
                mondrian.olap.Connection connection = open(SMALL);
                CancelProbe.CALLS.set(0);
                List<Throwable> failures = new ArrayList<>();
                List<Map<String, Long>> traces = slowQueryTraces(1, () -> failures.add(
                    assertThrows(MondrianException.class, () -> connection.execute(connection.parseQuery(mdx)))));
                Throwable failure = rootCause(failures.get(0));
                assertTrue(failure instanceof QueryCanceledException, mdx + ": " + failure);
                assertEquals(interval, CancelProbe.CALLS.get(), mdx);
                // The failed execution still reports its counters: a set larger than the walk, none of it kept.
                assertEquals(1, traces.size(), mdx);
                Map<String, Long> trace = traces.get(0);
                assertTrue(trace.get("nonEmptyTuplesIn") > interval, trace.toString());
                assertEquals(0L, trace.get("nonEmptyTuplesOut"), trace.toString());
                assertEquals(0L, trace.get("dimCtxConstraintBuilds"), trace.toString());
            }
        } finally {
            MondrianProperties.instance().CheckCancelOrTimeoutInterval.set(previous);
        }
    }

    private static final Pattern TRACE_FIELD = Pattern.compile("(\\w+)=(\\d+)");

    /**
     * The numeric fields of each "Slow query trace" line logged while
     * {@code action} ran, waiting for {@code expected} of them: the shepherd's
     * poll can hand a cancelled query back to its caller while the worker is
     * still on its way to the trace.
     */
    private static List<Map<String, Long>> slowQueryTraces(int expected, ThrowingRunnable action) throws Exception {
        MondrianProperties properties = MondrianProperties.instance();
        Logger logger = LogManager.getLogger(ResultBase.class);
        Level previousLevel = logger.getLevel();
        String previousEnabled = properties.getProperty(TRACE_ENABLED);
        String previousThreshold = properties.getProperty(TRACE_THRESHOLD_MS);
        StringWriter log = new StringWriter();
        Appender appender = Util.makeAppender("issue97SlowQueryTrace", log, "%m%n");
        properties.setProperty(TRACE_ENABLED, "true");
        properties.setProperty(TRACE_THRESHOLD_MS, "1");
        Util.setLevel(logger, Level.INFO);
        Util.addAppender(appender, logger, Level.INFO);
        try {
            action.run();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (traces(log).size() < expected && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
        } finally {
            Util.removeAppender(appender, logger);
            Util.setLevel(logger, previousLevel);
            restoreProperty(TRACE_ENABLED, previousEnabled);
            restoreProperty(TRACE_THRESHOLD_MS, previousThreshold);
        }
        return traces(log);
    }

    private static List<Map<String, Long>> traces(StringWriter log) {
        List<Map<String, Long>> traces = new ArrayList<>();
        for (String line : log.toString().split("\\R")) {
            if (line.startsWith("Slow query trace:")) {
                Map<String, Long> fields = new HashMap<>();
                Matcher field = TRACE_FIELD.matcher(line.substring(0, line.indexOf(", axes=")));
                while (field.find()) {
                    fields.put(field.group(1), Long.parseLong(field.group(2)));
                }
                traces.add(fields);
            }
        }
        return traces;
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Each execution resolves the subselect member again (the members are
     * remembered for one execution only) and reports its own count, not the
     * query's running total.
     */
    @Test void slowQueryTraceReportsTheNavigationCounters() throws Exception {
        mondrian.olap.Connection connection = open(LARGE);
        Query query = connection.parseQuery(productionShape("Navigation"));
        List<Long> builds = new ArrayList<>();
        List<Map<String, Long>> traces = slowQueryTraces(2, () -> {
            for (int execution = 1; execution <= 2; execution++) {
                Result result = connection.execute(query);
                builds.add((long) ((ResultBase) result).getExecution().getDimensionContextConstraintBuilds());
            }
        });
        assertEquals(2, traces.size(), traces.toString());
        assertEquals(2, query.getSubcubeIdResolutions());
        for (int execution = 0; execution < 2; execution++) {
            Map<String, Long> trace = traces.get(execution);
            assertTrue(trace.containsKey("loadAxesMs"), trace.toString());
            assertEquals(builds.get(execution), trace.get("dimCtxConstraintBuilds"), trace.toString());
            assertEquals(1L, trace.get("subcubeIdResolutions"), trace.toString());
            // Whatever the candidate set and however many walks: the axis is kept, and nothing is kept unwalked.
            long out = trace.get("nonEmptyTuplesOut");
            assertTrue(out >= closingWeek35(LARGE).size() && out <= trace.get("nonEmptyTuplesIn"), trace.toString());
        }
    }
}
