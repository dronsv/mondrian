package mondrian.rolap;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Calendar navigation must follow context without testing fact presence. */
public class OpeningClosingPeriodContextTest {
    private mondrian.olap.Connection connection;
    private String previousNativeQueryEngine;
    private int previousPreCache;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:period_" + UUID.randomUUID().toString().replace("-", "")
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR,MONTH,WEEK";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "")) {
            Statement sql = db.createStatement();
            sql.execute("CREATE TABLE calendar (id INT, year INT, month INT, week INT, flat_week INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025,12,52,202552),(2,2026,8,35,202635),"
                + "(3,2026,8,36,202636),(4,2026,9,37,202637),(5,2027,1,1,202701)");
            sql.execute("CREATE TABLE product (id INT, name VARCHAR, manufacturer VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'A','Red'),(2,'B','Blue'),(3,'C','Red')");
            sql.execute("CREATE TABLE fact (calendar_id INT, product_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (2,1,10),(3,2,20)");
            sql.execute("CREATE TABLE stock (calendar_id INT, product_id INT, qty INT)");
            sql.execute("INSERT INTO stock VALUES (2,1,100),(3,3,300)");
        }
        previousPreCache = MondrianProperties.instance().LevelPreCacheThreshold.get();
        MondrianProperties.instance().LevelPreCacheThreshold.set(0);
        previousNativeQueryEngine = MondrianProperties.instance().getProperty("mondrian.native.queryEngine.enable");
        MondrianProperties.instance().setProperty("mondrian.native.queryEngine.enable", "false");
        Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="CalendarContext">
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
              <Cube name="Navigation"><Table name="fact"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
              <Cube name="Stock"><Table name="stock"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="StockQuantity" column="qty" aggregator="sum"/>
              </Cube>
              <VirtualCube name="Combined">
                <VirtualCubeDimension name="Calendar"/>
                <VirtualCubeDimension name="Product"/>
                <VirtualCubeMeasure cubeName="Navigation" name="[Measures].[Quantity]"/>
                <VirtualCubeMeasure cubeName="Stock" name="[Measures].[StockQuantity]"/>
              </VirtualCube>
              <Role name="Only2026"><SchemaGrant access="all">
                <CubeGrant cube="Combined" access="all">
                  <HierarchyGrant hierarchy="[Calendar]" access="custom" rollupPolicy="partial">
                    <MemberGrant member="[Calendar].[2026]" access="all"/>
                  </HierarchyGrant>
                </CubeGrant>
              </SchemaGrant></Role>
              <Role name="OnlyWeek35"><SchemaGrant access="all">
                <CubeGrant cube="Combined" access="all">
                  <HierarchyGrant hierarchy="[Calendar]" access="all"/>
                  <HierarchyGrant hierarchy="[Calendar.FlatWeek]" access="custom" rollupPolicy="partial">
                    <MemberGrant member="[Calendar.FlatWeek].[202635]" access="all"/>
                  </HierarchyGrant>
                </CubeGrant>
              </SchemaGrant></Role>
            </Schema>
            """);
        connection = mondrian.olap.DriverManager.getConnection(props, null);
    }

    @AfterEach void close() {
        RolapUtil.setHook(null);
        MondrianProperties.instance().LevelPreCacheThreshold.set(previousPreCache);
        if (previousNativeQueryEngine == null) {
            MondrianProperties.instance().remove("mondrian.native.queryEngine.enable");
        } else {
            MondrianProperties.instance().setProperty("mondrian.native.queryEngine.enable", previousNativeQueryEngine);
        }
        if (connection != null) {
            connection.close();
        }
    }

    private Object navigate(String function, String from, String where) {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Boundary] AS " + function
            + "([Calendar].[Week],[Calendar].CurrentMember).UniqueName "
            + "SELECT {[Measures].[Boundary]} ON COLUMNS " + from + where));
        return result.getCell(new int[] {0}).getValue();
    }

    @Test void virtualOpeningAndClosingRemainIndependentOfEitherFact() {
        java.util.List<String> statements = new java.util.ArrayList<>();
        RolapUtil.setHook(statements::add);
        assertEquals("[Calendar].[2027].[1].[1]", navigate("ClosingPeriod", "FROM [Combined]", ""));
        assertEquals("[Calendar].[2025].[12].[52]", navigate("OpeningPeriod", "FROM [Combined]", ""));
        assertTrue(statements.stream().noneMatch(sql -> sql.contains("\"fact\"") || sql.contains("\"stock\"")),
            "Calendar navigation must not join a fact: " + statements);
    }

    @Test void virtualYearContextUsesPhysicalDimensionKeys() {
        assertEquals("[Calendar].[2026].[9].[37]", navigate("ClosingPeriod", "FROM [Combined]", " WHERE [Calendar].[2026]"));
        assertEquals("[Calendar].[2026].[8].[35]", navigate("OpeningPeriod", "FROM [Combined]", " WHERE [Calendar].[2026]"));
    }

    @Test void virtualSiblingSlicerAndNestedSubcubeKeepTheIntersection() {
        String from = "FROM (SELECT {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]} "
            + "ON COLUMNS FROM (SELECT {[Calendar].[2026]} ON COLUMNS FROM [Combined]))";
        assertEquals("[Calendar].[2026].[8].[36]", navigate("ClosingPeriod", from, " WHERE [Calendar.FlatWeek].[202636]"));
        assertEquals("[Calendar].[2026].[8].[35]", navigate("OpeningPeriod", from, ""));
        assertEquals("[Calendar].[2026].[8].[35]", navigate("ClosingPeriod",
            "FROM (SELECT {[Calendar.FlatWeek].[202635]} ON COLUMNS FROM [Combined])", ""));
    }

    @Test void virtualFlatNavigationAndMultiSlicerUseTheSameBinding() {
        Result flat = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Boundary] AS "
            + "ClosingPeriod([Calendar.FlatWeek].[Week],[Calendar.FlatWeek].CurrentMember).UniqueName "
            + "SELECT {[Measures].[Boundary]} ON COLUMNS "
            + "FROM (SELECT {[Calendar.FlatWeek].[202635]} ON COLUMNS FROM [Combined])"));
        assertEquals("[Calendar.FlatWeek].[202635]", flat.getCell(new int[] {0}).getValue());
        Result multiple = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Boundary] AS "
            + "VBA!Val(ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember).Name) "
            + "SELECT {[Measures].[Boundary]} ON COLUMNS FROM [Combined] "
            + "WHERE {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]}"));
        assertEquals(36d, ((Number) multiple.getCell(new int[] {0}).getValue()).doubleValue());
    }

    @Test void virtualRoleLimitsAndCacheStayIsolated() {
        assertEquals("[Calendar].[2027].[1].[1]", navigate("ClosingPeriod", "FROM [Combined]", ""));
        mondrian.olap.Role unrestricted = connection.getRole();
        connection.setRole(connection.getSchema().lookupRole("Only2026"));
        try {
            assertEquals("[Calendar].[2026].[9].[37]", navigate("ClosingPeriod", "FROM [Combined]", ""));
            assertEquals("[Calendar].[2026].[8].[35]", navigate("OpeningPeriod", "FROM [Combined]", ""));
        } finally {
            connection.setRole(unrestricted);
        }
        assertEquals("[Calendar].[2027].[1].[1]", navigate("ClosingPeriod", "FROM [Combined]", ""));
    }

    @Test void virtualSiblingRoleRestrictionIsAppliedToCalendarNavigation() {
        mondrian.olap.Role unrestricted = connection.getRole();
        assertEquals("[Calendar].[2027].[1].[1]", navigate("ClosingPeriod", "FROM [Combined]", ""));
        connection.setRole(connection.getSchema().lookupRole("OnlyWeek35"));
        try {
            assertEquals("[Calendar].[2026].[8].[35]", navigate("ClosingPeriod", "FROM [Combined]", ""));
            assertEquals("[Calendar].[2026].[8].[35]", navigate("OpeningPeriod", "FROM [Combined]", ""));
        } finally {
            connection.setRole(unrestricted);
        }
        assertEquals("[Calendar].[2027].[1].[1]", navigate("ClosingPeriod", "FROM [Combined]", ""));
    }

    @Test void closingHonorsSiblingFlatWeekSubselect() {
        assertEquals("[Calendar].[2026].[8].[35]", navigate("ClosingPeriod",
            "FROM (SELECT {[Calendar.FlatWeek].[202635]} ON COLUMNS FROM [Navigation])", ""));
    }

    @Test void closingHonorsSiblingFlatWeekSlicer() {
        assertEquals("[Calendar].[2026].[8].[35]", navigate("ClosingPeriod",
            "FROM [Navigation]", " WHERE [Calendar.FlatWeek].[202635]"));
    }

    @Test void multipleWeeksChooseCalendarBoundaryEvenWhenProductHasNoLatestFact() {
        String subcube = "FROM (SELECT {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]} "
            + "ON COLUMNS FROM [Navigation])";
        assertEquals("[Calendar].[2026].[8].[36]", navigate("ClosingPeriod", subcube, " WHERE [Product].[A]"));
        assertEquals("[Calendar].[2026].[8].[35]", navigate("OpeningPeriod", subcube, " WHERE [Product].[B]"));
    }

    @Test void allYearAndMonthNavigationRemainIndependentOfFacts() {
        assertEquals("[Calendar].[2027].[1].[1]", navigate("ClosingPeriod", "FROM [Navigation]", " WHERE [Product].[A]"));
        assertEquals("[Calendar].[2026].[9].[37]", navigate("ClosingPeriod", "FROM [Navigation]", " WHERE [Calendar].[2026]"));
        assertEquals("[Calendar].[2026].[8].[36]", navigate("ClosingPeriod", "FROM [Navigation]", " WHERE ([Calendar].[2026].[8],[Product].[A])"));
    }

    @Test void sameHierarchyAndNestedSubcubesSelectTheIntersection() {
        assertEquals("[Calendar].[2026].[8].[36]", navigate("ClosingPeriod",
            "FROM (SELECT {[Calendar].[2026].[8]} ON COLUMNS FROM [Navigation])", ""));
        assertEquals("[Calendar].[2026].[8].[36]", navigate("ClosingPeriod",
            "FROM (SELECT {[Calendar.FlatWeek].[202636],[Calendar.FlatWeek].[202637]} ON COLUMNS "
                + "FROM (SELECT {[Calendar].[2026].[8]} ON COLUMNS FROM [Navigation]))", ""));
    }

    @Test void multiSelectSlicersPreserveCalendarUnion() {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Boundary] AS "
            + "VBA!Val(ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember).Name) "
            + "SELECT {[Measures].[Boundary]} ON COLUMNS FROM [Navigation] "
            + "WHERE {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]}"));
        RolapUtil.setHook(null);
        assertEquals(36d, ((Number) result.getCell(new int[] {0}).getValue()).doubleValue());
    }

    @Test void independentMeasureAxisKeepsDimensionRestrictionWithoutSalesPruning() {
        java.util.List<String> statements = new java.util.ArrayList<>();
        RolapUtil.setHook(statements::add);
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Independent] AS 1 "
            + "SELECT NON EMPTY [Product].[Name].Members ON COLUMNS "
            + "FROM [Navigation] WHERE ([Measures].[Independent],[Product.Manufacturer].[Red])"));
        assertEquals(java.util.List.of("[Product].[A]", "[Product].[C]"),
            result.getAxes()[0].getPositions().stream()
                .map(position -> position.get(0).getUniqueName()).toList(), statements.toString());
    }

    @Test void calendarCacheDoesNotLeakBetweenDifferentSubcubes() {
        for (int week : new int[] {202635, 202636, 202635}) {
            assertEquals("[Calendar].[2026].[8].[" + (week % 100) + "]", navigate("ClosingPeriod",
                "FROM (SELECT {[Calendar.FlatWeek].[" + week + "]} ON COLUMNS FROM [Navigation])", ""));
        }
    }

    @Test void missingLatestFactStaysNullInsteadOfCarryingAnOlderValue() {
        String mdx = "WITH MEMBER [Measures].[ClosingQty] AS ([Measures].[Quantity],"
            + "ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember)) "
            + "SELECT {[Measures].[ClosingQty]} ON COLUMNS "
            + "FROM (SELECT {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]} "
            + "ON COLUMNS FROM [Navigation]) WHERE [Product].[A]";
        Result result = connection.execute(connection.parseQuery(mdx));
        assertTrue(result.getCell(new int[] {0}).isNull(), "Missing current stock must not carry the prior value");
    }

    @Test void existingFlatWeekHonorsSubcubeInPhysicalAndVirtualCubes() {
        for (String cube : new String[] {"Navigation", "Combined"}) {
            assertEquals("[Calendar.FlatWeek].[202635]", scalar(
                "Tail(Existing [Calendar.FlatWeek].[Week].Members,1).Item(0).UniqueName",
                "FROM (SELECT {[Calendar.FlatWeek].[202635]} ON COLUMNS FROM [" + cube + "])", ""));
        }
    }

    @Test void existingHonorsSiblingSlicerAndNestedSubcubeWithoutFactPresence() {
        for (String cube : new String[] {"Navigation", "Combined"}) {
            assertEquals("[Calendar].[2026].[8].[36]", scalar(
                "Tail(Existing [Calendar].[Week].Members,1).Item(0).UniqueName",
                "FROM (SELECT {[Calendar.FlatWeek].[202636],[Calendar.FlatWeek].[202637]} ON COLUMNS "
                    + "FROM (SELECT {[Calendar].[2026].[8]} ON COLUMNS FROM [" + cube + "]))",
                " WHERE [Product].[A]"));
            assertEquals("[Calendar].[2026].[8].[35]", scalar(
                "Tail(Existing [Calendar].[Week].Members,1).Item(0).UniqueName",
                "FROM [" + cube + "]", " WHERE [Calendar.FlatWeek].[202635]"));
        }
    }

    @Test void existingPreservesInputOrderAndDuplicates() {
        assertEquals("202636,202635,202636", scalar(
            "Generate(Existing Union({[Calendar.FlatWeek].[202636],[Calendar.FlatWeek].[202635]},"
                + "{[Calendar.FlatWeek].[202636]},ALL),[Calendar.FlatWeek].CurrentMember.Name,\",\")",
            "FROM (SELECT {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]} ON COLUMNS FROM [Combined])", ""));
    }

    @Test void existingPreservesMultiSlicerUnion() {
        assertEquals(2d, ((Number) scalar("Count(Existing [Calendar.FlatWeek].[Week].Members)",
            "FROM [Combined]", " WHERE {[Calendar.FlatWeek].[202635],[Calendar.FlatWeek].[202636]}" )).doubleValue());
    }

    @Test void existingPreservesSiblingTupleCorrelation() {
        assertEquals(3d, ((Number) scalar(
            "Count(Existing Crossjoin([Product].[Name].Members,[Product.Manufacturer].[Name].Members))",
            "FROM [Combined]", "")).doubleValue());
    }

    @Test void existingTracksSiblingContextAcrossCells() {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[CountExisting] AS Count(Existing [Product].[Name].Members) "
                + "SELECT {[Measures].[CountExisting]} ON COLUMNS, "
                + "{[Product.Manufacturer].[Red],[Product.Manufacturer].[Blue]} ON ROWS FROM [Combined]"));
        assertEquals(2d, ((Number) result.getCell(new int[] {0, 0}).getValue()).doubleValue());
        assertEquals(1d, ((Number) result.getCell(new int[] {0, 1}).getValue()).doubleValue());
    }

    @Test void lastChildHonorsFlatSubcubeAndDoesNotLeakBetweenSelections() {
        for (String cube : new String[] {"Navigation", "Combined"}) {
            for (int week : new int[] {202635, 202636, 202635}) {
                assertEquals("[Calendar.FlatWeek].[" + week + "]", scalar(
                    "[Calendar.FlatWeek].DefaultMember.LastChild.UniqueName",
                    "FROM (SELECT {[Calendar.FlatWeek].[" + week + "]} ON COLUMNS FROM [" + cube + "])", ""));
            }
        }
    }

    @Test void lastChildHonorsSiblingContextAndEmptyIntersection() {
        assertEquals("[Calendar].[2026].[8].[35]", scalar(
            "[Calendar].[2026].[8].LastChild.UniqueName", "FROM [Combined]",
            " WHERE [Calendar.FlatWeek].[202635]"));
        assertEquals(0d, ((Number) scalar(
            "Count({[Calendar].[2026].[9].LastChild})",
            "FROM (SELECT {[Calendar.FlatWeek].[202635]} ON COLUMNS FROM [Combined])", "")).doubleValue());
    }

    @Test void existingAndLastChildHonorSiblingRoleAndRoleCacheIsolation() {
        mondrian.olap.Role unrestricted = connection.getRole();
        for (String expression : new String[] {
            "Tail(Existing [Calendar].[Week].Members,1).Item(0).UniqueName",
            "[Calendar].[2026].[8].LastChild.UniqueName"})
        {
            connection.setRole(connection.getSchema().lookupRole("OnlyWeek35"));
            try {
                assertEquals("[Calendar].[2026].[8].[35]", scalar(expression, "FROM [Combined]", ""));
            } finally {
                connection.setRole(unrestricted);
            }
            assertEquals(expression.startsWith("Tail") ? "[Calendar].[2027].[1].[1]" : "[Calendar].[2026].[8].[36]",
                scalar(expression, "FROM [Combined]", ""));
        }
    }

    @Test void virtualIndependentAxisKeepsSiblingFilterWithNativeEnabledOrDisabled() {
        boolean previous = MondrianProperties.instance().EnableNativeNonEmpty.get();
        try {
            for (boolean enabled : new boolean[] {true, false}) {
                MondrianProperties.instance().EnableNativeNonEmpty.set(enabled);
                SqlConstraintFactory.setNativeNonEmptyValue();
                java.util.List<String> statements = new java.util.ArrayList<>();
                RolapUtil.setHook(statements::add);
                Result result = connection.execute(connection.parseQuery(
                    "WITH MEMBER [Measures].[Independent] AS 1 "
                        + "SELECT NON EMPTY [Product].[Name].Members ON COLUMNS FROM [Combined] "
                        + "WHERE ([Measures].[Independent],[Product.Manufacturer].[Red])"));
                assertEquals(java.util.List.of("[Product].[A]", "[Product].[C]"),
                    result.getAxes()[0].getPositions().stream()
                        .map(position -> position.get(0).getUniqueName()).toList(), statements.toString());
                assertTrue(statements.stream().noneMatch(sql -> sql.contains("\"fact\"") || sql.contains("\"stock\"")),
                    "Independent member enumeration must not join either fact: " + statements);
            }
        } finally {
            MondrianProperties.instance().EnableNativeNonEmpty.set(previous);
            SqlConstraintFactory.setNativeNonEmptyValue();
        }
    }

    @Test void existingBatchesSiblingTupleChecksInsteadOfQueryingPerMember() {
        java.util.List<String> statements = new java.util.ArrayList<>();
        RolapUtil.setHook(statements::add);
        assertEquals(3d, ((Number) scalar(
            "Count(Existing Crossjoin([Product.Manufacturer].[Name].Members,[Product].[Name].Members))",
            "FROM [Combined]", "")).doubleValue());
        assertTrue(statements.size() <= 4,
            "EXISTING must batch sibling membership, not query once per product: " + statements);
        assertTrue(statements.stream().noneMatch(sql -> sql.contains("\"fact\"") || sql.contains("\"stock\"")),
            "EXISTING must not use fact presence: " + statements);
    }

    @Test void lastChildOfMeasureIsNullInPhysicalAndVirtualCubes() {
        for (String cube : new String[] {"Navigation", "Combined"}) {
            assertEquals(0d, ((Number) scalar("Count({[Measures].[Quantity].LastChild})",
                "FROM [" + cube + "]", "")).doubleValue());
        }
    }

    @Test void calculatedAggregateSiblingSlicerRestrictsExistingAndLastChild() {
        for (String expression : new String[] {
            "Count(Existing [Product].[Name].Members)",
            "Count(Intersect({[Product].DefaultMember.LastChild},{[Product].[B]}))"})
        {
            Result result = connection.execute(connection.parseQuery(
                "WITH MEMBER [Product.Manufacturer].[Pick] AS Aggregate({[Product.Manufacturer].[Blue]}) "
                    + "MEMBER [Measures].[Probe] AS " + expression
                    + " SELECT {[Measures].[Probe]} ON COLUMNS FROM [Combined] "
                    + "WHERE [Product.Manufacturer].[Pick]"));
            assertEquals(1d, ((Number) result.getCell(new int[] {0}).getValue()).doubleValue());
        }
    }

    @Test void virtualIndependentAxisHonorsCalculatedAggregateSiblingSlicer() {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Product.Manufacturer].[Pick] AS Aggregate({[Product.Manufacturer].[Red]}) "
                + "MEMBER [Measures].[Independent] AS 1 "
                + "SELECT NON EMPTY [Product].[Name].Members ON COLUMNS FROM [Combined] "
                + "WHERE ([Measures].[Independent],[Product.Manufacturer].[Pick])"));
        assertEquals(java.util.List.of("[Product].[A]", "[Product].[C]"),
            result.getAxes()[0].getPositions().stream().map(position -> position.get(0).getUniqueName()).toList());
    }

    @Test void existingHandlesAllOnlyAndMixedAllTupleShapes() {
        assertEquals(1d, ((Number) scalar("Count(Existing {[Product].DefaultMember})",
            "FROM [Combined]", " WHERE [Product.Manufacturer].[Red]")).doubleValue());
        assertEquals(2d, ((Number) scalar(
            "Count(Existing Crossjoin({[Product].DefaultMember},[Product.Manufacturer].[Name].Members))",
            "FROM [Combined]", "")).doubleValue());
        assertEquals(0d, ((Number) scalar("Count(Existing {[Product].DefaultMember})",
            "FROM (SELECT {} ON COLUMNS FROM [Combined])", "")).doubleValue());
    }

    @Test void existingKeepsCorrelatedSiblingSubcubeTuples() {
        assertEquals(2d, ((Number) scalar(
            "Count(Existing Crossjoin([Product].[Name].Members,[Product.Manufacturer].[Name].Members))",
            "FROM (SELECT {([Product].[A],[Product.Manufacturer].[Red]),"
                + "([Product].[B],[Product.Manufacturer].[Blue])} ON COLUMNS FROM [Combined])", "")).doubleValue());
    }

    @Test void dynamicLastChildCacheTracksSiblingHierarchyContext() {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Boundary] AS "
                + "Cache(StrToMember([Product].DefaultMember.UniqueName).LastChild).Name "
                + "SELECT {[Measures].[Boundary]} ON COLUMNS, "
                + "{[Product.Manufacturer].[Red],[Product.Manufacturer].[Blue]} ON ROWS FROM [Combined]"));
        assertEquals("C", result.getCell(new int[] {0, 0}).getValue());
        assertEquals("B", result.getCell(new int[] {0, 1}).getValue());
    }

    private Object scalar(String expression, String from, String where) {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Probe] AS " + expression
                + " SELECT {[Measures].[Probe]} ON COLUMNS " + from + where));
        return result.getCell(new int[] {0}).getValue();
    }
}
