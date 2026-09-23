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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    @Test void dynamicCalendarFilterUsesItsStoredMeasureContext() {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Boundary] AS"
                + " ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember).UniqueName"
                + " SELECT {[Measures].[Boundary]} ON 0, {[Product].[A],[Product].[B]} ON 1"
                + " FROM (SELECT Filter([Calendar.FlatWeek].[Week].Members,"
                + " [Measures].[Quantity] > 0) ON 0 FROM [Navigation])"));
        assertEquals("[Calendar].[2026].[8].[35]", result.getCell(new int[] {0, 0}).getValue());
        assertEquals("[Calendar].[2026].[8].[36]", result.getCell(new int[] {0, 1}).getValue());
    }

    @Test void dynamicProductFilterKeepsAnEmptySubcubeEmpty() {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Boundary] AS"
                + " ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember).UniqueName"
                + " SELECT {[Measures].[Boundary]} ON 0,"
                + " {[Product.Manufacturer].[Blue],[Product.Manufacturer].[Red]} ON 1"
                + " FROM (SELECT Filter([Product].[Name].Members,"
                + " [Measures].[Quantity] > 0) ON 0 FROM [Navigation])"
                + " WHERE [Calendar.FlatWeek].[202636]"));
        assertEquals("[Calendar].[2026].[8].[36]", result.getCell(new int[] {0, 0}).getValue());
        assertEquals("[Calendar].[#null]", result.getCell(new int[] {0, 1}).getValue());
    }

    @Test void calculatedSiblingFilterUsesTheProductSlicer() {
        String prefix = "WITH MEMBER [Calendar.FlatWeek].[Pick] AS"
            + " Aggregate(Filter([Calendar.FlatWeek].[Week].Members,[Measures].[Quantity]>0))"
            + " MEMBER [Measures].[Boundary] AS"
            + " ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember).UniqueName"
            + " SELECT {[Measures].[Boundary]} ON 0 FROM [Navigation] WHERE ";
        Result first = connection.execute(connection.parseQuery(prefix
            + "([Calendar.FlatWeek].[Pick],[Product].[A])"));
        assertEquals("[Calendar].[2026].[8].[35]", first.getCell(new int[] {0}).getValue());
        Result second = connection.execute(connection.parseQuery(prefix
            + "([Calendar.FlatWeek].[Pick],[Product].[B])"));
        assertEquals("[Calendar].[2026].[8].[36]", second.getCell(new int[] {0}).getValue());
    }

    @Test void nativeStoredRankingIgnoresTheDenseDisplayedMeasure() {
        String mdx = "WITH MEMBER [Measures].[One] AS 1"
            + " SELECT NON EMPTY TopCount(CrossJoin([Product].[Name].Members,"
            + " [Calendar].[Week].Members),1,[Measures].[Quantity]) ON 0"
            + " FROM (SELECT {[Product.Manufacturer].[Red]} ON 0 FROM [Navigation])"
            + " WHERE [Measures].[One]";
        RolapNativeRegistry registry = ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previous = registry.isEnabled();
        try {
            for (boolean nativeEnabled : new boolean[] {false, true}) {
                registry.setEnabled(nativeEnabled);
                registry.flushAllNativeSetCache();
                Result result = connection.execute(connection.parseQuery(mdx));
                assertEquals(1, result.getAxes()[0].getPositions().size());
                assertEquals("[Product].[A]", result.getAxes()[0].getPositions().get(0).get(0).getUniqueName());
                assertEquals("[Calendar].[2026].[8].[35]",
                    result.getAxes()[0].getPositions().get(0).get(1).getUniqueName());
                assertEquals(1d, ((Number) result.getCell(new int[] {0}).getValue()).doubleValue());
            }
        } finally {
            registry.setEnabled(previous);
        }
    }

    @Test void denseTopCountRetainsNullRankedMembers() {
        assertDenseTopCountPadding(false);
    }

    /** A native padding read must not replace empty Red member C with Blue member B. */
    @ParameterizedTest(name = "native sets={0}")
    @ValueSource(booleans = {false, true})
    void nestedTopCountSubselectPaddingRetainsInnerManufacturer(boolean nativeEnabled) {
        String mdx = "WITH MEMBER [Measures].[One] AS 1"
            + " SELECT {[Measures].[One]} ON 0,"
            + " NON EMPTY [Product].[Name].Members ON 1 FROM (SELECT"
            + " TopCount([Product].[Name].Members,2,[Measures].[Quantity])"
            + " ON 0 FROM (SELECT {[Product.Manufacturer].[Red]}"
            + " ON 0 FROM [Navigation]))";
        RolapNativeRegistry registry = ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previous = registry.isEnabled();
        java.util.List<String> statements = new java.util.ArrayList<>();
        try {
            registry.setEnabled(nativeEnabled);
            registry.flushAllNativeSetCache();
            RolapUtil.setHook(statements::add);
            Result result = connection.execute(connection.parseQuery(mdx));
            java.util.List<String> rows = result.getAxes()[1].getPositions().stream()
                .map(position -> position.get(0).getName()).toList();
            assertEquals(java.util.List.of("A", "C"), rows, statements.toString());
            for (int row = 0; row < rows.size(); row++) {
                assertEquals(1d, ((Number) result.getCell(new int[] {0, row}).getValue()).doubleValue());
            }
        } finally {
            registry.setEnabled(previous);
            registry.flushAllNativeSetCache();
        }
    }

    @Test void denseTopCountPaddingRetainsTheSubselect() {
        assertDenseTopCountPadding(true);
    }

    @Test void denseTopCountPaddingRetainsTheSiblingSlicer() {
        assertDenseTopCountPadding(false, true);
    }

    private void assertDenseTopCountPadding(boolean subselect) {
        assertDenseTopCountPadding(subselect, false);
    }

    private void assertDenseTopCountPadding(boolean subselect, boolean siblingSlicer) {
        String[] expected = subselect || siblingSlicer
            ? new String[] {"A", "C"} : new String[] {"B", "A", "C"};
        String mdx = "WITH MEMBER [Measures].[One] AS 1"
            + " SELECT NON EMPTY TopCount([Product].[Name].Members,"
            + expected.length + ",[Measures].[Quantity]) ON 0 FROM "
            + (subselect
                ? "(SELECT {[Product.Manufacturer].[Red]} ON 0 FROM [Navigation])"
                : "[Navigation]")
            + (siblingSlicer
                ? " WHERE ([Measures].[One],[Product.Manufacturer].[Red])"
                : " WHERE [Measures].[One]");
        RolapNativeRegistry registry = ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previous = registry.isEnabled();
        try {
            for (boolean nativeEnabled : new boolean[] {false, true}) {
                registry.setEnabled(nativeEnabled);
                registry.flushAllNativeSetCache();
                java.util.List<String> statements = new java.util.ArrayList<>();
                RolapUtil.setHook(statements::add);
                Result result = connection.execute(connection.parseQuery(mdx));
                assertEquals(expected.length, result.getAxes()[0].getPositions().size());
                if (nativeEnabled && !subselect && !siblingSlicer) {
                    assertTrue(statements.stream().anyMatch(sql ->
                        sql.contains("sum(\"fact\".\"qty\") DESC")),
                        "Plain unary padding must retain native ranking: " + statements);
                }
                for (int i = 0; i < expected.length; i++) {
                    assertEquals("[Product].[" + expected[i] + "]",
                        result.getAxes()[0].getPositions().get(i).get(0).getUniqueName(),
                        "nativeEnabled=" + nativeEnabled);
                    assertEquals(1d, ((Number) result.getCell(new int[] {i}).getValue()).doubleValue());
                }
            }
        } finally {
            registry.setEnabled(previous);
        }
    }

    @Test void closingPeriodNavigatesFromAnchorNotCurrentPosition() {
        for (String cube : new String[] {"Navigation", "Combined"}) {
            assertEquals("[Calendar].[2025].[12].[52]", onRow(
                "ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember.PrevMember).UniqueName",
                "[Calendar].[2026]", cube), cube);
        }
    }

    @Test void openingPeriodOfParallelPeriodNavigatesFromAnchor() {
        assertEquals("[Calendar].[2025].[12]", onRow(
            "OpeningPeriod([Calendar].[Month],ParallelPeriod([Calendar].[Year],1,[Calendar].CurrentMember)).UniqueName",
            "[Calendar].[2026]", "Navigation"));
    }

    @Test void priorPeriodClosingStockReadsThePreviousPeriodBoundary() {
        assertEquals(300d, ((Number) onRow(
            "([Measures].[StockQuantity],ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember.PrevMember))",
            "[Calendar].[2026].[9]", "Combined")).doubleValue());
    }

    @Test void priorPeriodClosingStockSurvivesNativeNonEmptyEnumeration() {
        assertCalendarCrossJoin(
            "([Measures].[StockQuantity], ClosingPeriod([Calendar].[Week],"
                + " [Calendar].CurrentMember.PrevMember))",
            "Combined",
            java.util.List.of("[Product].[C],[Calendar].[2026].[9]=300.0"));
    }

    @Test void implicitYtdKeepsMonthsWithoutCurrentFacts() {
        assertCalendarCrossJoin(
            "Sum(Ytd(), [Measures].[Quantity])",
            "Navigation",
            java.util.List.of(
                "[Product].[A],[Calendar].[2026].[8]=10.0",
                "[Product].[A],[Calendar].[2026].[9]=10.0",
                "[Product].[B],[Calendar].[2026].[8]=20.0",
                "[Product].[B],[Calendar].[2026].[9]=20.0"));
    }

    @Test void openingPeriodKeepsMonthsWithoutCurrentFacts() {
        assertCalendarCrossJoin(
            "([Measures].[Quantity], OpeningPeriod([Calendar].[Week],"
                + " [Calendar].[Year].Members.Item(1)))",
            "Navigation",
            java.util.List.of(
                "[Product].[A],[Calendar].[2025].[12]=10.0",
                "[Product].[A],[Calendar].[2026].[8]=10.0",
                "[Product].[A],[Calendar].[2026].[9]=10.0",
                "[Product].[A],[Calendar].[2027].[1]=10.0"));
    }

    @Test void implicitYtdKeepsMonthsWhenCalendarHasNoVisibleAllMember() {
        mondrian.olap.Connection previous = connection;
        Util.PropertyList properties =
            Util.parseConnectString(previous.getConnectString());
        properties.put("CatalogContent", properties.get("CatalogContent").replace(
            "<Hierarchy hasAll=\"true\" primaryKey=\"id\"><Table name=\"calendar\"/>",
            "<Hierarchy hasAll=\"false\" primaryKey=\"id\"><Table name=\"calendar\"/>"));
        connection = mondrian.olap.DriverManager.getConnection(properties, null);
        try {
            mondrian.olap.Dimension calendar = java.util.Arrays.stream(
                connection.getSchema().lookupCube("Navigation", true).getDimensions())
                .filter(dimension -> dimension.getName().equals("Calendar"))
                .findFirst().orElseThrow();
            assertFalse(calendar.getHierarchy().hasAll());
            assertCalendarCrossJoin(
                "Sum(Ytd(), [Measures].[Quantity])",
                "Navigation",
                java.util.List.of(
                    "[Product].[A],[Calendar].[2026].[8]=10.0",
                    "[Product].[A],[Calendar].[2026].[9]=10.0",
                    "[Product].[B],[Calendar].[2026].[8]=20.0",
                    "[Product].[B],[Calendar].[2026].[9]=20.0"));
        } finally {
            connection.close();
            connection = previous;
        }
    }

    private void assertCalendarCrossJoin(
        String formula, String cube, java.util.List<String> expected)
    {
        MondrianProperties props = MondrianProperties.instance();
        boolean previous = props.EnableNativeNonEmpty.get();
        RolapNativeRegistry registry =
            ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previousRegistry = registry.isEnabled();
        try {
            for (boolean nativeEnabled : new boolean[] {false, true}) {
                props.EnableNativeNonEmpty.set(nativeEnabled);
                registry.setEnabled(nativeEnabled);
                registry.flushAllNativeSetCache();
                for (boolean onColumns : new boolean[] {false, true}) {
                    String rows = "CrossJoin([Product].[Name].Members,"
                        + " [Calendar].[Month].Members)";
                    Result result = connection.execute(connection.parseQuery(
                        "WITH MEMBER [Measures].[Shifted] AS " + formula + " SELECT "
                            + (onColumns
                                ? "{[Measures].[Shifted]} ON 0, NON EMPTY " + rows
                                    + " ON 1 FROM [" + cube + "]"
                                : "NON EMPTY " + rows
                                    + " ON 0 FROM [" + cube + "] WHERE [Measures].[Shifted]")));
                    int axis = onColumns ? 1 : 0;
                    java.util.List<String> actual = new java.util.ArrayList<>();
                    for (int i = 0; i < result.getAxes()[axis].getPositions().size(); i++) {
                        var position = result.getAxes()[axis].getPositions().get(i);
                        Object value = result.getCell(
                            onColumns ? new int[] {0, i} : new int[] {i}).getValue();
                        actual.add(position.get(0).getUniqueName() + ","
                            + position.get(1).getUniqueName() + "="
                            + ((Number) value).doubleValue());
                    }
                    assertEquals(expected, actual,
                        formula + ", native=" + nativeEnabled + ", columns=" + onColumns);
                }
            }
        } finally {
            props.EnableNativeNonEmpty.set(previous);
            SqlConstraintFactory.setNativeNonEmptyValue();
            registry.setEnabled(previousRegistry);
        }
    }

    @Test void lastChildNavigatesFromAnchorNotCurrentPosition() {
        for (String cube : new String[] {"Navigation", "Combined"}) {
            assertEquals("[Calendar].[2025].[12]", onRow(
                "[Calendar].CurrentMember.PrevMember.LastChild.UniqueName", "[Calendar].[2026]", cube), cube);
        }
        assertEquals("[Calendar].[2025].[12]", scalar(
            "[Calendar].[2025].LastChild.UniqueName", "FROM [Navigation]", " WHERE [Calendar].[2026]"));
    }

    @Test void sameHierarchyMultiSelectSlicerStillBoundsNavigation() {
        assertEquals("[Calendar].[2026].[8].[36]", scalar(
            "ClosingPeriod([Calendar].[Week],[Calendar].[2026]).UniqueName", "FROM [Navigation]",
            " WHERE {[Calendar].[2026].[8].[35],[Calendar].[2026].[8].[36]}"));
    }

    @Test void existingStillHonorsSameHierarchyCurrentMember() {
        assertEquals(3d, ((Number) onRow(
            "Count(Existing [Calendar].[Week].Members)", "[Calendar].[2026]", "Navigation")).doubleValue());
    }

    @Test void unsupportedCalculatedSiblingMemberDoesNotRestrictNavigation() {
        assertEquals("[Calendar].[2026].[9]", onRow(
            "[Calendar].[2026].LastChild.UniqueName", "[Calendar.FlatWeek].[Custom]", "Navigation"));
        assertEquals("[Calendar].[2026].[9].[37]", onRow(
            "ClosingPeriod([Calendar].[Week],[Calendar].[2026]).UniqueName", "[Calendar.FlatWeek].[Custom]",
            "Navigation"));
        assertEquals(5d, ((Number) onRow(
            "Count(Existing [Calendar].[Week].Members)", "[Calendar.FlatWeek].[Custom]", "Navigation"))
            .doubleValue());
    }

    @Test void existingAllMemberHonorsEmptySameHierarchyIntersection() {
        assertEquals(0d, ((Number) scalar("Count(Existing {[Product].DefaultMember})",
            "FROM [Combined]", " WHERE ([Product].[B],[Product.Manufacturer].[Red])")).doubleValue());
    }

    @Test void nonEmptyLevelMembersUnderCalculatedMeasureIgnoreSameHierarchyPosition() {
        assertEquals(5, nonEmptyRows("Generate({[Calendar].[2026]},[Calendar].[Week].Members)").size());
    }

    @Test void nonEmptyChildrenUnderCalculatedMeasureNavigateFromAnchor() {
        assertEquals(java.util.List.of("[Calendar].[2025].[12]"),
            nonEmptyRows("Generate({[Calendar].[2026]},[Calendar].CurrentMember.PrevMember.Children)"));
    }

    private java.util.List<String> nonEmptyRows(String set) {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[One] AS 1 SELECT NON EMPTY " + set + " ON COLUMNS "
                + "FROM [Navigation] WHERE [Measures].[One]"));
        return result.getAxes()[0].getPositions().stream()
            .map(position -> position.get(0).getUniqueName()).toList();
    }

    /** Evaluates a probe on one row; the row may name {@code [Calendar.FlatWeek].[Custom]}. */
    private Object onRow(String expression, String row, String cube) {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Calendar.FlatWeek].[Custom] AS 1 "
                + "MEMBER [Measures].[Probe] AS " + expression
                + " SELECT {[Measures].[Probe]} ON COLUMNS, {" + row + "} ON ROWS FROM [" + cube + "]"));
        return result.getCell(new int[] {0, 0}).getValue();
    }

    private Object scalar(String expression, String from, String where) {
        Result result = connection.execute(connection.parseQuery(
            "WITH MEMBER [Measures].[Probe] AS " + expression
                + " SELECT {[Measures].[Probe]} ON COLUMNS " + from + where));
        return result.getCell(new int[] {0}).getValue();
    }
}
