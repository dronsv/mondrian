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
            <Schema name="CalendarContext"><Cube name="Navigation">
              <Table name="fact"/>
              <Dimension name="Calendar" type="TimeDimension" foreignKey="calendar_id">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="calendar"/>
                  <Level name="Year" column="year" type="Integer" levelType="TimeYears" uniqueMembers="true"/>
                  <Level name="Month" column="month" type="Integer" levelType="TimeMonths" uniqueMembers="false"/>
                  <Level name="Week" column="week" type="Integer" levelType="TimeWeeks" uniqueMembers="false"/>
                </Hierarchy>
                <Hierarchy name="FlatWeek" hasAll="true" primaryKey="id"><Table name="calendar"/>
                  <Level name="Week" column="flat_week" type="Integer" levelType="TimeWeeks" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product" foreignKey="product_id">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
                <Hierarchy name="Manufacturer" hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="manufacturer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Measure name="Quantity" column="qty" aggregator="sum"/>
            </Cube></Schema>
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
}
