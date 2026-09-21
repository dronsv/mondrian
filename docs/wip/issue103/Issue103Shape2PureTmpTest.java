package mondrian.rolap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/** THROWAWAY investigation probe for emondrian-clickhouse#103 shape 2. */
public class Issue103Shape2PureTmpTest {
    private static final String NQE = "mondrian.native.queryEngine.enable";
    private static final String AXIS = "CrossJoin([Store].[Name].Members, [Product].[Name].Members)";
    private static final String RED = "FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS FROM [Navigation]) ";
    private static final String PLAIN = "FROM [Navigation] ";

    private static final List<String> LOGS = Collections.synchronizedList(new ArrayList<>());
    private static final List<String> SQLS = Collections.synchronizedList(new ArrayList<>());

    private static String with(String memberExp) {
        return "WITH MEMBER [Measures].[M] AS ([Measures].[Quantity], " + memberExp + ") ";
    }

    private static Map<String, String> shapes() {
        Map<String, String> s = new LinkedHashMap<>();
        String lit = "[Calendar].[2026].[8].[36]";
        String nav = "[Calendar].[2026].[9].PrevMember.LastChild";
        String lastChild = "[Calendar].[2026].[8].LastChild";
        String next = "[Calendar].[2026].[8].[35].NextMember";
        String closing = "ClosingPeriod([Calendar].[Week], [Calendar].[2026].[8])";
        String cur = "[Calendar].CurrentMember.PrevMember";
        // A: member kind, NON EMPTY, slicer measure, subselect
        s.put("A1 literal", with(lit) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        s.put("A2 navigated PrevMember.LastChild", with(nav) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        s.put("A3 literal.LastChild", with(lastChild) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        s.put("A4 literal.NextMember", with(next) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        s.put("A5 ClosingPeriod(literal)", with(closing) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        s.put("A6 CurrentMember.PrevMember slicer week36", with(cur) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED
            + "WHERE ([Calendar].[2026].[8].[36], [Measures].[M])");
        s.put("A7 CurrentMember.PrevMember slicer week37", with(cur) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED
            + "WHERE ([Calendar].[2026].[9].[37], [Measures].[M])");
        // B: without NON EMPTY
        s.put("B1 literal, no NON EMPTY", with(lit) + "SELECT " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        s.put("B2 navigated, no NON EMPTY", with(nav) + "SELECT " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        // C: measure on axis
        s.put("C1 literal, measure on axis", with(lit) + "SELECT {[Measures].[M]} ON COLUMNS, NON EMPTY " + AXIS + " ON ROWS " + RED);
        s.put("C2 navigated, measure on axis", with(nav) + "SELECT {[Measures].[M]} ON COLUMNS, NON EMPTY " + AXIS + " ON ROWS " + RED);
        // D: without subselect
        s.put("D1 literal, no subselect", with(lit) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + PLAIN + "WHERE [Measures].[M]");
        s.put("D2 navigated, no subselect", with(nav) + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + PLAIN + "WHERE [Measures].[M]");
        // E: null-guarded variants (the production idiom)
        s.put("E1 IIF-guarded literal", "WITH MEMBER [Measures].[M] AS IIF(([Measures].[Quantity], " + lit + ") = 0, NULL, ([Measures].[Quantity], " + lit + ")) "
            + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        s.put("E2 IIF-guarded navigated", "WITH MEMBER [Measures].[M] AS IIF(([Measures].[Quantity], " + nav + ") = 0, NULL, ([Measures].[Quantity], " + nav + ")) "
            + "SELECT NON EMPTY " + AXIS + " ON COLUMNS " + RED + "WHERE [Measures].[M]");
        // P: production-analog shapes, calc members defined IN THE SCHEMA, stored measure next to them
        String rowsProducts = "NON EMPTY [Product].[Name].Members ON ROWS ";
        String m8 = "WHERE [Calendar].[2026].[8]";
        s.put("P1 Avg(Existing weeks, stored) next to stored", "SELECT {[Measures].[Quantity], [Measures].[AvgWeekly]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("P1b Avg(Existing weeks, stored) alone", "SELECT {[Measures].[AvgWeekly]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("P2 share Sum(Existing months, (stored, All)) next to stored", "SELECT {[Measures].[Quantity], [Measures].[ShareSku]} ON COLUMNS, " + rowsProducts + RED + "WHERE [Calendar].[2026]");
        s.put("P3 schema ClosingQty(stored) next to stored", "SELECT {[Measures].[Quantity], [Measures].[ClosingQty]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("P3b schema ClosingQty(stored) alone", "SELECT {[Measures].[ClosingQty]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("P4 ClosingNative(nativeSql) next to stored", "SELECT {[Measures].[Quantity], [Measures].[ClosingNative]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("P4b ClosingNative(nativeSql) next to NativeQty", "SELECT {[Measures].[NativeQty], [Measures].[ClosingNative]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("P5 pin tuple (stored, All Products) under product slicer + evaluator measure",
            "SELECT {[Measures].[Quantity], [Measures].[PinAllProducts], [Measures].[AvgWeekly]} ON COLUMNS, [Store].[Name].Members ON ROWS FROM [Navigation] WHERE [Product].[P002]");
        // S09: shape 1 of the issue (Sum over stores of WITH-defined ClosingQty)
        s.put("S09 issue shape 1: Sum over stores of ClosingQty",
            "WITH MEMBER [Measures].[CQ] AS IIF(([Measures].[Quantity], ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember)) = 0, NULL, "
            + "([Measures].[Quantity], ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember))) "
            + "MEMBER [Measures].[SumClose] AS Sum([Store].[Name].Members, [Measures].[CQ]) "
            + "SELECT {[Measures].[SumClose]} ON COLUMNS, NON EMPTY [Product].[Name].Members ON ROWS " + RED + "WHERE [Calendar.FlatWeek].[202635]");
        s.put("P2w share Sum(Existing weeks, (stored, All)) under month slicer, next to stored",
            "SELECT {[Measures].[Quantity], [Measures].[ShareSkuW]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("P2all share Sum(Existing months, (stored, All)) no calendar slicer, next to stored",
            "SELECT {[Measures].[Quantity], [Measures].[ShareSku]} ON COLUMNS, " + rowsProducts + RED);
        // F: FormulaAnalyzer hole - tuple whose non-measure arg is a member-valued call NOT in UNSAFE_FUNCTIONS
        s.put("F1 share of parent: Q / (Q, Product.CurrentMember.Parent)",
            "WITH MEMBER [Measures].[SoP] AS [Measures].[Quantity] / ([Measures].[Quantity], [Product].CurrentMember.Parent) "
            + "SELECT {[Measures].[SoP]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("F2 (Q, literal.Parent)", with("[Calendar].[2026].[8].[36].Parent") + "SELECT {[Measures].[M]} ON COLUMNS, " + rowsProducts + RED + "WHERE [Calendar].[2026].[8].[35]");
        s.put("F3 (Q, literal.FirstChild)", with("[Calendar].[2026].[8].FirstChild") + "SELECT {[Measures].[M]} ON COLUMNS, " + rowsProducts + RED);
        s.put("F4 (Q, Store.DefaultMember) under store slicer", with("[Store].DefaultMember") + "SELECT {[Measures].[M]} ON COLUMNS, " + rowsProducts + RED + "WHERE [Store].[S2]");
        s.put("F5 Stddev over literal set (function not in block-list)",
            "WITH MEMBER [Measures].[SD] AS Stddev({[Calendar].[2026].[8].[35], [Calendar].[2026].[8].[36]}, [Measures].[Quantity]) "
            + "SELECT {[Measures].[SD]} ON COLUMNS, " + rowsProducts + RED);
        // N: non-unique level key in the slicer (month 8 exists in 2025 and 2026)
        s.put("N1 plain stored + evaluator measure, slicer on non-unique month", "SELECT {[Measures].[Quantity], [Measures].[AvgWeekly]} ON COLUMNS, " + rowsProducts + RED + m8);
        s.put("N2 plain stored only (FULL_RESULT), slicer on non-unique month", "SELECT {[Measures].[Quantity]} ON COLUMNS, " + rowsProducts + RED + m8);
        // L: PREFETCH_ONLY groups by the LEAF level; non-leaf axis member key may collide with a leaf key
        s.put("L1 months on rows, evaluator measure next to stored",
            "WITH MEMBER [Measures].[E] AS ([Measures].[Quantity], [Store].[S3]) "
            + "SELECT {[Measures].[Quantity], [Measures].[E]} ON COLUMNS, [Calendar].[Month].Members ON ROWS FROM [Navigation]");
        return s;
    }

    @Test void probe() throws Exception {
        installLogCapture();
        int previousPreCache = MondrianProperties.instance().LevelPreCacheThreshold.get();
        String previousNqe = MondrianProperties.instance().getProperty(NQE);
        MondrianProperties.instance().LevelPreCacheThreshold.set(0);
        boolean previousNativeSql = MondrianProperties.instance().NativeSqlEnable.get();
        MondrianProperties.instance().NativeSqlEnable.set(true);
        StringBuilder out = new StringBuilder();
        String only = System.getProperty("i103.only");
        try {
            for (Map.Entry<String, String> shape : shapes().entrySet()) {
                if (only != null && !shape.getKey().startsWith(only)) {
                    continue;
                }
                out.append("\n==================================================================\n")
                    .append("### ").append(shape.getKey()).append('\n').append(shape.getValue()).append('\n');
                List<String> off = null;
                for (String run : new String[] {"false/main", "true/main"}) {
                    String nqe = run.substring(0, run.indexOf('/'));
                    MondrianProperties.instance().setProperty(NQE, nqe);
                    LOGS.clear();
                    SQLS.clear();
                    mondrian.olap.Connection connection = open(16, shape.getKey().startsWith("N"));
                    RolapUtil.setHook(sql -> SQLS.add("[" + origin() + "] " + sql.replaceAll("\\s+", " ")));
                    try {
                        Query query = connection.parseQuery(shape.getValue());
                        Result result = connection.execute(query);
                        List<String> cells = cells(result);
                        out.append("--- NQE=").append(run).append(" positions=").append(cells.size())
                            .append(" preNonEmptyStripAxis0=").append(underlyingAxisSize(result)).append('\n');
                        cells.forEach(c -> out.append("    ").append(c).append('\n'));
                        if (off == null) {
                            off = cells;
                        } else {
                            out.append("    VERDICT: ").append(normalize(off).equals(normalize(cells)) ? "SAME" : "DIFFERENT").append('\n');
                        }
                        out.append("  SQL:\n");
                        SQLS.forEach(q -> out.append("    ").append(q).append('\n'));
                        out.append("  LOG:\n");
                        LOGS.forEach(l -> out.append("    ").append(l).append('\n'));
                    } catch (Throwable e) {
                        out.append("--- NQE=").append(run).append(" EXCEPTION ").append(e).append('\n');
                        java.io.StringWriter sw = new java.io.StringWriter(); e.printStackTrace(new java.io.PrintWriter(sw));
                        out.append(sw.toString(), 0, Math.min(3000, sw.toString().length())).append('\n');
                    } finally {
                        RolapUtil.setHook(null);
                        connection.close();
                    }
                }
            }
        } finally {
            MondrianProperties.instance().NativeSqlEnable.set(previousNativeSql);
            MondrianProperties.instance().LevelPreCacheThreshold.set(previousPreCache);
            if (previousNqe == null) {
                MondrianProperties.instance().remove(NQE);
            } else {
                MondrianProperties.instance().setProperty(NQE, previousNqe);
            }
        }
        Files.writeString(Path.of("target", "issue103-s2-pure.txt"), out.toString());
    }

    private static List<String> normalize(List<String> cells) {
        List<String> n = new ArrayList<>();
        for (String c : cells) {
            n.add(c.replaceAll("\\.0$", ""));
        }
        return n;
    }

    private static String origin() {
        StringBuilder tags = new StringBuilder();
        boolean nqe = false, nativeSet = false, segment = false, member = false;
        for (StackTraceElement f : Thread.currentThread().getStackTrace()) {
            String c = f.getClassName();
            if (c.contains("NativeQuery")) { nqe = true; }
            if (c.contains("RolapNativeSet") || c.contains("SqlTupleReader")) { nativeSet = true; }
            if (c.contains("SegmentLoader")) { segment = true; }
            if (c.contains("SqlMemberSource")) { member = true; }
        }
        if (nqe) { tags.append("NQE "); }
        if (nativeSet) { tags.append("TUPLES "); }
        if (segment) { tags.append("SEGMENT "); }
        if (member) { tags.append("MEMBERS "); }
        return tags.toString().trim();
    }

    private static int underlyingAxisSize(Result result) {
        Result r = result;
        while (r instanceof RolapConnection.NonEmptyResult) {
            r = ((RolapConnection.NonEmptyResult) r).underlying;
        }
        return r.getAxes()[r.getAxes().length - 1].getPositions().size();
    }

    private static void installLogCapture() {
        Object ctxObj = LogManager.getContext(false);
        if (!(ctxObj instanceof LoggerContext)) {
            LOGS.add("log4j-core not active: " + ctxObj.getClass());
            return;
        }
        LoggerContext ctx = (LoggerContext) ctxObj;
        Configuration config = ctx.getConfiguration();
        AbstractAppender appender = new AbstractAppender("i103cap", null, null, true, Property.EMPTY_ARRAY) {
            @Override public void append(LogEvent event) {
                String name = event.getLoggerName();
                LOGS.add(event.getLevel() + " " + name.substring(name.lastIndexOf('.') + 1) + ": "
                    + event.getMessage().getFormattedMessage());
            }
        };
        appender.start();
        config.addAppender(appender);
        for (String name : new String[] {
            "mondrian.rolap.NativeQueryEngine", "mondrian.rolap.DependencyResolver",
            "mondrian.rolap.NativeQuerySqlGenerator", "mondrian.rolap.CoordinateClassMerger",
            "mondrian.rolap.NqeTableStrategy", "mondrian.rolap.FastBatchingCellReader",
            "mondrian.rolap.PrefetchBridge", "mondrian.rolap.MeasureClassifier", "mondrian.rolap.NativeSqlCalc"})
        {
            LoggerConfig lc = new LoggerConfig(name, Level.DEBUG, false);
            lc.addAppender(appender, Level.DEBUG, null);
            config.addLogger(name, lc);
        }
        // RolapResult logs "NQE prefetch: hits=..." through the ResultBase logger at INFO.
        LoggerConfig rb = new LoggerConfig("mondrian.olap.ResultBase", Level.INFO, false);
        rb.addAppender(appender, Level.INFO, null);
        config.addLogger("mondrian.olap.ResultBase", rb);
        ctx.updateLoggers();
    }

    private static String names(Position position) {
        return String.join(",", position.stream().map(member -> member.getUniqueName()).toList());
    }

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

    private static String name(int product) {
        return String.format("P%03d", product);
    }

    /** Same fixture as the #97 generalization probe. */
    static mondrian.olap.Connection open(int products, boolean nonUniqueMonth) throws Exception {
        String jdbc = "jdbc:h2:mem:i103s2_" + UUID.randomUUID().toString().replace("-", "")
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
            // overlap: P002 also sells 1000 in week 35 (S1) -> all-weeks value != week-36 value
            sql.execute("INSERT INTO fact VALUES (2,2,1,1000)");
            // week 8 lives in month 2; month 8 key collides with week key 8 (leaf-level prefetch)
            sql.execute("INSERT INTO calendar VALUES (6,2026,2,8,202608)");
            sql.execute("INSERT INTO fact VALUES (6,1,3,77777)");
            if (nonUniqueMonth) {
                sql.execute("INSERT INTO calendar VALUES (7,2025,8,33,202533)");
                sql.execute("INSERT INTO fact VALUES (7,4,2,5000)");
            }
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
        }
        Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="Issue103Shape2">
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
                <CalculatedMember name="AvgWeekly" dimension="Measures">
                  <Formula>Avg(Existing [Calendar].[Week].Members, [Measures].[Quantity])</Formula>
                </CalculatedMember>
                <CalculatedMember name="ShareSku" dimension="Measures">
                  <Formula>IIF(IsEmpty(Sum(Existing [Calendar].[Month].Members, ([Measures].[Quantity], [Product].[All Products]))) OR Sum(Existing [Calendar].[Month].Members, ([Measures].[Quantity], [Product].[All Products])) = 0, NULL, Sum(Existing [Calendar].[Month].Members, [Measures].[Quantity]) / Sum(Existing [Calendar].[Month].Members, ([Measures].[Quantity], [Product].[All Products]))) * 100</Formula>
                </CalculatedMember>
                <CalculatedMember name="ShareSkuW" dimension="Measures">
                  <Formula>IIF(IsEmpty(Sum(Existing [Calendar].[Week].Members, ([Measures].[Quantity], [Product].[All Products]))) OR Sum(Existing [Calendar].[Week].Members, ([Measures].[Quantity], [Product].[All Products])) = 0, NULL, Sum(Existing [Calendar].[Week].Members, [Measures].[Quantity]) / Sum(Existing [Calendar].[Week].Members, ([Measures].[Quantity], [Product].[All Products]))) * 100</Formula>
                </CalculatedMember>
                <CalculatedMember name="PinAllProducts" dimension="Measures">
                  <Formula>([Measures].[Quantity], [Product].[All Products])</Formula>
                </CalculatedMember>
                <CalculatedMember name="ClosingQty" dimension="Measures">
                  <Formula>IIF(([Measures].[Quantity], ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember)) = 0, NULL, ([Measures].[Quantity], ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember)))</Formula>
                </CalculatedMember>
                <CalculatedMember name="NativeQty" dimension="Measures">
                  <Annotations>
                    <Annotation name="nativeSql.enabled">true</Annotation>
                    <Annotation name="nativeSql.maxAxes">10</Annotation>
                    <Annotation name="nativeSql.template">SELECT ${axisResultSelectList} sum(pr.measure_value) AS val FROM (SELECT f.qty AS measure_value, 1 AS one${axisPresenceSelectList} FROM fact f ${factJoins} WHERE ${whereClause}) pr GROUP BY ${axisGroupByList}pr.one</Annotation>
                  </Annotations>
                  <Formula>-999</Formula>
                </CalculatedMember>
                <CalculatedMember name="ClosingNative" dimension="Measures">
                  <Formula>IIF(([Measures].[NativeQty], ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember)) = 0, NULL, ([Measures].[NativeQty], ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember)))</Formula>
                </CalculatedMember>
              </Cube>
            </Schema>
            """);
        return mondrian.olap.DriverManager.getConnection(props, null);
    }
}
