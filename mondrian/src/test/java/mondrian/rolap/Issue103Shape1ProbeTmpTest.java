package mondrian.rolap;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.Test;

/** THROWAWAY probe for emondrian-clickhouse#103 shape 1. Never commit. */
public class Issue103Shape1ProbeTmpTest {
    private static final String NQE = "mondrian.native.queryEngine.enable";
    private static final String RED = "FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS FROM [Navigation]) ";
    private static final String PLAIN = "FROM [Navigation] ";
    private static final String W35 = "[Calendar.FlatWeek].[202635]";
    private static final String ROWS = "NON EMPTY [Product].[Name].Members ON ROWS ";
    private static final String CP = "ClosingPeriod([Calendar].[Week],[Calendar].CurrentMember)";
    private static final String CLOSING = "MEMBER [Measures].[ClosingQty] AS IIF(([Measures].[Quantity], " + CP
        + ") = 0, NULL, ([Measures].[Quantity], " + CP + ")) ";

    private static String q(String members, String measure, String from, String where) {
        return "WITH " + members + "SELECT {" + measure + "} ON COLUMNS, " + ROWS + from
            + (where == null ? "" : "WHERE " + where);
    }

    private static Map<String, String> shapes() {
        Map<String, String> s = new LinkedHashMap<>();
        String sumClose = "MEMBER [Measures].[SumClose] AS Sum([Store].[Name].Members, [Measures].[ClosingQty]) ";
        // --- A. the issue shape and its minimisation
        s.put("A0 issue shape (ClosingPeriod+IIF, subselect, FlatWeek slicer)",
            q(CLOSING + sumClose, "[Measures].[SumClose]", RED, W35));
        s.put("A1 no subselect",
            q(CLOSING + sumClose, "[Measures].[SumClose]", PLAIN, W35));
        s.put("A2 no subselect, no slicer",
            q(CLOSING + sumClose, "[Measures].[SumClose]", PLAIN, null));
        s.put("A3 no IIF (tuple with ClosingPeriod only)",
            q("MEMBER [Measures].[C] AS ([Measures].[Quantity], " + CP + ") "
                + "MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[C]) ",
                "[Measures].[S]", PLAIN, W35));
        s.put("A4 calc = Quantity*1",
            q("MEMBER [Measures].[C] AS [Measures].[Quantity]*1 "
                + "MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[C]) ",
                "[Measures].[S]", PLAIN, W35));
        s.put("A5 calc = Quantity (alias)",
            q("MEMBER [Measures].[C] AS [Measures].[Quantity] "
                + "MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[C]) ",
                "[Measures].[S]", PLAIN, W35));
        s.put("A6 plain Sum(stores, Quantity), slicer",
            q("MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[Quantity]) ",
                "[Measures].[S]", PLAIN, W35));
        s.put("A7 plain Sum(stores, Quantity), no slicer",
            q("MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[Quantity]) ",
                "[Measures].[S]", PLAIN, null));
        s.put("A8 plain Sum(stores, Quantity), no slicer, not NON EMPTY",
            "WITH MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[Quantity]) "
                + "SELECT {[Measures].[S]} ON COLUMNS, {[Product].[P004],[Product].[P002]} ON ROWS FROM [Navigation]");
        s.put("A9 Sum over single store {S3} of Quantity (expect null for Red products)",
            q("MEMBER [Measures].[S] AS Sum({[Store].[S3]}, [Measures].[Quantity]) ",
                "[Measures].[S]", PLAIN, null));
        s.put("A10 tuple (Quantity,[Store].[S3]) (expect null for Red products)",
            q("MEMBER [Measures].[S] AS ([Measures].[Quantity], [Store].[S3]) ",
                "[Measures].[S]", PLAIN, null));
        s.put("A11 Sum(stores, Quantity) next to Quantity column",
            "WITH MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[Quantity]) "
                + "SELECT {[Measures].[Quantity],[Measures].[S]} ON COLUMNS, " + ROWS + PLAIN);
        s.put("A12 Aggregate(stores) calc member on Store hierarchy, Quantity",
            "WITH MEMBER [Store].[AllS] AS Aggregate([Store].[Name].Members) "
                + "SELECT {[Measures].[Quantity]} ON COLUMNS, " + ROWS + PLAIN + "WHERE [Store].[AllS]");
        s.put("A13 Count(Filter(stores, Quantity>0))",
            q("MEMBER [Measures].[S] AS Count(Filter([Store].[Name].Members, [Measures].[Quantity] > 0)) ",
                "[Measures].[S]", PLAIN, null));
        s.put("A14 Sum(stores, Quantity) with Store ALSO on the axis (crossjoin)",
            "WITH MEMBER [Measures].[S] AS Sum([Store].[Name].Members, [Measures].[Quantity]) "
                + "SELECT {[Measures].[S]} ON COLUMNS, NON EMPTY CrossJoin([Store].[Name].Members,"
                + "[Product].[Name].Members) ON ROWS FROM [Navigation]");
        s.put("A15 Sum over weeks of Quantity (iterated hierarchy = Calendar)",
            q("MEMBER [Measures].[S] AS Sum([Calendar].[Week].Members, [Measures].[Quantity]) ",
                "[Measures].[S]", PLAIN, null));
        s.put("A16 Max(stores, Quantity) (value should equal per-store max, not total)",
            q("MEMBER [Measures].[S] AS Max([Store].[Name].Members, [Measures].[Quantity]) ",
                "[Measures].[S]", PLAIN, null));
        // --- P. production-analogue shapes
        s.put("P1 share: Sum(Existing weeks, (Quantity,[Product].[All])) , weeks slicer",
            q("MEMBER [Measures].[S] AS Sum(Existing [Calendar.FlatWeek].[Week].Members, "
                + "([Measures].[Quantity], [Product].[All Products])) ",
                "[Measures].[S]", PLAIN, W35));
        s.put("P2 share: same, no slicer",
            q("MEMBER [Measures].[S] AS Sum(Existing [Calendar.FlatWeek].[Week].Members, "
                + "([Measures].[Quantity], [Product].[All Products])) ",
                "[Measures].[S]", PLAIN, null));
        s.put("P3 share ratio: Quantity / Sum(Existing weeks,(Quantity,All Products))",
            q("MEMBER [Measures].[S] AS [Measures].[Quantity] / Sum(Existing [Calendar.FlatWeek].[Week].Members, "
                + "([Measures].[Quantity], [Product].[All Products])) ",
                "[Measures].[S]", PLAIN, W35));
        s.put("P4 Sum(Existing products, calc) with Store on rows",
            "WITH MEMBER [Measures].[C] AS [Measures].[Quantity]*1 "
                + "MEMBER [Measures].[S] AS Sum(Existing [Product].[Name].Members, [Measures].[C]) "
                + "SELECT {[Measures].[S]} ON COLUMNS, NON EMPTY [Store].[Name].Members ON ROWS FROM [Navigation] "
                + "WHERE " + W35);
        s.put("P5 Sum(Existing products, Quantity) with Store on rows, Manufacturer slicer",
            "WITH MEMBER [Measures].[S] AS Sum(Existing [Product].[Name].Members, [Measures].[Quantity]) "
                + "SELECT {[Measures].[S]} ON COLUMNS, NON EMPTY [Store].[Name].Members ON ROWS FROM [Navigation] "
                + "WHERE [Product.Manufacturer].[Red]");
        s.put("P6 share with All of ANOTHER hierarchy than rows: Sum(Existing weeks,(Quantity,[Store].[All]))",
            q("MEMBER [Measures].[S] AS Sum(Existing [Calendar.FlatWeek].[Week].Members, "
                + "([Measures].[Quantity], [Store].[All Stores])) ",
                "[Measures].[S]", PLAIN, W35));

        // --- S2. issue #103 shape 2 (same root cause?)
        s.put("S2 issue shape 2: tuple with navigated member, NON EMPTY crossjoin",
            "WITH MEMBER [Measures].[PrevLastQty] AS "
                + "([Measures].[Quantity], [Calendar].[2026].[9].PrevMember.LastChild) "
                + "SELECT NON EMPTY CrossJoin([Store].[Name].Members, [Product].[Name].Members) ON COLUMNS " + RED
                + "WHERE [Measures].[PrevLastQty]");
        // --- Q. production analogue of "Доля продаж, руб СКЮ":
        //   Sum(Existing months, M) / Sum(Existing months, (M, [All product]))
        String wk = "Existing [Calendar].[Week].Members";
        String num = "Sum(" + wk + ", [Measures].[Quantity])";
        String den = "Sum(" + wk + ", ([Measures].[Quantity], [Product].[All Products]))";
        String share = "MEMBER [Measures].[Num] AS " + num + " MEMBER [Measures].[Den] AS " + den
            + " MEMBER [Measures].[Share] AS IIF(IsEmpty(" + den + ") OR " + den + " = 0, NULL, " + num + " / " + den
            + ") * 100 ";
        String cols3 = "{[Measures].[Num],[Measures].[Den],[Measures].[Share]}";
        s.put("Q1 share num/den/ratio, rows=Product, no slicer (5 weeks exist)",
            q(share, cols3.substring(1, cols3.length() - 1), PLAIN, null));
        s.put("Q2 share, rows=Product, slicer = month [Calendar].[2026].[8] (2 weeks exist)",
            q(share, cols3.substring(1, cols3.length() - 1), PLAIN, "[Calendar].[2026].[8]"));
        s.put("Q3 share, rows=Product, slicer = single week [Calendar].[2026].[8].[35]",
            q(share, cols3.substring(1, cols3.length() - 1), PLAIN, "[Calendar].[2026].[8].[35]"));
        s.put("Q4 share, rows=Product, compound slicer {week35, week36}",
            q(share, cols3.substring(1, cols3.length() - 1), PLAIN,
                "{[Calendar].[2026].[8].[35],[Calendar].[2026].[8].[36]}"));
        s.put("Q5 share, rows=Store (pinned hierarchy NOT on rows), slicer = month",
            "WITH " + share + "SELECT " + cols3 + " ON COLUMNS, NON EMPTY [Store].[Name].Members ON ROWS "
                + PLAIN + "WHERE [Calendar].[2026].[8]");
        s.put("Q6 share, weeks ON ROWS (iterated hierarchy projected), crossjoin with products",
            "WITH " + share + "SELECT " + cols3 + " ON COLUMNS, NON EMPTY CrossJoin([Calendar].[Week].Members,"
                + "{[Product].[P004],[Product].[P002]}) ON ROWS " + PLAIN);
        s.put("Q7 share, months ON ROWS (iterated hierarchy projected at a HIGHER level)",
            "WITH " + share + "SELECT " + cols3 + " ON COLUMNS, NON EMPTY CrossJoin([Calendar].[Month].Members,"
                + "{[Product].[P004],[Product].[P002]}) ON ROWS " + PLAIN);
        // --- R. production analogue of "Кумулятивная взвешенная дистрибуция":
        //   Sum(Existing products, <calc measure>)
        s.put("R1 Sum(Existing products, IIF-guarded ratio calc) rows=Store, week slicer",
            "WITH MEMBER [Measures].[C] AS IIF(IsEmpty([Measures].[Quantity]) OR [Measures].[Quantity]=0, NULL, "
                + "[Measures].[Quantity] / [Measures].[Quantity] * 100) "
                + "MEMBER [Measures].[S] AS Sum(Existing [Product].[Name].Members, [Measures].[C]) "
                + "SELECT {[Measures].[S]} ON COLUMNS, NON EMPTY [Store].[Name].Members ON ROWS FROM [Navigation] "
                + "WHERE " + W35);
        s.put("R2 Sum(Existing products, nativeSql measure) rows=Store, week slicer",
            "WITH MEMBER [Measures].[S] AS Sum(Existing [Product].[Name].Members, [Measures].[NativeQty]) "
                + "SELECT {[Measures].[S]} ON COLUMNS, NON EMPTY [Store].[Name].Members ON ROWS FROM [Navigation] "
                + "WHERE " + W35);
        s.put("R3 control: nativeSql measure alone, rows=Product, week slicer",
            "SELECT {[Measures].[NativeQty],[Measures].[Quantity]} ON COLUMNS, NON EMPTY [Product].[Name].Members "
                + "ON ROWS FROM [Navigation] WHERE " + W35);
        s.put("R4 Sum(Existing products, nativeSql measure) + stored Quantity column, rows=Store",
            "WITH MEMBER [Measures].[S] AS Sum(Existing [Product].[Name].Members, [Measures].[NativeQty]) "
                + "SELECT {[Measures].[Quantity],[Measures].[S]} ON COLUMNS, NON EMPTY [Store].[Name].Members "
                + "ON ROWS FROM [Navigation] WHERE " + W35);
        // --- T. calculated NON-measure member (not seen by MeasureClassifier)
        s.put("T1 calc Store member on rows next to real ones + EVALUATOR measure (PREFETCH_ONLY)",
            "WITH MEMBER [Store].[S1+S2] AS Aggregate({[Store].[S1],[Store].[S2]}) "
                + "MEMBER [Measures].[X] AS Sum({[Calendar].[2026]}, [Measures].[Quantity]) "
                + "SELECT {[Measures].[Quantity],[Measures].[X]} ON COLUMNS, "
                + "{[Store].[S1],[Store].[S2],[Store].[S3],[Store].[S1+S2]} ON ROWS FROM [Navigation]");
        s.put("T2 calc Store member on rows, stored measure only (FULL_RESULT)",
            "WITH MEMBER [Store].[S1+S2] AS Aggregate({[Store].[S1],[Store].[S2]}) "
                + "SELECT {[Measures].[Quantity]} ON COLUMNS, "
                + "{[Store].[S1],[Store].[S2],[Store].[S3],[Store].[S1+S2]} ON ROWS FROM [Navigation]");
        // --- K. key-collision probes on the dup-week fixture (week 35 also in 2025; week 8 in 2026-02)
        s.put("K1 DUP fixture: Quantity by week rows, FULL_RESULT",
            "SELECT {[Measures].[Quantity]} ON COLUMNS, NON EMPTY [Calendar].[Week].Members ON ROWS FROM [Navigation]");
        s.put("K2 DUP fixture: Quantity + evaluator measure by week rows, PREFETCH_ONLY",
            "WITH MEMBER [Measures].[X] AS Count([Store].[Name].Members) "
                + "SELECT {[Measures].[Quantity],[Measures].[X]} ON COLUMNS, NON EMPTY [Calendar].[Week].Members "
                + "ON ROWS FROM [Navigation]");
        s.put("K3 DUP fixture: Quantity + evaluator measure by MONTH rows, PREFETCH_ONLY",
            "WITH MEMBER [Measures].[X] AS Count([Store].[Name].Members) "
                + "SELECT {[Measures].[Quantity],[Measures].[X]} ON COLUMNS, NON EMPTY [Calendar].[Month].Members "
                + "ON ROWS FROM [Navigation]");
        s.put("K4 DUP fixture: Quantity by MONTH rows, FULL_RESULT",
            "SELECT {[Measures].[Quantity]} ON COLUMNS, NON EMPTY [Calendar].[Month].Members ON ROWS FROM [Navigation]");
        return s;
    }

    @Test void probe() throws Exception {
        int previousPreCache = MondrianProperties.instance().LevelPreCacheThreshold.get();
        String previousNqe = MondrianProperties.instance().getProperty(NQE);
        MondrianProperties.instance().LevelPreCacheThreshold.set(0);
        boolean previousNativeSql = MondrianProperties.instance().NativeSqlEnable.get();
        MondrianProperties.instance().NativeSqlEnable.set(true);
        org.apache.logging.log4j.core.config.Configurator.setLevel(
            "mondrian.rolap.NativeSqlCalc", org.apache.logging.log4j.Level.INFO);
        org.apache.logging.log4j.core.config.Configurator.setLevel(
            "mondrian.rolap.NativeSqlConfig", org.apache.logging.log4j.Level.INFO);
        StringBuilder out = new StringBuilder();
        String only = System.getProperty("issue103.only");
        try {
            for (Map.Entry<String, String> shape : shapes().entrySet()) {
                if (only != null && !shape.getKey().startsWith(only)) {
                    continue;
                }
                out.append("\n#### ").append(shape.getKey()).append('\n').append(shape.getValue()).append('\n');
                Map<String, List<String>> byMode = new LinkedHashMap<>();
                for (String mode : new String[] {"off", "on", "on+guard", "on+guard2", "on+poison"}) {
                    MondrianProperties.instance().setProperty(NQE, mode.equals("off") ? "false" : "true");
                    System.setProperty("issue103.guard", mode.startsWith("on+guard") ? "true" : "false");
                    System.setProperty("issue103.guard2", mode.equals("on+guard2") ? "true" : "false");
                    System.setProperty("issue103.poison", mode.equals("on+poison") ? "true" : "false");
                    List<String> sqls = new ArrayList<>();
                    RolapUtil.setHook(sqls::add);
                    FastBatchingCellReader.ISSUE103_TRACE.clear();
                    mondrian.olap.Connection connection = open(16, shape.getKey().contains("DUP fixture"));
                    List<String> cells;
                    try {
                        Query query = connection.parseQuery(shape.getValue());
                        Result result = connection.execute(query);
                        cells = cells(result);
                    } catch (Throwable e) {
                        Throwable root = e;
                        while (root.getCause() != null) {
                            root = root.getCause();
                        }
                        cells = List.of("EXCEPTION " + root);
                    } finally {
                        RolapUtil.setHook(null);
                        connection.close();
                    }
                    byMode.put(mode, cells);
                    out.append("-- NQE ").append(mode).append(": ").append(cells).append('\n');
                    for (String sql : sqls) {
                        if (sql.contains("\"fact\"") || sql.contains("fact")) {
                            out.append("   SQL: ").append(sql.replaceAll("\\s+", " ")).append('\n');
                        }
                    }
                    List<String> trace = new ArrayList<>(FastBatchingCellReader.ISSUE103_TRACE);
                    trace.sort((a, b) -> Integer.compare(rank(a), rank(b)));
                    int n = 0;
                    for (String t : trace) {
                        if (n++ >= 10) {
                            out.append("   TRACE ... (").append(trace.size()).append(" total)\n");
                            break;
                        }
                        out.append("   TRACE ").append(t).append('\n');
                    }
                }
                out.append("== verdict: on ").append(norm(byMode.get("off")).equals(norm(byMode.get("on")))
                    ? "MATCHES off" : "DIFFERS from off")
                    .append("; on+guard ").append(norm(byMode.get("off")).equals(norm(byMode.get("on+guard")))
                    ? "MATCHES off" : "DIFFERS from off")
                    .append("; on+guard2 ").append(norm(byMode.get("off")).equals(norm(byMode.get("on+guard2")))
                    ? "MATCHES off" : "DIFFERS from off")
                    .append("; on+poison ").append(norm(byMode.get("off")).equals(norm(byMode.get("on+poison")))
                    ? "MATCHES off" : "DIFFERS from off").append('\n');
            }
        } finally {
            System.clearProperty("issue103.guard");
            System.clearProperty("issue103.poison");
            System.clearProperty("issue103.guard2");
            MondrianProperties.instance().LevelPreCacheThreshold.set(previousPreCache);
            MondrianProperties.instance().NativeSqlEnable.set(previousNativeSql);
            if (previousNqe == null) {
                MondrianProperties.instance().remove(NQE);
            } else {
                MondrianProperties.instance().setProperty(NQE, previousNqe);
            }
        }
        Files.writeString(Path.of("target", "issue103-shape1.txt"), out.toString());
        System.out.println(out);
    }

    private static int rank(String t) {
        return t.startsWith("NQE-SQL") || t.startsWith("POISON") ? 0 : t.startsWith("PREFETCH") ? 1 : 2;
    }

    /** 2.0 vs 2 is the known Integer/Double difference, not this bug. */
    private static List<String> norm(List<String> cells) {
        List<String> r = new ArrayList<>();
        for (String c : cells) {
            r.add(c.replaceAll("(\\d)\\.0\\b", "$1"));
        }
        return r;
    }

    private static String names(Position position) {
        return String.join(",", position.stream().map(member -> member.getName()).toList());
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
                cells.add(names(rows.get(r)) + (columns.size() > 1 ? "/" + names(columns.get(c)) : "") + "="
                    + result.getCell(new int[] {c, r}).getValue());
            }
        }
        return cells;
    }

    private static String name(int product) {
        return String.format("P%03d", product);
    }

    /** Same fixture as DimensionContextConstraintReuseTest.open. */
    private static mondrian.olap.Connection open(int products, boolean dup) throws Exception {
        String jdbc = "jdbc:h2:mem:genprobe_" + UUID.randomUUID().toString().replace("-", "")
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
            if (dup) {
                // week 35 also exists in 2025-08; week 8 exists in 2026-02 (collides with month key 8)
                sql.execute("INSERT INTO calendar VALUES (6,2025,8,35,202535),(7,2026,2,8,202608)");
                sql.execute("INSERT INTO fact VALUES (6,4,1,1000),(7,4,1,50000)");
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
            <Schema name="ConstraintReuseProbe">
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
                <CalculatedMember name="NativeQty" dimension="Measures">
                  <Annotations>
                    <Annotation name="nativeSql.enabled">true</Annotation>
                    <Annotation name="nativeSql.maxAxes">4</Annotation>
                    <Annotation name="nativeSql.template"><![CDATA[SELECT ${axisResultSelectList} SUM(pr.q) AS val FROM (SELECT f.qty AS q, 1 AS g${axisPresenceSelectList} FROM fact f ${factJoins} ${joinClauses} WHERE ${whereClause}) pr GROUP BY ${axisGroupByList}pr.g]]></Annotation>
                  </Annotations>
                  <Formula>NULL</Formula>
                </CalculatedMember>
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
            </Schema>
            """);
        return mondrian.olap.DriverManager.getConnection(props, null);
    }
}
