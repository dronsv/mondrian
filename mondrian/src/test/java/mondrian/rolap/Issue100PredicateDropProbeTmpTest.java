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
import mondrian.rolap.agg.AndPredicate;
import mondrian.rolap.agg.ListColumnPredicate;
import mondrian.rolap.agg.MemberColumnPredicate;
import mondrian.rolap.agg.NotPredicate;
import mondrian.rolap.agg.OrPredicate;
import mondrian.rolap.agg.SqlInSubqueryPredicate;
import mondrian.rolap.agg.ValueColumnPredicate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/**
 * THROWAWAY runtime probe for emondrian-clickhouse#100 (NQE silently drops
 * predicates). Not part of any change.
 */
public class Issue100PredicateDropProbeTmpTest {
    private static final String NQE = "mondrian.native.queryEngine.enable";
    private static final String NSC = "mondrian.native.sql.enable";
    private static final String USE_AGG = "mondrian.rolap.aggregates.Use";
    private static final String READ_AGG = "mondrian.rolap.aggregates.Read";

    private static final String STORES = "[Store].[Name].Members";

    private static String sub(String set) {
        return "FROM (SELECT " + set + " ON COLUMNS FROM [Navigation]) ";
    }

    private static String explicitList(int from, int to) {
        StringBuilder sb = new StringBuilder("{");
        for (int i = from; i <= to; i++) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append("[Product].[").append(name(i)).append("]");
        }
        return sb.append("}").toString();
    }

    private static Map<String, String> subselects() {
        Map<String, String> s = new LinkedHashMap<>();
        s.put("A Except(Members,{P016})",
            "Except([Product].[Name].Members, {[Product].[P016]})");
        s.put("B -{P016}", "-{[Product].[P016]}");
        s.put("C explicit list P001..P015", explicitList(1, 15));
        s.put("D caption contains '01'",
            "Filter([Product].[Name].AllMembers, "
                + "InStr([Product].CurrentMember.Member_Caption, \"01\") > 0)");
        s.put("E caption NOT contains '01'",
            "Filter([Product].[Name].AllMembers, "
                + "InStr([Product].CurrentMember.Member_Caption, \"01\") = 0)");
        s.put("F empty set (dynamic filter 1=0)",
            "Filter([Product].[Name].Members, 1 = 0)");
        s.put("G manufacturer Red (plain member, control)",
            "{[Product.Manufacturer].[Red]}");
        s.put("H tuple OR: (Red, AllStores) or (AllMfr, S3)",
            "{([Product.Manufacturer].[Red], [Store].[All Stores]), "
                + "([Product.Manufacturer].[All Mfr], [Store].[S3])}");
        s.put("I tuple AND-in-OR: (Red,S1) or (Blue,S3)",
            "{([Product.Manufacturer].[Red], [Store].[S1]), "
                + "([Product.Manufacturer].[Blue], [Store].[S3])}");
        return s;
    }

    private record Config(boolean agg, boolean nqe, boolean nsc) {
        String label() {
            return "agg=" + agg + " NQE=" + nqe + " NSC=" + nsc;
        }
    }

    @Test void probe() throws Exception {
        MondrianProperties p = MondrianProperties.instance();
        String prevNqe = p.getProperty(NQE);
        String prevNsc = p.getProperty(NSC);
        String prevUse = p.getProperty(USE_AGG);
        String prevRead = p.getProperty(READ_AGG);
        int prevPreCache = p.LevelPreCacheThreshold.get();
        p.LevelPreCacheThreshold.set(0);

        Capture capture = Capture.install();
        List<String> hookSql = Collections.synchronizedList(new ArrayList<>());
        RolapUtil.ExecuteQueryHook prevHook = RolapUtil.getHook();
        RolapUtil.setHook(hookSql::add);

        StringBuilder out = new StringBuilder();
        StringBuilder summary = new StringBuilder();
        try {
            // ---------- Part 1: stored measure on the axis ----------
            for (Map.Entry<String, String> s : subselects().entrySet()) {
                String mdx = "SELECT {[Measures].[Quantity]} ON COLUMNS, "
                    + STORES + " ON ROWS " + sub(s.getValue());
                runMatrix("STORED " + s.getKey(), mdx,
                    new Config[] {
                        new Config(false, false, true),
                        new Config(false, true, true),
                        new Config(true, false, true),
                        new Config(true, true, true)},
                    capture, hookSql, out, summary);
            }
            // stored + slicer on a non-unique level (other finding probe)
            runMatrix("STORED slicer [Calendar].[2026].[8], no subselect",
                "SELECT {[Measures].[Quantity]} ON COLUMNS, " + STORES
                    + " ON ROWS FROM [Navigation] WHERE [Calendar].[2026].[8]",
                new Config[] {
                    new Config(false, false, true),
                    new Config(false, true, true)},
                capture, hookSql, out, summary);

            Config[] four = {
                new Config(false, false, true), new Config(false, true, true),
                new Config(true, false, true), new Config(true, true, true)};
            runMatrix("STORED control: WHERE slicer [Product.Manufacturer].[Red]",
                "SELECT {[Measures].[Quantity]} ON COLUMNS, " + STORES
                    + " ON ROWS FROM [Navigation] WHERE [Product.Manufacturer].[Red]",
                four, capture, hookSql, out, summary);
            runMatrix("STORED rows [Calendar].[Month].Members (non-unique level projected)",
                "SELECT {[Measures].[Quantity]} ON COLUMNS, [Calendar].[Month].Members"
                    + " ON ROWS FROM [Navigation]",
                four, capture, hookSql, out, summary);
            runMatrix("STORED subselect {[Calendar].[2026].[8]} (control: subcube walks up parents)",
                "SELECT {[Measures].[Quantity]} ON COLUMNS, " + STORES
                    + " ON ROWS " + sub("{[Calendar].[2026].[8]}"),
                four, capture, hookSql, out, summary);
            runMatrix("STORED slicer FlatWeek 202635 + subselect Except (agg covers slicer)",
                "SELECT {[Measures].[Quantity]} ON COLUMNS, " + STORES
                    + " ON ROWS " + sub("Except([Product].[Name].Members, {[Product].[P016]})")
                    + "WHERE [Calendar.FlatWeek].[202635]",
                four, capture, hookSql, out, summary);

            // ---------- Part 2: native template measures ----------
            String[] nativeMeasures = {
                "[Measures].[NativeQty]",
                "[Measures].[NativeQtyX2]",
                "[Measures].[NativeQtyAlias]",
                "[Measures].[NativeExceptCalX2]",
                "[Measures].[NativeConstX2]",
                "[Measures].[NativeConstAlias]",
                "[Measures].[WideQty]",
                "[Measures].[WideQtyX2]",
                "[Measures].[WideQtyAlias]",
            };
            for (String measure : nativeMeasures) {
                for (Map.Entry<String, String> s : subselects().entrySet()) {
                    String mdx = "SELECT {" + measure + "} ON COLUMNS, "
                        + STORES + " ON ROWS " + sub(s.getValue())
                        + "WHERE [Calendar.FlatWeek].[202635]";
                    runMatrix("TEMPLATE " + measure + " " + s.getKey(), mdx,
                        new Config[] {
                            new Config(false, false, false),
                            new Config(false, false, true),
                            new Config(false, true, true),
                            new Config(false, true, false)},
                        capture, hookSql, out, summary);
                }
            }
            // mixed: stored + native on one axis (PREFETCH_ONLY)
            for (Map.Entry<String, String> s : subselects().entrySet()) {
                String mdx = "SELECT {[Measures].[Quantity], "
                    + "[Measures].[NativeQty]} ON COLUMNS, "
                    + STORES + " ON ROWS " + sub(s.getValue());
                runMatrix("MIXED stored+native " + s.getKey(), mdx,
                    new Config[] {
                        new Config(false, false, false),
                        new Config(false, true, true),
                        new Config(true, false, false),
                        new Config(true, true, true)},
                    capture, hookSql, out, summary);
            }
        } finally {
            RolapUtil.setHook(prevHook);
            capture.uninstall();
            p.LevelPreCacheThreshold.set(prevPreCache);
            restore(p, NQE, prevNqe);
            restore(p, NSC, prevNsc);
            restore(p, USE_AGG, prevUse);
            restore(p, READ_AGG, prevRead);
        }
        Files.writeString(Path.of("target", "issue100-probe-detail.txt"), out.toString());
        Files.writeString(Path.of("target", "issue100-probe-summary.txt"), summary.toString());
    }

    private static void restore(MondrianProperties p, String key, String prev) {
        if (prev == null) {
            p.remove(key);
        } else {
            p.setProperty(key, prev);
        }
    }

    private void runMatrix(
        String title, String mdx, Config[] configs,
        Capture capture, List<String> hookSql,
        StringBuilder out, StringBuilder summary)
    {
        out.append("\n\n################################################################\n")
            .append("### ").append(title).append('\n')
            .append("MDX: ").append(mdx).append('\n');
        summary.append("\n### ").append(title).append('\n')
            .append("MDX: ").append(mdx).append('\n');
        String oracle = null;
        for (Config c : configs) {
            MondrianProperties p = MondrianProperties.instance();
            p.setProperty(NQE, String.valueOf(c.nqe()));
            p.setProperty(NSC, String.valueOf(c.nsc()));
            p.setProperty(USE_AGG, String.valueOf(c.agg()));
            p.setProperty(READ_AGG, "false");
            capture.lines.clear();
            hookSql.clear();
            out.append("\n--- ").append(c.label()).append(" ---\n");
            String cellsLine;
            mondrian.olap.Connection connection = null;
            try {
                connection = open(16);
                Query query = connection.parseQuery(mdx);
                Result result = connection.execute(query);
                List<String> cells = cells(result);
                cellsLine = String.join("  ", cells);
                try {
                    RolapCube cube = (RolapCube) query.getCube();
                    out.append("predicate tree:\n");
                    dump(query.getSubcubePredicates(cube), 1, out);
                } catch (Throwable t) {
                    out.append("predicate tree: EXCEPTION ").append(t).append('\n');
                }
            } catch (Throwable e) {
                Throwable root = e;
                while (root.getCause() != null) {
                    root = root.getCause();
                }
                cellsLine = "EXCEPTION " + root.getClass().getName() + ": " + root.getMessage();
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
            out.append("cells: ").append(cellsLine).append('\n');
            out.append("legacy/hook SQL:\n");
            for (String sql : new ArrayList<>(hookSql)) {
                String flat = sql.replaceAll("\\s+", " ");
                if (flat.contains("\"fact\"") || flat.contains(" fact ")
                    || flat.contains("\"f\"") || flat.contains("FROM fact"))
                {
                    out.append("    ").append(flat).append('\n');
                }
            }
            out.append("native log:\n");
            for (String line : new ArrayList<>(capture.lines)) {
                out.append("    ").append(line.replaceAll("\\s+", " ")).append('\n');
            }
            String verdict;
            if (oracle == null) {
                oracle = cellsLine;
                verdict = "(oracle)";
            } else {
                verdict = normalize(oracle).equals(normalize(cellsLine)) ? "same" : "DIFFERENT";
            }
            summary.append(String.format("  %-28s %-10s %s%n", c.label(), verdict, cellsLine));
        }
    }

    /** 2.0 vs 2 is a known NQE-on/off type difference (#103 observation). */
    private static String normalize(String cells) {
        return cells.replaceAll("(\\d+)\\.0\\b", "$1");
    }

    private static void dump(StarPredicate pred, int depth, StringBuilder out) {
        String indent = "  ".repeat(depth);
        if (pred == null) {
            out.append(indent).append("null\n");
            return;
        }
        String cls = pred.getClass().getSimpleName();
        if (pred instanceof AndPredicate and) {
            out.append(indent).append(cls).append('\n');
            for (StarPredicate child : and.getChildren()) {
                dump(child, depth + 1, out);
            }
        } else if (pred instanceof OrPredicate or) {
            out.append(indent).append(cls).append(" (").append(or.getChildren().size())
                .append(" children)\n");
            int shown = 0;
            for (StarPredicate child : or.getChildren()) {
                if (shown++ >= 3) {
                    out.append(indent).append("  ...\n");
                    break;
                }
                dump(child, depth + 1, out);
            }
        } else if (pred instanceof NotPredicate not) {
            out.append(indent).append(cls).append('\n');
            dump(not.getInner(), depth + 1, out);
        } else if (pred instanceof ListColumnPredicate list) {
            out.append(indent).append(cls).append(" (").append(list.getPredicates().size())
                .append(" children)\n");
        } else if (pred instanceof MemberColumnPredicate mcp) {
            out.append(indent).append(cls).append(' ')
                .append(mcp.getConstrainedColumn().getExpression().getGenericExpression())
                .append(" = ").append(mcp.getMember().getUniqueName()).append('\n');
        } else if (pred instanceof ValueColumnPredicate vcp) {
            out.append(indent).append(cls).append(' ').append(vcp.getValue()).append('\n');
        } else if (pred instanceof SqlInSubqueryPredicate sip) {
            out.append(indent).append(cls).append(' ').append(sip.getSubquerySql()).append('\n');
        } else {
            out.append(indent).append(cls).append(' ').append(pred).append('\n');
        }
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
                cells.add(names(rows.get(r)) + "/" + names(columns.get(c)) + "="
                    + result.getCell(new int[] {c, r}).getValue());
            }
        }
        return cells;
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
        return """
            <CalculatedMember name="%s" dimension="Measures">
              <Annotations>
                <Annotation name="nativeSql.enabled">true</Annotation>
                <Annotation name="nativeSql.template"><![CDATA[%s]]></Annotation>
              </Annotations>
              <Formula>[Measures].[Quantity]</Formula>
            </CalculatedMember>
            """.formatted(name, TEMPLATE.formatted(where));
    }

    private static mondrian.olap.Connection open(int products) throws Exception {
        String jdbc = "jdbc:h2:mem:i100probe_" + UUID.randomUUID().toString().replace("-", "")
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
            <Schema name="Issue100Probe">
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
                %s
                %s
                <CalculatedMember name="NativeQtyX2" dimension="Measures">
                  <Formula>[Measures].[NativeQty] * 2</Formula>
                </CalculatedMember>
                <CalculatedMember name="NativeExceptCalX2" dimension="Measures">
                  <Formula>[Measures].[NativeExceptCal] * 2</Formula>
                </CalculatedMember>
                <CalculatedMember name="NativeQtyAlias" dimension="Measures">
                  <Formula>[Measures].[NativeQty]</Formula>
                </CalculatedMember>
                %s
                <CalculatedMember name="NativeConstX2" dimension="Measures">
                  <Formula>[Measures].[NativeConst] * 2</Formula>
                </CalculatedMember>
                <CalculatedMember name="NativeConstAlias" dimension="Measures">
                  <Formula>[Measures].[NativeConst]</Formula>
                </CalculatedMember>
                %s
                <CalculatedMember name="WideQtyX2" dimension="Measures">
                  <Formula>[Measures].[WideQty] * 2</Formula>
                </CalculatedMember>
                <CalculatedMember name="WideQtyAlias" dimension="Measures">
                  <Formula>[Measures].[WideQty]</Formula>
                </CalculatedMember>
              </Cube>
            </Schema>
            """.formatted(
                nativeMeasure("NativeQty", "${whereClause}"),
                nativeMeasure("NativeExceptCal", "${whereClauseExcept:Calendar}"),
                nativeMeasure("NativeConst", "${whereClause}").replace("SUM(pr.q) AS val", "777 AS val"),
                nativeMeasure("WideQty", "${whereClause}").replace("FROM fact f", "FROM wide f")));
        return mondrian.olap.DriverManager.getConnection(props, null);
    }

    /** Captures native-engine log lines at DEBUG. */
    private static final class Capture extends AbstractAppender {
        private static final String[] LOGGERS = {
            "mondrian.rolap.NativeQuerySqlGenerator",
            "mondrian.rolap.NativeQueryEngine",
            "mondrian.rolap.NqeTableStrategy",
            "mondrian.rolap.AggResolvedTable",
            "mondrian.rolap.NativeSqlCalc",
        };
        final List<String> lines = Collections.synchronizedList(new ArrayList<>());

        private Capture() {
            super("issue100-capture", null, null, true, Property.EMPTY_ARRAY);
        }

        static Capture install() {
            Capture capture = new Capture();
            capture.start();
            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration cfg = ctx.getConfiguration();
            cfg.addAppender(capture);
            for (String name : LOGGERS) {
                LoggerConfig lc = new LoggerConfig(name, Level.DEBUG, false);
                lc.addAppender(capture, Level.DEBUG, null);
                cfg.addLogger(name, lc);
            }
            ctx.updateLoggers();
            return capture;
        }

        void uninstall() {
            LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            Configuration cfg = ctx.getConfiguration();
            for (String name : LOGGERS) {
                cfg.removeLogger(name);
            }
            ctx.updateLoggers();
            stop();
        }

        @Override public void append(LogEvent event) {
            String logger = event.getLoggerName();
            String msg = event.getMessage().getFormattedMessage();
            if (logger.endsWith("NativeSqlCalc")
                && !(msg.contains("unsupported") || msg.contains("fallback")
                    || msg.contains("SQL") || msg.contains("sql")))
            {
                return;
            }
            lines.add(event.getLevel() + " "
                + logger.substring(logger.lastIndexOf('.') + 1) + ": " + msg);
        }
    }
}
