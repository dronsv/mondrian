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
                assertEquals(Arrays.asList("S1", "S2", "S3"), rows.stream()
                    .map(row -> row.get(0).getName()).toList());
                List<Double> values = new ArrayList<>();
                for (int r = 0; r < rows.size(); r++) {
                    for (int c = 0; c < result.getAxes()[0].getPositions().size(); c++) {
                        Cell cell = result.getCell(new int[] {c, r});
                        values.add(cell.isNull() ? null : ((Number) cell.getValue()).doubleValue());
                    }
                }
                if (nativeEnabled && nativeWhere != null) {
                    // Native-only queries intentionally use NSC directly.
                    assertTrue(capture.lines.stream().anyMatch(line ->
                        line.contains("NQE: mode=BYPASS")), capture.lines.toString());
                }
                if (nativeEnabled && nativeWhere == null) {
                    assertTrue(capture.lines.stream().anyMatch(line -> line.contains(
                        "NQE: mode=" + (prefetch ? "PREFETCH_ONLY" : "FULL_RESULT"))),
                        capture.lines.toString());
                    assertTrue(capture.lines.stream().anyMatch(line -> line.contains("executing SQL")),
                        "Regression must exercise NQE execution: " + capture.lines);
                    if (expectAggregate) {
                        assertTrue(capture.lines.stream().anyMatch(line -> line.contains("FROM f f")),
                            "Covered aggregate must remain usable: " + capture.lines);
                    }
                }
                return values;
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
                %s
              </Cube>
            </Schema>
            """.formatted(nativeMeasure("NativeQty",
                nativeWhere == null ? "${whereClause}" : nativeWhere)));
        return mondrian.olap.DriverManager.getConnection(props, null);
    }

    private static final class Capture extends AbstractAppender implements AutoCloseable {
        private static final String[] LOGGERS = {
            "mondrian.rolap.NativeQuerySqlGenerator", "mondrian.rolap.NativeQueryEngine"
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
