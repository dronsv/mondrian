package mondrian.rolap;

import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.ResourceLimitExceededException;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #97: a native crossjoin without a stored fact carrier is a cartesian of its
 * dimension tables. With the split on, each table is read by its own statement
 * and the product is built in Java; candidates, order and cells stay the same.
 * Every expected list was captured from the engine before the split existed.
 *
 * <p>Each run gets its own database, so its own schema and native set cache:
 * no run can answer from another run's cached list.
 */
public class FactlessCrossJoinSplitTest {
    private static final String NATIVE_QUERY_ENGINE = "mondrian.native.queryEngine.enable";

    private static final String ONE = "WITH MEMBER [Measures].[One] AS 1 ";
    private static final String DRILL_STORE =
        "Hierarchize({DrilldownLevel({[Store].[All Stores]}, , , INCLUDE_CALC_MEMBERS)})";
    private static final String DRILL_PRODUCT =
        "Hierarchize({DrilldownLevel({[Product].[All Products]}, , , INCLUDE_CALC_MEMBERS)})";
    /** The Excel first-level pivot of #97. */
    private static final String DRILLED = "SELECT NON EMPTY CrossJoin(" + DRILL_STORE + ", " + DRILL_PRODUCT
        + ") ON COLUMNS ";
    private static final String STORES = "[Store].[Store].Members";
    private static final String PRODUCTS = "[Product].[Product].Members";
    private static final String STORE_BY_PRODUCT = "CrossJoin(" + STORES + ", " + PRODUCTS + ")";
    private static final String MEMBERS = "SELECT NON EMPTY " + STORE_BY_PRODUCT + " ON COLUMNS ";
    /** A sibling hierarchy of the product axis: it restricts the product table only. */
    private static final String RED = "FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS FROM [Sales]) ";
    private static final String ONLY_ONE = "WHERE [Measures].[One]";
    private static final String WEEK_35_STOCK = "WHERE ([Calendar].[202635], [Measures].[Stock])";
    private static final String ISSUE_97 = ONE + DRILLED + RED + "WHERE ([Calendar].[202635], [Measures].[One])";
    private static final String ISSUE_97_STOCK = DRILLED + RED + WEEK_35_STOCK;
    /** Region and manufacturer are correlated: no predicate on one table alone says the same. */
    private static final String CORRELATED = "([Store.Geo].[E],[Product.Manufacturer].[Red]),"
        + "([Store.Geo].[W],[Product.Manufacturer].[Blue])";
    private static final String COMPOUND_SLICER = ONE + DRILLED + "FROM [Sales] WHERE CrossJoin({[Measures].[One]}, {"
        + CORRELATED + "})";
    private static final String CORRELATED_SQL =
        "((\"store\".\"region\" = 'E' and \"product\".\"manufacturer\" = 'Red') "
        + "or (\"store\".\"region\" = 'W' and \"product\".\"manufacturer\" = 'Blue'))";

    private static final List<String> ALL_STORES = members("[Store]", "All Stores", 3, 2, 4, 1);
    private static final List<String> RED_PRODUCTS = members("[Product]", 5, 6, 4, 3, 2, 1);
    private static final List<String> ALL_RED_PRODUCTS = members("[Product]", "All Products", 5, 6, 4, 3, 2, 1);
    /** Without the twins: South (4) sorts before Twin (2), P07 (6) before P08 (5). */
    private static final List<String> DISTINCT_STORES = members("[Store]", 3, 4, 2, 1);
    private static final List<String> DISTINCT_RED_PRODUCTS = members("[Product]", 6, 5, 4, 3, 2, 1);

    /** What the engine issues today for {@link #ISSUE_97}: one statement over both dimension tables. */
    private static final String LEGACY_SQL = "select \"store\".\"store_id\" as \"c0\", "
        + "\"store\".\"caption\" as \"c1\", \"store\".\"caption\" as \"c2\", \"product\".\"product_id\" as \"c3\", "
        + "\"product\".\"name\" as \"c4\", \"product\".\"name\" as \"c5\" "
        + "from \"store\" as \"store\", \"product\" as \"product\" "
        + "where (true and true and true and true and true and \"product\".\"manufacturer\" = 'Red') "
        + "group by \"store\".\"store_id\", \"store\".\"caption\", \"product\".\"product_id\", \"product\".\"name\" "
        + "order by CASE WHEN \"store\".\"caption\" IS NULL THEN 1 ELSE 0 END, \"store\".\"caption\" ASC, "
        + "CASE WHEN \"product\".\"name\" IS NULL THEN 1 ELSE 0 END, \"product\".\"name\" ASC";

    private final List<mondrian.olap.Connection> connections = new ArrayList<>();
    private String previousNativeQueryEngine;
    private int previousPreCache;
    private boolean previousNativeSql;
    private boolean previousSplit;
    private int previousMaxCandidates;
    private int previousResultLimit;
    private boolean previousExpandNonNative;

    @BeforeEach void saveProperties() {
        MondrianProperties properties = MondrianProperties.instance();
        previousPreCache = properties.LevelPreCacheThreshold.get();
        properties.LevelPreCacheThreshold.set(0);
        previousNativeQueryEngine = properties.getProperty(NATIVE_QUERY_ENGINE);
        properties.setProperty(NATIVE_QUERY_ENGINE, "false");
        previousNativeSql = properties.NativeSqlEnable.get();
        properties.NativeSqlEnable.set(true);
        previousSplit = properties.CrossJoinFactlessSplit.get();
        previousMaxCandidates = properties.CrossJoinFactlessSplitMaxCandidates.get();
        previousResultLimit = properties.ResultLimit.get();
        previousExpandNonNative = properties.ExpandNonNative.get();
    }

    @AfterEach void close() {
        RolapUtil.setHook(null);
        MondrianProperties properties = MondrianProperties.instance();
        properties.LevelPreCacheThreshold.set(previousPreCache);
        if (previousNativeQueryEngine == null) {
            properties.remove(NATIVE_QUERY_ENGINE);
        } else {
            properties.setProperty(NATIVE_QUERY_ENGINE, previousNativeQueryEngine);
        }
        properties.NativeSqlEnable.set(previousNativeSql);
        properties.CrossJoinFactlessSplit.set(previousSplit);
        properties.CrossJoinFactlessSplitMaxCandidates.set(previousMaxCandidates);
        properties.ResultLimit.set(previousResultLimit);
        properties.ExpandNonNative.set(previousExpandNonNative);
        connections.forEach(mondrian.olap.Connection::close);
    }

    /**
     * How one run differs from the default. {@code twins}: stores 2 and 4
     * share a caption and products 5 and 6 a name, and the caption is the
     * order key. The role is only part of the schema that uses it: resolving
     * its grants reads the stores before the query does.
     */
    private record Setup(boolean split, int maxCandidates, int resultLimit, String role, boolean twins,
        boolean expandNonNative)
    {
        static final Setup OFF = new Setup(false, 2_000_000, 0, null, true, false);
        static final Setup ON = OFF.split(true);

        Setup split(boolean value) {
            return new Setup(value, maxCandidates, resultLimit, role, twins, expandNonNative);
        }

        Setup maxCandidates(int value) {
            return new Setup(split, value, resultLimit, role, twins, expandNonNative);
        }

        Setup resultLimit(int value) {
            return new Setup(split, maxCandidates, value, role, twins, expandNonNative);
        }

        Setup role(String value) {
            return new Setup(split, maxCandidates, resultLimit, value, twins, expandNonNative);
        }

        Setup distinctCaptions() {
            return new Setup(split, maxCandidates, resultLimit, role, false, expandNonNative);
        }

        Setup expandingNonNative() {
            return new Setup(split, maxCandidates, resultLimit, role, twins, true);
        }

        void apply() {
            MondrianProperties properties = MondrianProperties.instance();
            properties.CrossJoinFactlessSplit.set(split);
            properties.CrossJoinFactlessSplitMaxCandidates.set(maxCandidates);
            properties.ResultLimit.set(resultLimit);
            properties.ExpandNonNative.set(expandNonNative);
        }
    }

    /**
     * Stock is an independent fact. As (store, product): (2,3), (4,5) and
     * (4,6) hold stock without any sale, (3,8) is Blue, (3,2) is week 36 only.
     */
    private mondrian.olap.Connection open(Setup setup) throws Exception {
        return open(setup, 0);
    }

    private mondrian.olap.Connection open(Setup setup, int storeAliases) throws Exception {
        String jdbc = "jdbc:h2:mem:split_" + UUID.randomUUID().toString().replace("-", "")
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;NON_KEYWORDS=WEEK";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "")) {
            Statement sql = db.createStatement();
            sql.execute("CREATE TABLE calendar (week_id INT, week INT)");
            sql.execute("INSERT INTO calendar VALUES (1,202635),(2,202636)");
            sql.execute("CREATE TABLE store (store_id INT, caption VARCHAR, region VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'West','W'),(2,'Twin','E'),(3,'North','N'),(4,'"
                + (setup.twins() ? "Twin" : "South") + "','W')");
            sql.execute("CREATE TABLE product (product_id INT, name VARCHAR, manufacturer VARCHAR, brand VARCHAR)");
            sql.execute("INSERT INTO product VALUES "
                + "(1,'P12','Red','Acme'),(2,'P11','Red','Acme'),(3,'P10','Red','Acme'),"
                + "(4,'P09','Red','Bolt'),(5,'P08','Red','Bolt'),(6,'" + (setup.twins() ? "P08" : "P07")
                + "','Red','Bolt'),(7,'P06','Blue','Cog'),(8,'P05','Blue','Cog'),(9,'P04','Blue','Dyn'),"
                + "(10,'P03','Blue','Dyn'),(11,'P02','Green','Elm'),(12,'P01','Green','Elm')");
            sql.execute("CREATE TABLE fact (week_id INT, product_id INT, store_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (1,1,1,10),(1,2,2,20),(1,4,3,40),(1,7,4,70),(2,5,2,50),"
                + "(1,11,1,110)");
            sql.execute("CREATE TABLE stock (week_id INT, product_id INT, store_id INT, qty INT)");
            sql.execute("INSERT INTO stock VALUES (1,1,1,100),(1,3,2,300),(1,5,4,500),(1,6,4,600),(1,8,3,800),"
                + "(2,2,3,900)");
        }
        Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="FactlessSplit">
              <Dimension name="Calendar">
                <Hierarchy hasAll="true" allMemberName="All Weeks" primaryKey="week_id"><Table name="calendar"/>
                  <Level name="Week" column="week" type="Integer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Store">
                <Hierarchy hasAll="true" allMemberName="All Stores" primaryKey="store_id"><Table name="store"/>
                  <Level name="Store" column="store_id" captionColumn="caption" ordinalColumn="caption"
                      type="Integer" uniqueMembers="true"/>
                </Hierarchy>
                <Hierarchy name="Geo" hasAll="true" allMemberName="All Geo" primaryKey="store_id">
                  <Table name="store"/>
                  <Level name="Region" column="region" uniqueMembers="true"/>
                  <Level name="Store" column="store_id" captionColumn="caption" ordinalColumn="caption"
                      type="Integer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" allMemberName="All Products" primaryKey="product_id">
                  <Table name="product"/>
                  <Level name="Product" column="product_id" captionColumn="name" ordinalColumn="name"
                      type="Integer" uniqueMembers="true"/>
                </Hierarchy>
                <Hierarchy name="Manufacturer" hasAll="true" primaryKey="product_id"><Table name="product"/>
                  <Level name="Manufacturer" column="manufacturer" uniqueMembers="true"/>
                </Hierarchy>
                <Hierarchy name="Brand" hasAll="true" allMemberName="All Brands" primaryKey="product_id">
                  <Table name="product"/>
                  <Level name="Brand" column="brand" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="fact"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="week_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                <CalculatedMember name="Stock" dimension="Measures">
                  <Annotations>
                    <Annotation name="nativeSql.enabled">true</Annotation>
                    <Annotation name="nativeSql.template"><![CDATA[
                      SELECT ${axisResultSelectList} sum(pr.measure_value) AS val
                      FROM (SELECT ${factAlias}.qty AS measure_value, 0 AS g${axisPresenceSelectList}
                            FROM stock f
                            ${factJoins}
                            WHERE ${whereClause}) pr
                      GROUP BY ${axisGroupByList} pr.g]]></Annotation>
                  </Annotations>
                  <Formula>CDbl("Independent fact unavailable: native SQL execution required")</Formula>
                </CalculatedMember>
              </Cube>
              <Cube name="Stock"><Table name="stock"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="week_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="StockQuantity" column="qty" aggregator="sum"/>
              </Cube>
              <VirtualCube name="Combined">
                <VirtualCubeDimension name="Calendar"/>
                <VirtualCubeDimension name="Store"/>
                <VirtualCubeDimension name="Product"/>
                <VirtualCubeMeasure cubeName="Sales" name="[Measures].[Quantity]"/>
                <VirtualCubeMeasure cubeName="Stock" name="[Measures].[StockQuantity]"/>
              </VirtualCube>
              %s
            </Schema>
            """.formatted(setup.role() == null ? "" : """
              <Role name="NoWest"><SchemaGrant access="all">
                <CubeGrant cube="Sales" access="all">
                  <HierarchyGrant hierarchy="[Store]" access="custom" rollupPolicy="partial">
                    <MemberGrant member="[Store].[2]" access="all"/>
                    <MemberGrant member="[Store].[3]" access="all"/>
                    <MemberGrant member="[Store].[4]" access="all"/>
                  </HierarchyGrant>
                </CubeGrant>
              </SchemaGrant></Role>
            """));
        if (storeAliases > 0) {
            String cube = "<Cube name=\"Sales\"><Table name=\"fact\"/>";
            StringBuilder usages = new StringBuilder(cube);
            for (int i = 0; i < storeAliases; i++) {
                usages.append("<DimensionUsage name=\"StoreAlias").append(i)
                    .append("\" source=\"Store\" foreignKey=\"store_id\"/>");
            }
            props.put("CatalogContent", props.get("CatalogContent").replace(cube, usages));
        }
        mondrian.olap.Connection connection = mondrian.olap.DriverManager.getConnection(props, null);
        connections.add(connection);
        if (setup.role() != null) {
            connection.setRole(connection.getSchema().lookupRole(setup.role()));
        }
        return connection;
    }

    /** One execution: its cells, every statement it issued, and what stopped it, if anything did. */
    private record Run(List<String> cells, List<String> sql, Throwable failure) {
        /** The statements that read both dimension tables: the cartesian, or its fact-joined form. */
        List<String> joint() {
            return sql.stream().filter(statement -> !isCount(statement)
                && reads(statement, "store") && reads(statement, "product"))
                .toList();
        }

        List<String> guards() {
            return sql.stream().filter(FactlessCrossJoinSplitTest::isCount).toList();
        }

        /** The statements of this run that {@code other} did not issue. */
        List<String> without(Run other) {
            List<String> rest = new ArrayList<>(sql);
            other.sql.forEach(rest::remove);
            return rest;
        }
    }

    private static boolean reads(String statement, String table) {
        return statement.contains("\"" + table + "\" as \"" + table + "\"");
    }

    /** A guard statement: scalar counts over derived tables, never sorted candidate rows. */
    private static boolean isCount(String statement) {
        return statement.contains("factless_count")
            && !statement.toLowerCase(java.util.Locale.ROOT).contains("order by");
    }

    private Run run(Setup setup, String mdx) throws Exception {
        setup.apply();
        return execute(open(setup), mdx);
    }

    private static Run execute(mondrian.olap.Connection connection, String mdx) {
        // Statements run on the executor's threads.
        List<String> statements = Collections.synchronizedList(new ArrayList<>());
        RolapUtil.setHook(statements::add);
        try {
            return new Run(cells(connection.execute(connection.parseQuery(mdx))), statements, null);
        } catch (RuntimeException failure) {
            Throwable root = failure;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            return new Run(null, statements, root);
        } finally {
            RolapUtil.setHook(null);
        }
    }

    private static String names(Position position) {
        return String.join(",", position.stream().map(member -> member.getUniqueName()).toList());
    }

    /** One entry per position of the only axis: {@code "members=value"}. */
    private static List<String> cells(Result result) {
        List<Position> columns = result.getAxes()[0].getPositions();
        List<String> cells = new ArrayList<>();
        for (int c = 0; c < columns.size(); c++) {
            cells.add(names(columns.get(c)) + "=" + result.getCell(new int[] {c}).getValue());
        }
        return cells;
    }

    private static List<String> members(String hierarchy, Object... names) {
        return Arrays.stream(names).map(name -> hierarchy + ".[" + name + "]").toList();
    }

    /** The product of the lists, in list order, every cell holding {@code value}. */
    @SafeVarargs
    private static List<String> product(Object value, List<String>... lists) {
        List<String> tuples = List.of("");
        for (List<String> list : lists) {
            List<String> longer = new ArrayList<>();
            for (String tuple : tuples) {
                for (String member : list) {
                    longer.add(tuple.isEmpty() ? member : tuple + "," + member);
                }
            }
            tuples = longer;
        }
        return tuples.stream().map(tuple -> tuple + "=" + value).toList();
    }

    /** {@code "3-5 2-6"} is (first 3, second 5), (first 2, second 6); every cell holds {@code value}. */
    private static List<String> pairs(String first, String second, String keys, Object value) {
        return Arrays.stream(keys.split(" ")).map(pair -> pair.split("-"))
            .map(pair -> first + ".[" + pair[0] + "]," + second + ".[" + pair[1] + "]=" + value).toList();
    }

    /** Runs {@code mdx} with the split off and on; the off result is {@code expected}, the on result equal to it. */
    private Run[] offAndOn(Setup setup, String mdx, List<String> expected) throws Exception {
        Run off = run(setup.split(false), mdx);
        Run on = run(setup.split(true), mdx);
        assertNull(off.failure(), () -> "split off: " + off.failure());
        assertNull(on.failure(), () -> "split on: " + on.failure());
        assertEquals(expected, off.cells(), "split off");
        assertEquals(off.cells(), on.cells(), "the split changed the result");
        return new Run[] {off, on};
    }

    /**
     * The split applies. Off: one fact-less statement over both tables, which
     * also shows that neither run was served from a cache. On: no statement
     * reads both tables; in its place one reads the store table alone and one
     * the product table alone.
     *
     * @return the store statement and the product statement
     */
    private String[] assertSplit(Setup setup, String mdx, List<String> expected) throws Exception {
        Run[] runs = offAndOn(setup, mdx, expected);
        Run off = runs[0];
        Run on = runs[1];
        assertEquals(1, off.joint().size(), "split off must read both tables at once: " + off.sql());
        assertFalse(reads(off.joint().get(0), "fact"), "not a fact-less context: " + off.joint());
        assertEquals(List.of(), on.joint(), "split on still reads both dimension tables in one statement");
        assertEquals(off.joint(), off.without(on), "split on must only drop the cartesian");
        List<String> added = on.without(off).stream().filter(sql -> !isCount(sql)).toList();
        assertEquals(2, added.size(), "split on must read each table once: " + added);
        assertTrue(on.guards().size() <= 1, "order ties and group sizes take one statement: " + on.guards());
        for (String statement : added) {
            assertFalse(reads(statement, "fact") || reads(statement, "stock"), "a fact was joined: " + statement);
        }
        return new String[] {
            added.stream().filter(statement -> reads(statement, "store")).findFirst().orElseThrow(),
            added.stream().filter(statement -> reads(statement, "product")).findFirst().orElseThrow()};
    }

    /**
     * The split must not apply: both runs issue the same statements, the
     * tuple statement among them. Sorted, because segments load in parallel.
     */
    private Run assertLegacy(Setup setup, String mdx, List<String> expected) throws Exception {
        Run[] runs = offAndOn(setup, mdx, expected);
        assertTrue(runs[0].sql().stream().anyMatch(statement -> reads(statement, "product")),
            "nothing was read: " + runs[0].sql());
        assertEquals(runs[0].sql().stream().sorted().toList(), runs[1].sql().stream().sorted().toList(),
            "the split changed the statements of a shape it must leave alone");
        return runs[0];
    }

    // (a) the #97 shape

    @Test void issue97ShapeReadsEachDimensionTableByItself() throws Exception {
        assertEquals(35, product(1, ALL_STORES, ALL_RED_PRODUCTS).size());
        String[] statements = assertSplit(Setup.ON, ISSUE_97, product(1, ALL_STORES, ALL_RED_PRODUCTS));
        // The subselect restricts the table it belongs to, and only that one.
        assertFalse(statements[0].contains("manufacturer"), statements[0]);
        assertTrue(statements[1].contains("\"product\".\"manufacturer\" = 'Red'"), statements[1]);
    }

    /** Emptiness is decided by the stock fact alone: three of the four pairs were never sold. */
    @Test void issue97ShapeUnderNativeSqlMeasureReadsEachDimensionTableByItself() throws Exception {
        String[] statements = assertSplit(Setup.ON, ISSUE_97_STOCK, List.of(
            "[Store].[All Stores],[Product].[All Products]=1500.0",
            "[Store].[All Stores],[Product].[5]=500.0",
            "[Store].[All Stores],[Product].[6]=600.0",
            "[Store].[All Stores],[Product].[3]=300.0",
            "[Store].[All Stores],[Product].[1]=100.0",
            "[Store].[2],[Product].[All Products]=300.0",
            "[Store].[2],[Product].[3]=300.0",
            "[Store].[4],[Product].[All Products]=1100.0",
            "[Store].[4],[Product].[5]=500.0",
            "[Store].[4],[Product].[6]=600.0",
            "[Store].[1],[Product].[All Products]=100.0",
            "[Store].[1],[Product].[1]=100.0"));
        assertTrue(statements[1].contains("\"product\".\"manufacturer\" = 'Red'"), statements[1]);
    }

    /** Subselect axes on different tables are separate conjuncts: each goes to its own statement. */
    @Test void separateSubselectAxesRestrictTheirOwnTable() throws Exception {
        String[] statements = assertSplit(Setup.ON, ONE + DRILLED
            + "FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS, {[Store.Geo].[E],[Store.Geo].[W]} ON ROWS "
            + "FROM [Sales]) " + ONLY_ONE, product(1, members("[Store]", "All Stores", 2, 4, 1), ALL_RED_PRODUCTS));
        assertTrue(statements[0].contains("\"store\".\"region\"") && !statements[0].contains("manufacturer"),
            statements[0]);
        assertTrue(statements[1].contains("\"product\".\"manufacturer\" = 'Red'")
            && !statements[1].contains("region"), statements[1]);
    }

    /** A compound slicer on one hierarchy is an OR over one table: still separable. */
    @Test void compoundSlicerOnOneTableStillSplits() throws Exception {
        String[] statements = assertSplit(Setup.ON, ONE + DRILLED + "FROM [Sales] WHERE CrossJoin({[Measures].[One]}, "
            + "{[Product.Manufacturer].[Red],[Product.Manufacturer].[Blue]})",
            product(1, ALL_STORES, members("[Product]", "All Products", 10, 9, 8, 7, 5, 6, 4, 3, 2, 1)));
        assertTrue(statements[1].contains(
            "(\"product\".\"manufacturer\" = 'Red' or \"product\".\"manufacturer\" = 'Blue')"), statements[1]);
    }

    // (b) plain Level.Members

    /**
     * The statement's ORDER BY is the only order here. Distinct captions: how
     * the split keeps the interleaved order of twins is its own choice, and
     * {@link #duplicateCaptionsKeepTodaysOrder} pins the result only.
     */
    @Test void levelMembersCrossJoinReadsEachDimensionTableByItself() throws Exception {
        assertSplit(Setup.ON.distinctCaptions(), ONE + MEMBERS + RED + ONLY_ONE,
            product(1, DISTINCT_STORES, DISTINCT_RED_PRODUCTS));
        assertSplit(Setup.ON.distinctCaptions(), ONE + "SELECT NON EMPTY CrossJoin(" + PRODUCTS + ", " + STORES
            + ") ON COLUMNS " + RED + ONLY_ONE, product(1, DISTINCT_RED_PRODUCTS, DISTINCT_STORES));
    }

    // (c) relation groups, not hierarchies

    /** A false split here would turn 12 brand-product pairs into 5 x 12 candidates. */
    @Test void twoHierarchiesOfOneTableStayOneStatement() throws Exception {
        Run members = assertLegacy(Setup.ON, ONE + "SELECT NON EMPTY CrossJoin([Product.Brand].[Brand].Members, "
            + PRODUCTS + ") ON COLUMNS FROM [Sales] " + ONLY_ONE, pairs("[Product.Brand]", "[Product]",
            "Acme-3 Acme-2 Acme-1 Bolt-5 Bolt-6 Bolt-4 Cog-8 Cog-7 Dyn-10 Dyn-9 Elm-12 Elm-11", 1));
        assertEquals(1, members.sql().stream().filter(statement -> reads(statement, "product")).count(),
            members.sql().toString());
        // 1 + 12 products under All Brands, then each brand with All Products and its own products.
        List<String> drilled = new ArrayList<>(product(1, members("[Product.Brand]", "All Brands"),
            members("[Product]", "All Products", 3, 2, 1, 5, 6, 4, 8, 7, 10, 9, 12, 11)));
        for (String[] brand : new String[][] {
            {"Acme", "3", "2", "1"}, {"Bolt", "5", "6", "4"}, {"Cog", "8", "7"}, {"Dyn", "10", "9"},
            {"Elm", "12", "11"}})
        {
            drilled.add("[Product.Brand].[" + brand[0] + "],[Product].[All Products]=1");
            for (int i = 1; i < brand.length; i++) {
                drilled.add("[Product.Brand].[" + brand[0] + "],[Product].[" + brand[i] + "]=1");
            }
        }
        assertEquals(30, drilled.size());
        assertLegacy(Setup.ON, ONE
            + "SELECT NON EMPTY CrossJoin(Hierarchize({DrilldownLevel({[Product.Brand].[All Brands]})}), "
            + DRILL_PRODUCT + ") ON COLUMNS FROM [Sales] " + ONLY_ONE, drilled);
    }

    /** Brand and product share a table, so they stay correlated in one statement beside the store statement. */
    @Test void threeLevelsOverTwoTablesSplitByTable() throws Exception {
        List<String> brandProducts = pairs("[Product.Brand]", "[Product]",
            "Acme-3 Acme-2 Acme-1 Bolt-6 Bolt-5 Bolt-4", 1);
        String[] statements = assertSplit(Setup.ON.distinctCaptions(), ONE + "SELECT NON EMPTY CrossJoin(CrossJoin("
            + STORES + ", [Product.Brand].[Brand].Members), " + PRODUCTS + ") ON COLUMNS " + RED + ONLY_ONE,
            DISTINCT_STORES.stream().flatMap(store -> brandProducts.stream().map(pair -> store + "," + pair))
                .toList());
        assertTrue(statements[1].contains("\"product\".\"brand\"") && statements[1].contains("\"product\".\"name\""),
            statements[1]);
    }

    /** The store column sits between the two product columns: whatever the engine does, order and cells stay. */
    @Test void relationGroupInterruptedByAnotherTableKeepsItsResult() throws Exception {
        List<String> expected = new ArrayList<>();
        for (String[] brand : new String[][] {{"Acme", "3", "2", "1"}, {"Bolt", "6", "5", "4"}}) {
            for (int store : new int[] {3, 4, 2, 1}) {
                for (int i = 1; i < brand.length; i++) {
                    expected.add("[Product.Brand].[" + brand[0] + "],[Store].[" + store + "],[Product].[" + brand[i]
                        + "]=1");
                }
            }
        }
        Run[] runs = offAndOn(Setup.ON.distinctCaptions(), ONE + "SELECT NON EMPTY CrossJoin(CrossJoin("
            + "[Product.Brand].[Brand].Members, " + STORES + "), " + PRODUCTS + ") ON COLUMNS " + RED + ONLY_ONE,
            expected);
        assertEquals(1, runs[0].joint().size(), runs[0].sql().toString());
    }

    /**
     * Non-separable contexts retain the exact joint SQL and add no statement:
     * the cap is enforced while that statement streams, not by counting it first.
     */
    private Run assertGuardedLegacy(Setup setup, String mdx, List<String> expected) throws Exception {
        Run[] runs = offAndOn(setup, mdx, expected);
        assertEquals(runs[0].joint(), runs[1].joint(), "the correlated SQL must stay byte-identical");
        assertEquals(List.of(), runs[0].without(runs[1]), "every legacy statement must remain");
        assertEquals(List.of(), runs[1].without(runs[0]), "no guard may re-read the correlated joint relation");
        return runs[0];
    }

    // (d) stored measure

    @Test void storedMeasureKeepsTheJointFactJoinedStatement() throws Exception {
        Run off = assertLegacy(Setup.ON, DRILLED + RED + "WHERE ([Calendar].[202635], [Measures].[Quantity])", List.of(
            "[Store].[All Stores],[Product].[All Products]=70.0",
            "[Store].[All Stores],[Product].[4]=40.0",
            "[Store].[All Stores],[Product].[2]=20.0",
            "[Store].[All Stores],[Product].[1]=10.0",
            "[Store].[3],[Product].[All Products]=40.0",
            "[Store].[3],[Product].[4]=40.0",
            "[Store].[2],[Product].[All Products]=20.0",
            "[Store].[2],[Product].[2]=20.0",
            "[Store].[1],[Product].[All Products]=10.0",
            "[Store].[1],[Product].[1]=10.0"));
        assertFalse(off.joint().isEmpty(), off.sql().toString());
        assertTrue(off.joint().stream().allMatch(statement -> reads(statement, "fact")), off.joint().toString());
    }

    // (e) non-separable context

    /**
     * (E, Blue) and (W, Red) pairs are outside the context; AS 1 would keep
     * them if the tables were read apart. Twin 4 precedes twin 2: its first
     * row, a Blue P03, sorts before every Red row of twin 2.
     */
    private static List<String> correlated() {
        List<String> blue = members("[Product]", "All Products", 10, 9, 8, 7);
        List<String> cells = new ArrayList<>(product(1, members("[Store]", "All Stores"),
            members("[Product]", "All Products", 10, 9, 8, 7, 5, 6, 4, 3, 2, 1)));
        cells.addAll(product(1, members("[Store]", 4), blue));
        cells.addAll(product(1, members("[Store]", 2), ALL_RED_PRODUCTS));
        cells.addAll(product(1, members("[Store]", 1), blue));
        return cells;
    }

    @Test void compoundSlicerAcrossBothTablesKeepsTheJointStatement() throws Exception {
        assertEquals(28, correlated().size());
        Run off = assertGuardedLegacy(Setup.ON, COMPOUND_SLICER, correlated());
        assertEquals(1, off.joint().size(), off.sql().toString());
        assertTrue(off.joint().get(0).contains(CORRELATED_SQL), off.joint().get(0));
    }

    @Test void subselectTupleSetAcrossBothTablesKeepsTheJointStatement() throws Exception {
        Run off = assertGuardedLegacy(Setup.ON, ONE + DRILLED + "FROM (SELECT {" + CORRELATED
            + "} ON COLUMNS FROM [Sales]) " + ONLY_ONE, correlated());
        assertEquals(1, off.joint().size(), off.sql().toString());
        assertTrue(off.joint().get(0).contains(CORRELATED_SQL), off.joint().get(0));
    }

    /** Of the week-35 stock only (2,3) is an (E, Red) or a (W, Blue) pair. */
    @Test void subselectTupleSetUnderNativeSqlMeasureKeepsTheJointStatement() throws Exception {
        Run off = assertGuardedLegacy(Setup.ON, DRILLED + "FROM (SELECT {" + CORRELATED + "} ON COLUMNS FROM [Sales]) "
            + WEEK_35_STOCK, List.of(
                "[Store].[All Stores],[Product].[All Products]=300.0",
                "[Store].[All Stores],[Product].[3]=300.0",
                "[Store].[2],[Product].[All Products]=300.0",
                "[Store].[2],[Product].[3]=300.0"));
        assertTrue(off.joint().get(0).contains(CORRELATED_SQL), off.joint().get(0));
    }

    // (f) role

    @Test void roleRestrictedHierarchyKeepsItsLimitInTheStoreStatement() throws Exception {
        String[] statements = assertSplit(Setup.ON.role("NoWest"), ISSUE_97,
            product(1, members("[Store]", "All Stores", 3, 2, 4), ALL_RED_PRODUCTS));
        assertTrue(statements[0].contains("\"store\".\"store_id\" in (3, 2, 4)"), statements[0]);
        assertFalse(statements[1].contains("store_id"), statements[1]);
    }

    /** The native SQL measure does not apply the role today: the All cells still count store 1. */
    @Test void roleRestrictedHierarchyUnderNativeSqlMeasure() throws Exception {
        assertSplit(Setup.ON.role("NoWest"), ISSUE_97_STOCK, List.of(
            "[Store].[All Stores],[Product].[All Products]=1500.0",
            "[Store].[All Stores],[Product].[5]=500.0",
            "[Store].[All Stores],[Product].[6]=600.0",
            "[Store].[All Stores],[Product].[3]=300.0",
            "[Store].[All Stores],[Product].[1]=100.0",
            "[Store].[2],[Product].[All Products]=300.0",
            "[Store].[2],[Product].[3]=300.0",
            "[Store].[4],[Product].[All Products]=1100.0",
            "[Store].[4],[Product].[5]=500.0",
            "[Store].[4],[Product].[6]=600.0"));
    }

    // (g) empty group

    /**
     * No Red product is a Cog: zero rows today, so not even the (All, All)
     * tuple of the drilldown. Products first: the empty group is not the last.
     */
    @Test void emptyGroupYieldsAnEmptyAxis() throws Exception {
        String productsFirst = "SELECT NON EMPTY CrossJoin(" + PRODUCTS + ", " + STORES + ") ON COLUMNS ";
        for (String select : List.of(DRILLED, MEMBERS, productsFirst)) {
            Run[] runs = offAndOn(Setup.ON, ONE + select + RED + "WHERE ([Product.Brand].[Cog], [Measures].[One])",
                List.of());
            assertEquals(1, runs[0].joint().size(), runs[0].sql().toString());
            assertEquals(List.of(), runs[1].joint(), "split on still reads both dimension tables in one statement");
            if (!select.equals(DRILLED)) {
                assertEquals(runs[1].guards(), runs[1].without(runs[0]),
                    "an empty group is known from the guard alone: no group is read");
            }
        }
    }

    // (h) arguments that constrain the statement: the legacy path in v1

    @Test void nonAllDrilledMemberKeepsTheJointStatement() throws Exception {
        Run off = assertLegacy(Setup.ON, ONE + "SELECT NON EMPTY CrossJoin("
            + "Hierarchize({DrilldownLevel({[Store.Geo].[W]})}), " + DRILL_PRODUCT + ") ON COLUMNS " + RED + ONLY_ONE,
            product(1, List.of("[Store.Geo].[W]", "[Store.Geo].[W].[4]", "[Store.Geo].[W].[1]"), ALL_RED_PRODUCTS));
        assertEquals(1, off.joint().size(), off.sql().toString());
        assertTrue(off.joint().get(0).contains("(\"store\".\"region\" = 'W')"), off.joint().get(0));
    }

    @Test void explicitSetAndChildrenKeepTheJointStatement() throws Exception {
        Run set = assertLegacy(Setup.ON, ONE + "SELECT NON EMPTY CrossJoin({[Store].[2],[Store].[4]}, " + PRODUCTS
            + ") ON COLUMNS " + RED + ONLY_ONE,
            pairs("[Store]", "[Product]", "2-5 2-6 4-5 4-6 2-4 4-4 2-3 4-3 2-2 4-2 2-1 4-1", 1));
        assertTrue(set.joint().get(0).contains("(\"store\".\"store_id\" in (2, 4))"), set.joint().toString());
        Run children = assertLegacy(Setup.ON, ONE + "SELECT NON EMPTY CrossJoin([Store.Geo].[W].Children, " + PRODUCTS
            + ") ON COLUMNS " + RED + ONLY_ONE,
            product(1, List.of("[Store.Geo].[W].[4]", "[Store.Geo].[W].[1]"), RED_PRODUCTS));
        assertTrue(children.joint().get(0).contains("(\"store\".\"region\" = 'W')"), children.joint().toString());
    }

    /** Not a native crossjoin today, expanded or not: each argument is read by its own statement already. */
    @Test void drilldownMemberKeepsItsStatements() throws Exception {
        String mdx = ONE + "SELECT NON EMPTY CrossJoin(DrilldownMember({[Store.Geo].[E],[Store.Geo].[W]}, "
            + "{[Store.Geo].[W]}), " + PRODUCTS + ") ON COLUMNS " + RED + ONLY_ONE;
        List<String> drilled = product(1,
            List.of("[Store.Geo].[E]", "[Store.Geo].[W]", "[Store.Geo].[W].[4]", "[Store.Geo].[W].[1]"), RED_PRODUCTS);
        assertEquals(List.of(), assertLegacy(Setup.ON, mdx, drilled).joint());
        assertEquals(List.of(), assertLegacy(Setup.ON.expandingNonNative(), mdx, drilled).joint());
    }

    // (i) virtual cube, TopCount, Filter

    /** A virtual cube already reads each target by itself, without the twins interleaved. */
    @Test void virtualCubeKeepsItsStatements() throws Exception {
        Run off = assertLegacy(Setup.ON, ONE + MEMBERS
            + "FROM (SELECT {[Product.Manufacturer].[Red]} ON COLUMNS FROM [Combined]) " + ONLY_ONE,
            product(1, members("[Store]", 3, 2, 4, 1), RED_PRODUCTS));
        assertEquals(List.of(), off.joint());
    }

    /** Dense output can require interpreter padding; the Red ranking is unchanged. */
    @Test void topCountUnderDenseMeasureKeepsTheRanking() throws Exception {
        Run[] runs = offAndOn(Setup.ON, ONE + "SELECT NON EMPTY TopCount(" + STORE_BY_PRODUCT
            + ", 3, [Measures].[Quantity]) ON COLUMNS " + RED + ONLY_ONE,
            pairs("[Store]", "[Product]", "2-5 3-4 2-2", 1));
        // Whether this ranking is native depends on padding rules outside the
        // split; either way every statement stays, beside at most one guard.
        assertEquals(List.of(), runs[0].without(runs[1]), "every flag-off statement must remain");
        List<String> extra = runs[1].without(runs[0]);
        assertTrue(extra.size() <= 1 && extra.stream().allMatch(FactlessCrossJoinSplitTest::isCount),
            "at most one guard statement, no group reads: " + extra);
    }

    /**
     * CoalesceEmpty keeps the ranking in Java, over the native crossjoin of
     * the calculated context, whose twin stores tie: that crossjoin keeps its
     * joint statement after one guard statement, and reads no group.
     */
    @Test void topCountRankedInJavaKeepsTheTieFallbackStatement() throws Exception {
        Run[] runs = offAndOn(Setup.ON, ONE + "SELECT NON EMPTY TopCount(" + STORE_BY_PRODUCT
            + ", 3, CoalesceEmpty([Measures].[Quantity], 0)) ON COLUMNS " + RED + ONLY_ONE,
            pairs("[Store]", "[Product]", "2-5 3-4 2-2", 1));
        List<String> cartesian = runs[0].joint().stream().filter(sql -> !reads(sql, "fact")).toList();
        assertEquals(1, cartesian.size(), "the ranked set is a fact-less crossjoin: " + runs[0].sql());
        assertEquals(runs[0].joint(), runs[1].joint(), "the tie fallback must keep the legacy statement");
        List<String> extra = runs[1].without(runs[0]);
        assertEquals(1, extra.size(), "one guard statement, no group reads: " + extra);
        assertTrue(isCount(extra.get(0)), extra.toString());
    }

    /** A stored output needs no empty padding and must keep the native ranking statement. */
    @Test void nativeTopCountKeepsTheJointStatement() throws Exception {
        Run off = assertLegacy(Setup.ON, "SELECT NON EMPTY TopCount(" + STORE_BY_PRODUCT
            + ", 3, [Measures].[Quantity]) ON COLUMNS " + RED + "WHERE [Measures].[Quantity]", List.of(
                "[Store].[2],[Product].[5]=50.0",
                "[Store].[3],[Product].[4]=40.0",
                "[Store].[2],[Product].[2]=20.0"));
        assertTrue(off.joint().get(0).contains("sum(\"fact\".\"qty\") DESC"), off.joint().toString());
    }

    /**
     * A native Filter declines a calculated context measure, so the filter
     * runs in Java over the crossjoin of (b), whose statements may be split.
     */
    @Test void filterOverTheCrossJoinKeepsItsResult() throws Exception {
        offAndOn(Setup.ON, ONE + "SELECT NON EMPTY Filter(" + STORE_BY_PRODUCT
            + ", [Measures].[Quantity] > 15) ON COLUMNS " + RED + ONLY_ONE,
            pairs("[Store]", "[Product]", "3-4 2-5 2-2", 1));
    }

    // (j) NonEmptyCrossJoin(): compare enumeration after an explicit final cell filter

    @Test void nonEmptyCrossJoinWithFinalFilterReturnsTheSameSet() throws Exception {
        String select = "SELECT NonEmpty(NonEmptyCrossJoin(" + STORES + ", " + PRODUCTS
            + ")) ON COLUMNS " + RED;
        assertSplit(Setup.ON.distinctCaptions(), ONE + select + ONLY_ONE,
            product(1, DISTINCT_STORES, DISTINCT_RED_PRODUCTS));
        // These are the Red stock rows for week 35, in store/product order.
        // CalculatedMeasureContextFactTest covers raw NECJ semantics (#37).
        // The final filter isolates split enumeration on either engine branch.
        assertSplit(Setup.ON.distinctCaptions(), select + WEEK_35_STOCK, List.of(
            "[Store].[4],[Product].[6]=600.0", "[Store].[4],[Product].[5]=500.0",
            "[Store].[2],[Product].[3]=300.0", "[Store].[1],[Product].[1]=100.0"));
    }

    // (k) the exact guard

    private static void assertBlocked(Run run, String setup) {
        assertRejected(run, setup);
        assertEquals(List.of(), run.joint(), "the cartesian was issued before the guard");
    }

    /**
     * A context the split cannot take is bounded while its own joint
     * statement streams: that statement is issued once, no count re-reads the
     * joint relation, and the failure reports the first candidate above the cap.
     */
    private static void assertBlockedWhileStreaming(Run run, int cap, String setup) {
        assertRejected(run, setup);
        assertEquals(1, run.joint().size(), "the joint statement itself is bounded: " + run.sql());
        assertEquals(List.of(), run.guards(), "no count may re-read the joint relation: " + run.sql());
        assertTrue(run.failure().getMessage().contains("jointCountAtLeast=" + (cap + 1)),
            run.failure().getMessage());
    }

    private static void assertRejected(Run run, String setup) {
        assertInstanceOf(ResourceLimitExceededException.class, run.failure(),
            setup + " must fail fast, got cells " + run.cells());
        assertTrue(run.failure().getMessage().contains("[Store].[Store]")
            && run.failure().getMessage().contains("[Product].[Product]"),
            "the levels are not named: " + run.failure().getMessage());
    }

    /** 5 x 7 expanded candidates, known after two small statements and before any product or cartesian exists. */
    @Test void candidateProductAboveTheCapFailsFastNamingTheLevels() throws Exception {
        assertBlocked(run(Setup.ON.maxCandidates(34), ISSUE_97), "a cap of 34 for 35 candidates");
    }

    @Test void candidateProductAtTheCapPasses() throws Exception {
        Run atTheCap = run(Setup.ON.maxCandidates(35), ISSUE_97);
        assertNull(atTheCap.failure(), () -> "a cap of 35 for 35 candidates: " + atTheCap.failure());
        assertEquals(product(1, ALL_STORES, ALL_RED_PRODUCTS), atTheCap.cells());
    }

    /** The cap belongs to the split: off, it is never read. */
    @Test void capIsIgnoredWhileTheSplitIsOff() throws Exception {
        Run off = run(Setup.OFF.maxCandidates(1), ISSUE_97);
        assertNull(off.failure(), () -> String.valueOf(off.failure()));
        assertEquals(product(1, ALL_STORES, ALL_RED_PRODUCTS), off.cells());
        assertEquals(List.of(LEGACY_SQL), off.joint());
    }

    /** Today the result limit stops this query too, but only after the cartesian was issued. */
    @Test void effectiveCapIsTheSmallerOfMaxCandidatesAndResultLimit() throws Exception {
        Run legacy = run(Setup.OFF.resultLimit(30), ISSUE_97);
        assertInstanceOf(ResourceLimitExceededException.class, legacy.failure());
        assertEquals(List.of(LEGACY_SQL), legacy.joint());
        assertBlocked(run(Setup.ON.resultLimit(30), ISSUE_97), "a result limit of 30 under the default cap");
        assertBlocked(run(Setup.ON.maxCandidates(0).resultLimit(30), ISSUE_97), "a result limit of 30 and no cap");
    }

    /** The joint statement of a context that cannot be split is bounded by its actual expanded size. */
    @Test void nonSeparableContextIsBoundedByTheSameCap() throws Exception {
        assertBlockedWhileStreaming(run(Setup.ON.maxCandidates(10), COMPOUND_SLICER), 10,
            "a cap of 10 for a compound slicer");
    }

    @Test void correlatedExpandedCountBelowItsMarginalProductPassesAtTheCap() throws Exception {
        // Marginal groups have (3 + All) * (10 + All) = 44 candidates;
        // the correlated joint result has 14 leaf + 3 store + 10 product + 1 All = 28.
        Run run = run(Setup.ON.maxCandidates(28), COMPOUND_SLICER);
        assertNull(run.failure(), () -> "28 actual tuples must fit despite a product of 44: " + run.failure());
        assertEquals(correlated(), run.cells());
        assertEquals(List.of(), run.guards(), run.sql().toString());
        assertEquals(1, run.joint().size(), run.sql().toString());
        assertBlockedWhileStreaming(run(Setup.ON.maxCandidates(27), COMPOUND_SLICER), 27, "28 actual tuples exceed 27");
        Run byResultLimit = run(Setup.ON.maxCandidates(0).resultLimit(28), COMPOUND_SLICER);
        assertNull(byResultLimit.failure(), () -> String.valueOf(byResultLimit.failure()));
        assertEquals(correlated(), byResultLimit.cells());
        assertBlockedWhileStreaming(run(Setup.ON.maxCandidates(0).resultLimit(27), COMPOUND_SLICER), 27,
            "28 actual tuples exceed a result limit of 27");
    }

    /**
     * Rows each statement fetched, read from the SQL log: a statement logs
     * {@code "<id>: <component>: executing sql [<sql>]"} when it starts and
     * {@code "<id>: , exec+fetch <ms> ms, <rows> rows"} when it closes.
     */
    private static java.util.Map<String, Integer> fetchedRows(Runnable action) {
        org.apache.logging.log4j.Logger logger = RolapUtil.SQL_LOGGER;
        org.apache.logging.log4j.Level previous = logger.getLevel();
        java.io.StringWriter log = new java.io.StringWriter();
        org.apache.logging.log4j.core.Appender appender = Util.makeAppender("factlessFetchedRows", log, "%m%n");
        Util.setLevel(logger, org.apache.logging.log4j.Level.DEBUG);
        Util.addAppender(appender, logger, org.apache.logging.log4j.Level.DEBUG);
        try {
            action.run();
        } finally {
            Util.removeAppender(appender, logger);
            Util.setLevel(logger, previous);
        }
        java.util.Map<String, String> sqlById = new java.util.HashMap<>();
        java.util.Map<String, Integer> rows = new java.util.HashMap<>();
        java.util.regex.Pattern end = java.util.regex.Pattern.compile("(\\d+): , exec\\+fetch \\d+ ms, (\\d+) rows");
        for (String line : log.toString().split("\\R")) {
            int start = line.indexOf(": executing sql [");
            java.util.regex.Matcher closed = end.matcher(line);
            if (start > 0 && line.endsWith("]")) {
                sqlById.put(line.substring(0, line.indexOf(':')), line.substring(start + 17, line.length() - 1));
            } else if (closed.matches()) {
                rows.put(sqlById.get(closed.group(1)), Integer.valueOf(closed.group(2)));
            }
        }
        return rows;
    }

    /**
     * With 200 more Red products store 2 alone pairs with 206 of them, 214
     * joint rows in all. A cap of 20 stops that statement at its 21st row,
     * plain or drilled, instead of counting all 214 first.
     */
    @Test void correlatedGuardStopsTheJointStatementAfterTheCap() throws Exception {
        String plain = ONE + MEMBERS + "FROM (SELECT {" + CORRELATED + "} ON COLUMNS FROM [Sales]) " + ONLY_ONE;
        for (String mdx : List.of(plain, COMPOUND_SLICER)) {
            Setup setup = Setup.ON.maxCandidates(20);
            setup.apply();
            mondrian.olap.Connection connection = open(setup);
            try (java.sql.Connection db = ((RolapConnection) connection).getDataSource().getConnection();
                 Statement statement = db.createStatement())
            {
                statement.execute("INSERT INTO product SELECT X + 99, 'Q' || X, 'Red', 'Acme' FROM SYSTEM_RANGE(1, 200)");
            }
            Run[] run = new Run[1];
            java.util.Map<String, Integer> fetched = fetchedRows(() -> run[0] = execute(connection, mdx));
            assertBlockedWhileStreaming(run[0], 20, "214 correlated rows under a cap of 20");
            int rows = fetched.get(run[0].joint().get(0));
            if (mdx.equals(plain)) {
                assertEquals(21, rows, "raw candidates: the statement is read up to cap + 1 rows");
            } else {
                assertTrue(rows <= 21, "every new raw row adds an expanded candidate: " + rows);
            }
        }
    }

    @Test void correlatedPlainMembersUseTheirActualCount() throws Exception {
        String mdx = ONE + MEMBERS + "FROM (SELECT {" + CORRELATED + "} ON COLUMNS FROM [Sales]) " + ONLY_ONE;
        offAndOn(Setup.ON.maxCandidates(14), mdx, pairs("[Store]", "[Product]",
            "4-10 4-9 4-8 4-7 2-5 2-6 2-4 2-3 2-2 2-1 1-10 1-9 1-8 1-7", 1));
        assertBlockedWhileStreaming(run(Setup.ON.maxCandidates(13), mdx), 13, "14 actual leaf tuples exceed 13");
        assertBlockedWhileStreaming(run(Setup.ON.maxCandidates(0).resultLimit(13), mdx), 13,
            "the cap, not the member fetch limit, names a result limit of 13");
    }

    private Run runWithDuplicateStore(Setup setup, String mdx) throws Exception {
        setup.apply();
        mondrian.olap.Connection connection = open(setup);
        try (java.sql.Connection db = ((RolapConnection) connection).getDataSource().getConnection();
             Statement statement = db.createStatement())
        {
            // The projected row differs, but its member key is still Store 2.
            statement.execute("INSERT INTO store VALUES (2, 'ZZZ', 'E')");
        }
        return execute(connection, mdx);
    }

    @Test void correlatedDrillCountDeduplicatesKeysAcrossDifferentOrdinals() throws Exception {
        Run off = runWithDuplicateStore(Setup.OFF, COMPOUND_SLICER);
        Run on = runWithDuplicateStore(Setup.ON.maxCandidates(28), COMPOUND_SLICER);
        assertNull(off.failure(), () -> String.valueOf(off.failure()));
        assertNull(on.failure(), () -> String.valueOf(on.failure()));
        assertEquals(correlated(), off.cells());
        assertEquals(off.cells(), on.cells(), "duplicate raw keys must not inflate the expanded count");
        assertBlockedWhileStreaming(runWithDuplicateStore(Setup.ON.maxCandidates(27), COMPOUND_SLICER), 27,
            "duplicate raw keys still expand to 28 candidates");
    }

    @Test void correlatedPlainCountPreservesDuplicateRows() throws Exception {
        String mdx = ONE + MEMBERS + "FROM (SELECT {" + CORRELATED + "} ON COLUMNS FROM [Sales]) " + ONLY_ONE;
        Run off = runWithDuplicateStore(Setup.OFF, mdx);
        Run on = runWithDuplicateStore(Setup.ON.maxCandidates(20), mdx);
        assertNull(off.failure(), () -> String.valueOf(off.failure()));
        assertNull(on.failure(), () -> String.valueOf(on.failure()));
        assertEquals(pairs("[Store]", "[Product]",
            "4-10 4-9 4-8 4-7 2-5 2-6 2-4 2-3 2-2 2-1 1-10 1-9 1-8 1-7 2-5 2-6 2-4 2-3 2-2 2-1", 1),
            off.cells());
        assertEquals(off.cells(), on.cells(), "plain member candidates keep the duplicate rows");
        assertBlockedWhileStreaming(runWithDuplicateStore(Setup.ON.maxCandidates(19), mdx), 19,
            "20 raw candidates exceed 19");
    }

    @Test void correlatedThreeDrillsCountSharedAllProjectionsOnce() throws Exception {
        String mdx = ONE + "SELECT NON EMPTY CrossJoin(CrossJoin(" + DRILL_STORE
            + ", Hierarchize({DrilldownLevel({[Product.Brand].[All Brands]})})), " + DRILL_PRODUCT
            + ") ON COLUMNS FROM (SELECT {" + CORRELATED + "} ON COLUMNS FROM [Sales]) " + ONLY_ONE;
        Run off = run(Setup.OFF, mdx);
        Run on = run(Setup.ON.maxCandidates(62), mdx);
        assertNull(off.failure(), () -> String.valueOf(off.failure()));
        assertNull(on.failure(), () -> String.valueOf(on.failure()));
        // Eight disjoint All/leaf projections contain 1+3+4+10+6+14+10+14 tuples.
        assertEquals(62, off.cells().size());
        assertEquals(off.cells(), on.cells());
        assertEquals(List.of(), on.guards(), on.sql().toString());
        assertBlockedWhileStreaming(run(Setup.ON.maxCandidates(61), mdx), 61, "62 expanded candidates exceed 61");
    }

    @Test void emptyCorrelatedResultDoesNotInventAnAllTuple() throws Exception {
        String mdx = ONE + DRILLED + "FROM (SELECT {" + CORRELATED + "} ON COLUMNS FROM [Sales]) "
            + "WHERE ([Store.Geo].[N], [Measures].[One])";
        Run[] runs = offAndOn(Setup.ON.maxCandidates(1), mdx, List.of());
        assertEquals(List.of(), runs[1].guards(), runs[1].sql().toString());
        assertEquals(1, runs[1].joint().size(), runs[1].sql().toString());
        assertEquals(runs[0].joint(), runs[1].joint(), "the empty joint statement is read as today");
    }

    @Test void rejectedCorrelatedCountDoesNotSeedMemberOrderOrCacheTheFailure() throws Exception {
        Setup.ON.maxCandidates(27).apply();
        mondrian.olap.Connection connection = open(Setup.ON);
        assertBlockedWhileStreaming(execute(connection, COMPOUND_SLICER), 27, "28 candidates must fail at 27");
        Setup.ON.maxCandidates(28).apply();
        Run accepted = execute(connection, COMPOUND_SLICER);
        assertNull(accepted.failure(), () -> String.valueOf(accepted.failure()));
        assertEquals(correlated(), accepted.cells());
        assertEquals(List.of(), execute(connection, COMPOUND_SLICER).sql(), "the accepted result is cached");
        Setup.ON.maxCandidates(27).apply();
        assertBlockedWhileStreaming(execute(connection, COMPOUND_SLICER), 27,
            "the cached 28 tuples cannot bypass a lowered cap");
    }

    @Test void correlatedNullLeafKeysRemainDistinctFromAllMembers() throws Exception {
        List<Run> runs = new ArrayList<>();
        for (Setup setup : List.of(Setup.OFF, Setup.ON.maxCandidates(30), Setup.ON.maxCandidates(29))) {
            setup.apply();
            mondrian.olap.Connection connection = open(setup);
            try (java.sql.Connection db = ((RolapConnection) connection).getDataSource().getConnection();
                 Statement statement = db.createStatement())
            {
                statement.execute("INSERT INTO product VALUES (NULL, 'P00', 'Red', 'Acme')");
            }
            runs.add(execute(connection, COMPOUND_SLICER));
        }
        assertNull(runs.get(0).failure(), () -> String.valueOf(runs.get(0).failure()));
        assertNull(runs.get(1).failure(), () -> String.valueOf(runs.get(1).failure()));
        // The null-key leaf adds (All Stores, null) and (Store 2, null),
        // neither of which is an existing tuple with All Products.
        assertEquals(30, runs.get(0).cells().size());
        assertEquals(runs.get(0).cells(), runs.get(1).cells());
        assertBlockedWhileStreaming(runs.get(2), 29, "the SQL-null leaf and All member are distinct candidates");
    }

    private Run wideCorrelatedGuard(int cap, boolean empty) throws Exception {
        int aliases = 8;
        Setup setup = Setup.ON.maxCandidates(cap);
        setup.apply();
        mondrian.olap.Connection connection = open(setup, aliases);
        String stores = DRILL_STORE;
        for (int i = 0; i < aliases; i++) {
            String dimension = "StoreAlias" + i;
            RolapHierarchy hierarchy = (RolapHierarchy) Arrays.stream(
                connection.getSchema().lookupCube("Sales", false).getDimensions())
                .filter(dim -> dim.getName().equals(dimension)).findFirst().orElseThrow().getHierarchies()[0];
            stores = "CrossJoin(" + stores + ", Hierarchize({DrilldownLevel({"
                + hierarchy.getAllMember().getUniqueName() + "})}))";
        }
        String mdx = ONE + "SELECT NON EMPTY CrossJoin(" + stores + ", " + DRILL_PRODUCT
            + ") ON COLUMNS FROM (SELECT {" + CORRELATED + "} ON COLUMNS FROM [Sales]) WHERE "
            + (empty ? "([Store.Geo].[N], [Measures].[One])" : "[Measures].[One]");
        // Ten drills: every source row expands to 2^10 All/leaf projections.
        return execute(connection, mdx);
    }

    /** Exact at scale: 8,698 expanded tuples pass a cap of 8,698 and fail one of 8,697. */
    @Test void wideCorrelatedGuardIsExactAtTheCap() throws Exception {
        Run atTheCap = wideCorrelatedGuard(8_698, false);
        assertNull(atTheCap.failure(), () -> String.valueOf(atTheCap.failure()));
        assertEquals(8_698, atTheCap.cells().size());
        assertBlockedWhileStreaming(wideCorrelatedGuard(8_697, false), 8_697,
            "8,698 actual expanded tuples exceed 8,697");
    }

    /** The first row alone expands to 2^10 All/leaf tuples; the guard stops at the second. */
    @Test void wideCorrelatedGuardStopsInsideTheFirstRowsExpansion() throws Exception {
        assertBlockedWhileStreaming(wideCorrelatedGuard(1, false), 1,
            "one source row generates 2^10 distinct All/leaf tuples");
    }

    @Test void wideEmptyCorrelatedInputStaysEmpty() throws Exception {
        Run run = wideCorrelatedGuard(1, true);
        assertNull(run.failure(), () -> String.valueOf(run.failure()));
        assertEquals(List.of(), run.cells());
        assertEquals(List.of(), run.guards(), run.sql().toString());
        assertEquals(1, run.joint().size(), run.sql().toString());
    }

    // (l) equal order keys

    /**
     * The twins tie on the order key of the statement, so today their rows
     * interleave per product; whatever the split does with ties, this order
     * stays. (The drilled shape of (a) has the twins too, but is re-sorted.)
     */
    @Test void duplicateCaptionsKeepTodaysOrder() throws Exception {
        Run[] byStore = offAndOn(Setup.ON, ONE + MEMBERS + RED + ONLY_ONE, pairs("[Store]", "[Product]",
            "3-5 3-6 3-4 3-3 3-2 3-1 2-5 2-6 4-5 4-6 2-4 4-4 2-3 4-3 2-2 4-2 2-1 4-1 1-5 1-6 1-4 1-3 1-2 1-1", 1));
        Run[] byProduct = offAndOn(Setup.ON, ONE + "SELECT NON EMPTY CrossJoin(" + PRODUCTS + ", " + STORES
            + ") ON COLUMNS " + RED + ONLY_ONE, pairs("[Product]", "[Store]",
            "5-3 6-3 5-2 5-4 6-2 6-4 5-1 6-1 4-3 4-2 4-4 4-1 3-3 3-2 3-4 3-1 2-3 2-2 2-4 2-1 1-3 1-2 1-4 1-1", 1));
        for (Run[] runs : List.of(byStore, byProduct)) {
            for (Run run : runs) {
                assertTrue(run.sql().stream().anyMatch(statement -> reads(statement, "store")),
                    "nothing was read: " + run.sql());
            }
            assertEquals(runs[0].joint(), runs[1].joint(), "ties must retain the legacy tuple statement");
            List<String> extra = runs[1].without(runs[0]);
            assertFalse(extra.isEmpty(), "the tied result still needs a candidate guard");
            assertTrue(extra.stream().allMatch(FactlessCrossJoinSplitTest::isCount),
                "tie fallback must not materialize then discard group members: " + extra);
            assertEquals(1, extra.size(), "ties and every group size come from one statement: " + extra);
        }
    }

    private static final String WEEKS = "[Calendar].[Week].Members";
    private static final String THREE_TABLES = ONE + "SELECT NON EMPTY CrossJoin(CrossJoin(" + WEEKS + ", " + STORES
        + "), " + PRODUCTS + ") ON COLUMNS " + RED + ONLY_ONE;

    /** Every week before every store before every product, in the order of the joint statement. */
    private static List<String> byWeek(List<String> storeByProduct) {
        return members("[Calendar]", 202635, 202636).stream()
            .flatMap(week -> storeByProduct.stream().map(cell -> week + "," + cell)).toList();
    }

    /** Three tables, three groups: one guard statement decides the order of all of them. */
    @Test void threeGroupsSplitAfterOneGuardStatement() throws Exception {
        Run[] runs = offAndOn(Setup.ON.distinctCaptions(), THREE_TABLES,
            byWeek(product(1, DISTINCT_STORES, DISTINCT_RED_PRODUCTS)));
        assertEquals(List.of(), runs[1].joint(), runs[1].sql().toString());
        List<String> added = runs[1].without(runs[0]);
        assertEquals(1, added.stream().filter(FactlessCrossJoinSplitTest::isCount).count(), added.toString());
        assertEquals(3, added.stream().filter(sql -> !isCount(sql)).count(), "one read per table: " + added);
    }

    /** Twin stores tie inside the middle group: the joint statement stays, after the same single guard. */
    @Test void tieInAMiddleGroupKeepsTheJointStatementAfterOneGuard() throws Exception {
        Run[] runs = offAndOn(Setup.ON, THREE_TABLES, byWeek(pairs("[Store]", "[Product]",
            "3-5 3-6 3-4 3-3 3-2 3-1 2-5 2-6 4-5 4-6 2-4 4-4 2-3 4-3 2-2 4-2 2-1 4-1 1-5 1-6 1-4 1-3 1-2 1-1", 1)));
        assertEquals(runs[0].joint(), runs[1].joint(), "ties must retain the legacy tuple statement");
        List<String> extra = runs[1].without(runs[0]);
        assertEquals(1, extra.size(), "one guard, whichever group ties: " + extra);
        assertTrue(isCount(extra.get(0)), extra.toString());
    }

    // (m) switch off

    @Test void switchOffKeepsTodaysStatementByteForByte() throws Exception {
        assertFalse(MondrianProperties.instance().CrossJoinFactlessSplit.get(), "the split must default to off");
        assertEquals(2_000_000, MondrianProperties.instance().CrossJoinFactlessSplitMaxCandidates.get());
        assertEquals(List.of(LEGACY_SQL), run(Setup.OFF, ISSUE_97).joint());
        assertEquals(List.of(LEGACY_SQL), run(Setup.OFF, ISSUE_97_STOCK).joint());
        // The arguments of Level.Members constrain nothing either: the same statement.
        assertEquals(List.of(LEGACY_SQL), run(Setup.OFF, ONE + MEMBERS + RED + ONLY_ONE).joint());
    }

    /**
     * Within one schema the native set cache outlives the query: unless the
     * switch is part of its key, the second half of a dual run compares the
     * first half's list with itself.
     */
    @Test void dualRunOnOneConnectionDoesNotAnswerFromTheOtherModesCache() throws Exception {
        Setup.OFF.apply();
        mondrian.olap.Connection connection = open(Setup.OFF);
        Run off = execute(connection, ISSUE_97);
        assertEquals(List.of(LEGACY_SQL), off.joint());
        assertEquals(List.of(), execute(connection, ISSUE_97).sql(), "the second run must come from the cache");
        Setup.ON.apply();
        Run on = execute(connection, ISSUE_97);
        assertEquals(off.cells(), on.cells());
        assertEquals(List.of(), on.joint());
        assertEquals(1, on.sql().stream().filter(statement -> reads(statement, "store")).count(),
            "split on must read the store table itself, not the list cached with the split off: " + on.sql());
        assertEquals(1, on.sql().stream().filter(statement -> reads(statement, "product")).count(),
            on.sql().toString());
    }
    @Test void loweringTheCapDoesNotReuseAnOversizedCachedList() throws Exception {
        Setup.ON.apply();
        mondrian.olap.Connection connection = open(Setup.ON);
        assertNull(execute(connection, ISSUE_97).failure());
        Setup.ON.maxCandidates(34).apply();
        assertBlocked(execute(connection, ISSUE_97), "a lowered cap on the same schema");
    }

    @Test void groupReadFailurePropagatesWithoutPartialProductOrFallback() throws Exception {
        Setup.ON.apply();
        mondrian.olap.Connection connection = open(Setup.ON);
        List<String> sql = new ArrayList<>();
        RuntimeException injected = new RuntimeException("second group failed");
        RolapUtil.setHook(statement -> {
            sql.add(statement);
            if (reads(statement, "product") && statement.contains("product_id")) {
                throw injected;
            }
        });
        RuntimeException failure = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
            () -> connection.execute(connection.parseQuery(ISSUE_97)));
        while (failure.getCause() instanceof RuntimeException cause) {
            failure = cause;
        }
        org.junit.jupiter.api.Assertions.assertSame(injected, failure);
        assertTrue(sql.stream().anyMatch(statement -> reads(statement, "store")));
        assertFalse(sql.stream().anyMatch(statement -> reads(statement, "store") && reads(statement, "product")));
        RolapUtil.setHook(null);
        assertEquals(product(1, ALL_STORES, ALL_RED_PRODUCTS), execute(connection, ISSUE_97).cells());
    }

    @Test void cancellationDuringAGroupReadStopsTheSplit() throws Exception {
        Setup.ON.apply();
        mondrian.olap.Connection connection = open(Setup.ON);
        mondrian.olap.Query query = connection.parseQuery(ISSUE_97);
        java.util.concurrent.atomic.AtomicBoolean canceled = new java.util.concurrent.atomic.AtomicBoolean();
        RolapUtil.setHook(statement -> {
            if (reads(statement, "product") && statement.contains("product_id")) {
                canceled.set(true);
                query.cancel();
            }
        });
        RuntimeException failure = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
            () -> connection.execute(query));
        Throwable root = failure;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        assertTrue(canceled.get(), "the cancellation must happen in a group SQL");
        assertInstanceOf(mondrian.olap.QueryCanceledException.class, root);
    }

}
