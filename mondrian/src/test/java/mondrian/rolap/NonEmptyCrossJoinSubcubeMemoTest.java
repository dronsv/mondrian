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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An explicit All reset changes subcube visibility even when the evaluator's
 * current member was already All. A memoized NECJ must distinguish the two.
 * Every invocation has a fresh database, schema, connection and tuple cache.
 * The interpreter isolates memo identity from native candidate enumeration;
 * NonEmptyCrossJoinJudgeCostTest covers native memo reuse.
 */
public class NonEmptyCrossJoinSubcubeMemoTest {
    private static final String CALCULATIONS = """
        WITH MEMBER [Measures].[Pair Count] AS
          Count(NonEmptyCrossJoin([Product].[Name].Members, [Store].[Name].Members))
        MEMBER [Measures].[Reset Pair Count] AS
          ([Measures].[Pair Count], [Calendar].[All Years])
        """;
    private static final String SUBSELECT =
        " FROM (SELECT {[Calendar].[2026]} ON 0 FROM [Sales])";

    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private final List<String> statements = new ArrayList<>();
    private final Map<String, String> savedProperties = new LinkedHashMap<>();
    private int previousPreCache;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:necj_subcube_memo_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE calendar (id INT, year INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025),(2,2026)");
            sql.execute("CREATE TABLE product (id INT, name VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'A'),(2,'B'),(3,'C')");
            sql.execute("CREATE TABLE store (id INT, name VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'S1'),(2,'S2')");
            sql.execute(
                "CREATE TABLE fact (calendar_id INT, product_id INT, store_id INT, qty INT)");
            // 2026 contains two populated pairs; all years contain three.
            sql.execute("INSERT INTO fact VALUES (1,1,1,10),(2,2,1,20),(2,3,2,30)");
        }
        MondrianProperties properties = MondrianProperties.instance();
        previousPreCache = properties.LevelPreCacheThreshold.get();
        properties.LevelPreCacheThreshold.set(0);
        // Isolate the NECJ memo from Count's optional aggregate Cache wrapper.
        set("mondrian.expCache.enable", "false");
        set("mondrian.native.queryEngine.enable", "false");
        set("mondrian.native.crossjoin.factlessSplit.enable", "false");
        set("mondrian.rolap.aggregates.Use", "false");

        Util.PropertyList connect =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        connect.put("JdbcUser", "sa");
        connect.put("JdbcDrivers", "org.h2.Driver");
        connect.put("Jdbc", jdbc);
        connect.put("CatalogContent", """
            <Schema name="NonEmptyCrossJoinSubcubeMemo">
              <Dimension name="Calendar">
                <Hierarchy hasAll="true" allMemberName="All Years" primaryKey="id">
                  <Table name="calendar"/>
                  <Level name="Year" column="year" type="Integer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Store">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="store"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="fact"/>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
            </Schema>
            """);
        connection = mondrian.olap.DriverManager.getConnection(connect, null);
        RolapUtil.setHook(statements::add);
    }

    @AfterEach void close() throws Exception {
        RolapUtil.setHook(null);
        MondrianProperties properties = MondrianProperties.instance();
        properties.LevelPreCacheThreshold.set(previousPreCache);
        for (Map.Entry<String, String> entry : savedProperties.entrySet()) {
            if (entry.getValue() == null) {
                properties.remove(entry.getKey());
            } else {
                properties.setProperty(entry.getKey(), entry.getValue());
            }
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } finally {
            if (database != null) {
                database.close();
            }
        }
    }

    /** The same compiled NECJ is evaluated with both subcube masks. */
    @ParameterizedTest(name = "reset first={0}")
    @ValueSource(booleans = {false, true})
    void explicitAllResetDoesNotReuseFilteredCrossings(boolean resetFirst) {
        String measures = resetFirst
            ? "[Measures].[Reset Pair Count], [Measures].[Pair Count]"
            : "[Measures].[Pair Count], [Measures].[Reset Pair Count]";
        String mdx = CALCULATIONS + " SELECT {" + measures + "} ON 0" + SUBSELECT;
        assertEquals(resetFirst ? List.of(3, 2) : List.of(2, 3),
            values(mdx), () -> mdx + "; SQL=" + statements);
    }

    /** Each context alone establishes its literal count without a competing memo entry. */
    @ParameterizedTest(name = "isolated reset={0}")
    @ValueSource(booleans = {false, true})
    void eachSubcubeContextHasItsOwnCount(boolean reset) {
        String measure = reset ? "[Measures].[Reset Pair Count]" : "[Measures].[Pair Count]";
        String mdx = CALCULATIONS + " SELECT {" + measure + "} ON 0" + SUBSELECT;
        assertEquals(reset ? List.of(3) : List.of(2), values(mdx),
            () -> mdx + "; SQL=" + statements);
    }

    private List<Integer> values(String mdx) {
        MondrianProperties properties = MondrianProperties.instance();
        boolean previous = properties.EnableNativeNonEmpty.get();
        RolapNativeRegistry registry =
            ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previousRegistry = registry.isEnabled();
        try {
            properties.EnableNativeNonEmpty.set(false);
            SqlConstraintFactory.setNativeNonEmptyValue();
            registry.setEnabled(false);
            registry.flushAllNativeSetCache();
            statements.clear();
            Result result = connection.execute(connection.parseQuery(mdx));
            List<Integer> values = new ArrayList<>();
            for (int column = 0; column < result.getAxes()[0].getPositions().size(); column++) {
                values.add(((Number) result.getCell(new int[] {column}).getValue()).intValue());
            }
            return values;
        } finally {
            properties.EnableNativeNonEmpty.set(previous);
            SqlConstraintFactory.setNativeNonEmptyValue();
            registry.setEnabled(previousRegistry);
        }
    }

    private void set(String name, String value) {
        MondrianProperties properties = MondrianProperties.instance();
        savedProperties.put(name, properties.getProperty(name));
        properties.setProperty(name, value);
    }
}
