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

import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Position;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sales aggregates cannot define the candidate domain of a literal or an
 * independent native-SQL measure. Each invocation gets its own database,
 * schema, connection and native tuple cache, including the interpreter oracle.
 */
public class FactlessAggCandidateCompletenessTest {
    private static final String AGG_TABLE = "agg_year_product_store";
    private static final String AXIS =
        " NON EMPTY CrossJoin([Product].[Name].Members, [Store].[Name].Members) ON 1"
        + " FROM [Sales] WHERE [Calendar].[2026]";

    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private final List<String> statements = new ArrayList<>();
    private final Map<String, String> savedProperties = new LinkedHashMap<>();
    private int previousPreCache;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:factless_agg_candidates_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE calendar (id INT, year INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025),(2,2026)");
            sql.execute("CREATE TABLE product (id INT, pcat VARCHAR, pname VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'X','A'),(2,'X','B'),(3,'Y','C')");
            sql.execute("CREATE TABLE store (id INT, sname VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'S1'),(2,'S2')");
            sql.execute(
                "CREATE TABLE fact (calendar_id INT, product_id INT, store_id INT, qty INT)");
            // Sales: A,S1 and B,S1 in 2026; C,S2 only in 2025.
            sql.execute("INSERT INTO fact VALUES (2,1,1,10),(2,2,1,20),(1,3,2,7)");
            sql.execute("CREATE TABLE " + AGG_TABLE
                + " (year INT, pname VARCHAR, sname VARCHAR, qty INT, fact_count INT)");
            sql.execute("INSERT INTO " + AGG_TABLE
                + " SELECT c.year, p.pname, s.sname, SUM(f.qty), COUNT(*) FROM fact f"
                + " JOIN calendar c ON c.id = f.calendar_id"
                + " JOIN product p ON p.id = f.product_id"
                + " JOIN store s ON s.id = f.store_id"
                + " GROUP BY c.year, p.pname, s.sname");
            sql.execute(
                "CREATE TABLE stock (calendar_id INT, product_id INT, store_id INT, qty INT)");
            // Independent stock is present at C,S2 in the selected year.
            sql.execute("INSERT INTO stock VALUES (2,3,2,300)");
        }
        MondrianProperties props = MondrianProperties.instance();
        previousPreCache = props.LevelPreCacheThreshold.get();
        props.LevelPreCacheThreshold.set(0);
        set("mondrian.rolap.aggregates.Use", "true");
        set("mondrian.rolap.aggregates.Read", "false");
        set("mondrian.rolap.tupleReader.aggFeasibleEnumeration.allowDimJoin", "true");
        set("mondrian.native.queryEngine.enable", "false");
        set("mondrian.native.sql.enable", "true");
        set("mondrian.native.crossjoin.factlessSplit.enable", "false");
        set("mondrian.native.nonEmptyFilter.enable", "false");
        Util.PropertyList connect =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        connect.put("JdbcUser", "sa");
        connect.put("JdbcDrivers", "org.h2.Driver");
        connect.put("Jdbc", jdbc);
        connect.put("CatalogContent", """
            <Schema name="FactlessAggCandidates">
              <Dimension name="Calendar" type="TimeDimension">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="calendar"/>
                  <Level name="Year" column="year" type="Integer" levelType="TimeYears" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="pname" uniqueMembers="true">
                    <Property name="Cat" column="pcat"/>
                  </Level>
                </Hierarchy>
              </Dimension>
              <Dimension name="Store">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="store"/>
                  <Level name="Name" column="sname" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales">
                <Table name="fact">
                  <AggName name="%s">
                    <AggFactCount column="fact_count"/>
                    <AggMeasure name="[Measures].[Quantity]" column="qty"/>
                    <AggLevel name="[Calendar].[Year]" column="year"/>
                    <AggLevel name="[Product].[Name]" column="pname"/>
                    <AggLevel name="[Store].[Name]" column="sname"/>
                  </AggName>
                </Table>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                <CalculatedMember name="Stock" dimension="Measures" formatString="0">
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
                  <Formula>NULL</Formula>
                </CalculatedMember>
              </Cube>
            </Schema>
            """.formatted(AGG_TABLE));
        connection = mondrian.olap.DriverManager.getConnection(connect, null);
        RolapUtil.setHook(statements::add);
    }

    @AfterEach void close() throws Exception {
        RolapUtil.setHook(null);
        MondrianProperties props = MondrianProperties.instance();
        props.LevelPreCacheThreshold.set(previousPreCache);
        for (Map.Entry<String, String> entry : savedProperties.entrySet()) {
            if (entry.getValue() == null) {
                props.remove(entry.getKey());
            } else {
                props.setProperty(entry.getKey(), entry.getValue());
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

    /** A constant is present at all six pairs, independently of sales. */
    @ParameterizedTest(name = "literal candidates, native={0}")
    @ValueSource(booleans = {false, true})
    void literalNonEmptyRetainsEveryDimensionPair(boolean nativeEnabled) {
        String mdx = "WITH MEMBER [Measures].[Label] AS 1"
            + " SELECT {[Measures].[Label]} ON 0," + AXIS;
        List<String> actual = rows(mdx, nativeEnabled);
        assertEquals(
            List.of("A,S1=1.0", "A,S2=1.0", "B,S1=1.0", "B,S2=1.0", "C,S1=1.0", "C,S2=1.0"),
            actual, () -> "native=" + nativeEnabled + "; SQL=" + statements);
        if (nativeEnabled) {
            assertNativeDimensionEnumeration();
        }
    }

    /**
     * Narrowing a sales aggregate by year loses valid independent stock.
     * The pre-PR60 enumeration happened to retain C,S2 from its 2025 sale.
     */
    @ParameterizedTest(name = "independent stock, native={0}")
    @ValueSource(booleans = {false, true})
    void independentStockRetainsPairWithSalesOnlyInAnotherYear(boolean nativeEnabled) {
        String mdx = "SELECT {[Measures].[Stock]} ON 0," + AXIS;
        List<String> actual = rows(mdx, nativeEnabled);
        // The MDX formula is NULL; the only source of 300 is the stock SQL.
        assertEquals(List.of("C,S2=300.0"), actual,
            () -> "native=" + nativeEnabled + "; SQL=" + statements);
        if (nativeEnabled) {
            assertNativeDimensionEnumeration();
        }
    }

    /** A stored measure remains eligible for its own aggregate. */
    @ParameterizedTest(name = "stored quantity, native={0}")
    @ValueSource(booleans = {false, true})
    void storedQuantityKeepsAggregateEligibility(boolean nativeEnabled) {
        String mdx = "SELECT {[Measures].[Quantity]} ON 0," + AXIS;
        assertEquals(List.of("A,S1=10.0", "B,S1=20.0"), rows(mdx, nativeEnabled));
        assertTrue(statements.stream().anyMatch(sql -> sql.contains(AGG_TABLE)),
            () -> "the stored measure should still read its aggregate: " + statements);
    }

    private void assertNativeDimensionEnumeration() {
        assertTrue(statements.stream().anyMatch(sql ->
            sql.contains("\"product\"") && sql.contains("\"store\"") && sql.contains("group by")
                && !sql.contains(AGG_TABLE) && !sql.contains("\"fact\"")
                && !sql.contains("stock")),
            () -> "native enumeration must still read the dimension product: " + statements);
    }

    private List<String> rows(String mdx, boolean nativeEnabled) {
        MondrianProperties props = MondrianProperties.instance();
        boolean previous = props.EnableNativeNonEmpty.get();
        RolapNativeRegistry registry =
            ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previousRegistry = registry.isEnabled();
        try {
            props.EnableNativeNonEmpty.set(nativeEnabled);
            registry.setEnabled(nativeEnabled);
            registry.flushAllNativeSetCache();
            statements.clear();
            Result result = connection.execute(connection.parseQuery(mdx));
            List<String> values = new ArrayList<>();
            for (int i = 0; i < result.getAxes()[1].getPositions().size(); i++) {
                Position position = result.getAxes()[1].getPositions().get(i);
                var cell = result.getCell(new int[] {0, i});
                values.add(String.join(",", position.stream().map(Member::getName).toList())
                    + "=" + (cell.isNull() ? "NULL" : ((Number) cell.getValue()).doubleValue()));
            }
            return values;
        } finally {
            props.EnableNativeNonEmpty.set(previous);
            SqlConstraintFactory.setNativeNonEmptyValue();
            registry.setEnabled(previousRegistry);
        }
    }

    private void set(String name, String value) {
        MondrianProperties props = MondrianProperties.instance();
        savedProperties.put(name, props.getProperty(name));
        props.setProperty(name, value);
    }
}
