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
import java.sql.ResultSet;
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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The FK pushdown of {@code ${factJoins}} end to end: the rendered SQL
 * runs, reaches the fact by its own key, and never changes a value —
 * including for fact rows without a dimension row. H2 shows NULLs on such
 * a row, so the null-keyed week is the member it can be mistaken for here;
 * the type defaults of ClickHouse are covered by
 * {@code NativeSqlFactJoinsTest}.
 */
public class NativeSqlFactJoinsFkPushdownTest {
    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private boolean previousNativeSql;
    private boolean previousPushdown;

    @BeforeEach void open() throws Exception {
        MondrianProperties props = MondrianProperties.instance();
        previousNativeSql = props.NativeSqlEnable.get();
        previousPushdown = props.NativeSqlFactJoinsFkPushdown.get();
        props.NativeSqlEnable.set(true);
        NativeSqlCalc.clearCache();
        String jdbc = "jdbc:h2:mem:fk_pushdown_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE period (id INT, week INT)");
            // week 0 equals what an unmatched outer-join row shows on a
            // database that fills defaults, the NULL week what it shows here
            sql.execute(
                "INSERT INTO period VALUES (1,35),(2,36),(3,0),(4,NULL)");
            sql.execute(
                "CREATE TABLE product (id INT, name VARCHAR, mfr VARCHAR)");
            sql.execute("INSERT INTO product VALUES"
                + " (1,'A','Red'),(2,'B','Blue'),(3,'C','Red')");
            sql.execute(
                "CREATE TABLE sales (period_id INT, product_id INT, qty INT)");
            sql.execute("INSERT INTO sales VALUES (1,1,1)");
            // the independent fact; period 9 and product 9 have no dim row
            sql.execute(
                "CREATE TABLE stock (period_id INT, product_id INT, qty INT)");
            sql.execute("INSERT INTO stock VALUES"
                + " (1,1,10),(1,2,20),(1,3,40),(2,1,80),(3,1,160),"
                + " (9,1,320),(1,9,640),(4,1,1000)");
            sql.execute("SET QUERY_STATISTICS TRUE");
        }
        Util.PropertyList connect =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        connect.put("JdbcUser", "sa");
        connect.put("JdbcDrivers", "org.h2.Driver");
        connect.put("Jdbc", jdbc);
        connect.put("CatalogContent", """
            <Schema name="FkPushdown">
              <Dimension name="Period">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="period"/>
                  <Level name="Week" column="week" type="Integer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
                <Hierarchy name="Manufacturer" hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="mfr" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="sales"/>
                <DimensionUsage name="Period" source="Period" foreignKey="period_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                <CalculatedMember name="Stock" dimension="Measures">
                  <Annotations>
                    <Annotation name="nativeSql.enabled">true</Annotation>
                    <Annotation name="nativeSql.template">SELECT SUM(f.qty) AS val FROM stock f ${factJoins} WHERE ${whereClause}</Annotation>
                  </Annotations>
                  <Formula>-1</Formula>
                </CalculatedMember>
              </Cube>
            </Schema>
            """);
        connection = mondrian.olap.DriverManager.getConnection(connect, null);
    }

    @AfterEach void close() throws Exception {
        MondrianProperties props = MondrianProperties.instance();
        props.NativeSqlEnable.set(previousNativeSql);
        props.NativeSqlFactJoinsFkPushdown.set(previousPushdown);
        NativeSqlCalc.clearCache();
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

    @Test void slicerOnAStarJoinedLevelReachesTheFactByItsKey()
        throws Exception
    {
        // 10 + 20 + 40, and the stock row of the unknown product 9
        assertEquals(710d, stock(true, "FROM [Sales] WHERE [Period].[35]"));
        assertTrue(
            stockSql().contains(
                "f.\"period_id\" IN (SELECT nscs0.\"id\" FROM \"period\""
                + " nscs0 WHERE nscs0.\"week\" = 35)"),
            stockSql());
    }

    @Test void disabledRendersNoFactSideCondition() throws Exception {
        assertEquals(710d, stock(false, "FROM [Sales] WHERE [Period].[35]"));
        assertFalse(stockSql().contains(" IN (SELECT "), stockSql());
    }

    @Test void valuesDoNotDependOnThePushdown() {
        // absolute values: the formula's -1 on both sides would also be equal
        final Map<String, Double> expected = new LinkedHashMap<>();
        expected.put("FROM [Sales]", 2270d);
        expected.put("FROM [Sales] WHERE [Period].[35]", 710d);
        expected.put("FROM [Sales] WHERE [Period].[36]", 80d);
        expected.put("FROM [Sales] WHERE [Period].[0]", 160d);
        expected.put(
            "FROM [Sales] WHERE ([Period].[35],"
            + " [Product.Manufacturer].[Red])",
            50d);
        expected.put(
            "FROM (SELECT {[Product.Manufacturer].[Red],"
            + " [Product.Manufacturer].[Blue]} ON 0 FROM [Sales])"
            + " WHERE [Period].[35]",
            70d);
        expected.put(
            "FROM (SELECT {[Period].[35], [Period].[36]} ON 0"
            + " FROM [Sales]) WHERE [Product].[A]",
            90d);
        // week IS NULL also holds on the period-9 row without a dim row
        // (320): the value a pushdown of that condition would change
        expected.put(
            "FROM (SELECT {[Period].[#null], [Period].[35]} ON 0"
            + " FROM [Sales])",
            2030d);
        expected.forEach((from, value) -> {
            assertEquals(value, stock(false, from), from);
            assertEquals(value, stock(true, from), from);
        });
    }

    @Test void memberKeyedLikeAnUnmatchedRowIsNotPushedDown()
        throws Exception
    {
        assertEquals(160d, stock(true, "FROM [Sales] WHERE [Period].[0]"));
        assertFalse(stockSql().contains(" IN (SELECT "), stockSql());
    }

    @Test void subselectDisjunctionIsPushedAsOneCondition() throws Exception {
        // Red and Blue under week 35: 10 + 40 + 20; product 9 has no row
        assertEquals(
            70d,
            stock(
                true,
                "FROM (SELECT {[Product.Manufacturer].[Red],"
                + " [Product.Manufacturer].[Blue]} ON 0 FROM [Sales])"
                + " WHERE [Period].[35]"));
        final String sql = stockSql();
        assertTrue(
            sql.contains("f.\"period_id\" IN (SELECT nscs0.\"id\""),
            sql);
        assertTrue(
            sql.contains(
                "f.\"product_id\" IN (SELECT nscs1.\"id\" FROM \"product\""
                + " nscs1 WHERE (nscs1.\"mfr\" = 'Red'"
                + " OR nscs1.\"mfr\" = 'Blue'))"),
            sql);
    }

    private Double stock(boolean pushdown, String from) {
        MondrianProperties.instance()
            .NativeSqlFactJoinsFkPushdown.set(pushdown);
        // a value cached for the other rendering must not answer this one
        NativeSqlCalc.clearCache();
        Result result = connection.execute(connection.parseQuery(
            "SELECT {[Measures].[Stock]} ON 0 " + from));
        Object value = result.getCell(new int[] {0}).getValue();
        return value == null ? null : ((Number) value).doubleValue();
    }

    /** The one statement the database ran against the stock table. */
    private String stockSql() throws Exception {
        List<String> statements = new ArrayList<>();
        try (Statement sql = database.createStatement();
             ResultSet rs = sql.executeQuery(
                 "SELECT SQL_STATEMENT FROM INFORMATION_SCHEMA.QUERY_STATISTICS"
                 + " WHERE SQL_STATEMENT LIKE '%FROM stock f%'"
                 + " AND SQL_STATEMENT NOT LIKE '%QUERY_STATISTICS%'"))
        {
            while (rs.next()) {
                statements.add(rs.getString(1));
            }
        }
        assertEquals(1, statements.size(), statements.toString());
        return statements.get(0);
    }
}
