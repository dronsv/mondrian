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
import java.util.List;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * #88: the native Head(Order(...)) rewrite must return exactly what the
 * Java path returns. A non-literal N keeps the Java path (the rewrite
 * fails closed), which gives the reference result in the same schema.
 * Product C has no fact rows, so it carries an empty value.
 */
public class RolapNativeTopCountHeadOrderParityTest {
    private mondrian.olap.Connection connection;
    private String previousNativeQueryEngine;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:head_order_" + UUID.randomUUID().toString().replace("-", "")
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "")) {
            Statement sql = db.createStatement();
            sql.execute("CREATE TABLE product (id INT, name VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'A'),(2,'B'),(3,'C'),(4,'D')");
            sql.execute("CREATE TABLE fact (product_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (1,10),(2,20),(4,5)");
        }
        previousNativeQueryEngine = MondrianProperties.instance().getProperty("mondrian.native.queryEngine.enable");
        MondrianProperties.instance().setProperty("mondrian.native.queryEngine.enable", "false");
        Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="HeadOrder">
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="fact"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
              </Cube>
            </Schema>
            """);
        connection = mondrian.olap.DriverManager.getConnection(props, null);
    }

    @AfterEach void close() {
        RolapUtil.setHook(null);
        if (previousNativeQueryEngine == null) {
            MondrianProperties.instance().remove("mondrian.native.queryEngine.enable");
        } else {
            MondrianProperties.instance().setProperty("mondrian.native.queryEngine.enable", previousNativeQueryEngine);
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test void bdescMatchesJavaAndRunsNative() {
        for (int n : new int[] {2, 4}) {
            List<String> statements = new ArrayList<>();
            assertEquals(rows(head("BDESC", n + "+0"), new ArrayList<>()), rows(head("BDESC", "" + n), statements));
            assertTrue(statements.stream().anyMatch(RolapNativeTopCountHeadOrderParityTest::isRankingSql),
                "BDESC must reach native TopCount: " + statements);
        }
    }

    @Test void bascKeepsJavaEmptyFirstOrder() {
        for (int n : new int[] {1, 2, 4}) {
            assertEquals(rows(head("BASC", n + "+0"), new ArrayList<>()), rows(head("BASC", "" + n), new ArrayList<>()),
                "N=" + n);
        }
    }

    private static String head(String direction, String count) {
        return "Head(Order([Product].[Name].Members,[Measures].[Quantity]," + direction + ")," + count + ")";
    }

    private static boolean isRankingSql(String sql) {
        String lower = sql.toLowerCase();
        return lower.contains("order by") && lower.contains("sum(\"fact\".\"qty\") desc");
    }

    private String rows(String set, List<String> statements) {
        RolapUtil.setHook(statements::add);
        Result result = connection.execute(connection.parseQuery(
            "SELECT {[Measures].[Quantity]} ON COLUMNS, " + set + " ON ROWS FROM [Sales]"));
        RolapUtil.setHook(null);
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < result.getAxes()[1].getPositions().size(); i++) {
            rows.add(result.getAxes()[1].getPositions().get(i).get(0).getName() + "="
                + result.getCell(new int[] {0, i}).getValue());
        }
        return String.join(" ", rows);
    }
}

// End RolapNativeTopCountHeadOrderParityTest.java
