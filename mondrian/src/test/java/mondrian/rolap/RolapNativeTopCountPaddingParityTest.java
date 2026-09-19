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
import java.util.function.Function;

import mondrian.olap.Member;
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
 * Native TopCount padding parity (#86, #88). A non-literal N keeps the
 * Java path, which gives the reference result in the same schema.
 *
 * <p>Fixture: products A..E, manufacturers X and Y. Under slicer X only
 * A and E have Quantity; D has no facts at all. {@code AllMfrQty} pins
 * the Mfr hierarchy to All, so it conflicts with the slicer and is
 * non-empty for B and C even where Quantity is empty.
 */
public class RolapNativeTopCountPaddingParityTest {
    private mondrian.olap.Connection connection;
    private String previousNativeQueryEngine;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:topcount_padding_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false";
        try (java.sql.Connection db = DriverManager.getConnection(jdbc, "sa", "")) {
            Statement sql = db.createStatement();
            sql.execute("CREATE TABLE product (id INT, name VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'A'),(2,'B'),(3,'C'),(4,'D'),(5,'E')");
            sql.execute("CREATE TABLE mfr (id INT, name VARCHAR)");
            sql.execute("INSERT INTO mfr VALUES (1,'X'),(2,'Y')");
            sql.execute("CREATE TABLE fact (product_id INT, mfr_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (1,1,10),(2,2,20),(3,2,5),(5,1,10)");
        }
        previousNativeQueryEngine = MondrianProperties.instance()
            .getProperty("mondrian.native.queryEngine.enable");
        MondrianProperties.instance()
            .setProperty("mondrian.native.queryEngine.enable", "false");
        Util.PropertyList props =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="TopCountPadding">
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Mfr">
                <Hierarchy hasAll="true" allMemberName="All Mfrs" primaryKey="id"><Table name="mfr"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="fact"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Mfr" source="Mfr" foreignKey="mfr_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                <CalculatedMember name="AllMfrQty" dimension="Measures"
                  formula="([Measures].[Quantity], [Mfr].[All Mfrs])"/>
              </Cube>
            </Schema>
            """);
        connection = mondrian.olap.DriverManager.getConnection(props, null);
    }

    @AfterEach void close() {
        RolapUtil.setHook(null);
        if (previousNativeQueryEngine == null) {
            MondrianProperties.instance()
                .remove("mondrian.native.queryEngine.enable");
        } else {
            MondrianProperties.instance().setProperty(
                "mondrian.native.queryEngine.enable", previousNativeQueryEngine);
        }
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * #86: the stored-only ranking bypasses the measure-conflict veto and
     * runs natively, but must still pad to N — B has no Quantity under X,
     * yet survives NON EMPTY through AllMfrQty.
     */
    @Test void nonEmptyTopCountWithConflictingCalcPadsLikeJava() {
        List<String> nativeSql = assertParity(
            n -> "SELECT {[Measures].[Quantity], [Measures].[AllMfrQty]} ON 0,"
                + " NON EMPTY TopCount([Product].[Name].Members, " + n
                + ", [Measures].[Quantity]) ON 1"
                + " FROM [Sales] WHERE [Mfr].[X]",
            3,
            "A | 10 | 10", "E | 10 | 10", "B | null | 20");
        assertTrue(nativeSql.stream().anyMatch(this::isRankingSql),
            "stored-only ranking must stay native: " + nativeSql);
    }

    /**
     * #86: the native set cache is shared across queries. An unpadded
     * result cached by a query without the conflicting calc must not be
     * reused by one that needs padding.
     */
    @Test void paddedTopCountIsNotServedFromUnpaddedCache() {
        String topCount = " NON EMPTY TopCount([Product].[Name].Members, 3,"
            + " [Measures].[Quantity]) ON 1 FROM [Sales] WHERE [Mfr].[X]";
        assertEquals(
            List.of("A | 10", "E | 10"),
            rows("SELECT {[Measures].[Quantity]} ON 0," + topCount,
                new ArrayList<>()));
        assertEquals(
            List.of("A | 10 | 10", "E | 10 | 10", "B | null | 20"),
            rows("SELECT {[Measures].[Quantity], [Measures].[AllMfrQty]} ON 0,"
                + topCount, new ArrayList<>()));
    }

    /**
     * #88: Head always pads, and padding reads one level only, so a
     * crossjoin set must stay on the Java path and keep all N tuples.
     */
    @Test void headOrderCrossJoinStaysOnJavaPathAndKeepsPadding() {
        List<String> nativeSql = assertParity(
            n -> "SELECT {[Measures].[Quantity]} ON 0,"
                + " Head(Order(CrossJoin([Product].[Name].Members,"
                + " [Mfr].[Name].Members), [Measures].[Quantity], BDESC), "
                + n + ") ON 1 FROM [Sales]",
            6,
            "B, Y | 20", "A, X | 10", "E, X | 10", "C, Y | 5",
            "A, Y | null", "B, X | null");
        assertFalse(nativeSql.stream().anyMatch(this::isRankingSql),
            "multi-hierarchy padding must not run natively: " + nativeSql);
    }

    /** #88: the single-level incident shape stays native, padding included. */
    @Test void headOrderSingleLevelRunsNativeWithPadding() {
        List<String> nativeSql = assertParity(
            n -> "SELECT {[Measures].[Quantity]} ON 0,"
                + " Head(Order([Product].[Name].Members, [Measures].[Quantity],"
                + " BDESC), " + n + ") ON 1 FROM [Sales]",
            5,
            "B | 20", "A | 10", "E | 10", "C | 5", "D | null");
        assertTrue(nativeSql.stream().anyMatch(this::isRankingSql),
            "single-level Head(Order) must reach native TopCount: " + nativeSql);
    }

    /**
     * Runs the query natively (literal N) and on the Java path (N+0),
     * asserts both return {@code expectedRows} with every column value,
     * and returns the SQL of the native run.
     */
    private List<String> assertParity(
        Function<String, String> query, int n, String... expectedRows)
    {
        List<String> javaSql = new ArrayList<>();
        List<String> nativeSql = new ArrayList<>();
        // A non-literal count fails the native eligibility check.
        List<String> java = rows(query.apply(n + "+0"), javaSql);
        assertFalse(javaSql.stream().anyMatch(this::isRankingSql),
            "Java control must not run native ranking SQL: " + javaSql);
        List<String> expected = List.of(expectedRows);
        assertEquals(expected, java, "Java path");
        assertEquals(
            expected, rows(query.apply(String.valueOf(n)), nativeSql),
            "native path");
        return nativeSql;
    }

    private boolean isRankingSql(String sql) {
        String lower = sql.toLowerCase();
        return lower.contains("order by")
            && lower.contains("sum(\"fact\".\"qty\") desc");
    }

    private List<String> rows(String mdx, List<String> statements) {
        RolapUtil.setHook(statements::add);
        try {
            Result result = connection.execute(connection.parseQuery(mdx));
            int columns = result.getAxes()[0].getPositions().size();
            List<String> rows = new ArrayList<>();
            for (int i = 0; i < result.getAxes()[1].getPositions().size(); i++) {
                List<String> names = new ArrayList<>();
                for (Member m : result.getAxes()[1].getPositions().get(i)) {
                    names.add(m.getName());
                }
                StringBuilder row = new StringBuilder(String.join(", ", names));
                for (int c = 0; c < columns; c++) {
                    Object v = result.getCell(new int[] {c, i}).getValue();
                    row.append(" | ").append(
                        v == null ? "null" : String.valueOf(((Number) v).intValue()));
                }
                rows.add(row.toString());
            }
            return rows;
        } finally {
            RolapUtil.setHook(null);
        }
    }
}

// End RolapNativeTopCountPaddingParityTest.java
