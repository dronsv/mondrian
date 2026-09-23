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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A fact-less context declines the fact <em>join</em>, not the context of a
 * relation the member query already reads.
 *
 * <p>{@code SqlTupleReader.chooseAggStar} binds the enumeration to an
 * aggregate table using the evaluator's current stored measure, while the
 * context constraint asks the measures that judge the axis - among them a
 * calculation that reads no stored fact (a label, a native-SQL measure over
 * another fact). When those disagree the enumeration stayed bounded by the
 * aggregate and lost its restriction, scanning every coordinate of the
 * aggregate to produce candidates for one.
 *
 * <p>The cost is in the statement, so the bound is on the statement: the
 * aggregate must be restricted to the context, and no dimension table of the
 * star may be joined to restrict it.
 */
public class FactlessAggContextRestrictionTest {
    /** Only the aggregate carries Year, Product and Store: no dimension table can restrict it. */
    private static final String AGG_TABLE = "agg_year_product_store";

    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private final List<String> statements = new ArrayList<>();
    private final Map<String, String> savedProperties = new LinkedHashMap<>();
    private int previousPreCache;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:factless_agg_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE calendar (id INT, year INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025),(2,2026)");
            // The level key is collapsed into the aggregate; a property of
            // that level is not, so the dimension table is joined back to
            // the aggregate to project it - the production shape.
            sql.execute("CREATE TABLE product (id INT, pcat VARCHAR, pname VARCHAR)");
            sql.execute("INSERT INTO product VALUES (1,'X','A'),(2,'X','B'),(3,'Y','C')");
            sql.execute("CREATE TABLE store (id INT, sname VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'S1'),(2,'S2')");
            sql.execute(
                "CREATE TABLE fact (calendar_id INT, product_id INT, store_id INT, qty INT)");
            // (A,S1) and (B,S1) in 2026; (C,S2) only in 2025.
            sql.execute("INSERT INTO fact VALUES (2,1,1,10),(2,2,1,20),(1,3,2,7)");
            sql.execute("CREATE TABLE " + AGG_TABLE
                + " (year INT, pname VARCHAR, sname VARCHAR, qty INT, fact_count INT)");
            sql.execute("INSERT INTO " + AGG_TABLE
                + " SELECT c.year, p.pname, s.sname, SUM(f.qty), COUNT(*) FROM fact f"
                + " JOIN calendar c ON c.id = f.calendar_id"
                + " JOIN product p ON p.id = f.product_id"
                + " JOIN store s ON s.id = f.store_id"
                + " GROUP BY c.year, p.pname, s.sname");
        }
        MondrianProperties props = MondrianProperties.instance();
        previousPreCache = props.LevelPreCacheThreshold.get();
        props.LevelPreCacheThreshold.set(0);
        set("mondrian.rolap.aggregates.Use", "true");
        set("mondrian.rolap.aggregates.Read", "false");
        set("mondrian.rolap.tupleReader.aggFeasibleEnumeration.allowDimJoin", "true");
        set("mondrian.native.queryEngine.enable", "false");
        Util.PropertyList connect =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        connect.put("JdbcUser", "sa");
        connect.put("JdbcDrivers", "org.h2.Driver");
        connect.put("Jdbc", jdbc);
        connect.put("CatalogContent", """
            <Schema name="FactlessAgg">
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

    /**
     * The regression: a label judges the axis, so the context is fact-less,
     * yet the enumeration reads the aggregate. Without the restriction the
     * statement produced every coordinate of the aggregate - here (C,S2),
     * which exists in 2025 only - and on a real cube read three years for one.
     */
    @Test void factlessAxisRestrictsTheAggregateItEnumeratesFrom() {
        String mdx = "WITH MEMBER [Measures].[Label] AS 1"
            + " SELECT {[Measures].[Label]} ON 0,"
            + " NON EMPTY CrossJoin([Product].[Name].Members, [Store].[Name].Members) ON 1"
            + " FROM [Sales] WHERE [Calendar].[2026]";
        assertEquals(List.of("A,S1", "B,S1"), rows(mdx));

        String sql = aggregateEnumerationSql();
        assertTrue(sql.contains("year"), "the slicer must restrict the aggregate: " + sql);
        assertEquals(1, restrictionCount(sql, "2026"),
            "exactly one year restriction on the aggregate: " + sql);
        // Fact-less means no fact join: the restriction must come from the
        // aggregate the query already reads, never from joining a dimension.
        assertFalse(sql.contains("calendar"),
            "the fact-less enumeration must not join a dimension table: " + sql);
        assertAggregateLeadsTheFromClause(sql);
    }

    /** A stored judge takes the fact path; the aggregate is restricted there too. */
    @Test void storedAxisRestrictsTheAggregateItEnumeratesFrom() {
        String mdx = "SELECT {[Measures].[Quantity]} ON 0,"
            + " NON EMPTY CrossJoin([Product].[Name].Members, [Store].[Name].Members) ON 1"
            + " FROM [Sales] WHERE [Calendar].[2026]";
        assertEquals(List.of("A,S1", "B,S1"), rows(mdx));
        String sql = aggregateEnumerationSql();
        assertEquals(1, restrictionCount(sql, "2026"), sql);
        assertAggregateLeadsTheFromClause(sql);
    }

    /**
     * The enumeration is driven by the aggregate and joins the dimension
     * table for the columns the aggregate collapsed away. A dialect that
     * builds a comma join in the order it is given - ClickHouse - then reads
     * an order of magnitude more when a dimension relation comes first, so
     * the aggregate must lead the from clause on both paths.
     */
    private static void assertAggregateLeadsTheFromClause(String sql) {
        int from = sql.indexOf("from");
        int where = sql.indexOf("where", from);
        String relations = sql.substring(from, where < 0 ? sql.length() : where);
        assertTrue(relations.contains("product"),
            "expected the collapsed level to join its dimension: " + sql);
        assertTrue(relations.indexOf(AGG_TABLE) < relations.indexOf("product"),
            "the aggregate must lead the from clause: " + sql);
    }

    /** The enumeration reads the aggregate, so the statement it issues is the whole cost. */
    private String aggregateEnumerationSql() {
        List<String> reads = statements.stream()
            .filter(sql -> sql.contains(AGG_TABLE) && sql.contains("group by"))
            .toList();
        assertFalse(reads.isEmpty(),
            "expected an enumeration over " + AGG_TABLE + ": " + statements);
        return reads.get(0);
    }

    private static int restrictionCount(String sql, String value) {
        int count = 0;
        for (int at = sql.indexOf(value); at >= 0; at = sql.indexOf(value, at + 1)) {
            count++;
        }
        return count;
    }

    private List<String> rows(String mdx) {
        statements.clear();
        Result result = connection.execute(connection.parseQuery(mdx));
        List<String> names = new ArrayList<>();
        for (Position position : result.getAxes()[1].getPositions()) {
            names.add(String.join(",", position.stream().map(Member::getName).toList()));
        }
        return names;
    }

    private void set(String name, String value) {
        MondrianProperties props = MondrianProperties.instance();
        savedProperties.put(name, props.getProperty(name));
        props.setProperty(name, value);
    }
}
