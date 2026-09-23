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
 * An aggregate that carries a level's key still joins that level's dimension
 * table to project the columns the aggregate collapsed away - an ordinal, a
 * caption, a property. The join is on the <em>level</em> column, which is not
 * a key of the dimension, so every aggregate row is multiplied by the number
 * of dimension rows sharing its level value. Two such levels multiply again.
 *
 * <p>On the house cube that is a 478k-row aggregate crossed with 327 products
 * per subcategory and 36 stores per city - five billion rows the database
 * groups back down to 322. The engine never asked for those rows: the
 * dimension contributes no aggregated value here and every column it
 * contributes is grouped, so the multiplicity of its rows cannot reach the
 * result, only the cost.
 *
 * <p>The bound is therefore on the rows the join may produce, not on a clock:
 * joining the dimensions must not make the statement read more rows than the
 * aggregate itself has. The control assertion measures what the bare join
 * would have produced, so the test says by how much it bites.
 */
public class CollapsedLevelDimensionProjectionTest {
    /** Carries Year, the product category and the store region. */
    private static final String AGG_TABLE = "agg_cat_region";

    /** Dimension rows sharing one category, and one region. */
    private static final int PRODUCTS_PER_CATEGORY = 4;
    private static final int STORES_PER_REGION = 3;

    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private final List<String> statements = new ArrayList<>();
    private final Map<String, String> savedProperties = new LinkedHashMap<>();
    private int previousPreCache;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:collapsed_projection_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE calendar (id INT, year INT)");
            sql.execute("INSERT INTO calendar VALUES (1,2025),(2,2026)");
            // pcat is the level key and is shared by PRODUCTS_PER_CATEGORY
            // rows; pgroup is a property that lives only here, so the
            // aggregate cannot avoid joining this table.
            sql.execute(
                "CREATE TABLE product (id INT, pcat VARCHAR, pgroup VARCHAR)");
            sql.execute("INSERT INTO product VALUES"
                + " (1,'X','G1'),(2,'X','G1'),(3,'X','G1'),(4,'X','G1'),"
                + " (5,'Y','G2'),(6,'Y','G2'),(7,'Y','G2'),(8,'Y','G2')");
            // sregion is the level key, shared by STORES_PER_REGION rows;
            // ssort is the ordinal column and lives only here.
            sql.execute(
                "CREATE TABLE store (id INT, sregion VARCHAR, ssort INT)");
            sql.execute("INSERT INTO store VALUES"
                + " (1,'North',1),(2,'North',1),(3,'North',1),"
                + " (4,'South',2),(5,'South',2),(6,'South',2)");
            sql.execute(
                "CREATE TABLE fact (calendar_id INT, product_id INT,"
                + " store_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES"
                + " (2,1,1,10),(2,5,4,20),(1,1,4,7)");
            sql.execute("CREATE TABLE " + AGG_TABLE
                + " (year INT, pcat VARCHAR, sregion VARCHAR, qty INT,"
                + " fact_count INT)");
            sql.execute("INSERT INTO " + AGG_TABLE
                + " SELECT c.year, p.pcat, s.sregion, SUM(f.qty), COUNT(*)"
                + " FROM fact f"
                + " JOIN calendar c ON c.id = f.calendar_id"
                + " JOIN product p ON p.id = f.product_id"
                + " JOIN store s ON s.id = f.store_id"
                + " GROUP BY c.year, p.pcat, s.sregion");
        }
        MondrianProperties props = MondrianProperties.instance();
        previousPreCache = props.LevelPreCacheThreshold.get();
        props.LevelPreCacheThreshold.set(0);
        set("mondrian.rolap.aggregates.Use", "true");
        set("mondrian.rolap.aggregates.Read", "false");
        set(
            "mondrian.rolap.tupleReader.aggFeasibleEnumeration.allowDimJoin",
            "true");
        set("mondrian.native.queryEngine.enable", "false");
        Util.PropertyList connect =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        connect.put("JdbcUser", "sa");
        connect.put("JdbcDrivers", "org.h2.Driver");
        connect.put("Jdbc", jdbc);
        connect.put("CatalogContent", """
            <Schema name="CollapsedProjection">
              <Dimension name="Calendar" type="TimeDimension">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="calendar"/>
                  <Level name="Year" column="year" type="Integer" levelType="TimeYears" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Cat" column="pcat" uniqueMembers="false">
                    <Property name="Grp" column="pgroup"/>
                  </Level>
                </Hierarchy>
              </Dimension>
              <Dimension name="Store">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="store"/>
                  <Level name="Region" column="sregion" ordinalColumn="ssort" uniqueMembers="false"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Item">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Id" column="id" type="Integer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales">
                <Table name="fact">
                  <AggName name="%s">
                    <AggFactCount column="fact_count"/>
                    <AggMeasure name="[Measures].[Quantity]" column="qty"/>
                    <AggLevel name="[Calendar].[Year]" column="year"/>
                    <AggLevel name="[Product].[Cat]" column="pcat"/>
                    <AggLevel name="[Store].[Region]" column="sregion"/>
                  </AggName>
                </Table>
                <DimensionUsage name="Calendar" source="Calendar" foreignKey="calendar_id"/>
                <DimensionUsage name="Product" source="Product" foreignKey="product_id"/>
                <DimensionUsage name="Store" source="Store" foreignKey="store_id"/>
                <DimensionUsage name="Item" source="Item" foreignKey="product_id"/>
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
     * The regression: two collapsed levels each join their dimension on a
     * non-unique column, so the enumeration read
     * {@code PRODUCTS_PER_CATEGORY * STORES_PER_REGION} rows per aggregate
     * row to produce the same answer.
     */
    @Test void collapsedLevelsDoNotMultiplyTheAggregate() throws Exception {
        String mdx = "SELECT {[Measures].[Quantity]} ON 0,"
            + " NON EMPTY CrossJoin([Product].[Cat].Members,"
            + " [Store].[Region].Members) ON 1"
            + " FROM [Sales] WHERE [Calendar].[2026]";
        assertEquals(List.of("X,North", "Y,South"), rows(mdx));

        String sql = aggregateEnumerationSql();
        assertTrue(sql.contains("product"),
            "expected the collapsed level to join its dimension: " + sql);
        assertTrue(sql.contains("store"),
            "expected the collapsed level to join its dimension: " + sql);

        long aggregateRows = count("SELECT COUNT(*) FROM " + AGG_TABLE);
        long joined = countJoinedRows(sql);
        assertTrue(joined <= aggregateRows,
            "joining the dimensions multiplied the aggregate: " + joined
            + " rows from " + aggregateRows + " aggregate rows: " + sql);

        // What the bare join would have cost, so the bound is not vacuous.
        long fannedOut = count(
            "SELECT COUNT(*) FROM " + AGG_TABLE + " a, product p, store s"
            + " WHERE p.pcat = a.pcat AND s.sregion = a.sregion");
        assertEquals(
            aggregateRows * PRODUCTS_PER_CATEGORY * STORES_PER_REGION,
            fannedOut,
            "fixture must reproduce the fan-out the bound rules out");
        assertTrue(joined < fannedOut,
            "the bound does not bite: " + joined + " vs " + fannedOut);
    }

    /**
     * The projection is a de-duplication, not a pruning: every column the
     * level needs must still come back, and a property that lives only on
     * the dimension must still reach the member.
     */
    @Test void collapsedLevelStillProjectsItsProperty() {
        String mdx = "SELECT {[Measures].[Quantity]} ON 0,"
            + " NON EMPTY [Product].[Cat].Members"
            + " DIMENSION PROPERTIES [Product].[Cat].[Grp] ON 1"
            + " FROM [Sales] WHERE [Calendar].[2026]";
        Result result = execute(mdx);
        List<String> groups = new ArrayList<>();
        for (Position position : result.getAxes()[1].getPositions()) {
            groups.add(
                String.valueOf(position.get(0).getPropertyValue("Grp")));
        }
        assertEquals(List.of("G1", "G2"), groups);
    }

    /**
     * A second hierarchy over the same dimension table, at a level the
     * aggregate does not carry, takes the enumeration off the aggregate
     * altogether and back to the fact table - where the dimension is joined
     * on its own key and no projection is offered. The answer must be the
     * same one either path produces.
     */
    @Test void aLevelTheAggregateDoesNotCarryReadsTheFact() {
        String mdx = "SELECT {[Measures].[Quantity]} ON 0,"
            + " NON EMPTY CrossJoin([Product].[Cat].Members,"
            + " [Item].[Id].Members) ON 1"
            + " FROM [Sales] WHERE [Calendar].[2026]";
        assertEquals(List.of("X,1", "Y,5"), rows(mdx));
        assertTrue(
            statements.stream().noneMatch(sql -> sql.contains(AGG_TABLE)),
            "expected the fact path for a level the aggregate lacks: "
            + statements);
    }

    /** The enumeration reads the aggregate, so its statement is the cost. */
    private String aggregateEnumerationSql() {
        List<String> reads = statements.stream()
            .filter(sql -> sql.contains(AGG_TABLE) && sql.contains("group by"))
            .toList();
        assertFalse(reads.isEmpty(),
            "expected an enumeration over " + AGG_TABLE + ": " + statements);
        return reads.get(0);
    }

    /**
     * Counts the rows the enumeration's joins produce before grouping, by
     * re-issuing its from and where clauses with {@code count(*)}.
     */
    private long countJoinedRows(String sql) throws Exception {
        int from = sql.indexOf(" from ");
        assertTrue(from > 0, "no from clause: " + sql);
        int end = sql.indexOf(" group by ", from);
        if (end < 0) {
            end = sql.indexOf(" order by ", from);
        }
        String body = end < 0 ? sql.substring(from) : sql.substring(from, end);
        return count("select count(*)" + body);
    }

    private long count(String sql) throws Exception {
        try (Statement statement = database.createStatement();
             ResultSet rows = statement.executeQuery(sql))
        {
            assertTrue(rows.next(), sql);
            return rows.getLong(1);
        }
    }

    private Result execute(String mdx) {
        statements.clear();
        return connection.execute(connection.parseQuery(mdx));
    }

    private List<String> rows(String mdx) {
        Result result = execute(mdx);
        List<String> names = new ArrayList<>();
        for (Position position : result.getAxes()[1].getPositions()) {
            names.add(
                String.join(
                    ",",
                    position.stream().map(Member::getName).toList()));
        }
        return names;
    }

    private void set(String name, String value) {
        MondrianProperties props = MondrianProperties.instance();
        savedProperties.put(name, props.getProperty(name));
        props.setProperty(name, value);
    }
}
