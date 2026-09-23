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
import java.sql.PreparedStatement;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What judging every NonEmptyCrossJoin crossing costs, and that it can be
 * turned off.
 *
 * <p>The fixture is 2001 products x 4 years, every crossing backed by a
 * fact row. Store's default member is S1 rather than its All member, and
 * two crossings belong to S2: 8004 crossings have a fact, 8002 of them have
 * one at S1. So the crossings a widened probe finds and the crossings the
 * query's own coordinate finds differ, and each pass over the candidates is
 * visible as its own read of the fact table.
 */
public class NonEmptyCrossJoinJudgeCostTest {
    private static final int PRODUCTS = 2001;
    private static final int YEARS = 4;
    /** Every crossing of the fixture; all of them have a fact row. */
    private static final int CROSSINGS = PRODUCTS * YEARS;
    /** Every crossing but (the last product) x (the last two years). */
    private static final int SOLD_BY_S1 = CROSSINGS - 2;

    private static final String CROSS_JOIN =
        "CrossJoin([Product].[Name].Members, [Calendar].[Year].Members)";
    private static final String NON_EMPTY_CROSS_JOIN = "NonEmpty" + CROSS_JOIN;

    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private final List<String> statements = new ArrayList<>();
    private int previousPreCache;
    private String previousNativeQueryEngine;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:necj_cost_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false;NON_KEYWORDS=YEAR";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE calendar (id INT, year INT)");
            sql.execute("CREATE TABLE product (id INT, name VARCHAR)");
            sql.execute("CREATE TABLE store (id INT, label VARCHAR)");
            sql.execute("INSERT INTO store VALUES (1,'S1'),(2,'S2')");
            sql.execute(
                "CREATE TABLE fact (calendar_id INT, product_id INT, store_id INT, qty INT)");
        }
        try (PreparedStatement insert =
            database.prepareStatement("INSERT INTO calendar VALUES (?,?)"))
        {
            for (int year = 1; year <= YEARS; year++) {
                insert.setInt(1, year);
                insert.setInt(2, 2022 + year);
                insert.addBatch();
            }
            insert.executeBatch();
        }
        try (PreparedStatement insert =
            database.prepareStatement("INSERT INTO product VALUES (?,?)"))
        {
            for (int product = 1; product <= PRODUCTS; product++) {
                insert.setInt(1, product);
                insert.setString(2, String.format("P%04d", product));
                insert.addBatch();
            }
            insert.executeBatch();
        }
        try (PreparedStatement insert =
            database.prepareStatement("INSERT INTO fact VALUES (?,?,?,?)"))
        {
            for (int product = 1; product <= PRODUCTS; product++) {
                for (int year = 1; year <= YEARS; year++) {
                    // The two crossings S1 never sold belong to S2, so the
                    // All Store probe finds every crossing non-empty.
                    insert.setInt(1, year);
                    insert.setInt(2, product);
                    insert.setInt(3, product == PRODUCTS && year > 2 ? 2 : 1);
                    insert.setInt(4, 1);
                    insert.addBatch();
                }
            }
            insert.executeBatch();
        }
        MondrianProperties props = MondrianProperties.instance();
        previousPreCache = props.LevelPreCacheThreshold.get();
        props.LevelPreCacheThreshold.set(0);
        previousNativeQueryEngine =
            props.getProperty("mondrian.native.queryEngine.enable");
        props.setProperty("mondrian.native.queryEngine.enable", "false");
        Util.PropertyList connect =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        connect.put("JdbcUser", "sa");
        connect.put("JdbcDrivers", "org.h2.Driver");
        connect.put("Jdbc", jdbc);
        connect.put("CatalogContent", """
            <Schema name="JudgeCost">
              <Dimension name="Calendar">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="calendar"/>
                  <Level name="Year" column="year" type="Integer" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Product">
                <Hierarchy hasAll="true" primaryKey="id"><Table name="product"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Dimension name="Store">
                <Hierarchy hasAll="true" primaryKey="id"
                    defaultMember="[Store].[S1]"><Table name="store"/>
                  <Level name="Label" column="label" uniqueMembers="true"/>
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
        MondrianProperties props = MondrianProperties.instance();
        props.LevelPreCacheThreshold.set(previousPreCache);
        if (previousNativeQueryEngine == null) {
            props.remove("mondrian.native.queryEngine.enable");
        } else {
            props.setProperty(
                "mondrian.native.queryEngine.enable", previousNativeQueryEngine);
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

    private static final String NO_MEASURE =
        "SELECT {} ON 0, " + NON_EMPTY_CROSS_JOIN + " ON 1 FROM [Sales]";
    private static final String ONE_MEASURE =
        "SELECT {[Measures].[Quantity]} ON 0, " + NON_EMPTY_CROSS_JOIN
            + " ON 1 FROM [Sales]";

    /**
     * The shape the judging costs the most: the query displays no measure,
     * so nothing but the judging reads a cell. The native enumeration finds
     * the 8002 fact-backed crossings and the judging aggregates them once.
     */
    @Test void nativeCrossJoinWithoutADisplayedMeasure() {
        assertEquals(1,
            aggregations(measure("no measure, native", NO_MEASURE, true, SOLD_BY_S1)));
    }

    /**
     * Switched off, the native enumeration is the answer: not one cell is
     * read, and the 8004 crossings with a fact row anywhere come back - two
     * of them NULL, because the query reads S1 and those belong to S2.
     */
    @Test void nativeCrossJoinIsNotJudgedWhenSwitchedOff() {
        withoutJudging(() -> assertEquals(0, aggregations(
            measure("no measure, native, judging off", NO_MEASURE, true, CROSSINGS))));
    }

    /** A displayed measure reads the same cells: judging adds no statement. */
    @Test void nativeCrossJoinWithADisplayedMeasure() {
        assertEquals(1, aggregations(
            measure("measure displayed, native", ONE_MEASURE, true, SOLD_BY_S1)));
    }

    /** Switched off, the same cells are read - for two more crossings. */
    @Test void nativeCrossJoinWithADisplayedMeasureIsNotJudgedWhenSwitchedOff() {
        withoutJudging(() -> assertEquals(1, aggregations(measure(
            "measure displayed, native, judging off", ONE_MEASURE, true, CROSSINGS))));
    }

    /**
     * The interpreter reads each crossing once, at its own coordinate. The
     * candidate probe that used to precede the judging read them a second
     * time with Store widened to its All member, because S1 is the default
     * member rather than the All member.
     */
    @Test void interpretedCrossJoinReadsEachCrossingOnlyAtItsOwnCoordinate() {
        List<String> facts =
            measure("no measure, interpreted", NO_MEASURE, false, SOLD_BY_S1);
        assertEquals(1, aggregations(facts));
        assertTrue(facts.get(0).contains("\"store\".\"label\" = 'S1'"),
            "the fact must be read at the default member only: " + facts);
    }

    /**
     * Switched off, the probe is the answer and it is the widened one: the
     * fact is aggregated over every store and the two crossings S1 never
     * sold come back as NULL rows. That is the pre-judging behaviour, and
     * the reason the judging is on by default.
     */
    @Test void interpretedCrossJoinKeepsWidenedCandidatesWhenSwitchedOff() {
        withoutJudging(() -> {
            List<String> facts = measure(
                "no measure, interpreted, judging off", NO_MEASURE, false, CROSSINGS);
            assertEquals(1, aggregations(facts));
            assertFalse(facts.get(0).contains("\"store\".\"label\" = 'S1'"),
                "the probe widens Store to All: " + facts);
        });
    }

    private void withoutJudging(Runnable body) {
        MondrianProperties props = MondrianProperties.instance();
        boolean previous = props.NonEmptyCrossJoinJudgeCellsEnable.get();
        assertTrue(previous, "judging must be on by default");
        try {
            props.NonEmptyCrossJoinJudgeCellsEnable.set(false);
            body.run();
        } finally {
            props.NonEmptyCrossJoinJudgeCellsEnable.set(previous);
        }
    }

    private static int aggregations(List<String> factStatements) {
        return factStatements.size();
    }

    /**
     * Runs the query, checks it returns the expected crossings and reports
     * what it cost.
     *
     * @return the statements that aggregated the fact table
     */
    private List<String> measure(
        String label, String mdx, boolean nativeEnabled, int expectedTuples)
    {
        statements.clear();
        long started = System.nanoTime();
        Result result = execute(mdx, nativeEnabled);
        long elapsed = (System.nanoTime() - started) / 1000000L;
        assertEquals(expectedTuples, result.getAxes()[1].getPositions().size(), mdx);
        List<String> facts = statements.stream()
            .filter(sql -> sql.contains("sum(")).toList();
        System.out.println("NECJ-COST " + label
            + " factAggregations=" + facts.size()
            + " statements=" + statements.size()
            + " ms=" + elapsed);
        return facts;
    }

    private Result execute(String mdx, boolean nativeEnabled) {
        MondrianProperties props = MondrianProperties.instance();
        boolean previous = props.EnableNativeNonEmpty.get();
        RolapNativeRegistry registry =
            ((RolapSchema) connection.getSchema()).getNativeRegistry();
        boolean previousRegistry = registry.isEnabled();
        try {
            props.EnableNativeNonEmpty.set(nativeEnabled);
            registry.setEnabled(nativeEnabled);
            registry.flushAllNativeSetCache();
            return connection.execute(connection.parseQuery(mdx));
        } finally {
            props.EnableNativeNonEmpty.set(previous);
            SqlConstraintFactory.setNativeNonEmptyValue();
            registry.setEnabled(previousRegistry);
        }
    }
}

// End NonEmptyCrossJoinJudgeCostTest.java
