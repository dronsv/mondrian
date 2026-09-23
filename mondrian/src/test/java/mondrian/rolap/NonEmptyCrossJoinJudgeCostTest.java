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
import mondrian.olap.ResultBase;
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
    /**
     * Years in the fixture. The judging is per crossing, so this is the
     * knob that scales its cost: {@code -Dmondrian.test.necj.years=370}
     * makes the shape 740,370 candidates, the size of the pivot #97 was
     * reported on. Widening this dimension rather than Product keeps the
     * enumeration's own IN lists small, so what grows is the judging and
     * not H2's evaluation of a 740,000-key predicate.
     */
    private static final int DEFAULT_YEARS = 4;
    private static final int YEARS =
        Integer.getInteger("mondrian.test.necj.years", DEFAULT_YEARS);
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
    private int judgePasses;
    private long judgedCrossings;

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
                    insert.setInt(3, product == PRODUCTS && year > YEARS - 2 ? 2 : 1);
                    insert.setInt(4, 1);
                    insert.addBatch();
                }
            }
            insert.executeBatch();
        }
        try (Statement sql = database.createStatement()) {
            // Without these the fixture measures H2 rather than Mondrian:
            // a segment load constrains the dimension key by a list as long
            // as the level, and H2 walks such a list per row when it has no
            // index to look it up in.
            sql.execute("CREATE INDEX ix_calendar_id ON calendar (id)");
            sql.execute("CREATE INDEX ix_calendar_year ON calendar (year)");
            sql.execute("CREATE INDEX ix_product_id ON product (id)");
            sql.execute("CREATE INDEX ix_product_name ON product (name)");
            sql.execute("CREATE INDEX ix_store_id ON store (id)");
            sql.execute("CREATE INDEX ix_fact_product ON fact (product_id)");
            sql.execute("CREATE INDEX ix_fact_calendar ON fact (calendar_id)");
            sql.execute("CREATE INDEX ix_fact_store ON fact (store_id)");
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
        assertJudgedTwice();
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
        assertJudgedTwice();
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
        assertJudgedTwice();
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

    /**
     * The judging passes over the candidates exactly twice, whatever else
     * the query does, and each pass sees every candidate.
     *
     * <p>RolapResult evaluates one axis three times: a batch-load pass
     * whose cells are not loaded yet - an unloaded cell reads back as a
     * non-null placeholder, so this pass keeps every candidate and exists
     * only to register the requests the next phase loads - then the pass
     * that answers with those cells in hand, then the axis-construction
     * pass. NonEmptyCrossJoinFunDef holds the answer in the query's
     * expression result cache, which stores a result as valid only when
     * the cell reader was clean and missed nothing while it ran, so the
     * load pass's answer is dropped with its phase and the third pass is
     * served from the memo. A counter, not a clock: this pins the passes,
     * not how long one takes.
     */
    private void assertJudgedTwice() {
        if (YEARS != DEFAULT_YEARS) {
            // The scale knob is for measuring, not for pinning: past the
            // cell-request quantum one load pass becomes several, each
            // reaching further into the candidates than the last. The
            // counters are still printed, which is what a measuring run
            // reads.
            return;
        }
        assertEquals(2, judgePasses,
            "the load pass and the answering pass, and no repeat of either");
        assertEquals(2L * CROSSINGS, judgedCrossings,
            "each pass judges every candidate the enumeration produced");
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
        judgePasses = ((ResultBase) result).getExecution().getCrossJoinJudgePasses();
        judgedCrossings =
            ((ResultBase) result).getExecution().getCrossJoinJudgedCrossings();
        System.out.println("NECJ-COST " + label
            + " factAggregations=" + facts.size()
            + " statements=" + statements.size()
            + " judgePasses=" + judgePasses
            + " judgedCrossings=" + judgedCrossings
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
