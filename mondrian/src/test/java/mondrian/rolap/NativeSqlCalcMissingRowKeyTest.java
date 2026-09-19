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
import java.util.UUID;

import mondrian.calc.Calc;
import mondrian.calc.impl.GenericCalc;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * #89: a SUCCESS batch without the cell's rowKey stays null unless the
 * measure opts in to {@code nativeSql.fallbackOnMissingRowKey}, in the
 * grand-total context too. Each native measure's formula is 99, so a
 * fallback is always visible in the cell value.
 */
public class NativeSqlCalcMissingRowKeyTest {
    private static final String EMPTY_SQL =
        "SELECT SUM(qty) AS val FROM fact HAVING COUNT(*) < 0";

    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private boolean previousNativeSql;

    @BeforeEach void open() throws Exception {
        previousNativeSql = MondrianProperties.instance().NativeSqlEnable.get();
        MondrianProperties.instance().NativeSqlEnable.set(true);
        String jdbc = "jdbc:h2:mem:missing_row_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement sql = database.createStatement()) {
            sql.execute("CREATE TABLE mfr (id INT, name VARCHAR)");
            sql.execute("INSERT INTO mfr VALUES (1,'X'),(2,'Y')");
            sql.execute("CREATE TABLE fact (mfr_id INT, qty INT)");
            sql.execute("INSERT INTO fact VALUES (1,10),(2,25)");
        }
        Util.PropertyList props =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="MissingRowKey">
              <Dimension name="Mfr">
                <Hierarchy hasAll="true" allMemberName="All Mfrs" primaryKey="id"><Table name="mfr"/>
                  <Level name="Name" column="name" uniqueMembers="true"/>
                </Hierarchy>
              </Dimension>
              <Cube name="Sales"><Table name="fact"/>
                <DimensionUsage name="Mfr" source="Mfr" foreignKey="mfr_id"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                %s
                %s
                %s
                %s
              </Cube>
            </Schema>
            """.formatted(
                nativeMeasure("EmptyNative", EMPTY_SQL, false),
                nativeMeasure("EmptyOptIn", EMPTY_SQL, true),
                nativeMeasure(
                    "NullNative",
                    "SELECT SUM(qty) AS val FROM fact WHERE 1 = 0", true),
                nativeMeasure(
                    "ScalarNative", "SELECT SUM(qty) AS val FROM fact", false)));
        connection = mondrian.olap.DriverManager.getConnection(props, null);
    }

    @AfterEach void close() throws Exception {
        MondrianProperties.instance().NativeSqlEnable.set(previousNativeSql);
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

    private static String nativeMeasure(
        String name, String sql, boolean fallbackOnMissingRowKey)
    {
        return """
            <CalculatedMember name="%s" dimension="Measures">
              <Annotations>
                <Annotation name="nativeSql.enabled">true</Annotation>
                <Annotation name="nativeSql.fallbackOnMissingRowKey">%s</Annotation>
                <Annotation name="nativeSql.template">%s</Annotation>
              </Annotations>
              <Formula>99</Formula>
            </CalculatedMember>
            """.formatted(
                name, fallbackOnMissingRowKey, sql.replace("<", "&lt;"));
    }

    /**
     * Each measure appears twice under the same context, so the second
     * cell reuses the first cell's resolved batch (per-query cache).
     */
    @Test void grandTotalCellsUnderNonAllSlicer() {
        Result result = connection.execute(connection.parseQuery(
            "SELECT {[Measures].[EmptyNative], [Measures].[EmptyNative],"
            + " [Measures].[EmptyOptIn], [Measures].[EmptyOptIn],"
            + " [Measures].[NullNative], [Measures].[NullNative],"
            + " [Measures].[ScalarNative], [Measures].[ScalarNative]} ON 0"
            + " FROM [Sales] WHERE [Mfr].[X]"));
        for (int i = 0; i < 2; i++) {
            // Legitimately empty scalar result: null, not the formula.
            assertNull(value(result, i), "EmptyNative cell " + i);
            // Explicit opt-in: the formula fills the missing row.
            assertEquals(99d, number(result, 2 + i), "EmptyOptIn cell " + i);
            // A present SQL NULL row is a hit, not a miss.
            assertNull(value(result, 4 + i), "NullNative cell " + i);
            // The #89 symptom: a scalar value under a non-All slicer must
            // be keyed without the slicer member and read natively.
            assertEquals(35d, number(result, 6 + i), "ScalarNative cell " + i);
        }
    }

    /**
     * The same default miss evaluated directly: the first evaluation
     * resolves the template, the second takes the cached fast path;
     * neither may reach the MDX fallback.
     */
    @Test void emptyBatchStaysNullOnInitialAndCachedPath() {
        Query query = connection.parseQuery(
            "SELECT {[Measures].[EmptyNative]} ON 0 FROM [Sales]");
        RolapCalculatedMember member = (RolapCalculatedMember)
            query.getMeasuresMembers().stream()
                .filter(m -> m.getName().equals("EmptyNative"))
                .findFirst().orElseThrow();
        RolapEvaluatorRoot root =
            spy(new RolapEvaluatorRoot(query.getStatement()));
        CountingFallback fallback =
            new CountingFallback(member.getExpression());
        doReturn(fallback).when(root).getCompiled(
            member.getExpression(), true, null);
        Calc calc = NativeSqlCalc.create(
            member, root, NativeSqlConfig.fromMember(member));
        RolapEvaluator evaluator = new RolapEvaluator(root);

        assertNull(calc.evaluate(evaluator));
        assertNull(calc.evaluate(evaluator));
        assertEquals(0, fallback.calls);
    }

    private static Object value(Result result, int column) {
        return result.getCell(new int[] {column}).getValue();
    }

    private static double number(Result result, int column) {
        return ((Number) value(result, column)).doubleValue();
    }

    private static class CountingFallback extends GenericCalc {
        private int calls;

        CountingFallback(Exp exp) {
            super(exp, new Calc[0]);
        }

        @Override public Object evaluate(Evaluator evaluator) {
            calls++;
            return 99;
        }
    }
}

// End NativeSqlCalcMissingRowKeyTest.java
