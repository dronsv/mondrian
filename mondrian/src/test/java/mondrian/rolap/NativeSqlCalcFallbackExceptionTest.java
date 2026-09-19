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
import java.util.UUID;
import java.util.stream.Stream;

import mondrian.calc.Calc;
import mondrian.calc.impl.GenericCalc;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.Query;
import mondrian.olap.QueryCanceledException;
import mondrian.olap.QueryTimeoutException;
import mondrian.olap.Util;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/** Cached missing-row fallback must propagate its first exception without retry. */
public class NativeSqlCalcFallbackExceptionTest {
    private static final String SQL =
        "SELECT qty AS val FROM fact WHERE 1 = 0";

    private java.sql.Connection database;
    private mondrian.olap.Connection connection;
    private RolapEvaluator evaluator;
    private Calc nativeCalc;
    private ThrowOnceFallback fallback;

    @BeforeEach void open() throws Exception {
        String jdbc = "jdbc:h2:mem:fallback_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false";
        database = DriverManager.getConnection(jdbc, "sa", "");
        try (Statement statement = database.createStatement()) {
            statement.execute("CREATE TABLE fact (qty INT)");
            statement.execute("SET QUERY_STATISTICS TRUE");
        }
        Util.PropertyList props =
            Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
        props.put("JdbcUser", "sa");
        props.put("JdbcDrivers", "org.h2.Driver");
        props.put("Jdbc", jdbc);
        props.put("CatalogContent", """
            <Schema name="FallbackException">
              <Cube name="Sales"><Table name="fact"/>
                <Measure name="Quantity" column="qty" aggregator="sum"/>
                <CalculatedMember name="NativeMissing" dimension="Measures">
                  <Annotations>
                    <Annotation name="nativeSql.enabled">true</Annotation>
                    <Annotation name="nativeSql.fallbackOnMissingRowKey">true</Annotation>
                    <Annotation name="nativeSql.template">%s</Annotation>
                  </Annotations>
                  <Formula>17</Formula>
                </CalculatedMember>
              </Cube>
            </Schema>
            """.formatted(SQL));
        connection = mondrian.olap.DriverManager.getConnection(props, null);
        Query query = connection.parseQuery(
            "SELECT {[Measures].[NativeMissing]} ON 0 FROM [Sales]");
        RolapCalculatedMember member = (RolapCalculatedMember)
            query.getMeasuresMembers().stream()
                .filter(m -> m.getName().equals("NativeMissing"))
                .findFirst().orElseThrow();
        RolapEvaluatorRoot root =
            spy(new RolapEvaluatorRoot(query.getStatement()));
        evaluator = new RolapEvaluator(root);
        fallback = new ThrowOnceFallback(member.getExpression());
        // Only the fallback body is substituted. SQL execution, template
        // resolution and population/reuse of the statement cache stay real.
        doReturn(fallback).when(root).getCompiled(
            member.getExpression(), true, null);
        nativeCalc = NativeSqlCalc.create(
            member, root, NativeSqlConfig.fromMember(member));
    }

    @AfterEach void close() throws Exception {
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

    static Stream<RuntimeException> fallbackFailures() {
        return Stream.of(
            new IllegalStateException("MDX fallback failed"),
            new QueryCanceledException("MDX fallback canceled"),
            new QueryTimeoutException("MDX fallback timed out"));
    }

    @ParameterizedTest
    @MethodSource("fallbackFailures")
    void cachedMissingRowPropagatesFallbackFailureOnce(
        RuntimeException failure) throws Exception
    {
        // Populate a successful, empty native batch through the real H2
        // path. The next evaluation must use its cached row-key lookup.
        assertEquals(17, nativeCalc.evaluate(evaluator));
        assertEquals(1, fallback.calls);
        assertEquals(1, nativeSqlExecutions());

        fallback.nextFailure = failure;
        assertSame(failure, assertThrows(
            RuntimeException.class, () -> nativeCalc.evaluate(evaluator)));
        assertEquals(2, fallback.calls,
            "one warm-up call and exactly one failing cached fallback");
        assertEquals(1, nativeSqlExecutions());
    }

    private int nativeSqlExecutions() throws Exception {
        try (Statement statement = database.createStatement();
             ResultSet result = statement.executeQuery(
                 "SELECT EXECUTION_COUNT FROM INFORMATION_SCHEMA.QUERY_STATISTICS"
                 + " WHERE SQL_STATEMENT = '" + SQL + "'"))
        {
            assertTrue(result.next(), "native template must execute on H2");
            return result.getInt(1);
        }
    }

    private static class ThrowOnceFallback extends GenericCalc {
        private int calls;
        private RuntimeException nextFailure;

        ThrowOnceFallback(Exp exp) {
            super(exp, new Calc[0]);
        }

        @Override public Object evaluate(Evaluator evaluator) {
            calls++;
            if (nextFailure != null) {
                RuntimeException failure = nextFailure;
                nextFailure = null;
                throw failure;
            }
            return 17;
        }
    }
}
