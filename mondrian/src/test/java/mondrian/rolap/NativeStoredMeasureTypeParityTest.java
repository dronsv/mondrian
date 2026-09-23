/*
 // This software is subject to the terms of the Eclipse Public License v1.0
 // Agreement, available at http://www.eclipse.org/legal/epl-v10.html.
 // Copyright (C) 2026 Hitachi Vantara and others
 // All Rights Reserved.
 */
package mondrian.rolap;

import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.UUID;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Result;
import mondrian.olap.Util;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Native materialization must honor each stored measure's numeric datatype. */
class NativeStoredMeasureTypeParityTest {
    private static final String NQE = "mondrian.native.queryEngine.enable";
    private static final BigInteger EXACT = new BigInteger("9007199254740993");

    @ParameterizedTest
    @ValueSource(strings = {"min", "max"})
    void nonnumericObjectMeasureKeepsItsExactInteger(String aggregator)
        throws Exception
    {
        withFixture(aggregator, EXACT, "JAVA_OBJECT", Types.JAVA_OBJECT, connection -> {
            assertEquals(EXACT, value(connection, "Exact", false));
            Object nativeValue = value(connection, "Exact", true);
            assertInstanceOf(BigInteger.class, nativeValue);
            assertEquals(EXACT, nativeValue);
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sameSqlKeepsEachMeasuresDatatypeAcrossCachedReads(boolean numericFirst)
        throws Exception
    {
        withFixture("max", EXACT, "JAVA_OBJECT", Types.JAVA_OBJECT, connection -> {
            String[] measures = numericFirst
                ? new String[] {"Numeric", "Exact", "Numeric", "Exact"}
                : new String[] {"Exact", "Numeric", "Exact", "Numeric"};
            for (String measure : measures) {
                Object actual = value(connection, measure, true);
                if (measure.equals("Numeric")) {
                    assertInstanceOf(Double.class, actual);
                    assertEquals(9007199254740992d, actual);
                } else {
                    assertInstanceOf(BigInteger.class, actual);
                    assertEquals(EXACT, actual);
                }
            }
        });
    }

    @Test
    void numericStringMeasureUsesTheSegmentNumericCoercion() throws Exception {
        withFixture("max", "2.50", "VARCHAR(32)", Types.VARCHAR, connection -> {
            assertEquals(2.5d, value(connection, "Numeric", false));
            assertEquals(2.5d, value(connection, "Numeric", true));
            assertEquals("2.50", value(connection, "Exact", true));
        });
    }

    @Test
    void numericObjectBytesUseTheSegmentNumericCoercion() throws Exception {
        withFixture("max", "2.50".getBytes(StandardCharsets.UTF_8), "JAVA_OBJECT",
            Types.JAVA_OBJECT, connection -> {
                assertEquals(2.5d, value(connection, "Numeric", false));
                assertEquals(2.5d, value(connection, "Numeric", true));
            });
    }

    private interface FixtureBody {
        void run(mondrian.olap.Connection connection) throws Exception;
    }

    private static void withFixture(
        String aggregator, Object raw, String sqlType, int jdbcType, FixtureBody body)
        throws Exception
    {
        MondrianProperties properties = MondrianProperties.instance();
        String previous = properties.getProperty(NQE);
        mondrian.olap.Connection connection = open(aggregator, raw, sqlType, jdbcType);
        try {
            body.run(connection);
        } finally {
            connection.close();
            if (previous == null) {
                properties.remove(NQE);
            } else {
                properties.setProperty(NQE, previous);
            }
        }
    }

    private static Object value(
        mondrian.olap.Connection connection, String measure, boolean nqe)
    {
        MondrianProperties.instance().setProperty(NQE, Boolean.toString(nqe));
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration config = context.getConfiguration();
        String loggerName = NativeQueryEngine.class.getName();
        LoggerConfig previous = config.getLoggers().get(loggerName);
        StringWriter log = new StringWriter();
        Appender appender = Util.makeAppender("nativeMeasureType", log, "%m%n");
        appender.start();
        LoggerConfig capture = new LoggerConfig(loggerName, Level.INFO, false);
        capture.addAppender(appender, Level.INFO, null);
        config.removeLogger(loggerName);
        config.addLogger(loggerName, capture);
        context.updateLoggers();
        try {
            Result result = connection.execute(connection.parseQuery(
                "SELECT {[Measures].[" + measure + "]} ON COLUMNS FROM [Sales]"));
            if (nqe) {
                assertTrue(log.toString().contains("successfully populated 1 cells"),
                    "The native materializer must serve the cell: " + log);
            }
            return result.getCell(new int[] {0}).getValue();
        } finally {
            config.removeLogger(loggerName);
            if (previous != null) {
                config.addLogger(loggerName, previous);
            }
            context.updateLoggers();
            appender.stop();
        }
    }

    private static mondrian.olap.Connection open(
        String aggregator, Object raw, String sqlType, int jdbcType)
        throws Exception
    {
        String jdbc = "jdbc:h2:mem:nqe_measure_type_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false";
        try (java.sql.Connection db = java.sql.DriverManager.getConnection(jdbc, "sa", "");
             Statement sql = db.createStatement())
        {
            // JAVA_OBJECT exposes an exact BigInteger through JDBC OBJECT,
            // the same materialization boundary as ClickHouse UInt64.
            sql.execute("CREATE TABLE fact (amount " + sqlType + ")");
            try (PreparedStatement insert = db.prepareStatement("INSERT INTO fact VALUES (?)")) {
                insert.setObject(1, raw, jdbcType);
                insert.executeUpdate();
            }
            Util.PropertyList props = Util.parseConnectString("Provider=mondrian;JdbcPassword=;");
            props.put("JdbcUser", "sa");
            props.put("JdbcDrivers", "org.h2.Driver");
            props.put("Jdbc", jdbc);
            props.put("CatalogContent", """
                <Schema name="NativeMeasureTypes">
                  <Cube name="Sales"><Table name="fact"/>
                    <Measure name="Exact" column="amount" aggregator="%s" datatype="String"/>
                    <Measure name="Numeric" column="amount" aggregator="%s" datatype="Numeric"/>
                  </Cube>
                </Schema>
                """.formatted(aggregator, aggregator));
            return mondrian.olap.DriverManager.getConnection(props, null);
        }
    }
}
