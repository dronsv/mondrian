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

import mondrian.rolap.nativesql.NativeSqlFingerprint;
import mondrian.spi.Dialect;
import mondrian.spi.DialectManager;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A cell's Java class is client-visible: the XMLA serializer derives
 * {@code xsi:type} from it and nothing else (see
 * {@code XmlaCellValueTypeTest}). Paths that materialize cells outside the
 * segment loader must therefore land on the same class the segment loader
 * would have stored, or the same measure changes its XMLA type depending on
 * which path happened to serve the cell.
 *
 * <p>Pins that class at the two native materialization sites: the NQE
 * prefetch ({@code NativeQuerySqlGenerator.NqeBatchWork}) for stored
 * measures, and {@code NativeSqlCalc.parseResultSetWithGroupingFlags} for a
 * native-SQL template measure under rollupAxes.
 */
public class NativeCellValueTypeTest {

    /**
     * {@code SUM} over a {@code DECIMAL} measure column, which every JDBC
     * driver returns as {@link BigDecimal} from {@code getObject}. The
     * segment path reads it as a {@code double} (the dialect maps a scaled
     * DECIMAL to {@link SqlStatement.Type#DOUBLE}), so the prefetch must
     * too.
     */
    @Test
    public void prefetchedStoredMeasureIsDoubleNotBigDecimal()
        throws Exception
    {
        withGroupedSales((dataSource, rs) -> {
            final List<Object[]> rows = consumePrefetch(dataSource, rs, 1);

            assertEquals(1, rows.size());
            final Object[] row = rows.get(0);
            // The key part still goes through getObject, unchanged.
            assertEquals(
                NativeQuerySqlGenerator.encodeProjectedKey(
                    Collections.singletonList("mfr")),
                row[0]);
            assertInstanceOf(Double.class, row[1]);
            assertEquals(68014599.15d, (Double) row[1], 0d);
        });
    }

    /**
     * The rollupAxes reader of a native-SQL template measure. Its
     * non-rollup sibling {@code parseResultSet} has always used
     * {@code getDouble}; this one used {@code getObject}, so the same
     * template measure changed class with the shape of the query.
     */
    @Test
    public void nativeTemplateMeasureIsDoubleNotBigDecimal()
        throws Exception
    {
        withTemplateShapedSales(rs -> {
            final Map<String, Object> values =
                NativeSqlCalc.parseResultSetWithGroupingFlags(
                    rs,
                    Arrays.asList((NativeSqlCalc.AxisBinding) null));

            assertEquals(1, values.size());
            final Object value = values.values().iterator().next();
            assertInstanceOf(Double.class, value);
            assertEquals(68014599.15d, (Double) value, 0d);
        });
    }

    /** A NULL measure stays null rather than becoming 0.0. */
    @Test
    public void prefetchedNullMeasureStaysNull() throws Exception {
        withNullSales((dataSource, rs) -> {
            final List<Object[]> rows = consumePrefetch(dataSource, rs, 1);
            assertEquals(1, rows.size());
            assertNull(rows.get(0)[1]);
        });
    }

    /**
     * A column the dialect cannot map to a Java primitive stays
     * {@link SqlStatement.Type#OBJECT}. ClickHouse maps {@code UInt64}
     * there deliberately (it can exceed Java long range), and a
     * distinct-count measure over one arrives as {@link BigInteger}.
     * {@code SegmentLoader.processData} coerces such a value to a double
     * for a numeric measure, so the native reader must as well — otherwise
     * a distinct-count measure flips from {@code xsd:double} to
     * {@code xsd:int}.
     */
    @Test
    public void objectTypedNumberIsCoercedLikeTheSegmentLoader()
        throws Exception
    {
        final Object value = SqlStatement.readMeasureValue(
            singleValueResultSet(BigInteger.valueOf(20)),
            1,
            SqlStatement.Type.OBJECT);
        assertInstanceOf(Double.class, value);
        assertEquals(20d, (Double) value, 0d);
    }

    /**
     * ...but a {@link BigDecimal} under {@code OBJECT} is left alone,
     * because that is what {@code SegmentLoader.processData} does
     * (PDI-16761: the cast costs precision). A dialect that wants the cast
     * maps the column to {@link SqlStatement.Type#DECIMAL} instead, which
     * does convert.
     */
    @Test
    public void objectTypedBigDecimalIsLeftAloneButDecimalTypedIsNot()
        throws Exception
    {
        final BigDecimal raw = new BigDecimal("68014599.15");

        assertEquals(
            raw,
            SqlStatement.readMeasureValue(
                singleValueResultSet(raw), 1, SqlStatement.Type.OBJECT));

        final Object converted = SqlStatement.readMeasureValue(
            singleValueResultSet(raw), 1, SqlStatement.Type.DECIMAL);
        assertInstanceOf(Double.class, converted);
        assertEquals(68014599.15d, (Double) converted, 0d);
    }

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    private interface SourcedResultSetBody {
        void accept(JdbcDataSource dataSource, ResultSet rs) throws Exception;
    }

    private interface ResultSetBody {
        void accept(ResultSet rs) throws Exception;
    }

    private List<Object[]> consumePrefetch(
        JdbcDataSource dataSource, ResultSet rs, int measureCount)
        throws Exception
    {
        final Dialect dialect = DialectManager.createDialect(
            dataSource, rs.getStatement().getConnection());
        final NativeQuerySqlGenerator.NqeBatchWork work =
            new NativeQuerySqlGenerator.NqeBatchWork(
                NativeSqlFingerprint.of(
                    "select", Collections.emptyList(), dataSource, null),
                dataSource,
                "select",
                measureCount,
                dialect);
        @SuppressWarnings("unchecked")
        final List<Object[]> rows = (List<Object[]>) work.consume(rs);
        return rows;
    }

    /** {@code k0, sum(measure)} — the shape NQE's stored SQL produces. */
    private void withGroupedSales(SourcedResultSetBody body) throws Exception {
        withSales(
            "select mfr as k0, sum(sales_rub) as v0"
            + " from sales group by mfr",
            body);
    }

    /** Same, with every contributing row NULL. */
    private void withNullSales(SourcedResultSetBody body) throws Exception {
        withSales(
            "select mfr as k0, sum(cast(null as decimal(18,2))) as v0"
            + " from sales group by mfr",
            body);
    }

    /**
     * {@code k0, k0_isAll, val} — the shape a native-SQL template produces
     * under {@code ${axisCubeSelectFlags}}.
     */
    private void withTemplateShapedSales(ResultSetBody body) throws Exception {
        withSales(
            "select mfr as k0, 0 as k0_isAll, sum(sales_rub) as val"
            + " from sales group by mfr",
            (dataSource, rs) -> body.accept(rs));
    }

    private void withSales(String query, SourcedResultSetBody body)
        throws Exception
    {
        final JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(
            "jdbc:h2:mem:" + UUID.randomUUID()
            + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement())
        {
            statement.execute(
                "create table sales("
                + "mfr varchar(32), sales_rub decimal(18,2))");
            statement.execute(
                "insert into sales values"
                + " ('mfr', 34007299.57), ('mfr', 34007299.58)");
            try (ResultSet rs = statement.executeQuery(query)) {
                body.accept(dataSource, rs);
            }
        }
    }

    /** A one-column, one-row result set already positioned on its row. */
    private ResultSet singleValueResultSet(final Object value) {
        final ResultSetMetaData metaData = (ResultSetMetaData)
            Proxy.newProxyInstance(
                ResultSetMetaData.class.getClassLoader(),
                new Class[] { ResultSetMetaData.class },
                (proxy, method, args) ->
                    "getColumnName".equals(method.getName())
                        ? "v0"
                        : defaultValue(method.getReturnType()));

        return (ResultSet) Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(),
            new Class[] { ResultSet.class },
            new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    switch (method.getName()) {
                    case "getObject":
                        return value;
                    case "getBigDecimal":
                        return value;
                    case "wasNull":
                        return value == null;
                    case "getMetaData":
                        return metaData;
                    default:
                        return defaultValue(method.getReturnType());
                    }
                }
            });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == double.class) {
            return 0d;
        }
        if (type == float.class) {
            return 0f;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        return null;
    }
}
