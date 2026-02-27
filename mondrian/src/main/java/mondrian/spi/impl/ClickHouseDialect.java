/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2021-2025 Sergei Semenkov
 */

package mondrian.spi.impl;

import mondrian.rolap.SqlStatement;
import mondrian.olap.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Implementation of {@link mondrian.spi.Dialect} for ClickHouse
 */
public class ClickHouseDialect extends JdbcDialectImpl {
    private static final Log LOGGER = LogFactory.getLog(ClickHouseDialect.class);

    public static final JdbcDialectFactory FACTORY =
            new JdbcDialectFactory(
                    ClickHouseDialect.class,
                    DatabaseProduct.CLICKHOUSE);

    /**
     * Creates a Db2OldAs400Dialect.
     *
     * @param connection Connection
     */
    public ClickHouseDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean requiresDrillthroughMaxRowsInLimit() {
        return true;
    }

    public void quoteStringLiteral(
            StringBuilder buf,
            String s)
    {
        buf.append('\'');

        String s0 = Util.replace(s, "\\", "\\\\");
        s0 = Util.replace(s0, "'", "\\'");
        buf.append(s0);

        buf.append('\'');
    }

    @Override
    public String getDefaultUnion() {
        return "union distinct";
    }

    @Override
    public SqlStatement.Type getType(
        ResultSetMetaData metaData,
        int columnIndex) throws SQLException
    {
        final int jdbcType = metaData.getColumnType(columnIndex + 1);

        if (jdbcType == Types.TINYINT
            || jdbcType == Types.SMALLINT
            || jdbcType == Types.INTEGER)
        {
            logTypeInfo(metaData, columnIndex, SqlStatement.Type.INT);
            return SqlStatement.Type.INT;
        }
        if (jdbcType == Types.BIGINT) {
            // Keep ClickHouse BIGINT keys as integer values, not DOUBLE.
            logTypeInfo(metaData, columnIndex, SqlStatement.Type.LONG);
            return SqlStatement.Type.LONG;
        }
        if (jdbcType == Types.NUMERIC || jdbcType == Types.DECIMAL) {
            final int precision = metaData.getPrecision(columnIndex + 1);
            final int scale = metaData.getScale(columnIndex + 1);
            if (scale == 0 && precision <= 9) {
                logTypeInfo(metaData, columnIndex, SqlStatement.Type.INT);
                return SqlStatement.Type.INT;
            }
            if (scale == 0 && precision <= 18) {
                logTypeInfo(metaData, columnIndex, SqlStatement.Type.LONG);
                return SqlStatement.Type.LONG;
            }
            return super.getType(metaData, columnIndex);
        }
        if (jdbcType == Types.OTHER) {
            final String typeName = unwrapClickHouseType(
                metaData.getColumnTypeName(columnIndex + 1));
            if (isStringLike(typeName)) {
                logTypeInfo(metaData, columnIndex, SqlStatement.Type.STRING);
                return SqlStatement.Type.STRING;
            }
            if (isInt32Like(typeName)) {
                logTypeInfo(metaData, columnIndex, SqlStatement.Type.INT);
                return SqlStatement.Type.INT;
            }
            if (isInt64Like(typeName)) {
                logTypeInfo(metaData, columnIndex, SqlStatement.Type.LONG);
                return SqlStatement.Type.LONG;
            }
            if (typeName.startsWith("Float")) {
                logTypeInfo(metaData, columnIndex, SqlStatement.Type.DOUBLE);
                return SqlStatement.Type.DOUBLE;
            }
            if (typeName.startsWith("Decimal")) {
                logTypeInfo(metaData, columnIndex, SqlStatement.Type.DECIMAL);
                return SqlStatement.Type.DECIMAL;
            }
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(
                    "ClickHouseDialect: unrecognized Types.OTHER type '"
                    + metaData.getColumnTypeName(columnIndex + 1)
                    + "' (unwrapped='"
                    + typeName
                    + "'), falling back to default OBJECT mapping.");
            }
        }
        return super.getType(metaData, columnIndex);
    }

    private static String unwrapClickHouseType(String typeName) {
        if (typeName == null) {
            return "";
        }
        String inner = typeName.trim();
        while (inner.startsWith("LowCardinality(")
            || inner.startsWith("Nullable("))
        {
            final int open = inner.indexOf('(');
            final int close = inner.lastIndexOf(')');
            if (open < 0 || close <= open) {
                break;
            }
            inner = inner.substring(open + 1, close).trim();
        }
        return inner;
    }

    private static boolean isInt32Like(String typeName) {
        return typeName.startsWith("Int8")
            || typeName.startsWith("Int16")
            || typeName.startsWith("Int32")
            || typeName.startsWith("UInt8")
            || typeName.startsWith("UInt16")
            || typeName.startsWith("UInt32");
    }

    private static boolean isInt64Like(String typeName) {
        return typeName.startsWith("Int64")
            || typeName.startsWith("UInt64");
    }

    private static boolean isStringLike(String typeName) {
        return typeName.startsWith("String")
            || typeName.startsWith("FixedString")
            || typeName.startsWith("Enum8")
            || typeName.startsWith("Enum16")
            || typeName.startsWith("UUID");
    }
}
