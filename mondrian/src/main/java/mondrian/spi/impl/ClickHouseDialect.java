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
    private static final int GROUPING_SETS_MIN_MAJOR = 22;
    private static final int GROUPING_SETS_MIN_MINOR = 1;

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
    public boolean supportsGroupingSets() {
        return isVersionAtLeast(GROUPING_SETS_MIN_MAJOR, GROUPING_SETS_MIN_MINOR);
    }

    @Override
    public boolean supportsMultiValueInExpr() {
        return true;
    }

    @Override
    public SqlStatement.Type getType(
        ResultSetMetaData metaData,
        int columnIndex) throws SQLException
    {
        final int jdbcType = metaData.getColumnType(columnIndex + 1);
        final String rawTypeName = metaData.getColumnTypeName(columnIndex + 1);
        final String typeName = unwrapClickHouseType(rawTypeName);

        // Some ClickHouse JDBC variants report LowCardinality(String) as
        // NUMERIC/DECIMAL. Type-name hint is more reliable for string-like
        // keys and prevents scientific-notation key serialization later.
        if (isStringLike(typeName)) {
            logTypeInfo(metaData, columnIndex, SqlStatement.Type.STRING);
            return SqlStatement.Type.STRING;
        }

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
            final SqlStatement.Type mapped = mapClickHouseOtherType(typeName);
            if (mapped != null) {
                logTypeInfo(metaData, columnIndex, mapped);
                return mapped;
            }
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(
                    "ClickHouseDialect: unrecognized Types.OTHER type '"
                    + rawTypeName
                    + "' (unwrapped='"
                    + typeName
                    + "'), falling back to default OBJECT mapping.");
            }
        }
        return super.getType(metaData, columnIndex);
    }

    static SqlStatement.Type mapClickHouseOtherType(String unwrappedTypeName) {
        if (isStringLike(unwrappedTypeName)) {
            return SqlStatement.Type.STRING;
        }
        if (isInt32Like(unwrappedTypeName)) {
            return SqlStatement.Type.INT;
        }
        if (isSignedInt64Like(unwrappedTypeName)) {
            return SqlStatement.Type.LONG;
        }
        if (isUnsignedInt64Like(unwrappedTypeName)) {
            // UInt64 may exceed Java long range; keep as OBJECT to avoid
            // silent overflow in JDBC getLong() paths.
            return SqlStatement.Type.OBJECT;
        }
        if (isBigIntegerLike(unwrappedTypeName)) {
            // Int128/UInt128/Int256/UInt256 are not representable as Java long.
            return SqlStatement.Type.OBJECT;
        }
        if (startsWithIgnoreCase(unwrappedTypeName, "Float")) {
            return SqlStatement.Type.DOUBLE;
        }
        if (startsWithIgnoreCase(unwrappedTypeName, "Decimal")) {
            return SqlStatement.Type.DECIMAL;
        }
        if (startsWithIgnoreCase(unwrappedTypeName, "Bool")) {
            return SqlStatement.Type.INT;
        }
        return null;
    }

    static String unwrapClickHouseType(String typeName) {
        if (typeName == null) {
            return "";
        }
        String inner = typeName.trim();
        while (true) {
            String unwrapped = unwrapClickHouseSingleArgWrapper(
                inner, "LowCardinality");
            if (unwrapped != null) {
                inner = unwrapped;
                continue;
            }
            unwrapped = unwrapClickHouseSingleArgWrapper(inner, "Nullable");
            if (unwrapped != null) {
                inner = unwrapped;
                continue;
            }
            unwrapped = unwrapClickHouseSecondArgWrapper(
                inner, "SimpleAggregateFunction");
            if (unwrapped != null) {
                inner = unwrapped;
                continue;
            }
            return inner;
        }
    }

    private static String unwrapClickHouseSingleArgWrapper(
        String typeName,
        String wrapperName)
    {
        final String prefix = wrapperName + "(";
        if (!startsWithIgnoreCase(typeName, prefix)
            || !typeName.endsWith(")"))
        {
            return null;
        }
        return typeName.substring(
            prefix.length(),
            typeName.length() - 1).trim();
    }

    private static String unwrapClickHouseSecondArgWrapper(
        String typeName,
        String wrapperName)
    {
        final String prefix = wrapperName + "(";
        if (!startsWithIgnoreCase(typeName, prefix)
            || !typeName.endsWith(")"))
        {
            return null;
        }
        final String args = typeName.substring(
            prefix.length(),
            typeName.length() - 1).trim();
        int depth = 0;
        for (int i = 0; i < args.length(); i++) {
            final char ch = args.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                if (depth == 0) {
                    return null;
                }
                depth--;
            } else if (ch == ',' && depth == 0) {
                return args.substring(i + 1).trim();
            }
        }
        return null;
    }

    private static boolean isInt32Like(String typeName) {
        return startsWithIgnoreCase(typeName, "Int8")
            || startsWithIgnoreCase(typeName, "Int16")
            || startsWithIgnoreCase(typeName, "Int32")
            || startsWithIgnoreCase(typeName, "UInt8")
            || startsWithIgnoreCase(typeName, "UInt16")
            || startsWithIgnoreCase(typeName, "UInt32");
    }

    private static boolean isSignedInt64Like(String typeName) {
        return startsWithIgnoreCase(typeName, "Int64");
    }

    private static boolean isUnsignedInt64Like(String typeName) {
        return startsWithIgnoreCase(typeName, "UInt64");
    }

    private static boolean isBigIntegerLike(String typeName) {
        return startsWithIgnoreCase(typeName, "Int128")
            || startsWithIgnoreCase(typeName, "UInt128")
            || startsWithIgnoreCase(typeName, "Int256")
            || startsWithIgnoreCase(typeName, "UInt256");
    }

    private static boolean isStringLike(String typeName) {
        return startsWithIgnoreCase(typeName, "String")
            || startsWithIgnoreCase(typeName, "FixedString")
            || startsWithIgnoreCase(typeName, "Enum8")
            || startsWithIgnoreCase(typeName, "Enum16")
            || startsWithIgnoreCase(typeName, "UUID")
            || startsWithIgnoreCase(typeName, "IPv4")
            || startsWithIgnoreCase(typeName, "IPv6");
    }

    private static boolean startsWithIgnoreCase(String value, String prefix) {
        if (value == null || prefix == null || value.length() < prefix.length()) {
            return false;
        }
        return value.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    private boolean isVersionAtLeast(int requiredMajor, int requiredMinor) {
        return isVersionAtLeast(productVersion, requiredMajor, requiredMinor);
    }

    static boolean isVersionAtLeast(
        String productVersion,
        int requiredMajor,
        int requiredMinor)
    {
        if (productVersion == null || productVersion.trim().isEmpty()) {
            return false;
        }
        final String[] parts = productVersion.split("[^0-9]+");
        final int major = parseVersionPart(parts, 0);
        final int minor = parseVersionPart(parts, 1);
        if (major > requiredMajor) {
            return true;
        }
        if (major < requiredMajor) {
            return false;
        }
        return minor >= requiredMinor;
    }

    private static int parseVersionPart(String[] parts, int index) {
        if (parts == null || index >= parts.length) {
            return 0;
        }
        final String part = parts[index];
        if (part == null || part.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(part);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
