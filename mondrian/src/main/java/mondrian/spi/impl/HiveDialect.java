/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2017 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Hive database.
 *
 * @author Hongwei Fu
 * @since Jan 10, 2011
 */
public class HiveDialect extends JdbcDialectImpl {
    private static final int MAX_COLUMN_NAME_LENGTH = 128;

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            HiveDialect.class,
            DatabaseProduct.HIVE)
        {
            @Override protected boolean acceptsConnection(Connection connection) {
                return super.acceptsConnection(connection)
                    && !isDatabase(DatabaseProduct.IMPALA, connection);
            }
        };

    /**
     * Creates a HiveDialect.
     *
     * @param connection Connection
     *
     * @throws SQLException on error
     */
    public HiveDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override protected String deduceIdentifierQuoteString(
        DatabaseMetaData databaseMetaData)
    {
        return null;
    }

    @Override protected Set<List<Integer>> deduceSupportedResultSetStyles(
        DatabaseMetaData databaseMetaData)
    {
        // Hive don't support this, so just return an empty set.
        return Collections.emptySet();
    }

    @Override protected boolean deduceReadOnly(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.isReadOnly();
        } catch (SQLException e) {
            // Hive is read only (as of release 0.7)
            return true;
        }
    }

    @Override protected int deduceMaxColumnNameLength(DatabaseMetaData databaseMetaData) {
        try {
            return databaseMetaData.getMaxColumnNameLength();
        } catch (SQLException e) {
            return MAX_COLUMN_NAME_LENGTH;
        }
    }

    @Override public boolean allowsCompoundCountDistinct() {
        return true;
    }

    @Override public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override
    public boolean requiresOrderByAlias() {
        return true;
    }

    @Override
    public boolean allowsOrderByAlias() {
        return true;
    }

    @Override
    public boolean requiresGroupByAlias() {
        return false;
    }

    @Override public boolean requiresUnionOrderByExprToBeInSelectClause() {
        return false;
    }

    @Override public boolean requiresUnionOrderByOrdinal() {
        return false;
    }

    @Override public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return "select * from ("
            + generateInlineGeneric(
                columnNames, columnTypes, valueList, " from dual", false)
            + ") x limit " + valueList.size();
    }

    @Override protected void quoteDateLiteral(
        StringBuilder buf,
        String value,
        Date date)
    {
        // Hive doesn't support Date type; treat date as a string '2008-01-23'
        Util.singleQuoteString(value, buf);
    }

    @Override
    protected String generateOrderByNulls(
        String expr,
        boolean ascending,
        boolean collateNullsLast)
    {
        // In Hive, Null values are worth negative infinity.
        if (collateNullsLast) {
            if (ascending) {
                return "ISNULL(" + expr + ") ASC, " + expr + " ASC";
            } else {
                return expr + " DESC";
            }
        } else {
            if (ascending) {
                return expr + " ASC";
            } else {
                return "ISNULL(" + expr + ") DESC, " + expr + " DESC";
            }
        }
    }

    @Override public boolean allowsAs() {
        return false;
    }

    @Override public boolean allowsJoinOn() {
        return false;
    }

    @Override public void quoteTimestampLiteral(
        StringBuilder buf,
        String value)
    {
        try {
            Timestamp.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new NumberFormatException(
                "Illegal TIMESTAMP literal:  " + value);
        }
        buf.append("cast( ");
        Util.singleQuoteString(value, buf);
        buf.append(" as timestamp )");
    }
}

// End HiveDialect.java
