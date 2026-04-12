/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.spi.impl;

import mondrian.olap.Util;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Apache Derby database.
 *
 * @author jhyde
 * @since Nov 23, 2008
 */
public class DerbyDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            DerbyDialect.class,
            DatabaseProduct.DERBY);

    /**
     * Creates a DerbyDialect.
     *
     * @param connection Connection
     */
    public DerbyDialect(Connection connection) throws SQLException {
        super(connection);
    }

    @Override protected void quoteDateLiteral(
        StringBuilder buf,
        String value,
        Date date)
    {
        // Derby accepts DATE('2008-01-23') but not SQL:2003 format.
        buf.append("DATE(");
        Util.singleQuoteString(value, buf);
        buf.append(")");
    }

    @Override public boolean requiresAliasForFromQuery() {
        return true;
    }

    @Override public boolean allowsMultipleCountDistinct() {
        // Derby allows at most one distinct-count per query.
        return false;
    }

    @Override public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineForAnsi(
            "t", columnNames, columnTypes, valueList, true);
    }

    @Override public boolean supportsGroupByExpressions() {
        return false;
    }
}

// End DerbyDialect.java
