/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mondrian.olap.MondrianDef;
import mondrian.spi.impl.JdbcDialectImpl;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlQueryDistinctFromDialectTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void dialectCapabilityPreservesDecorationsAndPredicates(boolean allowsFromQuery)
        throws Exception
    {
        String jdbc = "jdbc:h2:mem:distinct_dialect_"
            + UUID.randomUUID().toString().replace("-", "")
            + ";DATABASE_TO_UPPER=false";
        try (Connection database = DriverManager.getConnection(jdbc, "sa", "");
             Statement statement = database.createStatement())
        {
            statement.execute("CREATE SCHEMA reporting");
            statement.execute("CREATE TABLE reporting.product"
                + " (category VARCHAR, caption VARCHAR, sort_order INT, enabled INT)");
            statement.execute("INSERT INTO reporting.product VALUES"
                + " ('A','Alpha',1,1),('A','Alpha',1,1),"
                + " ('B','Hidden',2,0),('C',NULL,3,1)");

            // Exercise the real SQL builder with only the dialect capability
            // varied; H2 executes both forms to check the fallback payload.
            JdbcDialectImpl dialect = new JdbcDialectImpl(database) {
                @Override public boolean allowsFromQuery() {
                    return allowsFromQuery;
                }
            };
            SqlQuery query = new SqlQuery(dialect, false);
            MondrianDef.Table table = new MondrianDef.Table();
            table.schema = "reporting";
            table.name = "product";
            table.alias = "p";
            boolean projected = query.addFromTableDistinct(
                table, null, List.of("category", "caption", "sort_order", "enabled"));
            if (!projected) {
                assertTrue(query.addFrom(table, null, false));
            }
            assertEquals(allowsFromQuery, projected,
                "A dialect that forbids derived FROM tables must use the bare table");

            for (String column : List.of("category", "caption", "sort_order")) {
                query.addSelectGroupBy(dialect.quoteIdentifier("p", column), null);
            }
            query.addWhere(dialect.quoteIdentifier("p", "enabled") + " = 1");
            query.addOrderBy(dialect.quoteIdentifier("p", "sort_order"),
                "c2", true, false, false, false);

            String sql = query.toString();
            assertEquals(allowsFromQuery, sql.contains("(select distinct "), sql);
            assertTrue(sql.contains("\"reporting\".\"product\""), sql);
            assertEquals(sql, query.toString(), "Rendering must stay repeatable");
            List<String> rows = new ArrayList<>();
            try (ResultSet result = statement.executeQuery(sql)) {
                while (result.next()) {
                    rows.add(result.getString(1) + "/" + result.getString(2)
                        + "/" + result.getInt(3));
                }
            }
            assertEquals(List.of("A/Alpha/1", "C/null/3"), rows, sql);
        }
    }
}
