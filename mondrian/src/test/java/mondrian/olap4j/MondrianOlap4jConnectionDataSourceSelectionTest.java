/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.olap4j;

import junit.framework.TestCase;
import mondrian.olap.Util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MondrianOlap4jConnectionDataSourceSelectionTest extends TestCase {

    public void testSelectByDataSourceName() {
        List<Map<String, Object>> databases = new ArrayList<Map<String, Object>>();
        databases.add(database("Konfet", "Provider=mondrian;Jdbc=jdbc:clickhouse://host/default"));
        databases.add(database("AlcoholAll", "Provider=mondrian;Jdbc=jdbc:clickhouse://host/alcohol_all"));

        Util.PropertyList connectInfo =
            Util.parseConnectString("DataSource=AlcoholAll;Catalog=/WEB-INF/schema/alcohol.xml");

        Map<String, Object> selected =
            MondrianOlap4jConnection.selectDatabaseProperties(databases, connectInfo);

        assertEquals("AlcoholAll", selected.get("DataSourceName"));
    }

    public void testSelectBySanitizedDataSourceInfo() {
        List<Map<String, Object>> databases = new ArrayList<Map<String, Object>>();
        databases.add(
            database(
                "Konfet",
                "Provider=mondrian;Flavor=konfet;Jdbc=jdbc:clickhouse://host/default;JdbcUser=user;JdbcPassword=secret"));
        databases.add(
            database(
                "AlcoholAll",
                "Provider=mondrian;Flavor=alcohol;Jdbc=jdbc:clickhouse://host/alcohol_all;JdbcUser=user;JdbcPassword=secret"));

        Util.PropertyList connectInfo = new Util.PropertyList();
        connectInfo.put("DataSourceInfo", "Provider=mondrian;Flavor=alcohol");

        Map<String, Object> selected =
            MondrianOlap4jConnection.selectDatabaseProperties(databases, connectInfo);

        assertEquals("AlcoholAll", selected.get("DataSourceName"));
    }

    public void testFallbackToFirstWhenNoMatch() {
        List<Map<String, Object>> databases = new ArrayList<Map<String, Object>>();
        databases.add(database("First", "Provider=mondrian;Jdbc=jdbc:clickhouse://host/first"));
        databases.add(database("Second", "Provider=mondrian;Jdbc=jdbc:clickhouse://host/second"));

        Util.PropertyList connectInfo = Util.parseConnectString("Catalog=/WEB-INF/schema/unknown.xml");

        Map<String, Object> selected =
            MondrianOlap4jConnection.selectDatabaseProperties(databases, connectInfo);

        assertEquals("First", selected.get("DataSourceName"));
    }

    private Map<String, Object> database(String name, String dataSourceInfo) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("DataSourceName", name);
        map.put("DataSourceInfo", dataSourceInfo);
        map.put("ProviderType", "MDP");
        map.put("AuthenticationMode", "Unauthenticated");
        map.put("DataSourceDescription", name + " ds");
        map.put("ProviderName", "Mondrian");
        map.put("URL", "http://localhost/xmla");
        return map;
    }
}
