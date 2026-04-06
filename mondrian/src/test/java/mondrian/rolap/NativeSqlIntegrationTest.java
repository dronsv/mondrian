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

import junit.framework.TestCase;
import org.olap4j.*;
import org.olap4j.metadata.*;

import java.sql.*;

/**
 * Integration test for NativeSqlCalc against a real ClickHouse instance.
 *
 * <p>Requires:
 * <ul>
 *   <li>ClickHouse on localhost:28123 (debug-dump container)</li>
 *   <li>Schema at config/schema/schema_demo.xml</li>
 *   <li>mondrian.properties at .docker-local/debug-dump/config/</li>
 * </ul>
 *
 * <p>Run: {@code mvn test -Dtest=NativeSqlIntegrationTest
 *   -Dch.available=true}
 *
 * <p>Skipped by default (ch.available != true) to avoid CI failures.
 */
public class NativeSqlIntegrationTest extends TestCase {

    private static final String ROOT =
        System.getProperty("project.root",
            "/home/andrey/work/emodrian_changes");
    private static final String SCHEMA =
        ROOT + "/config/schema/schema_demo.xml";
    private static final String PROPS =
        ROOT + "/.docker-local/debug-dump/config/mondrian.properties";
    private static final String CH_HOST =
        System.getProperty("ch.host", "localhost");
    private static final String CH_PORT =
        System.getProperty("ch.port", "28123");

    @Override
    protected void setUp() throws Exception {
        String available = System.getProperty("ch.available", "false");
        if (!"true".equalsIgnoreCase(available)) {
            return;
        }
        Class.forName("mondrian.olap4j.MondrianOlap4jDriver");
    }

    private OlapConnection connect() throws SQLException {
        String url = "jdbc:mondrian:"
            + "Jdbc=jdbc:clickhouse://" + CH_HOST + ":" + CH_PORT
            + "/default;"
            + "JdbcUser=default;JdbcPassword=;"
            + "JdbcDrivers=com.clickhouse.jdbc.ClickHouseDriver;"
            + "Catalog=" + SCHEMA + ";"
            + "MondrianProperties=" + PROPS + ";";
        Connection conn = DriverManager.getConnection(url);
        return conn.unwrap(OlapConnection.class);
    }

    private CellSet executeMdx(OlapConnection conn, String mdx)
        throws OlapException
    {
        OlapStatement stmt = conn.createStatement();
        return stmt.executeOlapQuery(mdx);
    }

    private boolean isChAvailable() {
        return "true".equalsIgnoreCase(
            System.getProperty("ch.available", "false"));
    }

    public void testBasicQuery() throws Exception {
        if (!isChAvailable()) return;
        try (OlapConnection conn = connect()) {
            CellSet cs = executeMdx(conn,
                "SELECT {[Measures].[Продажи руб]} ON 0"
                + " FROM [Продажи]"
                + " WHERE [Период.Год].[2024]");
            Cell cell = cs.getCell(0);
            assertNotNull("Cell should not be null", cell.getValue());
            double sales = ((Number) cell.getValue()).doubleValue();
            assertTrue("Sales should be positive", sales > 0);
            System.out.println("Продажи руб = " + sales);
        }
    }

    public void testVirtualCubeWd() throws Exception {
        if (!isChAvailable()) return;
        try (OlapConnection conn = connect()) {
            CellSet cs = executeMdx(conn,
                "SELECT {[Measures].[Взвеш. дистрибуция %],"
                + " [Measures].[Продажи руб]} ON 0,"
                + " {[Продукт.Производитель].[Алтайский Выпечка]} ON 1"
                + " FROM [Кондитерка]"
                + " WHERE [Период.Год].[2024]");
            Cell wdCell = cs.getCell(0);
            Cell salesCell = cs.getCell(1);
            assertNotNull("Sales should not be null", salesCell.getValue());
            double sales = ((Number) salesCell.getValue()).doubleValue();
            assertTrue("Sales should be positive", sales > 0);
            System.out.println("WD% = " + wdCell.getValue()
                + ", Sales = " + sales);
        }
    }

    public void testWdWithChainSubselect() throws Exception {
        if (!isChAvailable()) return;
        try (OlapConnection conn = connect()) {
            CellSet cs = executeMdx(conn,
                "SELECT {[Measures].[Взвеш. дистрибуция %],"
                + " [Measures].[Продажи руб]} ON 0,"
                + " {[Продукт.Производитель].[Алтайский Выпечка]} ON 1"
                + " FROM (SELECT {[ТТ.Сеть].[Сеть].[МегаМарт]} ON 0"
                + "   FROM [Кондитерка])"
                + " WHERE [Период.Год].[2024]");
            Cell salesCell = cs.getCell(1);
            assertNotNull("Sales should not be null", salesCell.getValue());
            double sales = ((Number) salesCell.getValue()).doubleValue();
            assertTrue("Sales with chain filter should be positive",
                sales > 0);
            System.out.println("WD% (МегаМарт) = " + cs.getCell(0).getValue()
                + ", Sales = " + sales);
        }
    }

    public void testWdInCategory() throws Exception {
        if (!isChAvailable()) return;
        try (OlapConnection conn = connect()) {
            CellSet cs = executeMdx(conn,
                "SELECT {[Measures].[Взвеш. дистрибуция %]} ON 0,"
                + " {[Продукт.Производитель].[Алтайский Выпечка]} ON 1"
                + " FROM (SELECT"
                + "   {[Продукт.Категория].[Категория].[Шоколад]} ON 0"
                + "   FROM [Кондитерка])"
                + " WHERE [Период.Год].[2024]");
            Cell wdCell = cs.getCell(0);
            assertNotNull("WD should not be null", wdCell.getValue());
            double wd = ((Number) wdCell.getValue()).doubleValue();
            System.out.println("WD% (Шоколад) = " + wd);
            assertTrue("In-category WD should be < 50%", wd < 50);
        }
    }
}
