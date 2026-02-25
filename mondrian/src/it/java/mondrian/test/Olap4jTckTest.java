/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/
package mondrian.test;

import mondrian.olap.Util;

import junit.framework.*;

import java.lang.reflect.Method;
import java.util.Properties;

/**
 * Test suite that runs the olap4j Test Compatiblity Kit (TCK) against
 * mondrian's olap4j driver.
 *
 * @author jhyde
 * @since 2010/11/22
 */
public class Olap4jTckTest extends TestCase {
    private static final String OLAP4J_TEST_CONTEXT_CLASS =
        "org.olap4j.test.TestContext";

    private static final Util.Functor1<Boolean, Test> CONDITION =
        new Util.Functor1<Boolean, Test>() {
            public Boolean apply(Test test) {
                if (!(test instanceof TestCase)) {
                    return true;
                }
                final TestCase testCase = (TestCase) test;
                final String testCaseName = testCase.getName();
                return !testCaseName.equals("testStatementTimeout")
                    // olap4j-tck does not close ResultSet, and that's a
                    // resource leak
                    && !testCaseName.startsWith(
                        "testCubesDrillthroughReturnClause")
                    && !testCaseName.equals("testStatementCancel")
                    && !testCaseName.equals("testDatabaseMetaDataGetCatalogs")
                    && !testCaseName.equals("testCellSetBug")
                    // Disabling the following until the olap4j test expected
                    // value is updated.
                    && !testCaseName.equals(
                        "testDatabaseMetaDataGetDatasources");
            }
        };

    public static TestSuite suite() {
        if (!isOlap4jTckAvailable()) {
            return new TestSuite("olap4j TCK (skipped: olap4j-tck unavailable)");
        }

        final Util.PropertyList list =
            mondrian.test.TestContext.instance()
                .getConnectionProperties();
        final String connStr = "jdbc:mondrian:" + list;
        final String catalog = list.get("Catalog");

        final TestSuite suite = new TestSuite();

        suite.setName("olap4j TCK");
        suite.addTest(createMondrianSuite(connStr, false));
        suite.addTest(createMondrianSuite(connStr, true));
        suite.addTest(createXmlaSuite(connStr, catalog, false));
        suite.addTest(createXmlaSuite(connStr, catalog, true));
        return suite;
    }

    private static TestSuite createXmlaSuite(
        String connStr, String catalog, boolean wrapper)
    {
        final Properties properties = new Properties();
        properties.setProperty("org.olap4j.test.connectUrl", connStr);
        properties.setProperty(
            "org.olap4j.test.helperClassName",
            "org.olap4j.XmlaTester");
        properties.setProperty("org.olap4j.XmlaTester.CatalogUrl", catalog);
        properties.setProperty(
            "org.olap4j.test.wrapper",
            wrapper ? "NONE" : "DBCP");
        String name =
            "XMLA olap4j driver talking to mondrian's XMLA server";
        if (wrapper) {
            name += " (DBCP wrapper)";
        }
        final TestSuite suite = createTckSuite(properties, name);
        if (suite == null) {
            return new TestSuite(name + " (skipped: unable to initialize)");
        }
        if (CONDITION == null) {
            return suite;
        }
        return mondrian.test.TestContext.copySuite(suite, CONDITION);
    }

    private static TestSuite createMondrianSuite(
        String connStr, boolean wrapper)
    {
        final Properties properties = new Properties();
        properties.setProperty("org.olap4j.test.connectUrl", connStr);
        properties.setProperty(
            "org.olap4j.test.helperClassName",
            MondrianOlap4jTester.class.getName());
        properties.setProperty(
            "org.olap4j.test.wrapper",
            wrapper ? "NONE" : "DBCP");
        final String name =
            "mondrian olap4j driver"
            + (wrapper ? " (DBCP wrapper)" : "");
        final TestSuite suite = createTckSuite(properties, name);
        if (suite == null) {
            return new TestSuite(name + " (skipped: unable to initialize)");
        }
        if (CONDITION == null) {
            return suite;
        }
        return mondrian.test.TestContext.copySuite(suite, CONDITION);
    }

    private static boolean isOlap4jTckAvailable() {
        try {
            Class.forName(OLAP4J_TEST_CONTEXT_CLASS);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static TestSuite createTckSuite(Properties properties, String name) {
        try {
            Class<?> testContextClass = Class.forName(OLAP4J_TEST_CONTEXT_CLASS);
            Method createTckSuite =
                testContextClass.getMethod(
                    "createTckSuite",
                    Properties.class,
                    String.class);
            Object suite = createTckSuite.invoke(null, properties, name);
            if (suite instanceof TestSuite) {
                return (TestSuite) suite;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}

// End Olap4jTckTest.java
