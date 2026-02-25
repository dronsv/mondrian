/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.test;

import java.lang.reflect.Method;
import java.sql.*;
import java.util.Properties;

/**
 * Abstract implementation for Mondrian's olap4j TCK integration helper.
 *
 * <p>This class intentionally avoids compile-time references to
 * {@code org.olap4j.test.*} so IT sources can compile even when
 * {@code olap4j-tck} is not on the classpath.
 *
 * @author Julian Hyde
 */
abstract class AbstractMondrianOlap4jTester {
    enum Flavor {
        MONDRIAN
    }

    enum Wrapper {
        NONE
    }

    private static final String CONNECT_URL_PROPERTY =
        "org.olap4j.test.connectUrl";

    private final Object testContext;
    private final Properties properties;
    private final String driverUrlPrefix;
    private final String driverClassName;
    private final Flavor flavor;

    protected AbstractMondrianOlap4jTester(
        Object testContext,
        String driverUrlPrefix,
        String driverClassName,
        Flavor flavor)
    {
        this.testContext = testContext;
        this.properties = extractProperties(testContext);
        this.driverUrlPrefix = driverUrlPrefix;
        this.driverClassName = driverClassName;
        this.flavor = flavor;
    }

    public Object getTestContext() {
        return testContext;
    }

    public Connection createConnection() throws SQLException {
        try {
            Class.forName(getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("oops", e);
        }
        return
            DriverManager.getConnection(
                getURL(),
                new Properties());
    }

    public Connection createConnectionWithUserPassword() throws SQLException
    {
        return DriverManager.getConnection(
            getURL(), USER, PASSWORD);
    }

    public String getDriverUrlPrefix() {
        return driverUrlPrefix;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getURL() {
        return properties.getProperty(CONNECT_URL_PROPERTY);
    }

    public Flavor getFlavor() {
        return flavor;
    }

    public Wrapper getWrapper() {
        return Wrapper.NONE;
    }

    private static final String USER = "sa";
    private static final String PASSWORD = "sa";

    private static Properties extractProperties(Object testContext) {
        if (testContext == null) {
            return new Properties();
        }
        try {
            Method method = testContext.getClass().getMethod("getProperties");
            Object value = method.invoke(testContext);
            if (value instanceof Properties) {
                return (Properties) value;
            }
        } catch (Exception e) {
            // Keep empty properties when olap4j-tck is unavailable.
        }
        return new Properties();
    }
}

// End AbstractMondrianOlap4jTester.java
