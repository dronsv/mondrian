/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.test;

/**
 * TCK helper for Mondrian's olap4j driver.
 *
 * @author Julian Hyde
 */
public class MondrianOlap4jTester extends AbstractMondrianOlap4jTester
{
    /**
     * Constructor used reflectively by olap4j-tck when available.
     *
     * @param testContext Test context
     */
    public MondrianOlap4jTester(Object testContext) {
        super(
            testContext,
            DRIVER_URL_PREFIX,
            DRIVER_CLASS_NAME,
            Flavor.MONDRIAN);
    }

    public static final String DRIVER_CLASS_NAME =
        "mondrian.olap4j.MondrianOlap4jDriver";

    public static final String DRIVER_URL_PREFIX = "jdbc:mondrian:";
}

// End MondrianOlap4jTester.java
