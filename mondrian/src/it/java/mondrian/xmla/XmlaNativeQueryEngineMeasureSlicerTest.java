/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.xmla;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.olap4j.MondrianOlap4jDriver;
import mondrian.rolap.NativeQueryEngine;
import mondrian.test.DiffRepository;
import mondrian.test.TestAppender;
import mondrian.test.TestContext;
import mondrian.tui.XmlUtil;
import mondrian.tui.XmlaSupport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.Servlet;

/**
 * XMLA regression coverage for NQE queries whose measure is supplied by the
 * WHERE slicer rather than by a visible axis.
 */
public class XmlaNativeQueryEngineMeasureSlicerTest
    extends XmlaBaseTestCase
{
    private static final Logger NQE_LOGGER =
        LogManager.getLogger(NativeQueryEngine.class);

    public XmlaNativeQueryEngineMeasureSlicerTest() {
    }

    public XmlaNativeQueryEngineMeasureSlicerTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Class.forName(MondrianOlap4jDriver.class.getName());
    }

    @Override
    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(
            XmlaNativeQueryEngineMeasureSlicerTest.class);
    }

    @Override
    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return CallBack.class;
    }

    @Override
    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }

    public void testDimensionAxesWithMeasureSlicerReturnXmlaCells()
        throws Exception
    {
        MondrianProperties properties = MondrianProperties.instance();
        propSaver.set(properties.NativeQueryEngineEnable, true);
        propSaver.set(properties.DisableCaching, true);
        propSaver.set(NQE_LOGGER, Level.INFO);

        TestAppender appender = new TestAppender();
        Map<List<String>, Servlet> servletCache =
            new HashMap<List<String>, Servlet>();
        Util.addAppender(appender, NQE_LOGGER, Level.INFO);
        try {
            byte[] bytes = executeXmla(
                slicerMeasureRequest(),
                TestContext.instance().withCube(SALES_CUBE),
                servletCache);

            String responseText = new String(bytes, "UTF-8");
            assertNoSoapFault(bytes, responseText);
            assertNqeExecuted(appender);

            Document response = XmlUtil.parse(bytes);
            assertAxisHasTuples(response, "Axis0");
            assertAxisHasTuples(response, "Axis1");
            assertCellDataHasCells(response);
        } finally {
            Util.removeAppender(appender, NQE_LOGGER);
            for (Servlet servlet : servletCache.values()) {
                servlet.destroy();
            }
        }
    }

    private byte[] executeXmla(
        String requestText,
        TestContext testContext,
        Map<List<String>, Servlet> servletCache)
        throws Exception
    {
        return XmlaSupport.processSoapXmla(
            requestText,
            filterConnectString(testContext.getConnectString()),
            getCatalogNameUrls(testContext),
            CallBack.class.getName(),
            null,
            servletCache);
    }

    private static void assertNqeExecuted(TestAppender appender) {
        for (LogEvent event : appender.getLogEvents()) {
            if (event.getMessage().getFormattedMessage().contains(
                    "NativeQueryEngine: successfully populated "))
            {
                return;
            }
        }
        fail("Expected XMLA request to execute through NativeQueryEngine");
    }

    private static void assertNoSoapFault(byte[] bytes, String responseText)
        throws Exception
    {
        Node[] faultNodes = XmlaSupport.extractFaultNodesFromSoap(bytes);
        assertTrue(
            "Unexpected SOAP Fault in XMLA response: " + responseText,
            faultNodes == null || faultNodes.length == 0);
    }

    private static void assertAxisHasTuples(Document document, String name) {
        Element axis = findAxis(document, name);
        assertNotNull(name + " is missing from XMLA response", axis);
        assertTrue(
            name + " has no Tuple elements",
            countDescendants(axis, "Tuple") > 0);
    }

    private static void assertCellDataHasCells(Document document) {
        Element cellData = findFirstElement(document, "CellData");
        assertNotNull("CellData is missing from XMLA response", cellData);
        assertTrue(
            "CellData has no Cell elements",
            countDescendants(cellData, "Cell") > 0);
    }

    private static Element findAxis(Document document, String name) {
        NodeList nodes = document.getElementsByTagName("*");
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (named(node, "Axis")
                && name.equals(((Element) node).getAttribute("name")))
            {
                return (Element) node;
            }
        }
        return null;
    }

    private static Element findFirstElement(
        Document document,
        String localName)
    {
        NodeList nodes = document.getElementsByTagName("*");
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (named(node, localName)) {
                return (Element) node;
            }
        }
        return null;
    }

    private static int countDescendants(Element element, String localName) {
        int count = 0;
        NodeList nodes = element.getElementsByTagName("*");
        for (int i = 0; i < nodes.getLength(); i++) {
            if (named(nodes.item(i), localName)) {
                count++;
            }
        }
        return count;
    }

    private static boolean named(Node node, String localName) {
        String nodeLocalName = node.getLocalName();
        if (nodeLocalName == null) {
            nodeLocalName = node.getNodeName();
        }
        return localName.equals(nodeLocalName);
    }

    private static String slicerMeasureRequest() {
        String query =
            "SELECT "
            + "NON EMPTY {[Time].[1997].Children} ON COLUMNS, "
            + "NON EMPTY {[Store].[Store State].Members} ON ROWS "
            + "FROM [Sales] "
            + "WHERE ([Measures].[Unit Sales])";
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<soapenv:Envelope\n"
            + "    xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\"\n"
            + "    xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"\n"
            + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
            + "  <soapenv:Body>\n"
            + "    <Execute xmlns=\"urn:schemas-microsoft-com:xml-analysis\">\n"
            + "      <Command><Statement>" + query + "</Statement></Command>\n"
            + "      <Properties>\n"
            + "        <PropertyList>\n"
            + "          <Catalog>FoodMart</Catalog>\n"
            + "          <DataSourceInfo>FoodMart</DataSourceInfo>\n"
            + "          <Format>Multidimensional</Format>\n"
            + "          <AxisFormat>TupleFormat</AxisFormat>\n"
            + "        </PropertyList>\n"
            + "      </Properties>\n"
            + "    </Execute>\n"
            + "  </soapenv:Body>\n"
            + "</soapenv:Envelope>";
    }
}

// End XmlaNativeQueryEngineMeasureSlicerTest.java
