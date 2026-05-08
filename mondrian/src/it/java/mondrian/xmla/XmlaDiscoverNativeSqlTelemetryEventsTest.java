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

import mondrian.olap.Util;
import mondrian.olap4j.MondrianOlap4jDriver;
import mondrian.rolap.nativesql.NativeSqlRegistry;
import mondrian.rolap.nativesql.NativeSqlTelemetry;
import mondrian.rolap.nativesql.NativeSqlTelemetryEvents;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;
import mondrian.tui.XmlUtil;
import mondrian.tui.XmlaSupport;

import org.w3c.dom.Document;

import javax.servlet.Servlet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Phase 8f wire-level integration test for the
 * {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS} rowset.
 *
 * <p>Runs against the embedded H2 FoodMart fixture (see
 * {@code scripts/test-it-h2.sh} and the {@code it-h2-foodmart} Maven
 * profile).
 *
 * <p>Sibling to {@link XmlaDiscoverNativeSqlTelemetryTest}; the two
 * cover the two telemetry rowsets independently:
 * <ul>
 *   <li>{@link #testSchemaRowsetsEnumerationIncludesNativeSqlTelemetryEvents}
 *       — the standard auto-discovery path (DISCOVER_SCHEMA_ROWSETS).
 *       Uses the same golden-XML diff helper
 *       ({@code doTestInline}) the existing telemetry IT uses, since
 *       the response is fully deterministic (one row, frozen
 *       SchemaName + SchemaGuid + Description).</li>
 *   <li>{@link #testEventsRowsetReturnsRecordedEvents} — round-trips
 *       a synthetic event recorded via
 *       {@link NativeSqlTelemetryEvents#record} through the XMLA
 *       servlet. Uses raw response-byte assertions instead of a
 *       golden-XML diff because EVENT_TIME_MS is non-deterministic
 *       (wall-clock {@code System.currentTimeMillis()} at recording
 *       time).</li>
 * </ul>
 */
public class XmlaDiscoverNativeSqlTelemetryEventsTest
    extends XmlaBaseTestCase
{

    // -- Boilerplate ----------------------------------------------------------

    public XmlaDiscoverNativeSqlTelemetryEventsTest() {
    }

    public XmlaDiscoverNativeSqlTelemetryEventsTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Class.forName(MondrianOlap4jDriver.class.getName());

        // Reset all three substrate states so each test starts from a
        // clean, deterministic baseline. Mirrors the existing
        // XmlaDiscoverNativeSqlTelemetryTest setUp pattern, extended
        // with the events buffer reset and the registry cache clear.
        NativeSqlRegistry.clearGlobalCache();
        NativeSqlTelemetry.resetForTests();
        NativeSqlTelemetryEvents.resetForTests();
    }

    @Override
    protected void tearDown() throws Exception {
        NativeSqlTelemetryEvents.resetForTests();
        NativeSqlTelemetry.resetForTests();
        super.tearDown();
    }

    @Override
    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(
            XmlaDiscoverNativeSqlTelemetryEventsTest.class);
    }

    @Override
    protected Class<? extends XmlaRequestCallback> getServletCallbackClass() {
        return null;
    }

    @Override
    protected String getSessionId(Action action) {
        throw new UnsupportedOperationException();
    }

    // -- Tests ----------------------------------------------------------------

    /**
     * Verifies that {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS}
     * appears in the {@code DISCOVER_SCHEMA_ROWSETS} enumeration with
     * the spec-frozen SchemaName and SchemaGuid, confirming standard
     * XMLA auto-discoverability.
     *
     * <p>Issues a restricted {@code DISCOVER_SCHEMA_ROWSETS} request
     * ({@code SchemaName = DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS})
     * so the golden response contains exactly one row and remains
     * stable across rowset additions.
     */
    public void testSchemaRowsetsEnumerationIncludesNativeSqlTelemetryEvents()
        throws Exception
    {
        String requestType = "DISCOVER_SCHEMA_ROWSETS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        // Inline SOAP request with a SchemaName restriction so the
        // response contains only our single row. The
        // ${data.source.info} and ${content} placeholders are
        // substituted from props by doTestInline before the request
        // is forwarded to the XMLA engine.
        String requestText =
            "<SOAP-ENV:Envelope\n"
            + "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"\n"
            + "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\n"
            + "  <SOAP-ENV:Body>\n"
            + "    <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"\n"
            + "        SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\n"
            + "    <RequestType>DISCOVER_SCHEMA_ROWSETS</RequestType>\n"
            + "    <Restrictions>\n"
            + "      <RestrictionList>\n"
            + "        <SchemaName>DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS</SchemaName>\n"
            + "      </RestrictionList>\n"
            + "    </Restrictions>\n"
            + "    <Properties>\n"
            + "      <PropertyList>\n"
            + "        <DataSourceInfo>${data.source.info}</DataSourceInfo>\n"
            + "        <Content>${content}</Content>\n"
            + "      </PropertyList>\n"
            + "    </Properties>\n"
            + "    </Discover>\n"
            + "  </SOAP-ENV:Body>\n"
            + "</SOAP-ENV:Envelope>";

        doTestInline(
            requestType, requestText, "response",
            props, TestContext.instance());
    }

    /**
     * Records a synthetic event into the
     * {@link NativeSqlTelemetryEvents} ring buffer, issues a
     * {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS} XMLA
     * request, and asserts that the event surfaces in the response
     * with the expected EVENT_TYPE / EVENT_SEQUENCE / SCHEMA_VERSION.
     *
     * <p>Uses raw response-byte assertions (not a golden-XML diff)
     * because EVENT_TIME_MS is non-deterministic — the buffer stamps
     * {@code System.currentTimeMillis()} at append time, so a frozen
     * golden file would diff on every run.
     */
    public void testEventsRowsetReturnsRecordedEvents() throws Exception {
        // Seed exactly one event into the ring buffer. The buffer was
        // reset in setUp, so this event is guaranteed to be at
        // EVENT_SEQUENCE = 0.
        NativeSqlTelemetryEvents.record(
            NativeSqlTelemetryEvents.EventType.EXECUTION_SUCCESS,
            "test-fingerprint",
            /* classification */ null,
            /* durationMs    */ 42L,
            /* rawMessage    */ null);

        String requestText =
            "<SOAP-ENV:Envelope\n"
            + "    xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"\n"
            + "    SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\n"
            + "  <SOAP-ENV:Body>\n"
            + "    <Discover xmlns=\"urn:schemas-microsoft-com:xml-analysis\"\n"
            + "        SOAP-ENV:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\n"
            + "    <RequestType>"
            + "DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS"
            + "</RequestType>\n"
            + "    <Restrictions>\n"
            + "      <RestrictionList>\n"
            + "      </RestrictionList>\n"
            + "    </Restrictions>\n"
            + "    <Properties>\n"
            + "      <PropertyList>\n"
            + "        <DataSourceInfo>${data.source.info}</DataSourceInfo>\n"
            + "        <Content>${content}</Content>\n"
            + "      </PropertyList>\n"
            + "    </Properties>\n"
            + "    </Discover>\n"
            + "  </SOAP-ENV:Body>\n"
            + "</SOAP-ENV:Envelope>";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP,
            "DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS");
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        // Request the Data content variant so the response contains
        // <row> elements (not just the schema). Mirrors the wire-level
        // contract a real XMLA client would use.
        props.setProperty("content", "data");

        // Substitute placeholders, parse to a DOM, then drive the
        // SOAP/XMLA path directly so we can inspect the raw response
        // bytes. This is the same execution path doTests() uses
        // internally; here we just skip the golden-XML diff because
        // EVENT_TIME_MS is non-deterministic. We use the connect-
        // string + catalog-URL form (not getServlet) so we don't
        // collide with the unsupported getSessionId(CLEAR) override
        // mirrored from XmlaDiscoverNativeSqlTelemetryTest.
        TestContext testContext = TestContext.instance();
        String connectString = testContext.getConnectString();
        Map<String, String> catalogNameUrls = getCatalogNameUrls(testContext);
        String resolvedRequest =
            Util.replaceProperties(requestText, Util.toMap(props));
        Document reqDoc = XmlUtil.parseString(resolvedRequest);
        Map<List<String>, Servlet> servletCache = new HashMap<>();
        byte[] responseBytes = XmlaSupport.processSoapXmla(
            reqDoc,
            filterConnectString(connectString),
            catalogNameUrls,
            CallBack.class.getName(),
            /* role */ null,
            servletCache);
        String response = new String(responseBytes, "UTF-8");

        // Sanity: response is a well-formed SOAP envelope with no
        // <Exception/> faults from the engine.
        assertTrue(
            "response should be a SOAP envelope; got: " + response,
            response.contains("<SOAP-ENV:Envelope")
            || response.contains("<soap:Envelope"));
        assertFalse(
            "response should not contain an XMLA fault; got: " + response,
            response.contains("<SOAP-ENV:Fault")
            || response.contains("<faultcode>"));

        // Content assertions corresponding to the seeded event:
        //   EVENT_TYPE        == "EXECUTION_SUCCESS"
        //   EVENT_SEQUENCE    present and non-null
        //   SCHEMA_VERSION    == 1
        // Match on the wire-level element forms emitted by the
        // rowset framework's emit() path.
        assertTrue(
            "expected <EVENT_TYPE>EXECUTION_SUCCESS</EVENT_TYPE> "
            + "in response; got: " + response,
            response.contains(
                "<EVENT_TYPE>EXECUTION_SUCCESS</EVENT_TYPE>"));
        assertTrue(
            "expected <EVENT_SEQUENCE>...</EVENT_SEQUENCE> in "
            + "response (any non-null value); got: " + response,
            response.matches(
                "(?s).*<EVENT_SEQUENCE>\\d+</EVENT_SEQUENCE>.*"));
        assertTrue(
            "expected <SCHEMA_VERSION>1</SCHEMA_VERSION> in "
            + "response; got: " + response,
            response.contains(
                "<SCHEMA_VERSION>1</SCHEMA_VERSION>"));
    }
}

// End XmlaDiscoverNativeSqlTelemetryEventsTest.java
