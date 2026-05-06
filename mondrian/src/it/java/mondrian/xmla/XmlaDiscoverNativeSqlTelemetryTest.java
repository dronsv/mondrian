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

import mondrian.olap4j.MondrianOlap4jDriver;
import mondrian.rolap.nativesql.NativeSqlError;
import mondrian.rolap.nativesql.NativeSqlTelemetry;
import mondrian.test.DiffRepository;
import mondrian.test.TestContext;

import java.util.Properties;

/**
 * Integration tests for {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY}
 * XMLA rowset (Phase 8d / 8e v2 contract).
 *
 * <p>Test 1 ({@link #testDiscoverRowsetReturnsKnownXml}): seeds a known
 * telemetry state into {@link NativeSqlTelemetry} before issuing the SOAP
 * request, then performs a golden-XML diff against the expected SOAP response
 * recorded in {@code XmlaDiscoverNativeSqlTelemetryTest.ref.xml}.
 *
 * <p>Test 2 ({@link #testSchemaRowsetsEnumerationIncludesNativeSqlTelemetry}):
 * issues a restricted {@code DISCOVER_SCHEMA_ROWSETS} request
 * ({@code SchemaName = DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY}) and performs a
 * golden-XML diff to confirm the rowset is discoverable via the standard XMLA
 * schema-discovery mechanism.
 */
public class XmlaDiscoverNativeSqlTelemetryTest extends XmlaBaseTestCase {

    // -- Seeded fingerprints used by testDiscoverRowsetReturnsKnownXml --------

    /** fp-A: one fresh successful execution, zero cached hits. */
    private static final String FP_A = "fp-A";
    /** fp-B: zero fresh attempts, one cached hit. */
    private static final String FP_B = "fp-B";
    /** fp-C: one fresh successful execution and one cached hit. */
    private static final String FP_C = "fp-C";
    /** fp-D: zero fresh successes, one fresh failure (FALLBACK class). */
    private static final String FP_D = "fp-D";

    // -- Seed helpers ---------------------------------------------------------

    private static void seedSuccessOnly(String fp) {
        NativeSqlTelemetry.executionSuccess(fp, 0L);
    }

    private static void seedCachedOnly(String fp) {
        NativeSqlTelemetry.cachedSuccessHit(fp);
    }

    private static void seedSuccessAndCached(String fp) {
        NativeSqlTelemetry.executionSuccess(fp, 0L);
        NativeSqlTelemetry.cachedSuccessHit(fp);
    }

    private static void seedFailedOnly(String fp) {
        NativeSqlTelemetry.executionFailed(
            fp,
            new RuntimeException("seed-failure"),
            NativeSqlError.Classification.FALLBACK,
            0L);
    }

    // -- Boilerplate ----------------------------------------------------------

    public XmlaDiscoverNativeSqlTelemetryTest() {
    }

    public XmlaDiscoverNativeSqlTelemetryTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Class.forName(MondrianOlap4jDriver.class.getName());

        // Bring telemetry to a clean, deterministic state before each test,
        // then seed the 4-fingerprint v2 scenario used by
        // testDiscoverRowsetReturnsKnownXml so every counter family —
        // including FRESH_FAILED_COUNT — is exercised.
        NativeSqlTelemetry.resetForTests();
        seedSuccessOnly(FP_A);
        seedCachedOnly(FP_B);
        seedSuccessAndCached(FP_C);
        seedFailedOnly(FP_D);
    }

    @Override
    protected void tearDown() throws Exception {
        NativeSqlTelemetry.resetForTests();
        super.tearDown();
    }

    @Override
    protected DiffRepository getDiffRepos() {
        return DiffRepository.lookup(XmlaDiscoverNativeSqlTelemetryTest.class);
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
     * Verifies that {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY} returns the
     * exact expected SOAP response for a known telemetry state.
     *
     * <p>Seeded state (see {@link #setUp}); SCHEMA_VERSION = 2 on every row:
     * <ul>
     *   <li>fp-A: FRESH_ATTEMPT_COUNT=1, CACHED_SUCCESS_HIT_COUNT=0,
     *       FRESH_SUCCESS_COUNT=1, FRESH_FAILED_COUNT=0</li>
     *   <li>fp-B: FRESH_ATTEMPT_COUNT=0, CACHED_SUCCESS_HIT_COUNT=1,
     *       FRESH_SUCCESS_COUNT=0, FRESH_FAILED_COUNT=0</li>
     *   <li>fp-C: FRESH_ATTEMPT_COUNT=1, CACHED_SUCCESS_HIT_COUNT=1,
     *       FRESH_SUCCESS_COUNT=1, FRESH_FAILED_COUNT=0</li>
     *   <li>fp-D: FRESH_ATTEMPT_COUNT=1, CACHED_SUCCESS_HIT_COUNT=0,
     *       FRESH_SUCCESS_COUNT=0, FRESH_FAILED_COUNT=1</li>
     * </ul>
     * Rows are emitted in ascending fingerprint-id order (TreeSet sort).
     */
    public void testDiscoverRowsetReturnsKnownXml() throws Exception {
        String requestType = "DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY";
        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);
        doTest(requestType, props, TestContext.instance());
    }

    /**
     * Verifies that {@code DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY} appears in
     * the {@code DISCOVER_SCHEMA_ROWSETS} enumeration, confirming auto-
     * discoverability.
     *
     * <p>Issues a restricted {@code DISCOVER_SCHEMA_ROWSETS} request
     * ({@code SchemaName = DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY}) so that
     * the golden response contains exactly one row and remains stable across
     * rowset additions.
     */
    public void testSchemaRowsetsEnumerationIncludesNativeSqlTelemetry()
        throws Exception
    {
        String requestType = "DISCOVER_SCHEMA_ROWSETS";

        Properties props = new Properties();
        props.setProperty(REQUEST_TYPE_PROP, requestType);
        props.setProperty(DATA_SOURCE_INFO_PROP, DATA_SOURCE_INFO);

        // Inline SOAP request with a SchemaName restriction so the response
        // contains only our single row. The ${data.source.info} and ${content}
        // placeholders are substituted from props by doTestInline before the
        // request is forwarded to the XMLA engine.
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
            + "        <SchemaName>DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY</SchemaName>\n"
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
}

// End XmlaDiscoverNativeSqlTelemetryTest.java
