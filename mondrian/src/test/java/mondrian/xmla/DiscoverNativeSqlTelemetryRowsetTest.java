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

import mondrian.rolap.nativesql.NativeSqlTelemetry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.olap4j.OlapConnection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Layer 1 unit tests for {@link RowsetDefinition#DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY}.
 *
 * <p>Locks Java-level semantics: enum metadata, column schema, populated
 * row content (union of fingerprint keys with 0-default), and the
 * read-only invariant of populateImpl().
 *
 * <p>SOAP wire format is locked separately by
 * {@code XmlaDiscoverNativeSqlTelemetryTest} (integration layer).
 *
 * <p>Phase 8d plan task 2.
 * Spec: docs/superpowers/specs/2026-05-05-phase-8d-discover-native-sql-telemetry-design.md
 */
public class DiscoverNativeSqlTelemetryRowsetTest {

    @BeforeEach public void setUp() {
        NativeSqlTelemetry.resetForTests();
    }

    @AfterEach public void tearDown() {
        NativeSqlTelemetry.resetForTests();
    }

    /**
     * Invokes {@code populateImpl} against whatever is currently seeded in
     * {@link NativeSqlTelemetry} and returns the accumulated rows.  The seed
     * scenario is the test's responsibility; this helper is generic.
     */
    private List<Rowset.Row> populate() {
        XmlaRequest request = mock(XmlaRequest.class);
        XmlaHandler handler = mock(XmlaHandler.class);
        XmlaResponse response = mock(XmlaResponse.class);
        OlapConnection connection = mock(OlapConnection.class);

        Rowset rowset =
            RowsetDefinition.DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY
                .getRowset(request, handler);

        List<Rowset.Row> rows = new ArrayList<>();
        // Call populateImpl directly to test the rowset's own emission semantics.
        // The test class lives in package mondrian.xmla, so protected access is
        // available.  Rowset.populate(...)'s public sort-then-emit wrapper is
        // tested as part of Layer 2 (golden-XML SOAP envelope diff).
        try {
            ((RowsetDefinition.DiscoverMondrianNativeSqlTelemetryRowset) rowset)
                .populateImpl(response, connection, rows);
        } catch (XmlaException e) {
            throw new AssertionError(
                "populateImpl threw — should be a pure read", e);
        }
        return rows;
    }

    private static void seedFreshOnly(String fp) {
        NativeSqlTelemetry.executionSuccess(fp, 1L);
    }

    private static void seedCachedOnly(String fp) {
        NativeSqlTelemetry.cachedSuccessHit(fp);
    }

    private static void seedBoth(String fp) {
        NativeSqlTelemetry.executionSuccess(fp, 1L);
        NativeSqlTelemetry.cachedSuccessHit(fp);
    }

    @Test public void enumerationDefinitionMetadataIsStable() {
        RowsetDefinition def =
            RowsetDefinition.DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY;
        assertNotNull(def, "enum value must exist");
        assertEquals("DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY", def.name(),
            "enum name is part of the wire contract");
        // SchemaGuid is locked at the integration layer (Layer 2).

        // v2 enum-level description: pinned here so any drift in the
        // RowsetDefinition constructor argument (the human-readable
        // description string) is caught at Layer 1.  This same string is
        // emitted on the wire by DISCOVER_SCHEMA_ROWSETS and is locked
        // again at Layer 2 in XmlaBasicTest.ref.xml.
        String expectedDescription =
            "Returns per-fingerprint native SQL telemetry counters: fresh "
            + "native attempts (success + failure), successful cached "
            + "re-deliveries, and the Phase 8e v2 success/failure split. "
            + "Read-only diagnostic surface; SCHEMA_VERSION = 2 indicates "
            + "the trailing FRESH_SUCCESS_COUNT / FRESH_FAILED_COUNT columns "
            + "are present and populated.";
        assertEquals(expectedDescription, def.getDescription(),
            "enum description string is part of the wire contract "
            + "(emitted by DISCOVER_SCHEMA_ROWSETS)");
    }

    @Test public void columnSchemaIsStable() {
        RowsetDefinition def =
            RowsetDefinition.DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY;

        // RowsetDefinition.columnDefinitions is package-private; we are in
        // the same package, so access is allowed.
        RowsetDefinition.Column[] cols = def.columnDefinitions;

        // v2 contract: 6 columns in the order documented in spec §3.
        // Positions 1-4 are the v1 stable columns.  Positions 5 and 6
        // are the Phase 8e additions, appended after SCHEMA_VERSION
        // (position-as-contract: SCHEMA_VERSION stays at position 4).
        assertEquals(6, cols.length, "v2 wire contract: exactly 6 columns");

        List<String> expectedNames = Arrays.asList(
            "FINGERPRINT_ID",
            "FRESH_ATTEMPT_COUNT",
            "CACHED_SUCCESS_HIT_COUNT",
            "SCHEMA_VERSION",
            "FRESH_SUCCESS_COUNT",
            "FRESH_FAILED_COUNT");
        List<String> actualNames = new ArrayList<>();
        for (RowsetDefinition.Column c : cols) {
            actualNames.add(c.name);
        }
        assertEquals(expectedNames, actualNames,
            "column order is part of the wire contract");

        // All six columns must be NOT_RESTRICTION + OPTIONAL per spec §3.
        // Verified field names on RowsetDefinition.Column (line ~1632):
        //   restriction (boolean)  — true for RESTRICTION, false for NOT_RESTRICTION
        //   nullable    (boolean)  — true for OPTIONAL,    false for REQUIRED
        // Both fields are package-private; this test is in the same package.
        for (RowsetDefinition.Column c : cols) {
            assertEquals(false, c.restriction,
                "column " + c.name + " must be NOT_RESTRICTION");
            assertEquals(true, c.nullable,
                "column " + c.name + " must be OPTIONAL (nullable)");
        }

        // Type contract: FINGERPRINT_ID is String; the five counter columns
        // are UnsignedInteger.  Pinned per spec §3.
        assertEquals(RowsetDefinition.Type.String, cols[0].type,
            "FINGERPRINT_ID must be String");
        for (int i = 1; i < cols.length; i++) {
            assertEquals(RowsetDefinition.Type.UnsignedInteger, cols[i].type,
                "column " + cols[i].name + " must be UnsignedInteger");
        }
    }

    @Test public void populateImplEmitsExpectedRowsForUnionOfSnapshots() {
        // Seed via rich events so split counters populate.
        NativeSqlTelemetry.executionSuccess("fp-A", 0L);   // success only
        NativeSqlTelemetry.cachedSuccessHit("fp-B");       // cached only
        NativeSqlTelemetry.executionSuccess("fp-C", 0L);   // success + cached
        NativeSqlTelemetry.cachedSuccessHit("fp-C");
        NativeSqlTelemetry.executionFailed("fp-D",         // failed only
            new RuntimeException(),
            mondrian.rolap.nativesql.NativeSqlError.Classification.FALLBACK,
            0L);

        List<Rowset.Row> rows = populate();
        assertEquals(4, rows.size(),
            "one row per fingerprint, sorted ASC");

        // Row order matches FINGERPRINT_ID ASC by virtue of TreeSet keying
        // in populateImpl.  6-column v2 form:
        //   fp-id, freshAttempt, cachedHit, schemaVersion, freshSuccess, freshFailed
        assertRow(rows.get(0), "fp-A", 1, 0, 2, 1, 0);
        assertRow(rows.get(1), "fp-B", 0, 1, 2, 0, 0);
        assertRow(rows.get(2), "fp-C", 1, 1, 2, 1, 0);
        assertRow(rows.get(3), "fp-D", 1, 0, 2, 0, 1);
    }

    @Test public void populateImplIsReadOnly() {
        seedFreshOnly("fp-A");
        seedCachedOnly("fp-B");
        seedBoth("fp-C");

        SortedMap<String, Integer> freshBefore =
            NativeSqlTelemetry.snapshot();
        SortedMap<String, Integer> cachedBefore =
            NativeSqlTelemetry.cachedHitsSnapshot();
        SortedMap<String, Integer> successBefore =
            NativeSqlTelemetry.executionSuccessSnapshot();
        SortedMap<String, Integer> failedBefore =
            NativeSqlTelemetry.executionFailedSnapshot();

        populate();  // discard the rows; we're asserting state invariance

        SortedMap<String, Integer> freshAfter =
            NativeSqlTelemetry.snapshot();
        SortedMap<String, Integer> cachedAfter =
            NativeSqlTelemetry.cachedHitsSnapshot();
        SortedMap<String, Integer> successAfter =
            NativeSqlTelemetry.executionSuccessSnapshot();
        SortedMap<String, Integer> failedAfter =
            NativeSqlTelemetry.executionFailedSnapshot();

        assertEquals(freshBefore, freshAfter,
            "populateImpl must not mutate the fresh-execution snapshot");
        assertEquals(cachedBefore, cachedAfter,
            "populateImpl must not mutate the cached-hits snapshot");
        assertEquals(successBefore, successAfter,
            "populateImpl must not mutate the execution-success snapshot");
        assertEquals(failedBefore, failedAfter,
            "populateImpl must not mutate the execution-failed snapshot");
    }

    @Test public void populateImplEmitsZeroedSplitCountsWhenOnlyLegacyAttempts() {
        // Seed via the documented primitive bypass: bumps the legacy
        // attempt counter without firing the rich executionSuccess /
        // executionFailed events.  The v2 split columns must default to
        // 0 for such fingerprints — round-trip through the wire.
        NativeSqlTelemetry.incExecutionCount("fp-raw");

        List<Rowset.Row> rows = populate();
        assertEquals(1, rows.size());
        assertRow(rows.get(0),
            "fp-raw",
            /* attempt    */ 1,
            /* cached     */ 0,
            /* version    */ 2,
            /* success    */ 0,
            /* failed     */ 0);
    }

    private static void assertRow(
        Rowset.Row row,
        String fp,
        int freshAttempt,
        int cachedHit,
        int schemaVersion,
        int freshSuccess,
        int freshFailed)
    {
        assertEquals(fp, row.get("FINGERPRINT_ID"),
            "FINGERPRINT_ID");
        assertEquals(freshAttempt, ((Number) row.get("FRESH_ATTEMPT_COUNT")).intValue(),
            "FRESH_ATTEMPT_COUNT for " + fp);
        assertEquals(cachedHit, ((Number) row.get("CACHED_SUCCESS_HIT_COUNT")).intValue(),
            "CACHED_SUCCESS_HIT_COUNT for " + fp);
        assertEquals(schemaVersion, ((Number) row.get("SCHEMA_VERSION")).intValue(),
            "SCHEMA_VERSION literal for " + fp);
        assertEquals(freshSuccess, ((Number) row.get("FRESH_SUCCESS_COUNT")).intValue(),
            "FRESH_SUCCESS_COUNT for " + fp);
        assertEquals(freshFailed, ((Number) row.get("FRESH_FAILED_COUNT")).intValue(),
            "FRESH_FAILED_COUNT for " + fp);
    }
}
