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
     * Builds a populated rowset for the test seed (fp-A fresh, fp-B cached,
     * fp-C both).  Returns the accumulated rows.
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
    }

    @Test public void columnSchemaIsStable() {
        RowsetDefinition def =
            RowsetDefinition.DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY;

        // RowsetDefinition.columnDefinitions is package-private; we are in
        // the same package, so access is allowed.
        RowsetDefinition.Column[] cols = def.columnDefinitions;

        assertEquals(4, cols.length, "v1 wire contract: exactly 4 columns");

        List<String> expectedNames = Arrays.asList(
            "FINGERPRINT_ID",
            "FRESH_ATTEMPT_COUNT",
            "CACHED_SUCCESS_HIT_COUNT",
            "SCHEMA_VERSION");
        List<String> actualNames = new ArrayList<>();
        for (RowsetDefinition.Column c : cols) {
            actualNames.add(c.name);
        }
        assertEquals(expectedNames, actualNames,
            "column order is part of the wire contract");

        // All four columns must be NOT_RESTRICTION + OPTIONAL per spec §3.
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
    }

    @Test public void populateImplEmitsExpectedRowsForUnionOfSnapshots() {
        seedFreshOnly("fp-A");
        seedCachedOnly("fp-B");
        seedBoth("fp-C");

        List<Rowset.Row> rows = populate();
        assertEquals(3, rows.size(),
            "expected 3 rows for the union of {fp-A,fp-C} U {fp-B,fp-C}");

        // Row order matches FINGERPRINT_ID ASC by virtue of TreeSet keying
        // in populateImpl.
        assertRow(rows.get(0), "fp-A", 1, 0);
        assertRow(rows.get(1), "fp-B", 0, 1);
        assertRow(rows.get(2), "fp-C", 1, 1);
    }

    @Test public void populateImplIsReadOnly() {
        seedFreshOnly("fp-A");
        seedCachedOnly("fp-B");
        seedBoth("fp-C");

        SortedMap<String, Integer> freshBefore =
            NativeSqlTelemetry.snapshot();
        SortedMap<String, Integer> cachedBefore =
            NativeSqlTelemetry.cachedHitsSnapshot();

        populate();  // discard the rows; we're asserting state invariance

        SortedMap<String, Integer> freshAfter =
            NativeSqlTelemetry.snapshot();
        SortedMap<String, Integer> cachedAfter =
            NativeSqlTelemetry.cachedHitsSnapshot();

        assertEquals(freshBefore, freshAfter,
            "populateImpl must not mutate the fresh-execution snapshot");
        assertEquals(cachedBefore, cachedAfter,
            "populateImpl must not mutate the cached-hits snapshot");
    }

    private static void assertRow(
        Rowset.Row row, String fp, int freshAttempt, int cachedHit)
    {
        assertEquals(fp, row.get("FINGERPRINT_ID"),
            "FINGERPRINT_ID");
        assertEquals(freshAttempt, ((Number) row.get("FRESH_ATTEMPT_COUNT")).intValue(),
            "FRESH_ATTEMPT_COUNT for " + fp);
        assertEquals(cachedHit, ((Number) row.get("CACHED_SUCCESS_HIT_COUNT")).intValue(),
            "CACHED_SUCCESS_HIT_COUNT for " + fp);
        assertEquals(1, ((Number) row.get("SCHEMA_VERSION")).intValue(),
            "SCHEMA_VERSION literal for " + fp);
    }
}
