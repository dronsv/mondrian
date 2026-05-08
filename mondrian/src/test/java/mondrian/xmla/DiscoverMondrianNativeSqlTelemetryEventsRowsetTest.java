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

import mondrian.rolap.nativesql.NativeSqlError;
import mondrian.rolap.nativesql.NativeSqlTelemetryEvents;
import mondrian.rolap.nativesql.NativeSqlTelemetryEvents.EventType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.olap4j.OlapConnection;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Layer 1 unit tests for
 * {@link RowsetDefinition#DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS}.
 *
 * <p>Locks Java-level semantics of {@code populateImpl}: empty-buffer
 * behavior, ordered emission, restriction filters in isolation, combined
 * (AND) restrictions, unknown-type tolerance, SCHEMA_VERSION literal,
 * and null-fingerprintId surfacing.
 *
 * <p>The framework's wire-level restriction validation in the parent
 * {@link Rowset} constructor requires that restriction keys correspond
 * to columns flagged with {@code Column.RESTRICTION}.  These unit tests
 * therefore stub {@code XmlaRequest.getRestrictions()} to return an empty
 * map on construction (avoiding the per-restriction validation path), then
 * inject the test-specific restriction map directly via reflection on the
 * protected {@code restrictions} field before {@code populateImpl} runs.
 * SOAP wire format and full request-level restriction routing are locked
 * separately by the Layer-4 IT (Task 4 of the Phase 8f plan).
 *
 * <p>Phase 8f plan task 3.
 * Spec: docs/superpowers/specs/2026-05-08-phase-8f-events-ring-buffer-design.md §4
 */
public class DiscoverMondrianNativeSqlTelemetryEventsRowsetTest {

    @BeforeEach public void setUp() {
        NativeSqlTelemetryEvents.resetForTests();
    }

    @AfterEach public void tearDown() {
        NativeSqlTelemetryEvents.resetForTests();
    }

    /**
     * Constructs the rowset with {@code restrictions} as the
     * pre-populated restriction map, invokes
     * {@link RowsetDefinition.DiscoverMondrianNativeSqlTelemetryEventsRowset#populateImpl}
     * directly, and returns the accumulated rows.
     *
     * <p>{@code restrictions} is null → mock {@code XmlaRequest} returns
     * the default empty map (no restrictions).  Otherwise, the
     * {@code XmlaRequest} mock is stubbed so the parent constructor sees
     * an empty restriction set (avoiding its restriction-flag validation
     * path), and the restriction map is then injected via reflection on
     * the protected {@code restrictions} field before {@code populateImpl}
     * runs.
     */
    private List<Rowset.Row> populate(Map<String, Object> restrictions) {
        XmlaRequest request = mock(XmlaRequest.class);
        XmlaHandler handler = mock(XmlaHandler.class);
        XmlaResponse response = mock(XmlaResponse.class);
        OlapConnection connection = mock(OlapConnection.class);

        // Stub getRestrictions/getProperties so the parent constructor
        // sees empty maps.  Mockito 5 RETURNS_DEFAULTS already returns
        // empty for collection-returning methods, but we make this
        // explicit for clarity.
        when(request.getRestrictions()).thenReturn(Collections.emptyMap());
        when(request.getProperties()).thenReturn(Collections.emptyMap());

        Rowset rowset =
            RowsetDefinition.DISCOVER_MONDRIAN_NATIVE_SQL_TELEMETRY_EVENTS
                .getRowset(request, handler);
        assertNotNull(rowset, "rowset factory must return non-null");

        if (restrictions != null && !restrictions.isEmpty()) {
            injectRestrictions(rowset, restrictions);
        }

        List<Rowset.Row> rows = new ArrayList<>();
        try {
            ((RowsetDefinition.DiscoverMondrianNativeSqlTelemetryEventsRowset)
                rowset).populateImpl(response, connection, rows);
        } catch (XmlaException e) {
            throw new AssertionError(
                "populateImpl threw — should be a pure read", e);
        }
        return rows;
    }

    /**
     * Injects {@code restrictions} into the rowset's protected
     * {@code restrictions} field via reflection.  This bypasses the
     * parent constructor's restriction-flag validation path, allowing
     * tests to exercise {@code populateImpl} filtering with arbitrary
     * restriction maps regardless of whether each restriction key has
     * {@code Column.RESTRICTION} declared in the rowset definition.
     * See class javadoc for the full rationale.
     */
    private static void injectRestrictions(
        Rowset rowset, Map<String, Object> restrictions)
    {
        try {
            Field f = Rowset.class.getDeclaredField("restrictions");
            f.setAccessible(true);
            f.set(rowset, restrictions);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(
                "test seam failed to inject restrictions", e);
        }
    }

    private static void recordEvent(
        EventType type,
        String fingerprintId,
        NativeSqlError.Classification classification,
        Long durationMs,
        String message)
    {
        NativeSqlTelemetryEvents.record(
            type, fingerprintId, classification, durationMs, message);
    }

    @Test
    public void populateImpl_emptyBuffer_emitsZeroRows() {
        // No events recorded; populateImpl must short-circuit cleanly.
        List<Rowset.Row> rows = populate(null);
        assertEquals(0, rows.size(),
            "empty ring buffer must produce zero rows");
    }

    @Test
    public void populateImpl_threeEventsNoRestrictions_emitsThreeRowsAscBySequence() {
        recordEvent(EventType.EXECUTION_START, "fp1", null, null, null);
        recordEvent(
            EventType.EXECUTION_SUCCESS, "fp1", null, 42L, null);
        recordEvent(
            EventType.EXECUTION_FAILED, "fp1",
            NativeSqlError.Classification.FALLBACK, 7L,
            "RuntimeException: boom");

        List<Rowset.Row> rows = populate(null);
        assertEquals(3, rows.size(),
            "three recorded events → three rows");

        // populateImpl iterates snapshot() in sequence-ASC order;
        // assert the wire sequence numbers come out 0, 1, 2 in that order.
        assertEquals(0L, ((Number) rows.get(0).get("EVENT_SEQUENCE")).longValue());
        assertEquals(1L, ((Number) rows.get(1).get("EVENT_SEQUENCE")).longValue());
        assertEquals(2L, ((Number) rows.get(2).get("EVENT_SEQUENCE")).longValue());

        // First event has no classification or duration → those columns
        // must be absent from the row payload.
        assertNull(rows.get(0).get("CLASSIFICATION"),
            "EXECUTION_START has no classification");
        assertNull(rows.get(0).get("DURATION_MS"),
            "EXECUTION_START has no duration");

        // Second event has duration but no classification.
        assertEquals(42L,
            ((Number) rows.get(1).get("DURATION_MS")).longValue());
        assertNull(rows.get(1).get("CLASSIFICATION"));

        // Third event has both classification and duration.
        assertEquals("FALLBACK", rows.get(2).get("CLASSIFICATION"));
        assertEquals(7L,
            ((Number) rows.get(2).get("DURATION_MS")).longValue());
        assertEquals("RuntimeException: boom", rows.get(2).get("MESSAGE"));
    }

    @Test
    public void populateImpl_fingerprintIdRestriction_filtersToMatchingFingerprint() {
        recordEvent(EventType.EXECUTION_START, "fp1", null, null, null);
        recordEvent(EventType.EXECUTION_START, "fp2", null, null, null);
        recordEvent(
            EventType.EXECUTION_SUCCESS, "fp1", null, 1L, null);
        recordEvent(
            EventType.EXECUTION_SUCCESS, "fp2", null, 2L, null);

        Map<String, Object> r = new LinkedHashMap<>();
        // singleStringRestriction accepts both List<String> and direct
        // String forms; the wire path uses List, so mirror that here.
        r.put("FINGERPRINT_ID", Collections.singletonList("fp1"));

        List<Rowset.Row> rows = populate(r);
        assertEquals(2, rows.size(),
            "FINGERPRINT_ID = fp1 keeps fp1 events only");
        for (Rowset.Row row : rows) {
            assertEquals("fp1", row.get("FINGERPRINT_ID"),
                "all surviving rows match the fingerprint filter");
        }
    }

    @Test
    public void populateImpl_eventTypeRestriction_filtersToMatchingType() {
        recordEvent(EventType.EXECUTION_START, "fp1", null, null, null);
        recordEvent(
            EventType.EXECUTION_SUCCESS, "fp1", null, 1L, null);
        recordEvent(
            EventType.EXECUTION_FAILED, "fp1",
            NativeSqlError.Classification.FALLBACK, 1L,
            "Err: x");

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("EVENT_TYPE",
            Collections.singletonList("EXECUTION_FAILED"));

        List<Rowset.Row> rows = populate(r);
        assertEquals(1, rows.size(),
            "EVENT_TYPE = EXECUTION_FAILED keeps the failed event only");
        assertEquals("EXECUTION_FAILED", rows.get(0).get("EVENT_TYPE"));
        assertEquals("FALLBACK", rows.get(0).get("CLASSIFICATION"));
    }

    @Test
    public void populateImpl_minEventSequenceRestriction_filtersOlderEvents() {
        for (int i = 0; i < 5; i++) {
            recordEvent(
                EventType.EXECUTION_START, "fp" + i, null, null, null);
        }

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("MIN_EVENT_SEQUENCE",
            Collections.singletonList("3"));

        List<Rowset.Row> rows = populate(r);
        assertEquals(2, rows.size(),
            "MIN_EVENT_SEQUENCE = 3 keeps sequences 3 and 4 only");
        assertEquals(3L,
            ((Number) rows.get(0).get("EVENT_SEQUENCE")).longValue());
        assertEquals(4L,
            ((Number) rows.get(1).get("EVENT_SEQUENCE")).longValue());
    }

    @Test
    public void populateImpl_combinedRestrictions_andCombines() {
        recordEvent(EventType.EXECUTION_START, "fp1", null, null, null);
        recordEvent(
            EventType.EXECUTION_SUCCESS, "fp1", null, 1L, null);
        recordEvent(EventType.EXECUTION_START, "fp2", null, null, null);
        recordEvent(
            EventType.EXECUTION_SUCCESS, "fp2", null, 1L, null);

        // FINGERPRINT_ID = fp1 AND EVENT_TYPE = EXECUTION_SUCCESS →
        // exactly one row (the second event recorded).
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("FINGERPRINT_ID", Collections.singletonList("fp1"));
        r.put("EVENT_TYPE",
            Collections.singletonList("EXECUTION_SUCCESS"));

        List<Rowset.Row> rows = populate(r);
        assertEquals(1, rows.size(),
            "AND-combined restrictions keep the single matching event");
        assertEquals("fp1", rows.get(0).get("FINGERPRINT_ID"));
        assertEquals("EXECUTION_SUCCESS", rows.get(0).get("EVENT_TYPE"));
    }

    @Test
    public void populateImpl_unknownEventTypeRestriction_emitsZeroRowsWithoutError() {
        recordEvent(EventType.EXECUTION_START, "fp1", null, null, null);
        recordEvent(
            EventType.EXECUTION_SUCCESS, "fp1", null, 1L, null);

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("EVENT_TYPE", Collections.singletonList("BOGUS"));

        List<Rowset.Row> rows = populate(r);
        assertEquals(0, rows.size(),
            "unknown EVENT_TYPE values must filter out all rows; "
            + "populateImpl must NOT throw");
    }

    @Test
    public void populateImpl_schemaVersionAlwaysOne() {
        recordEvent(EventType.EXECUTION_START, "fp1", null, null, null);
        recordEvent(
            EventType.EXECUTION_SUCCESS, "fp1", null, 1L, null);
        recordEvent(EventType.CACHED_SUCCESS_HIT, "fp1", null, null, null);

        List<Rowset.Row> rows = populate(null);
        assertEquals(3, rows.size());
        for (Rowset.Row row : rows) {
            assertEquals(1,
                ((Number) row.get("SCHEMA_VERSION")).intValue(),
                "v1 wire contract: SCHEMA_VERSION literal must be 1 "
                + "for every row");
        }
    }

    @Test
    public void populateImpl_nullFingerprintIdEventEmitsRowWithoutFingerprintIdField() {
        // Diagnostic events without a fingerprint context may pass
        // null for fingerprintId; the rowset must surface the row but
        // omit the FINGERPRINT_ID cell rather than emit a null literal.
        recordEvent(
            EventType.FINGERPRINT_KIND_VIOLATION, null, null, null,
            "kind=KIND_X expected=KIND_Y");

        List<Rowset.Row> rows = populate(null);
        assertEquals(1, rows.size(),
            "null-fingerprint event still surfaces as a row");
        Rowset.Row row = rows.get(0);
        assertNull(row.get("FINGERPRINT_ID"),
            "FINGERPRINT_ID column must be absent for null-fp events "
            + "(populateImpl skips row.set when fingerprintId is null)");
        assertEquals("FINGERPRINT_KIND_VIOLATION",
            row.get("EVENT_TYPE"),
            "EVENT_TYPE must still be populated");
        assertEquals(1,
            ((Number) row.get("SCHEMA_VERSION")).intValue());
        // Sanity: the row contains a sequence number and timestamp.
        assertNotNull(row.get("EVENT_SEQUENCE"));
        assertTrue(
            ((Number) row.get("EVENT_TIME_MS")).longValue() > 0L,
            "EVENT_TIME_MS captured from System.currentTimeMillis()");
    }

}
