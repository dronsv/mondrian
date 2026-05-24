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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Smoke tests for the {@link PropertyProjectionDiagnostic} emitter
 * (issue #21 — P1 instrumentation). Verifies that emission methods are
 * safe to call with edge-case inputs (null level, null props, empty
 * props) and that the public surface is stable. Log-output format
 * verification is left to integration tests; here we ensure the
 * emitter never throws and never costs more than the gating check
 * when the log category is disabled.
 */
public class PropertyProjectionDiagnosticTest {

    @Test
    public void enumsHaveExpectedMembers() {
        // Stability check — the reason codes and site names are part of
        // the observability contract documented in issue #21; the
        // follow-up #22 optimisation expects to find these in logs.
        assertNotNull(
            PropertyProjectionDiagnostic.ReaderSite.valueOf("TUPLE_READER"));
        assertNotNull(
            PropertyProjectionDiagnostic.ReaderSite
                .valueOf("MEMBER_SOURCE_KEYS_SQL"));
        assertNotNull(
            PropertyProjectionDiagnostic.ReaderSite
                .valueOf("MEMBER_SOURCE_CHILD_MEMBER_SQL"));
        assertNotNull(
            PropertyProjectionDiagnostic.ReaderSite
                .valueOf("MEMBER_SOURCE_ADD_LEVEL"));
        assertNotNull(
            PropertyProjectionDiagnostic.ReaderSite
                .valueOf("MEMBER_SOURCE_CHILD_MEMBER_SQL_PC"));
        assertNotNull(
            PropertyProjectionDiagnostic.Reason
                .valueOf("LEVEL_PROPERTY_EAGER_DEFAULT"));
        assertNotNull(
            PropertyProjectionDiagnostic.Reason
                .valueOf("DIMENSION_PROPERTIES_REQUESTED"));
    }

    @Test
    public void isEnabledIsCallable() {
        // No assertion on the return value — the test runtime's log4j
        // config decides it. Just ensures the gate method is wired.
        assertDoesNotThrow(PropertyProjectionDiagnostic::isEnabled);
    }

    @Test
    public void recordEagerLevelProperties_nullLevelIsNoOp() {
        // Level can be null in degenerate test setups; emitter must
        // never throw from observability code.
        assertDoesNotThrow(() ->
            PropertyProjectionDiagnostic.recordEagerLevelProperties(
                PropertyProjectionDiagnostic.ReaderSite.TUPLE_READER,
                null, new RolapProperty[0]));
    }

    @Test
    public void recordEagerLevelProperties_nullPropertiesIsNoOp() {
        RolapLevel level = mock(RolapLevel.class);
        lenient().when(level.getUniqueName()).thenReturn("[L]");
        assertDoesNotThrow(() ->
            PropertyProjectionDiagnostic.recordEagerLevelProperties(
                PropertyProjectionDiagnostic.ReaderSite.TUPLE_READER,
                level, null));
    }

    @Test
    public void recordEagerLevelProperties_emptyPropertiesIsNoOp() {
        RolapLevel level = mock(RolapLevel.class);
        lenient().when(level.getUniqueName()).thenReturn("[L]");
        assertDoesNotThrow(() ->
            PropertyProjectionDiagnostic.recordEagerLevelProperties(
                PropertyProjectionDiagnostic.ReaderSite.TUPLE_READER,
                level, new RolapProperty[0]));
    }

    @Test
    public void recordEagerLevelProperties_normalPathDoesNotThrow() {
        RolapLevel level = mock(RolapLevel.class);
        when(level.getUniqueName()).thenReturn("[Product].[SKU]");
        RolapProperty p1 = mock(RolapProperty.class);
        when(p1.getName()).thenReturn("GTIN");
        RolapProperty p2 = mock(RolapProperty.class);
        when(p2.getName()).thenReturn("Brand");
        assertDoesNotThrow(() ->
            PropertyProjectionDiagnostic.recordEagerLevelProperties(
                PropertyProjectionDiagnostic.ReaderSite.TUPLE_READER,
                level, new RolapProperty[] { p1, p2 }));
    }

    @Test
    public void recordDimensionPropertiesRequested_nullAndEmptyAreNoOp() {
        // Both null axis label and null property list must be tolerated
        // — defensive because Query.axes can have unusual shapes.
        assertDoesNotThrow(() ->
            PropertyProjectionDiagnostic
                .recordDimensionPropertiesRequested(null, null));
        assertDoesNotThrow(() ->
            PropertyProjectionDiagnostic
                .recordDimensionPropertiesRequested("ROWS",
                    Collections.emptyList()));
        assertDoesNotThrow(() ->
            PropertyProjectionDiagnostic
                .recordDimensionPropertiesRequested("ROWS",
                    Arrays.asList("[Product].[SKU].[GTIN]",
                        "[Product].[SKU].[Brand]")));
    }
}

// End PropertyProjectionDiagnosticTest.java
