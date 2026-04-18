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

import mondrian.rolap.agg.CellRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link PrefetchKeyBuilder} identity symmetry and
 * normalisation contracts.
 */
public class PrefetchKeyBuilderTest {

    private PrefetchKeyBuilder builder;

    @BeforeEach
    public void setUp() {
        builder = new PrefetchKeyBuilder();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Creates a mock CellRequest with the given measure bit-position and single values. */
    private static CellRequest mockRequest(
        int measureBitPos,
        Object[] singleValues)
    {
        RolapStar.Measure measure = mock(RolapStar.Measure.class);
        when(measure.getBitPosition()).thenReturn(measureBitPos);

        CellRequest request = mock(CellRequest.class);
        when(request.getMeasure()).thenReturn(measure);
        when(request.getSingleValues()).thenReturn(singleValues);
        return request;
    }

    // -----------------------------------------------------------------------
    // Symmetry tests
    // -----------------------------------------------------------------------

    /**
     * Same cell described via CellRequest and via NQE row must produce
     * identical keys (both hash and equals).
     */
    @Test
    public void testSymmetry_sameCell_producesEqualKeys() {
        Object[] values = { "brand-A", 42L, "2024-01" };
        PrefetchKey fromRequest = builder.fromCellRequest(mockRequest(7, values));
        PrefetchKey fromRow    = builder.fromNqeRow(7, values.clone());
        assertEquals(fromRequest, fromRow,
            "Keys from CellRequest and NQE row must be equal for the same cell");
        assertEquals(fromRequest.hashCode(), fromRow.hashCode(),
            "Hash codes must match");
    }

    // -----------------------------------------------------------------------
    // Integer / Long unification
    // -----------------------------------------------------------------------

    /**
     * Integer from one side, Long from the other — must normalise to equal keys.
     */
    @Test
    public void testIntegerLongUnification_producesEqualKeys() {
        // CellRequest side uses Integer; NQE row side uses Long
        Object[] intValues  = { "brand-A", 42 };    // Integer
        Object[] longValues = { "brand-A", 42L };   // Long

        PrefetchKey fromRequest = builder.fromCellRequest(mockRequest(3, intValues));
        PrefetchKey fromRow    = builder.fromNqeRow(3, longValues);
        assertEquals(fromRequest, fromRow,
            "Integer(42) must equal Long(42) after normalisation");
    }

    // -----------------------------------------------------------------------
    // Different measures → different keys
    // -----------------------------------------------------------------------

    @Test
    public void testDifferentMeasures_produceDifferentKeys() {
        Object[] values = { "brand-A", 1L };
        PrefetchKey k1 = builder.fromNqeRow(1, values);
        PrefetchKey k2 = builder.fromNqeRow(2, values.clone());
        assertNotEquals(k1, k2,
            "Different measure bit-positions must produce different keys");
    }

    // -----------------------------------------------------------------------
    // Different values → different keys
    // -----------------------------------------------------------------------

    @Test
    public void testDifferentValues_produceDifferentKeys() {
        PrefetchKey k1 = builder.fromNqeRow(5, new Object[]{ "brand-A", 1L });
        PrefetchKey k2 = builder.fromNqeRow(5, new Object[]{ "brand-B", 1L });
        assertNotEquals(k1, k2,
            "Different constrained values must produce different keys");
    }

    // -----------------------------------------------------------------------
    // Null value in array
    // -----------------------------------------------------------------------

    @Test
    public void testNullValueInArray_handledGracefully() {
        Object[] values = { "brand-A", null, "2024-01" };
        PrefetchKey k1 = builder.fromNqeRow(4, values);
        PrefetchKey k2 = builder.fromNqeRow(4, values.clone());
        assertEquals(k1, k2,
            "Keys with null element must be stable and equal");
    }

    // -----------------------------------------------------------------------
    // Empty values array
    // -----------------------------------------------------------------------

    @Test
    public void testEmptyValues_producesStableKey() {
        PrefetchKey k1 = builder.fromNqeRow(0, new Object[0]);
        PrefetchKey k2 = builder.fromNqeRow(0, new Object[0]);
        assertEquals(k1, k2, "Empty value arrays must produce equal keys");
    }

    @Test
    public void testNullValuesArray_treatedAsEmpty() {
        PrefetchKey k1 = builder.fromNqeRow(0, null);
        PrefetchKey k2 = builder.fromNqeRow(0, new Object[0]);
        assertEquals(k1, k2,
            "null values array must be treated the same as empty array");
    }

    // -----------------------------------------------------------------------
    // MISS sentinel distinctness
    // -----------------------------------------------------------------------

    @Test
    public void testMissSentinel_isNotNull() {
        assertNotNull(PrefetchKey.MISS, "MISS sentinel must not be null");
    }

    @Test
    public void testMissSentinel_isDistinctFromPrefetchKey() {
        PrefetchKey key = builder.fromNqeRow(1, new Object[]{ "x" });
        assertNotSame(PrefetchKey.MISS, key,
            "MISS sentinel must be a distinct object from a real key");
        assertFalse(PrefetchKey.MISS.equals(key),
            "MISS sentinel must not equal a PrefetchKey");
    }

    @Test
    public void testMissSentinel_sameReference() {
        // Multiple accesses to MISS must return the same singleton
        assertSame(PrefetchKey.MISS, PrefetchKey.MISS);
    }
}

// End PrefetchKeyBuilderTest.java
