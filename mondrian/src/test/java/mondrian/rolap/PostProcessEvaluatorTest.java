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

import junit.framework.TestCase;

import mondrian.calc.Calc;
import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.type.ScalarType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Tests for {@link PostProcessEvaluator} -- Calc-based Phase D.3
 * of NativeQueryEngine.
 *
 * <p>These tests verify the evaluateWithCalc dispatch layer:
 * null-safety, NaN/DoubleNull mapping, and exception handling.
 * Full integration (real Calc tree + ContextBackedCellReader) is
 * tested via the XMLA integration pack (q48, q49).
 */
public class PostProcessEvaluatorTest extends TestCase {

    // -----------------------------------------------------------------------
    // Null-safety
    // -----------------------------------------------------------------------

    /** Null calc returns null */
    public void testNullCalcReturnsNull() {
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        Object result = PostProcessEvaluator.evaluateWithCalc(
            null, evaluator, new NativeQueryResultContext(),
            Collections.<String, String>emptyMap(),
            null,
            Collections.<String, CoordinateClassPlan>emptyMap());
        assertNull(result);
    }

    /** Null evaluator returns null */
    public void testNullEvaluatorReturnsNull() {
        Calc calc = mock(Calc.class);
        Object result = PostProcessEvaluator.evaluateWithCalc(
            calc, null, new NativeQueryResultContext(),
            Collections.<String, String>emptyMap(),
            null,
            Collections.<String, CoordinateClassPlan>emptyMap());
        assertNull(result);
    }

    // -----------------------------------------------------------------------
    // Normal evaluation
    // -----------------------------------------------------------------------

    /** Calc that returns a numeric value passes through */
    public void testCalcReturnsNumericValue() {
        Calc calc = mock(Calc.class);
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapEvaluator child = mock(RolapEvaluator.class);
        when(evaluator.push()).thenReturn(child);
        when(calc.evaluate(child)).thenReturn(42.0);

        Object result = PostProcessEvaluator.evaluateWithCalc(
            calc, evaluator, new NativeQueryResultContext(),
            Collections.<String, String>emptyMap(),
            null,
            Collections.<String, CoordinateClassPlan>emptyMap());

        assertNotNull(result);
        assertEquals(42.0, ((Number) result).doubleValue(), 0.001);
    }

    // -----------------------------------------------------------------------
    // NaN / DoubleNull handling
    // -----------------------------------------------------------------------

    /** Calc that returns NaN → null */
    public void testCalcReturnsNaNMapsToNull() {
        Calc calc = mock(Calc.class);
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapEvaluator child = mock(RolapEvaluator.class);
        when(evaluator.push()).thenReturn(child);
        when(calc.evaluate(child)).thenReturn(Double.NaN);

        Object result = PostProcessEvaluator.evaluateWithCalc(
            calc, evaluator, new NativeQueryResultContext(),
            Collections.<String, String>emptyMap(),
            null,
            Collections.<String, CoordinateClassPlan>emptyMap());

        assertNull("NaN should map to null", result);
    }

    /** Calc that returns FunUtil.DoubleNull → null */
    public void testCalcReturnsDoubleNullMapsToNull() {
        Calc calc = mock(Calc.class);
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapEvaluator child = mock(RolapEvaluator.class);
        when(evaluator.push()).thenReturn(child);
        when(calc.evaluate(child)).thenReturn(FunUtil.DoubleNull);

        Object result = PostProcessEvaluator.evaluateWithCalc(
            calc, evaluator, new NativeQueryResultContext(),
            Collections.<String, String>emptyMap(),
            null,
            Collections.<String, CoordinateClassPlan>emptyMap());

        assertNull("DoubleNull should map to null", result);
    }

    // -----------------------------------------------------------------------
    // Exception handling
    // -----------------------------------------------------------------------

    /** Calc that throws → null (logged as warning) */
    public void testCalcExceptionReturnsNull() {
        Calc calc = mock(Calc.class);
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapEvaluator child = mock(RolapEvaluator.class);
        when(evaluator.push()).thenReturn(child);
        when(calc.evaluate(child)).thenThrow(
            new RuntimeException("test failure"));

        Object result = PostProcessEvaluator.evaluateWithCalc(
            calc, evaluator, new NativeQueryResultContext(),
            Collections.<String, String>emptyMap(),
            null,
            Collections.<String, CoordinateClassPlan>emptyMap());

        assertNull("Exception should map to null", result);
    }

    // -----------------------------------------------------------------------
    // CellReader wiring verification
    // -----------------------------------------------------------------------

    /** Verifies setCellReader is called on the child evaluator */
    public void testChildEvaluatorGetsCellReader() {
        Calc calc = mock(Calc.class);
        RolapEvaluator evaluator = mock(RolapEvaluator.class);
        RolapEvaluator child = mock(RolapEvaluator.class);
        when(evaluator.push()).thenReturn(child);
        when(calc.evaluate(child)).thenReturn(99.0);

        PostProcessEvaluator.evaluateWithCalc(
            calc, evaluator, new NativeQueryResultContext(),
            Collections.<String, String>emptyMap(),
            null,
            Collections.<String, CoordinateClassPlan>emptyMap());

        // Verify push was called (child isolation)
        verify(evaluator).push();
        // Verify setCellReader was called on the child
        verify(child).setCellReader(any(ContextBackedCellReader.class));
    }
}

// End PostProcessEvaluatorTest.java
