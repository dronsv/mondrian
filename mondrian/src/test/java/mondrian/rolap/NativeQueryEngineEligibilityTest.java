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

import mondrian.olap.Member;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link NativeQueryEngine} eligibility (Phase A classification).
 * Tests operate at the MeasureClassifier level so no full engine wiring is needed.
 */
public class NativeQueryEngineEligibilityTest {

    @Test
    public void testAllStoredMeasuresEligible() {
        Set<Member> measures = new LinkedHashSet<Member>();
        measures.add(mockStoredMeasure("sales_qty"));
        measures.add(mockStoredMeasure("sales_rub"));

        List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        assertNotNull(candidates, "All stored measures should be eligible");
        assertEquals(2, candidates.size());
    }

    @Test
    public void testUnsupportedCalcPoisonsQuery() {
        Set<Member> measures = new LinkedHashSet<Member>();
        measures.add(mockStoredMeasure("sales_qty"));
        measures.add(mockEvaluatorMeasure("complex"));

        List<MeasureClassifier.Candidate> candidates =
            MeasureClassifier.classifyAll(measures);
        assertNull(candidates, "Evaluator measure should poison entire query");
    }

    private Member mockStoredMeasure(String name) {
        Member m = mock(Member.class);
        when(m.getName()).thenReturn(name);
        when(m.isMeasure()).thenReturn(true);
        when(m.isCalculated()).thenReturn(false);
        return m;
    }

    private Member mockEvaluatorMeasure(String name) {
        Member m = mock(Member.class);
        when(m.getName()).thenReturn(name);
        when(m.isMeasure()).thenReturn(true);
        when(m.isCalculated()).thenReturn(true);
        when(m.getExpression()).thenReturn(null);
        return m;
    }
}

// End NativeQueryEngineEligibilityTest.java
