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
import mondrian.olap.*;

import java.util.*;

import static org.mockito.Mockito.*;

public class NativeNonEmptyFilterTest extends TestCase {

    public void testReturnsNullWhenDisabled() {
        // Feature flag off (default) → returns null (fallback)
        assertNull(NativeNonEmptyFilter.tryPrune(null, null, null));
    }

    public void testReturnsNullForEmptyCandidates() {
        MondrianProperties.instance().NativeNonEmptyFilterEnable.set(true);
        try {
            RolapEvaluator evaluator = mock(RolapEvaluator.class);
            mondrian.calc.TupleList emptyList =
                mock(mondrian.calc.TupleList.class);
            when(emptyList.isEmpty()).thenReturn(true);

            assertNull(NativeNonEmptyFilter.tryPrune(
                evaluator, emptyList, Collections.<Member>emptySet()));
        } finally {
            MondrianProperties.instance().NativeNonEmptyFilterEnable.set(false);
        }
    }
}
