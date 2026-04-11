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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RolapNativeTopCountLimitPushdownTest {

    @Test public void testLimitPushdownEnabledForSafeShape() {
        assertNull(
            RolapNativeTopCount.TopCountConstraint
                .getRowLimitPushdownSkipReason(10, true, false));
    }

    @Test public void testLimitPushdownSkippedForNonPositiveCount() {
        assertEquals(
            "count<=0",
            RolapNativeTopCount.TopCountConstraint
                .getRowLimitPushdownSkipReason(0, true, false));
    }

    @Test public void testLimitPushdownSkippedWhenDialectDoesNotNeedClause() {
        assertEquals(
            "dialect-limit-clause-disabled",
            RolapNativeTopCount.TopCountConstraint
                .getRowLimitPushdownSkipReason(10, false, false));
    }

    @Test public void testLimitPushdownSkippedForVirtualCubeQuery() {
        assertEquals(
            "virtual-cube-union-path",
            RolapNativeTopCount.TopCountConstraint
                .getRowLimitPushdownSkipReason(10, true, true));
    }
}
