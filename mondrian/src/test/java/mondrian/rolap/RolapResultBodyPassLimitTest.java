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

public class RolapResultBodyPassLimitTest extends TestCase {

    public void testKeepsLegacyLimitForSmallQueries() {
        assertEquals(
            50,
            RolapResult.computeBodyPassLimit(50, new int[] {5, 6}, 1));
    }

    public void testScalesBudgetForMediumQueries() {
        assertEquals(
            121,
            RolapResult.computeBodyPassLimit(50, new int[] {5, 115}, 1));
    }

    public void testCapsBudgetForLargeQueries() {
        assertEquals(
            1000,
            RolapResult.computeBodyPassLimit(50, new int[] {5, 11776}, 1));
    }
}
