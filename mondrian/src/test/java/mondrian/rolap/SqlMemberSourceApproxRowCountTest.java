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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlMemberSourceApproxRowCountTest {

    @Test public void testBuildApproxDistinctExprSingleKey() {
        assertEquals(
            "uniqHLL12(city_id)",
            SqlMemberSource.buildApproxDistinctExpr(
                Collections.singletonList("city_id")));
    }

    @Test public void testBuildApproxDistinctExprCompositeKey() {
        assertEquals(
            "uniqHLL12(tuple(region_id, city_id))",
            SqlMemberSource.buildApproxDistinctExpr(
                Arrays.asList("region_id", "city_id")));
    }

    @Test public void testBuildApproxDistinctExprEmptyInput() {
        assertEquals(
            "uniqHLL12(0)",
            SqlMemberSource.buildApproxDistinctExpr(
                Collections.<String>emptyList()));
    }
}
