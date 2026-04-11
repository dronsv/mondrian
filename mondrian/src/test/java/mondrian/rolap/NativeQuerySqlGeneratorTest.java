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

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class NativeQuerySqlGeneratorTest {

    @Test public void testExtractSimpleNameFromUniqueName() {
        assertEquals("Sales Qty",
            NativeQuerySqlGenerator.extractSimpleName("[Measures].[Sales Qty]"));
        assertEquals("AKB",
            NativeQuerySqlGenerator.extractSimpleName("[Measures].[AKB]"));
        assertNull(
            NativeQuerySqlGenerator.extractSimpleName(null));
        assertEquals("plain",
            NativeQuerySqlGenerator.extractSimpleName("plain"));
    }

    @Test public void testExtractSimpleNameNestedBrackets() {
        // Unique name like [Dim].[Hier].[Level]
        assertEquals("Level",
            NativeQuerySqlGenerator.extractSimpleName("[Dim].[Hier].[Level]"));
    }

    @Test public void testExtractSimpleNameEmptyBrackets() {
        assertEquals("",
            NativeQuerySqlGenerator.extractSimpleName("[Measures].[]"));
    }

    @Test public void testEncodeProjectedKeyEmpty() {
        assertEquals("",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Collections.emptyList()));
    }

    @Test public void testEncodeProjectedKeySingleValue() {
        assertEquals("Brand1",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Collections.singletonList("Brand1")));
    }

    @Test public void testEncodeProjectedKeyMultipleValues() {
        assertEquals("Brand1\0" + "2025\0" + "Category3",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Arrays.asList("Brand1", "2025", "Category3")));
    }

    @Test public void testEncodeProjectedKeyWithNullValues() {
        assertEquals("Brand1\0" + "null\0" + "2025",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Arrays.asList("Brand1", null, "2025")));
    }

    @Test public void testEncodeProjectedKeyWithNumbers() {
        assertEquals("42\0" + "3.14",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Arrays.asList(42, 3.14)));
    }

    @Test public void testEncodeProjectedKeyWithPipeInValue() {
        // Pipe characters in values are preserved (no collision with
        // separator since we now use \0)
        assertEquals("A|B\0" + "C|D",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Arrays.asList("A|B", "C|D")));
    }

    @Test public void testGenerateSqlReturnsNullForEmptyRequests() {
        // Can't construct full objects without Mondrian runtime,
        // but we can test via the plan with empty requests list
        CoordinateClassPlan plan = new CoordinateClassPlan(
            "empty", Collections.<PhysicalValueRequest>emptyList());

        NativeQuerySqlGenerator gen = new NativeQuerySqlGenerator(null, null);
        assertNull(gen.generateSql(plan));
    }

    @Test public void testGenerateSqlReturnsNullForNativeTemplateWithoutCube() {
        // NATIVE_TEMPLATE requires a baseCube with star for placeholder
        // resolution. With null baseCube, generateSql returns null.
        Set<mondrian.olap.Hierarchy> projected = new LinkedHashSet<mondrian.olap.Hierarchy>();
        PhysicalValueRequest req = new PhysicalValueRequest(
            "[Measures].[Sales]",
            projected,
            null,
            PhysicalValueRequest.AggregationKind.NATIVE_EXPRESSION,
            PhysicalValueRequest.ExpressionProviderKind.NATIVE_TEMPLATE,
            "SELECT brand, SUM(qty) FROM fact GROUP BY brand");

        CoordinateClassPlan plan = new CoordinateClassPlan(
            "native1", Collections.singletonList(req));

        NativeQuerySqlGenerator gen = new NativeQuerySqlGenerator(null, null);
        assertNull(gen.generateSql(plan));
    }
}
