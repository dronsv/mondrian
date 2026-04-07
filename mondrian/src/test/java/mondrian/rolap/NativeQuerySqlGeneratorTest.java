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

import java.util.*;

public class NativeQuerySqlGeneratorTest extends TestCase {

    public void testExtractSimpleNameFromUniqueName() {
        assertEquals("Sales Qty",
            NativeQuerySqlGenerator.extractSimpleName("[Measures].[Sales Qty]"));
        assertEquals("AKB",
            NativeQuerySqlGenerator.extractSimpleName("[Measures].[AKB]"));
        assertNull(
            NativeQuerySqlGenerator.extractSimpleName(null));
        assertEquals("plain",
            NativeQuerySqlGenerator.extractSimpleName("plain"));
    }

    public void testExtractSimpleNameNestedBrackets() {
        // Unique name like [Dim].[Hier].[Level]
        assertEquals("Level",
            NativeQuerySqlGenerator.extractSimpleName("[Dim].[Hier].[Level]"));
    }

    public void testExtractSimpleNameEmptyBrackets() {
        assertEquals("",
            NativeQuerySqlGenerator.extractSimpleName("[Measures].[]"));
    }

    public void testEncodeProjectedKeyEmpty() {
        assertEquals("",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Collections.emptyList()));
    }

    public void testEncodeProjectedKeySingleValue() {
        assertEquals("Brand1",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Collections.singletonList("Brand1")));
    }

    public void testEncodeProjectedKeyMultipleValues() {
        assertEquals("Brand1\0" + "2025\0" + "Category3",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Arrays.asList("Brand1", "2025", "Category3")));
    }

    public void testEncodeProjectedKeyWithNullValues() {
        assertEquals("Brand1\0" + "null\0" + "2025",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Arrays.asList("Brand1", null, "2025")));
    }

    public void testEncodeProjectedKeyWithNumbers() {
        assertEquals("42\0" + "3.14",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Arrays.asList(42, 3.14)));
    }

    public void testEncodeProjectedKeyWithPipeInValue() {
        // Pipe characters in values are preserved (no collision with
        // separator since we now use \0)
        assertEquals("A|B\0" + "C|D",
            NativeQuerySqlGenerator.encodeProjectedKey(
                Arrays.asList("A|B", "C|D")));
    }

    public void testGenerateSqlReturnsNullForEmptyRequests() {
        // Can't construct full objects without Mondrian runtime,
        // but we can test via the plan with empty requests list
        CoordinateClassPlan plan = new CoordinateClassPlan(
            "empty", Collections.<PhysicalValueRequest>emptyList());

        NativeQuerySqlGenerator gen = new NativeQuerySqlGenerator(null, null);
        assertNull(gen.generateSql(plan));
    }

    public void testGenerateSqlReturnsNullForNativeTemplateWithoutCube() {
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
