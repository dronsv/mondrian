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

/**
 * Tests for {@link NativeSqlCalc} template substitution logic.
 */
public class NativeSqlCalcTest extends TestCase {

    public void testSubstitutePlaceholders_basic() {
        String template =
            "SELECT ${axisExpr1} AS k1, sum(${factAlias}.${wt}) AS val "
            + "FROM ${factTable} ${factAlias} "
            + "${joinClauses} WHERE ${whereClause} GROUP BY k1";

        Map<String, String> placeholders = new LinkedHashMap<String, String>();
        placeholders.put("factTable", "mart_konfet_monthly");
        placeholders.put("factAlias", "f");
        placeholders.put("joinClauses",
            "JOIN dim_konfet_store s ON f.store_key = s.store_key");
        placeholders.put("whereClause", "s.chain_group = 'Магнит'");
        placeholders.put("axisExpr1", "p.manufacturer_group");
        placeholders.put("axisCount", "1");
        placeholders.put("wt", "sales_rub");

        String result = NativeSqlCalc.substitutePlaceholders(
            template, placeholders);

        assertTrue(result.contains("mart_konfet_monthly f"));
        assertTrue(result.contains("p.manufacturer_group AS k1"));
        assertTrue(result.contains("sum(f.sales_rub)"));
        assertTrue(result.contains("s.chain_group = 'Магнит'"));
        assertFalse(result.contains("${"));
    }

    public void testSubstitutePlaceholders_emptyWhere() {
        String template = "WHERE ${whereClause}";
        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("whereClause", "1 = 1");
        String result = NativeSqlCalc.substitutePlaceholders(template, ph);
        assertEquals("WHERE 1 = 1", result);
    }

    public void testSubstitutePlaceholders_unresolvedPlaceholder() {
        String template = "SELECT ${axisExpr3} AS k3";
        Map<String, String> ph = Collections.emptyMap();
        try {
            NativeSqlCalc.substitutePlaceholders(template, ph);
            fail("Expected exception for unresolved placeholder");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("axisExpr3"));
        }
    }

    public void testSubstitutePlaceholders_multipleAxes() {
        String template =
            "SELECT ${axisExpr1} AS k1, ${axisExpr2} AS k2, "
            + "sum(${factAlias}.val) AS val "
            + "FROM ${factTable} ${factAlias} "
            + "GROUP BY k1, k2";

        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("factTable", "fact_sales");
        ph.put("factAlias", "f");
        ph.put("axisExpr1", "f.region");
        ph.put("axisExpr2", "d.brand_name");

        String result = NativeSqlCalc.substitutePlaceholders(template, ph);

        assertTrue(result.contains("f.region AS k1"));
        assertTrue(result.contains("d.brand_name AS k2"));
        assertTrue(result.contains("fact_sales f"));
        assertFalse(result.contains("${"));
    }

    public void testSubstitutePlaceholders_noPlaceholders() {
        String template = "SELECT 1 AS val";
        Map<String, String> ph = Collections.emptyMap();
        String result = NativeSqlCalc.substitutePlaceholders(template, ph);
        assertEquals("SELECT 1 AS val", result);
    }

    public void testSubstitutePlaceholders_duplicatePlaceholder() {
        String template =
            "SELECT ${col} AS k1, sum(${col}) AS val FROM t";
        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("col", "amount");

        String result = NativeSqlCalc.substitutePlaceholders(template, ph);

        assertEquals(
            "SELECT amount AS k1, sum(amount) AS val FROM t",
            result);
    }

    public void testSubstitutePlaceholders_staticVariables() {
        String template =
            "SELECT ${axisExpr1} AS k1, "
            + "sum(${factAlias}.${weightMeasure}) * ${multiplier} AS val "
            + "FROM ${factTable} ${factAlias}";

        Map<String, String> ph = new LinkedHashMap<String, String>();
        ph.put("factTable", "mart_weekly");
        ph.put("factAlias", "f");
        ph.put("axisExpr1", "f.brand");
        ph.put("weightMeasure", "sales_rub");
        ph.put("multiplier", "100");

        String result = NativeSqlCalc.substitutePlaceholders(template, ph);

        assertTrue(result.contains("sum(f.sales_rub) * 100"));
        assertTrue(result.contains("f.brand AS k1"));
    }

    public void testFormatLiteral_number() {
        assertEquals("42", NativeSqlCalc.formatLiteral(42));
        assertEquals("3.14", NativeSqlCalc.formatLiteral(3.14));
    }

    public void testFormatLiteral_string() {
        assertEquals("'hello'", NativeSqlCalc.formatLiteral("hello"));
    }

    public void testFormatLiteral_stringWithQuotes() {
        assertEquals("'it''s'", NativeSqlCalc.formatLiteral("it's"));
    }

    public void testFormatLiteral_null() {
        assertEquals("NULL", NativeSqlCalc.formatLiteral(null));
    }

    public void testFormatLiteral_cyrillic() {
        assertEquals("'Магнит'", NativeSqlCalc.formatLiteral("Магнит"));
    }
}
