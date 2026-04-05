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

import mondrian.olap.Annotation;
import junit.framework.TestCase;
import java.util.*;

public class NativeSqlConfigTest extends TestCase {

    public void testFromAnnotations_fullConfig() {
        Map<String, Annotation> anns = new LinkedHashMap<String, Annotation>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann(
            "SELECT ${axisExpr1} AS k1, sum(${factAlias}.${weightMeasure}) AS val "
            + "FROM ${factTable} ${factAlias} ${joinClauses} "
            + "WHERE ${whereClause} GROUP BY k1"));
        anns.put("nativeSql.variables", ann(
            "weightMeasure=sales_rub;multiplier=100"));
        anns.put("nativeSql.maxAxes", ann("2"));
        anns.put("nativeSql.fallbackMdx", ann("false"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("WD %", anns);

        assertNotNull(def);
        assertTrue(def.getTemplate().contains("${axisExpr1}"));
        assertEquals("sales_rub", def.getVariable("weightMeasure"));
        assertEquals("100", def.getVariable("multiplier"));
        assertEquals(2, def.getMaxAxes());
        assertFalse(def.isFallbackMdx());
    }

    public void testFromAnnotations_noAnnotations() {
        Map<String, Annotation> anns = Collections.emptyMap();
        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);
        assertNull(def);
    }

    public void testFromAnnotations_disabledExplicitly() {
        Map<String, Annotation> anns = new LinkedHashMap<String, Annotation>();
        anns.put("nativeSql.enabled", ann("false"));
        anns.put("nativeSql.template", ann("SELECT 1 AS val"));
        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);
        assertNull(def);
    }

    public void testFromAnnotations_defaults() {
        Map<String, Annotation> anns = new LinkedHashMap<String, Annotation>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT 1 AS val"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);

        assertNotNull(def);
        assertEquals(10, def.getMaxAxes());
        assertTrue(def.isFallbackMdx());
        assertTrue(def.getVariables().isEmpty());
    }

    private static Annotation ann(final String value) {
        return new Annotation() {
            public String getName() { return null; }
            public Object getValue() { return value; }
        };
    }
}
