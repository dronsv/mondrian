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
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class NativeSqlConfigTest {

    @Test public void testFromAnnotations_fullConfig() {
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

    @Test public void testFromAnnotations_noAnnotations() {
        Map<String, Annotation> anns = Collections.emptyMap();
        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);
        assertNull(def);
    }

    @Test public void testFromAnnotations_disabledExplicitly() {
        Map<String, Annotation> anns = new LinkedHashMap<String, Annotation>();
        anns.put("nativeSql.enabled", ann("false"));
        anns.put("nativeSql.template", ann("SELECT 1 AS val"));
        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);
        assertNull(def);
    }

    @Test public void testFromAnnotations_defaults() {
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

    @Test public void testFromAnnotations_singleTemplate_backCompat() {
        Map<String, Annotation> anns = new LinkedHashMap<>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT 1 AS val FROM agg_table"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);

        assertNotNull(def);
        assertEquals(1, def.getTemplates().size());
        assertEquals("SELECT 1 AS val FROM agg_table", def.getTemplates().get(0));
        assertEquals("SELECT 1 AS val FROM agg_table", def.getTemplate());
    }

    @Test public void testFromAnnotations_multipleTemplates() {
        Map<String, Annotation> anns = new LinkedHashMap<>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT ${sku_key} FROM agg_table"));
        anns.put("nativeSql.template.1", ann("SELECT ${sku_key} FROM fact_table"));
        anns.put("nativeSql.template.2", ann("SELECT 1 FROM fallback_table"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("WD", anns);

        assertNotNull(def);
        assertEquals(3, def.getTemplates().size());
        assertEquals("SELECT ${sku_key} FROM agg_table", def.getTemplates().get(0));
        assertEquals("SELECT ${sku_key} FROM fact_table", def.getTemplates().get(1));
        assertEquals("SELECT 1 FROM fallback_table", def.getTemplates().get(2));
    }

    @Test public void testFromAnnotations_gapInNumberingStopsCollection() {
        Map<String, Annotation> anns = new LinkedHashMap<>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT 1 FROM t0"));
        anns.put("nativeSql.template.1", ann("SELECT 1 FROM t1"));
        anns.put("nativeSql.template.3", ann("SELECT 1 FROM t3"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);

        assertNotNull(def);
        assertEquals(2, def.getTemplates().size());
    }

    @Test public void testFromAnnotations_emptyFallbackTemplateStops() {
        Map<String, Annotation> anns = new LinkedHashMap<>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT 1 FROM t0"));
        anns.put("nativeSql.template.1", ann("  "));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);

        assertNotNull(def);
        assertEquals(1, def.getTemplates().size());
    }

    @Test public void testFromAnnotations_defaultRelationAlias() {
        Map<String, Annotation> anns = new LinkedHashMap<>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT ${axisResultSelectList} val FROM (...) pr"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);

        assertNotNull(def);
        assertEquals("pr", def.getRelationAlias());
    }

    @Test public void testFromAnnotations_customRelationAlias() {
        Map<String, Annotation> anns = new LinkedHashMap<>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT ${axisResultSelectList} val FROM agg_table goods"));
        anns.put("nativeSql.relationAlias", ann("goods"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);

        assertNotNull(def);
        assertEquals("goods", def.getRelationAlias());
    }

    @Test public void testFromAnnotations_scalarMode() {
        Map<String, Annotation> anns = new LinkedHashMap<>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT count(*) AS val FROM dim_konfet_store"));
        anns.put("nativeSql.scalar", ann("true"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("ОКБ", anns);

        assertNotNull(def);
        assertTrue(def.isScalar());
    }

    @Test public void testFromAnnotations_scalarDefaultFalse() {
        Map<String, Annotation> anns = new LinkedHashMap<>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT 1 AS val"));

        NativeSqlConfig.NativeSqlDef def =
            NativeSqlConfig.fromAnnotations("Test", anns);

        assertNotNull(def);
        assertFalse(def.isScalar());
    }

    private static Annotation ann(final String value) {
        return new Annotation() {
            @Override public String getName() { return null; }

            @Override public Object getValue() { return value; }
        };
    }
}
