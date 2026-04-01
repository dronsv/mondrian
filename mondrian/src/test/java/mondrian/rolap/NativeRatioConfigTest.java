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
import mondrian.olap.Annotation;
import java.util.*;

public class NativeRatioConfigTest extends TestCase {

    private static Map<String, Annotation> makeAnnotations(Map<String, String> values) {
        Map<String, Annotation> result = new LinkedHashMap<String, Annotation>();
        for (final Map.Entry<String, String> e : values.entrySet()) {
            result.put(e.getKey(), new Annotation() {
                public String getName() { return e.getKey(); }
                public Object getValue() { return e.getValue(); }
            });
        }
        return result;
    }

    public void testFromAnnotations_fullConfig() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("nativeRatio.numerator", "WD numerator cat");
        values.put("nativeRatio.denominator", "Продажи руб");
        values.put("nativeRatio.denominator.reset", "Продукт.Бренд,ТТ.Адрес");
        values.put("nativeRatio.multiplier", "100");
        values.put("nativeRatio.nullIfDenominatorZero", "true");

        RolapCalculatedMember member =
            org.mockito.Mockito.mock(RolapCalculatedMember.class);
        org.mockito.Mockito.when(member.getName()).thenReturn("WD %");
        org.mockito.Mockito.when(member.getAnnotationMap())
            .thenReturn(makeAnnotations(values));

        NativeRatioConfig.RatioMeasureDef def =
            NativeRatioConfig.fromAnnotations(member);
        assertNotNull(def);
        assertEquals("WD numerator cat", def.getNumeratorMeasureName());
        assertEquals("Продажи руб", def.getDenominatorMeasureName());
        assertEquals(100.0, def.getMultiplier());
        assertTrue(def.isNullIfDenominatorZero());
        assertEquals(2, def.getResetHierarchyNames().size());
        assertEquals("Продукт.Бренд", def.getResetHierarchyNames().get(0));
        assertEquals("ТТ.Адрес", def.getResetHierarchyNames().get(1));
    }

    public void testFromAnnotations_noAnnotations() {
        RolapCalculatedMember member =
            org.mockito.Mockito.mock(RolapCalculatedMember.class);
        org.mockito.Mockito.when(member.getAnnotationMap())
            .thenReturn(Collections.<String, Annotation>emptyMap());

        assertNull(NativeRatioConfig.fromAnnotations(member));
    }

    public void testFromAnnotations_missingDenominator() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("nativeRatio.numerator", "WD numerator cat");
        // no denominator

        RolapCalculatedMember member =
            org.mockito.Mockito.mock(RolapCalculatedMember.class);
        org.mockito.Mockito.when(member.getName()).thenReturn("Test");
        org.mockito.Mockito.when(member.getAnnotationMap())
            .thenReturn(makeAnnotations(values));

        assertNull(NativeRatioConfig.fromAnnotations(member));
    }

    public void testFromAnnotations_defaults() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("nativeRatio.numerator", "num");
        values.put("nativeRatio.denominator", "denom");

        RolapCalculatedMember member =
            org.mockito.Mockito.mock(RolapCalculatedMember.class);
        org.mockito.Mockito.when(member.getName()).thenReturn("Test");
        org.mockito.Mockito.when(member.getAnnotationMap())
            .thenReturn(makeAnnotations(values));

        NativeRatioConfig.RatioMeasureDef def =
            NativeRatioConfig.fromAnnotations(member);
        assertNotNull(def);
        assertEquals(1.0, def.getMultiplier());
        assertTrue(def.isNullIfDenominatorZero());
        assertTrue(def.getResetHierarchyNames().isEmpty());
    }
}
