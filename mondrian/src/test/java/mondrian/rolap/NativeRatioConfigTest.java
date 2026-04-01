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
import java.util.List;

public class NativeRatioConfigTest extends TestCase {

    public void testParsesSingleMeasure() {
        NativeRatioConfig config = NativeRatioConfig.fromProperties(
            true,
            "Взвеш. дистрибуция %",
            "WD numerator cat",
            "Продажи руб",
            "Продукт.Бренд,ТТ.Адрес",
            "100",
            "true");
        assertNotNull(config);
        assertTrue(config.isEnabled());

        NativeRatioConfig.RatioMeasureDef def =
            config.getDefinition("Взвеш. дистрибуция %");
        assertNotNull(def);
        assertEquals("WD numerator cat", def.getNumeratorMeasureName());
        assertEquals("Продажи руб", def.getDenominatorMeasureName());
        assertEquals(100.0, def.getMultiplier());
        assertTrue(def.isNullIfDenominatorZero());
        List<String> resets = def.getResetHierarchyNames();
        assertEquals(2, resets.size());
        assertEquals("Продукт.Бренд", resets.get(0));
        assertEquals("ТТ.Адрес", resets.get(1));
    }

    public void testDisabledReturnsNoDefinitions() {
        NativeRatioConfig config = NativeRatioConfig.fromProperties(
            false, "Взвеш. дистрибуция %",
            "WD numerator cat", "Продажи руб",
            "Продукт.Бренд", "100", "true");
        assertFalse(config.isEnabled());
        assertNull(config.getDefinition("Взвеш. дистрибуция %"));
    }

    public void testUnknownMeasureReturnsNull() {
        NativeRatioConfig config = NativeRatioConfig.fromProperties(
            true, "Взвеш. дистрибуция %",
            "WD numerator cat", "Продажи руб",
            "Продукт.Бренд", "100", "true");
        assertNull(config.getDefinition("Unknown measure"));
    }

    public void testDefaultMultiplier() {
        NativeRatioConfig config = NativeRatioConfig.fromProperties(
            true, "Test", "num", "denom", "", null, "false");
        NativeRatioConfig.RatioMeasureDef def = config.getDefinition("Test");
        assertNotNull(def);
        assertEquals(1.0, def.getMultiplier());
        assertFalse(def.isNullIfDenominatorZero());
        assertTrue(def.getResetHierarchyNames().isEmpty());
    }

    public void testMultipleMeasureNames() {
        NativeRatioConfig config = NativeRatioConfig.fromProperties(
            true, "Мера A, Мера B",
            "num", "denom", "H1", "100", "true");
        assertNotNull(config.getDefinition("Мера A"));
        assertNotNull(config.getDefinition("Мера B"));
        assertNull(config.getDefinition("Мера C"));
    }
}
