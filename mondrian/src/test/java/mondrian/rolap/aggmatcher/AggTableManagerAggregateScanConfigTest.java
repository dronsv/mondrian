/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.aggmatcher;

import mondrian.olap.Annotation;
import mondrian.olap.Util.PropertyList;
import mondrian.rolap.RolapConnectionProperties;

import junit.framework.TestCase;

import java.util.LinkedHashMap;
import java.util.Map;

public class AggTableManagerAggregateScanConfigTest extends TestCase {

    public void testConnectionValueTakesPrecedenceOverSchemaAnnotation() {
        final PropertyList connectInfo = new PropertyList();
        connectInfo.put(
            RolapConnectionProperties.AggregateScanSchema.name(),
            "from_connection");

        final Map<String, Annotation> annotations = new LinkedHashMap<String, Annotation>();
        annotations.put("aggregateScanSchema", annotation("aggregateScanSchema", "from_schema"));

        assertEquals(
            "from_connection",
            AggTableManager.resolveAggregateScanProperty(
                connectInfo,
                annotations,
                RolapConnectionProperties.AggregateScanSchema,
                AggTableManager.AGGREGATE_SCAN_SCHEMA_ANNOTATION_NAMES));
    }

    public void testSchemaAnnotationUsedWhenConnectionValueMissing() {
        final PropertyList connectInfo = new PropertyList();
        final Map<String, Annotation> annotations = new LinkedHashMap<String, Annotation>();
        annotations.put("aggregateScanSchema", annotation("aggregateScanSchema", "from_schema"));

        assertEquals(
            "from_schema",
            AggTableManager.resolveAggregateScanProperty(
                connectInfo,
                annotations,
                RolapConnectionProperties.AggregateScanSchema,
                AggTableManager.AGGREGATE_SCAN_SCHEMA_ANNOTATION_NAMES));
    }

    public void testLegacyAnnotationNameIsSupported() {
        final PropertyList connectInfo = new PropertyList();
        final Map<String, Annotation> annotations = new LinkedHashMap<String, Annotation>();
        annotations.put(
            "mondrian.rolap.aggregates.AggregateScanSchema",
            annotation(
                "mondrian.rolap.aggregates.AggregateScanSchema",
                "legacy_schema"));

        assertEquals(
            "legacy_schema",
            AggTableManager.resolveAggregateScanProperty(
                connectInfo,
                annotations,
                RolapConnectionProperties.AggregateScanSchema,
                AggTableManager.AGGREGATE_SCAN_SCHEMA_ANNOTATION_NAMES));
    }

    public void testExplicitEmptyConnectionValueDisablesAnnotationFallback() {
        final PropertyList connectInfo = new PropertyList();
        connectInfo.put(
            RolapConnectionProperties.AggregateScanSchema.name(),
            "");

        final Map<String, Annotation> annotations = new LinkedHashMap<String, Annotation>();
        annotations.put("aggregateScanSchema", annotation("aggregateScanSchema", "from_schema"));

        assertEquals(
            "",
            AggTableManager.resolveAggregateScanProperty(
                connectInfo,
                annotations,
                RolapConnectionProperties.AggregateScanSchema,
                AggTableManager.AGGREGATE_SCAN_SCHEMA_ANNOTATION_NAMES));
    }

    public void testCatalogAnnotationLookupIsCaseInsensitive() {
        final PropertyList connectInfo = new PropertyList();
        final Map<String, Annotation> annotations = new LinkedHashMap<String, Annotation>();
        annotations.put(
            "aggregatescancatalog",
            annotation("aggregatescancatalog", "from_annotation"));

        assertEquals(
            "from_annotation",
            AggTableManager.resolveAggregateScanProperty(
                connectInfo,
                annotations,
                RolapConnectionProperties.AggregateScanCatalog,
                AggTableManager.AGGREGATE_SCAN_CATALOG_ANNOTATION_NAMES));
    }

    private static Annotation annotation(final String name, final String value) {
        return new Annotation() {
            public String getName() {
                return name;
            }

            public Object getValue() {
                return value;
            }
        };
    }
}
