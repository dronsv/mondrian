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

import mondrian.olap.MondrianDef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema-load warnings for annotations the engine would otherwise ignore
 * without a word (#102): a repeated name keeps only its last value, and
 * nativeSql annotations are read only on calculated members.
 *
 * <p>Works on the schema XML, once per load, where the repeated names are
 * still visible and each element occurs once, however many (virtual) cubes
 * include it. Messages name elements and annotations, never values.
 */
final class SchemaAnnotationDiagnostics {

    private static final Logger LOGGER =
        LogManager.getLogger(SchemaAnnotationDiagnostics.class);

    private SchemaAnnotationDiagnostics() {}

    static void validate(MondrianDef.Schema schema) {
        element("Schema '" + schema.name + "'", schema.annotations);
        for (MondrianDef.Dimension dimension : schema.dimensions) {
            dimension(dimension, "");
        }
        for (MondrianDef.Cube cube : schema.cubes) {
            if (!cube.isEnabled()) {
                continue;
            }
            final String in = " in cube '" + cube.name + "'";
            element("Cube '" + cube.name + "'", cube.annotations);
            for (MondrianDef.CubeDimension dimension : cube.dimensions) {
                dimension(dimension, in);
            }
            for (MondrianDef.Measure measure : cube.measures) {
                element(
                    "Measure '" + measure.name + "'" + in,
                    measure.annotations);
            }
            calculatedMembers(cube.calculatedMembers);
            namedSets(cube.namedSets, in);
            actions(cube.actions, in);
            if (cube.writebacks != null) {
                for (MondrianDef.WritebackTable writeback : cube.writebacks) {
                    element(
                        "WritebackTable '" + writeback.name + "'" + in,
                        writeback.annotations);
                }
            }
        }
        for (MondrianDef.VirtualCube cube : schema.virtualCubes) {
            if (!cube.isEnabled()) {
                continue;
            }
            final String in = " in virtual cube '" + cube.name + "'";
            element("VirtualCube '" + cube.name + "'", cube.annotations);
            for (MondrianDef.CubeDimension dimension : cube.dimensions) {
                dimension(dimension, in);
            }
            for (MondrianDef.VirtualCubeMeasure measure : cube.measures) {
                element(
                    "VirtualCubeMeasure '" + measure.name + "'" + in,
                    measure.annotations);
            }
            calculatedMembers(cube.calculatedMembers);
            namedSets(cube.namedSets, in);
            actions(cube.actions, in);
        }
        namedSets(schema.namedSets, "");
        for (MondrianDef.Role role : schema.roles) {
            element("Role '" + role.name + "'", role.annotations);
        }
    }

    /** A calculated member: the one element that reads nativeSql. */
    static void validate(MondrianDef.CalculatedMember member) {
        warnRepeatedNames(
            "CalculatedMember '" + member.name + "'", member.annotations);
        NativeSqlConfig.validateAnnotations(
            member.name,
            RolapHierarchy.createAnnotationMap(member.annotations));
    }

    private static void calculatedMembers(
        MondrianDef.CalculatedMember[] members)
    {
        for (MondrianDef.CalculatedMember member : members) {
            validate(member);
        }
    }

    private static void dimension(
        MondrianDef.CubeDimension dimension,
        String in)
    {
        element(
            "Dimension '" + dimension.name + "'" + in, dimension.annotations);
        if (dimension instanceof MondrianDef.Dimension xml) {
            for (MondrianDef.Hierarchy hierarchy : xml.hierarchies) {
                final String name = hierarchy.name == null
                    ? dimension.name
                    : hierarchy.name;
                element(
                    "Hierarchy '" + name + "'" + in, hierarchy.annotations);
                for (MondrianDef.Level level : hierarchy.levels) {
                    element(
                        "Level '" + level.name + "' of hierarchy '" + name
                        + "'" + in,
                        level.annotations);
                }
            }
        }
    }

    private static void namedSets(MondrianDef.NamedSet[] sets, String in) {
        for (MondrianDef.NamedSet set : sets) {
            element("NamedSet '" + set.name + "'" + in, set.annotations);
        }
    }

    private static void actions(MondrianDef.Action[] actions, String in) {
        if (actions != null) {
            for (MondrianDef.Action action : actions) {
                element(
                    "Action '" + action.name + "'" + in, action.annotations);
            }
        }
    }

    /** An element that never reads nativeSql annotations. */
    private static void element(
        String element,
        MondrianDef.Annotations annotations)
    {
        warnRepeatedNames(element, annotations);
        NativeSqlConfig.validateIgnoredAnnotations(
            element, names(annotations));
    }

    /** The parsed map keeps the last of several equal names. */
    private static void warnRepeatedNames(
        String element,
        MondrianDef.Annotations annotations)
    {
        final Map<String, Integer> counts =
            new LinkedHashMap<String, Integer>();
        for (String name : names(annotations)) {
            counts.merge(name, 1, Integer::sum);
        }
        counts.forEach((name, count) -> {
            if (count > 1) {
                LOGGER.warn(
                    "{}: annotation '{}' is declared {} times;"
                    + " only the last value is used",
                    element, name, count);
            }
        });
    }

    private static List<String> names(MondrianDef.Annotations annotations) {
        if (annotations == null || annotations.array == null) {
            return Collections.emptyList();
        }
        final List<String> names = new ArrayList<String>();
        for (MondrianDef.Annotation annotation : annotations.array) {
            names.add(annotation.name);
        }
        return names;
    }
}
