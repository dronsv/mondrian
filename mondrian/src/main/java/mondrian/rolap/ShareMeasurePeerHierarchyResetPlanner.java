/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2026 Hitachi Vantara..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.olap.Annotation;
import mondrian.olap.Category;
import mondrian.olap.Exp;
import mondrian.olap.Hierarchy;
import mondrian.olap.Id;
import mondrian.olap.Member;
import mondrian.olap.SchemaReader;
import mondrian.olap.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Builds the peer-hierarchy reset plan for share-measure companion
 * denominators.
 */
class ShareMeasurePeerHierarchyResetPlanner {
    private static final String SEMANTICS_KIND = "semantics.kind";
    private static final String SEMANTICS_KIND_COMPANION_DENOMINATOR =
        "companion_denominator";
    private static final String SEMANTICS_CHILD_HIERARCHY =
        "semantics.childHierarchy";
    private static final String SEMANTICS_TOP_HIERARCHY =
        "semantics.topHierarchy";

    static InjectionPlan createPlan(
        SchemaReader schemaReader,
        RolapCube cube,
        RolapCalculatedMember member)
    {
        if (member == null) {
            return InjectionPlan.EMPTY;
        }
        return createPlan(
            schemaReader,
            cube,
            member.getAnnotationMap(),
            member.getExpression());
    }

    static InjectionPlan createPlan(
        SchemaReader schemaReader,
        RolapCube cube,
        Map<String, Annotation> annotationMap,
        Exp expression)
    {
        if (schemaReader == null || cube == null || expression == null) {
            return InjectionPlan.EMPTY;
        }
        if (annotationMap == null || annotationMap.isEmpty()) {
            return InjectionPlan.EMPTY;
        }
        if (!SEMANTICS_KIND_COMPANION_DENOMINATOR.equalsIgnoreCase(
            stringValue(annotationMap.get(SEMANTICS_KIND))))
        {
            return InjectionPlan.EMPTY;
        }

        final Hierarchy childHierarchy =
            resolveHierarchy(
                cube,
                schemaReader,
                stringValue(annotationMap.get(SEMANTICS_CHILD_HIERARCHY)));
        final Hierarchy topHierarchy =
            resolveHierarchy(
                cube,
                schemaReader,
                stringValue(annotationMap.get(SEMANTICS_TOP_HIERARCHY)));
        if (childHierarchy == null || topHierarchy == null) {
            return InjectionPlan.EMPTY;
        }
        if (childHierarchy.getDimension() == null
            || topHierarchy.getDimension() == null
            || childHierarchy.getDimension() != topHierarchy.getDimension())
        {
            return InjectionPlan.EMPTY;
        }

        final Set<Hierarchy> explicitHierarchies =
            new LinkedHashSet<Hierarchy>(
                ExplicitHierarchyReferenceFinder.find(expression));
        return createPlan(
            schemaReader,
            cube,
            annotationMap,
            explicitHierarchies);
    }

    static InjectionPlan createPlan(
        SchemaReader schemaReader,
        RolapCube cube,
        Map<String, Annotation> annotationMap,
        String formulaText)
    {
        if (schemaReader == null || cube == null || formulaText == null) {
            return InjectionPlan.EMPTY;
        }
        final Hierarchy childHierarchy =
            resolveHierarchy(
                cube,
                schemaReader,
                stringValue(annotationMap.get(SEMANTICS_CHILD_HIERARCHY)));
        final Hierarchy topHierarchy =
            resolveHierarchy(
                cube,
                schemaReader,
                stringValue(annotationMap.get(SEMANTICS_TOP_HIERARCHY)));
        if (childHierarchy == null || topHierarchy == null) {
            return InjectionPlan.EMPTY;
        }
        if (childHierarchy.getDimension() == null
            || topHierarchy.getDimension() == null
            || childHierarchy.getDimension() != topHierarchy.getDimension())
        {
            return InjectionPlan.EMPTY;
        }
        final Set<Hierarchy> explicitHierarchies = new LinkedHashSet<Hierarchy>();
        for (Hierarchy hierarchy : childHierarchy.getDimension().getHierarchies()) {
            if (containsHierarchyReference(formulaText, hierarchy)) {
                explicitHierarchies.add(hierarchy);
            }
        }
        return createPlan(schemaReader, cube, annotationMap, explicitHierarchies);
    }

    private static InjectionPlan createPlan(
        SchemaReader schemaReader,
        RolapCube cube,
        Map<String, Annotation> annotationMap,
        Set<Hierarchy> explicitHierarchies)
    {
        final Hierarchy childHierarchy =
            resolveHierarchy(
                cube,
                schemaReader,
                stringValue(annotationMap.get(SEMANTICS_CHILD_HIERARCHY)));
        final Hierarchy topHierarchy =
            resolveHierarchy(
                cube,
                schemaReader,
                stringValue(annotationMap.get(SEMANTICS_TOP_HIERARCHY)));
        if (childHierarchy == null || topHierarchy == null) {
            return InjectionPlan.EMPTY;
        }
        if (childHierarchy.getDimension() == null
            || topHierarchy.getDimension() == null
            || childHierarchy.getDimension() != topHierarchy.getDimension())
        {
            return InjectionPlan.EMPTY;
        }
        explicitHierarchies.add(childHierarchy);
        explicitHierarchies.add(topHierarchy);

        final List<Member> resetMembers = new ArrayList<Member>();
        for (Hierarchy hierarchy : childHierarchy.getDimension().getHierarchies()) {
            if (hierarchy == null || explicitHierarchies.contains(hierarchy)) {
                continue;
            }
            final Member replacement =
                hierarchy.hasAll()
                    ? hierarchy.getAllMember()
                    : hierarchy.getDefaultMember();
            if (replacement != null) {
                resetMembers.add(replacement);
            }
        }
        return resetMembers.isEmpty()
            ? InjectionPlan.EMPTY
            : new InjectionPlan(resetMembers);
    }

    private static boolean containsHierarchyReference(
        String formulaText,
        Hierarchy hierarchy)
    {
        if (formulaText == null || hierarchy == null) {
            return false;
        }
        final String lowerFormula = formulaText.toLowerCase(Locale.ROOT);
        final String uniqueName = hierarchy.getUniqueName();
        if (uniqueName != null
            && lowerFormula.contains(uniqueName.toLowerCase(Locale.ROOT)))
        {
            return true;
        }
        final String normalizedUnique = normalizeIdentifier(uniqueName);
        return normalizedUnique != null
            && lowerFormula.contains(normalizedUnique);
    }

    private static String stringValue(Annotation annotation) {
        if (annotation == null || annotation.getValue() == null) {
            return null;
        }
        final String value = String.valueOf(annotation.getValue()).trim();
        return value.isEmpty() ? null : value;
    }

    private static Hierarchy resolveHierarchy(
        RolapCube cube,
        SchemaReader schemaReader,
        String hierarchyRef)
    {
        if (hierarchyRef == null) {
            return null;
        }

        final String normalizedRef = normalizeIdentifier(hierarchyRef);
        Hierarchy match = null;
        for (Hierarchy hierarchy : cube.getHierarchies()) {
            if (!matchesHierarchyReference(hierarchy, hierarchyRef, normalizedRef)) {
                continue;
            }
            if (match != null && match != hierarchy) {
                return null;
            }
            match = hierarchy;
        }
        if (match != null) {
            return match;
        }

        try {
            final mondrian.olap.OlapElement element =
                schemaReader.lookupCompound(
                    cube,
                    Util.parseIdentifier(hierarchyRef),
                    false,
                    Category.Hierarchy);
            return element instanceof Hierarchy ? (Hierarchy) element : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean matchesHierarchyReference(
        Hierarchy hierarchy,
        String rawReference,
        String normalizedRef)
    {
        if (equalsIgnoreCase(rawReference, hierarchy.getUniqueName())
            || equalsIgnoreCase(rawReference, hierarchy.getName())
            || equalsIgnoreCase(
                rawReference,
                hierarchy.getDimension().getName() + "." + hierarchy.getName()))
        {
            return true;
        }
        final String normalizedUnique = normalizeIdentifier(hierarchy.getUniqueName());
        final String normalizedName = normalizeIdentifier(hierarchy.getName());
        final String normalizedDimensionHierarchy =
            normalizeIdentifier(
                hierarchy.getDimension().getName() + "." + hierarchy.getName());
        return equalsIgnoreCase(normalizedRef, normalizedUnique)
            || equalsIgnoreCase(normalizedRef, normalizedName)
            || equalsIgnoreCase(normalizedRef, normalizedDimensionHierarchy);
    }

    private static boolean equalsIgnoreCase(String left, String right) {
        return left != null && right != null && left.equalsIgnoreCase(right);
    }

    private static String normalizeIdentifier(String value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        try {
            final List<Id.Segment> segments = Util.parseIdentifier(trimmed);
            if (!segments.isEmpty()) {
                final List<String> names = new ArrayList<String>(segments.size());
                for (Id.Segment segment : segments) {
                    if (segment instanceof Id.NameSegment) {
                        names.add(
                            ((Id.NameSegment) segment)
                                .getName()
                                .toLowerCase(Locale.ROOT));
                    }
                }
                if (!names.isEmpty()) {
                    final StringBuilder builder = new StringBuilder();
                    for (int i = 0; i < names.size(); i++) {
                        if (i > 0) {
                            builder.append('.');
                        }
                        builder.append(names.get(i));
                    }
                    return builder.toString();
                }
            }
        } catch (Exception e) {
            // Fall through to the simpler normalization below.
        }
        return trimmed
            .replace("[", "")
            .replace("]", "")
            .toLowerCase(Locale.ROOT);
    }

    static final class InjectionPlan {
        static final InjectionPlan EMPTY =
            new InjectionPlan(Collections.<Member>emptyList());

        private final Member[] injectedMembers;

        InjectionPlan(List<Member> injectedMembers) {
            this.injectedMembers =
                injectedMembers.toArray(new Member[injectedMembers.size()]);
        }

        boolean isEmpty() {
            return injectedMembers.length == 0;
        }

        Member[] getInjectedMembers() {
            return injectedMembers.clone();
        }
    }
}
