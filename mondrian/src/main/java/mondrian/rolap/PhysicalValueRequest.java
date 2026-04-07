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

import mondrian.olap.Hierarchy;
import java.util.*;

/**
 * A request for a single physical value in a query-wide SQL plan.
 * Output of DependencyResolver (Phase B).
 */
public class PhysicalValueRequest {
    public enum AggregationKind {
        SUM, COUNT, DISTINCT_MERGE, MIN, MAX, NATIVE_EXPRESSION
    }

    public enum ExpressionProviderKind {
        STORED_COLUMN,
        STATE_AGGREGATE,
        NATIVE_TEMPLATE
    }

    private final String physicalMeasureId;
    private final Set<Hierarchy> projectedHierarchies;
    private final Set<Hierarchy> resetHierarchies;
    private final AggregationKind aggregationKind;
    private final ExpressionProviderKind providerKind;
    private final String nativeTemplate;

    /**
     * User-defined variables from {@code nativeSql.variables} annotation.
     * Only populated for NATIVE_TEMPLATE requests.
     */
    private final Map<String, String> nativeVariables;

    /**
     * Name of the base cube that owns the physical measure.
     * {@code null} means the measure belongs to the query's primary cube
     * (the first stored-measure cube found by {@code findBaseCube}).
     * Non-null when the measure lives in a different base cube of a
     * VirtualCube (e.g. "География" for ОКБ base while the primary
     * cube is "Продажи").
     *
     * <p>Used by {@link #isCompatibleWith} so that cross-cube measures
     * are placed into separate {@link CoordinateClassPlan}s and thus
     * separate SQL queries against their own fact tables.
     */
    private final String sourceCubeName;

    /**
     * Constructs a request without an explicit source cube (assumes
     * the query's primary cube).
     */
    public PhysicalValueRequest(
        String physicalMeasureId,
        Set<Hierarchy> projectedHierarchies,
        Set<Hierarchy> resetHierarchies,
        AggregationKind aggregationKind,
        ExpressionProviderKind providerKind,
        String nativeTemplate)
    {
        this(physicalMeasureId, projectedHierarchies, resetHierarchies,
             aggregationKind, providerKind, nativeTemplate, null, null);
    }

    /**
     * Constructs a NATIVE_TEMPLATE request with variables but without
     * an explicit source cube.
     */
    public PhysicalValueRequest(
        String physicalMeasureId,
        Set<Hierarchy> projectedHierarchies,
        Set<Hierarchy> resetHierarchies,
        AggregationKind aggregationKind,
        ExpressionProviderKind providerKind,
        String nativeTemplate,
        Map<String, String> nativeVariables)
    {
        this(physicalMeasureId, projectedHierarchies, resetHierarchies,
             aggregationKind, providerKind, nativeTemplate,
             nativeVariables, null);
    }

    /**
     * Constructs a request with an explicit source cube name.
     *
     * @param sourceCubeName the base cube name that owns this measure,
     *        or {@code null} for the query's primary cube
     */
    public PhysicalValueRequest(
        String physicalMeasureId,
        Set<Hierarchy> projectedHierarchies,
        Set<Hierarchy> resetHierarchies,
        AggregationKind aggregationKind,
        ExpressionProviderKind providerKind,
        String nativeTemplate,
        Map<String, String> nativeVariables,
        String sourceCubeName)
    {
        this.physicalMeasureId = physicalMeasureId;
        this.projectedHierarchies = Collections.unmodifiableSet(
            new LinkedHashSet<Hierarchy>(projectedHierarchies));
        this.resetHierarchies = resetHierarchies == null
            ? Collections.<Hierarchy>emptySet()
            : Collections.unmodifiableSet(
                new LinkedHashSet<Hierarchy>(resetHierarchies));
        this.aggregationKind = aggregationKind;
        this.providerKind = providerKind;
        this.nativeTemplate = nativeTemplate;
        this.nativeVariables = nativeVariables == null
            ? Collections.<String, String>emptyMap()
            : Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(nativeVariables));
        this.sourceCubeName = sourceCubeName;
    }

    public String getPhysicalMeasureId() { return physicalMeasureId; }
    public Set<Hierarchy> getProjectedHierarchies() { return projectedHierarchies; }
    public Set<Hierarchy> getResetHierarchies() { return resetHierarchies; }
    public AggregationKind getAggregationKind() { return aggregationKind; }
    public ExpressionProviderKind getProviderKind() { return providerKind; }
    public String getNativeTemplate() { return nativeTemplate; }
    public Map<String, String> getNativeVariables() { return nativeVariables; }

    /**
     * Returns the name of the base cube that owns this measure, or
     * {@code null} if the measure belongs to the query's primary cube.
     */
    public String getSourceCubeName() { return sourceCubeName; }

    /**
     * Two requests are compatible (can share a single SQL query) when
     * they have the same projected hierarchies, the same reset
     * hierarchies, compatible provider kinds, <b>and</b> the same
     * source cube.  Cross-cube measures must be in separate plans
     * because they query different fact tables.
     */
    public boolean isCompatibleWith(PhysicalValueRequest other) {
        if (!projectedHierarchies.equals(other.projectedHierarchies)) {
            return false;
        }
        if (!resetHierarchies.equals(other.resetHierarchies)) {
            return false;
        }
        // Cross-cube measures are never compatible
        if (!Objects.equals(sourceCubeName, other.sourceCubeName)) {
            return false;
        }
        if (providerKind == ExpressionProviderKind.NATIVE_TEMPLATE
            || other.providerKind == ExpressionProviderKind.NATIVE_TEMPLATE)
        {
            return providerKind == other.providerKind
                && Objects.equals(nativeTemplate, other.nativeTemplate);
        }
        return true;
    }
}
