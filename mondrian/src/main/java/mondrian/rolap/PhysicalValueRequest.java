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

    public PhysicalValueRequest(
        String physicalMeasureId,
        Set<Hierarchy> projectedHierarchies,
        Set<Hierarchy> resetHierarchies,
        AggregationKind aggregationKind,
        ExpressionProviderKind providerKind,
        String nativeTemplate)
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
    }

    public String getPhysicalMeasureId() { return physicalMeasureId; }
    public Set<Hierarchy> getProjectedHierarchies() { return projectedHierarchies; }
    public Set<Hierarchy> getResetHierarchies() { return resetHierarchies; }
    public AggregationKind getAggregationKind() { return aggregationKind; }
    public ExpressionProviderKind getProviderKind() { return providerKind; }
    public String getNativeTemplate() { return nativeTemplate; }

    public boolean isCompatibleWith(PhysicalValueRequest other) {
        if (!projectedHierarchies.equals(other.projectedHierarchies)) {
            return false;
        }
        if (!resetHierarchies.equals(other.resetHierarchies)) {
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
