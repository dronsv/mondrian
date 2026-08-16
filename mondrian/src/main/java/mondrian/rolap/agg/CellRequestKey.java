/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap.agg;

import mondrian.rolap.BitKey;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable structural identity of a pending {@link CellRequest}
 * (emondrian-clickhouse#84).
 *
 * <p>Re-evaluating the same expression across batch-drain phases creates
 * fresh {@link CellRequest} instances; this key lets the phase-progress
 * guard tell a genuinely new request from a repeat of one already loaded.
 *
 * <p>Identity components: measure (compared by reference — measure
 * instances are stable within a statement, which is the key's only
 * scope), constrained-columns {@link BitKey}, per-column predicate
 * values in bit order ({@code CellRequest.check()} bit-orders columns,
 * so constraint insertion order is normalized for free), compound
 * predicate strings (already cached on the request), canonical subcube
 * predicate string (only computed when a subcube predicate is present),
 * and the extendedContext/drillThrough flags.
 *
 * <p>No digests participate in identity — hashing only accelerates
 * lookup, equality resolves collisions. {@link #describe()} renders a
 * compact human-readable form for failure diagnostics.
 */
public final class CellRequestKey {

    /**
     * Marker for a constrained column added without a value predicate
     * (output-only column): distinct from any real value and from SQL
     * NULL's sentinel.
     */
    private static final Object UNCONSTRAINED = new Object() {
        @Override public String toString() {
            return "<unconstrained>";
        }
    };

    private final RolapStar.Measure measure;
    private final BitKey constrainedColumnsBitKey;
    private final Object[] values;
    private final List<String> compoundPredicates;
    private final String subcubePredicate;
    private final boolean extendedContext;
    private final boolean drillThrough;
    private final int hash;

    private CellRequestKey(
        RolapStar.Measure measure,
        BitKey constrainedColumnsBitKey,
        Object[] values,
        List<String> compoundPredicates,
        String subcubePredicate,
        boolean extendedContext,
        boolean drillThrough)
    {
        this.measure = measure;
        this.constrainedColumnsBitKey = constrainedColumnsBitKey;
        this.values = values;
        this.compoundPredicates = compoundPredicates;
        this.subcubePredicate = subcubePredicate;
        this.extendedContext = extendedContext;
        this.drillThrough = drillThrough;
        this.hash = Objects.hash(
            System.identityHashCode(measure),
            constrainedColumnsBitKey,
            Arrays.hashCode(values),
            compoundPredicates,
            subcubePredicate,
            extendedContext,
            drillThrough);
    }

    /**
     * Builds the key for a request, or returns null for requests that
     * carry no loadable identity (null or unsatisfiable — those never
     * resolve and are excluded from progress accounting).
     */
    public static CellRequestKey of(CellRequest request) {
        if (request == null || request.isUnsatisfiable()) {
            return null;
        }
        final int numValues = request.getNumValues();
        final Object[] values = new Object[numValues];
        for (int i = 0; i < numValues; i++) {
            final StarColumnPredicate predicate = request.getValueAt(i);
            if (predicate == null) {
                values[i] = UNCONSTRAINED;
            } else if (predicate instanceof ValueColumnPredicate) {
                values[i] = ((ValueColumnPredicate) predicate).getValue();
            } else {
                // Rare non-value column predicate — canonical string is
                // the safe structural fallback.
                values[i] = PredicateCanonicalizer.canonicalize(predicate);
            }
        }
        return new CellRequestKey(
            request.getMeasure(),
            request.getConstrainedColumnsBitKey().copy(),
            values,
            request.getCompoundPredicateStrings(),
            request.getSubcubePredicate() == null
                ? ""
                : request.getSubcubePredicateString(),
            request.extendedContext,
            request.drillThrough);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CellRequestKey)) {
            return false;
        }
        final CellRequestKey that = (CellRequestKey) obj;
        return this.hash == that.hash
            && this.measure == that.measure
            && this.extendedContext == that.extendedContext
            && this.drillThrough == that.drillThrough
            && this.constrainedColumnsBitKey.equals(
                that.constrainedColumnsBitKey)
            && Arrays.equals(this.values, that.values)
            && this.compoundPredicates.equals(that.compoundPredicates)
            && this.subcubePredicate.equals(that.subcubePredicate);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    /** Compact rendering for failure diagnostics and logs. */
    public String describe() {
        final StringBuilder sb = new StringBuilder(96);
        sb.append("measure=").append(measure.getCubeName())
            .append('.').append(measure.getName())
            .append(" columns=").append(constrainedColumnsBitKey)
            .append(" values=").append(Arrays.toString(values));
        if (!compoundPredicates.isEmpty()) {
            sb.append(" compound=").append(compoundPredicates);
        }
        if (!subcubePredicate.isEmpty()) {
            sb.append(" subcube=").append(subcubePredicate);
        }
        if (extendedContext) {
            sb.append(" extendedContext");
        }
        if (drillThrough) {
            sb.append(" drillThrough");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return describe();
    }
}
