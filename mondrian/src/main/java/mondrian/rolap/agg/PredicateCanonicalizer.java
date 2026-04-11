/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;
import mondrian.rolap.RolapUtil;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Builds a deterministic, structural fingerprint for {@link StarPredicate}.
 *
 * <p>The output is part of cache identity and must not rely on debug
 * representations such as {@code toString()}.</p>
 */
public final class PredicateCanonicalizer {
    private static final Logger LOGGER =
        LogManager.getLogger(PredicateCanonicalizer.class);

    private static final Comparator<String> STRING_COMPARATOR =
        new Comparator<String>() {
            public int compare(String left, String right) {
                return left.compareTo(right);
            }
        };

    /**
     * Tracks which predicate class names have been observed in the
     * describe()-based fallback path.  Logged at WARN the first time
     * each new class appears so we can audit production for pathological
     * types whose {@code describe()} output is non-canonical (identity
     * aliasing risk).  Kept at DEBUG on subsequent observations to avoid
     * log spam.
     */
    private static final ConcurrentMap<String, Boolean> FALLBACK_SEEN =
        new ConcurrentHashMap<>();

    private PredicateCanonicalizer() {
    }

    public static String canonicalize(final StarPredicate predicate) {
        if (predicate == null) {
            return "";
        }
        final StringBuilder sb = new StringBuilder(128);
        appendCanonical(predicate, sb);
        return sb.toString();
    }

    private static void appendCanonical(
        final StarPredicate predicate,
        final StringBuilder sb)
    {
        if (predicate == null) {
            sb.append("null");
            return;
        }
        if (predicate instanceof AndPredicate) {
            appendListPredicate("and", ((AndPredicate) predicate).getChildren(), sb);
            return;
        }
        if (predicate instanceof OrPredicate) {
            appendListPredicate("or", ((OrPredicate) predicate).getChildren(), sb);
            return;
        }
        if (predicate instanceof NotPredicate) {
            sb.append("not(");
            appendCanonical(((NotPredicate) predicate).getInner(), sb);
            sb.append(')');
            return;
        }
        if (predicate instanceof ListColumnPredicate) {
            appendListColumnPredicate((ListColumnPredicate) predicate, sb);
            return;
        }
        if (predicate instanceof RangeColumnPredicate) {
            appendRangeColumnPredicate((RangeColumnPredicate) predicate, sb);
            return;
        }
        if (predicate instanceof MemberColumnPredicate) {
            appendMemberColumnPredicate((MemberColumnPredicate) predicate, sb);
            return;
        }
        if (predicate instanceof ValueColumnPredicate) {
            appendValueColumnPredicate((ValueColumnPredicate) predicate, sb);
            return;
        }
        if (predicate instanceof LiteralStarPredicate) {
            appendLiteralPredicate((LiteralStarPredicate) predicate, sb);
            return;
        }
        if (predicate instanceof MinusStarPredicate) {
            // Structural canonicalization: MinusStarPredicate is
            // (plus - minus) over a single column.  Canonicalize both
            // halves structurally so identity is stable across the
            // field order the Mondrian planner happens to pick.
            appendMinusStarPredicate((MinusStarPredicate) predicate, sb);
            return;
        }
        // MemberTuplePredicate falls through to describe()-based fallback
        // below with an explicit observation log: its describe() output
        // IS deterministic today (see MemberTuplePredicate.describe in
        // same package), but we want production visibility if a new
        // predicate type appears on the fallback path.
        appendFallback(predicate, sb);
    }

    private static void appendMinusStarPredicate(
        MinusStarPredicate predicate,
        StringBuilder sb)
    {
        sb.append("minus(");
        appendColumnIdentity(predicate.getConstrainedColumn(), sb);
        // The plus and minus halves are inner StarColumnPredicates; we
        // canonicalize each via the same visitor.  Emission order is
        // fixed (plus then minus) — MinusStarPredicate is not
        // commutative, so we do NOT sort these.
        sb.append(",plus=");
        final StringBuilder plusBuf = new StringBuilder(64);
        appendCanonical(predicate.getPlus(), plusBuf);
        sb.append(plusBuf);
        sb.append(",minus=");
        final StringBuilder minusBuf = new StringBuilder(64);
        appendCanonical(predicate.getMinus(), minusBuf);
        sb.append(minusBuf);
        sb.append(')');
    }

    private static void appendListPredicate(
        String op,
        List<StarPredicate> children,
        StringBuilder sb)
    {
        sb.append(op).append('(');
        appendSortedChildren(children, sb);
        sb.append(')');
    }

    private static void appendListColumnPredicate(
        ListColumnPredicate predicate,
        StringBuilder sb)
    {
        sb.append("list(");
        appendColumnIdentity(predicate.getConstrainedColumn(), sb);
        sb.append(':');
        final List<StarColumnPredicate> children = predicate.getPredicates();
        final List<String> canonicalChildren =
            new ArrayList<String>(children.size());
        for (StarColumnPredicate child : children) {
            canonicalChildren.add(canonicalize(child));
        }
        Collections.sort(canonicalChildren, STRING_COMPARATOR);
        appendList(canonicalChildren, sb);
        sb.append(')');
    }

    private static void appendRangeColumnPredicate(
        RangeColumnPredicate predicate,
        StringBuilder sb)
    {
        sb.append("range(");
        appendColumnIdentity(predicate.getConstrainedColumn(), sb);
        sb.append(",lower=");
        appendBound(
            predicate.getLowerBound(),
            predicate.getLowerInclusive(),
            sb);
        sb.append(",upper=");
        appendBound(
            predicate.getUpperBound(),
            predicate.getUpperInclusive(),
            sb);
        sb.append(')');
    }

    private static void appendBound(
        ValueColumnPredicate bound,
        boolean inclusive,
        StringBuilder sb)
    {
        if (bound == null) {
            sb.append("unbounded");
            return;
        }
        sb.append(inclusive ? "inclusive:" : "exclusive:");
        appendValue(bound.getValue(), sb);
    }

    private static void appendMemberColumnPredicate(
        MemberColumnPredicate predicate,
        StringBuilder sb)
    {
        sb.append("member(");
        appendColumnIdentity(predicate.getConstrainedColumn(), sb);
        sb.append(':');
        sb.append(predicate.getMember().getUniqueName());
        sb.append(",key=");
        appendValue(predicate.getValue(), sb);
        sb.append(')');
    }

    private static void appendValueColumnPredicate(
        ValueColumnPredicate predicate,
        StringBuilder sb)
    {
        sb.append("value(");
        appendColumnIdentity(predicate.getConstrainedColumn(), sb);
        sb.append(':');
        appendValue(predicate.getValue(), sb);
        sb.append(')');
    }

    private static void appendLiteralPredicate(
        LiteralStarPredicate predicate,
        StringBuilder sb)
    {
        sb.append("literal(").append(predicate.getValue());
        if (predicate.getConstrainedColumn() != null) {
            sb.append(',');
            appendColumnIdentity(predicate.getConstrainedColumn(), sb);
        }
        sb.append(')');
    }

    private static void appendFallback(StarPredicate predicate, StringBuilder sb) {
        final String className = predicate.getClass().getName();
        // Observe once at WARN so we can audit production for pathological
        // predicate types whose describe() output may not be canonical.
        // Subsequent observations stay at DEBUG to avoid log spam.
        if (FALLBACK_SEEN.putIfAbsent(className, Boolean.TRUE) == null) {
            LOGGER.warn(
                "PredicateCanonicalizer fallback path used for class={} — "
                + "verify that describe() output is deterministic and "
                + "order-insensitive, otherwise cache identity may alias",
                className);
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "PredicateCanonicalizer fallback path used for class={}",
                className);
        }

        sb.append("fallback(").append(className);
        sb.append(",columns=");
        final List<RolapStar.Column> columns = predicate.getConstrainedColumnList();
        if (columns == null) {
            sb.append("null");
        } else {
            final List<String> columnIds = new ArrayList<String>(columns.size());
            for (RolapStar.Column column : columns) {
                final StringBuilder columnId = new StringBuilder(32);
                appendColumnIdentity(column, columnId);
                columnIds.add(columnId.toString());
            }
            Collections.sort(columnIds, STRING_COMPARATOR);
            appendList(columnIds, sb);
        }
        sb.append(",description=");
        final StringBuilder description = new StringBuilder(64);
        predicate.describe(description);
        sb.append(description);
        sb.append(')');
    }

    /** Test hook: reset the per-class observation set. */
    public static void resetFallbackObservationsForTests() {
        FALLBACK_SEEN.clear();
    }

    private static void appendColumnIdentity(
        RolapStar.Column column,
        StringBuilder sb)
    {
        if (column == null) {
            sb.append("col:null");
            return;
        }
        sb.append("col{bit=").append(column.getBitPosition());
        sb.append(",expr=");
        if (column.getExpression() == null) {
            sb.append("null");
        } else {
            sb.append(column.getExpression().getGenericExpression());
        }
        sb.append(",tableAlias=");
        if (column.getTable() == null) {
            sb.append("null");
        } else {
            sb.append(column.getTable().getAlias());
        }
        sb.append(",name=").append(column.getName());
        sb.append('}');
    }

    private static void appendValue(Object value, StringBuilder sb) {
        if (value == null) {
            sb.append("null");
            return;
        }
        if (value == RolapUtil.sqlNullValue) {
            sb.append("sqlNullValue");
            return;
        }
        if (value.getClass().isArray()) {
            appendArrayValue(value, sb);
            return;
        }
        sb.append(value.getClass().getName());
        sb.append(':').append(String.valueOf(value));
    }

    private static void appendArrayValue(Object value, StringBuilder sb) {
        if (value instanceof Object[]) {
            sb.append(Arrays.deepToString((Object[]) value));
        } else if (value instanceof int[]) {
            sb.append(Arrays.toString((int[]) value));
        } else if (value instanceof long[]) {
            sb.append(Arrays.toString((long[]) value));
        } else if (value instanceof short[]) {
            sb.append(Arrays.toString((short[]) value));
        } else if (value instanceof byte[]) {
            sb.append(Arrays.toString((byte[]) value));
        } else if (value instanceof boolean[]) {
            sb.append(Arrays.toString((boolean[]) value));
        } else if (value instanceof char[]) {
            sb.append(Arrays.toString((char[]) value));
        } else if (value instanceof float[]) {
            sb.append(Arrays.toString((float[]) value));
        } else if (value instanceof double[]) {
            sb.append(Arrays.toString((double[]) value));
        } else {
            sb.append(value.getClass().getName());
        }
    }

    private static void appendSortedChildren(
        List<? extends StarPredicate> children,
        StringBuilder sb)
    {
        final List<String> canonicalChildren =
            new ArrayList<String>(children.size());
        for (StarPredicate child : children) {
            canonicalChildren.add(canonicalize(child));
        }
        Collections.sort(canonicalChildren, STRING_COMPARATOR);
        appendList(canonicalChildren, sb);
    }

    private static void appendList(List<String> values, StringBuilder sb) {
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(values.get(i));
        }
    }
}

