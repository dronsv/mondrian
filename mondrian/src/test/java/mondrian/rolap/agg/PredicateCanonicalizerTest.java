/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap.agg;

import junit.framework.TestCase;
import mondrian.olap.MondrianDef;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.StarPredicate;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PredicateCanonicalizerTest extends TestCase {

    public void testAndOrderInsensitive() {
        final RolapStar.Column colA = mockColumn(1, "c_a");
        final RolapStar.Column colB = mockColumn(2, "c_b");
        final StarPredicate left = new AndPredicate(
            Arrays.<StarPredicate>asList(
                new ValueColumnPredicate(colA, "A"),
                new ValueColumnPredicate(colB, "B")));
        final StarPredicate right = new AndPredicate(
            Arrays.<StarPredicate>asList(
                new ValueColumnPredicate(colB, "B"),
                new ValueColumnPredicate(colA, "A")));

        assertEquals(
            PredicateCanonicalizer.canonicalize(left),
            PredicateCanonicalizer.canonicalize(right));
    }

    public void testOrOrderInsensitive() {
        final RolapStar.Column col = mockColumn(3, "c_x");
        final StarPredicate left = new OrPredicate(
            Arrays.<StarPredicate>asList(
                new ValueColumnPredicate(col, "X"),
                new ValueColumnPredicate(col, "Y")));
        final StarPredicate right = new OrPredicate(
            Arrays.<StarPredicate>asList(
                new ValueColumnPredicate(col, "Y"),
                new ValueColumnPredicate(col, "X")));

        assertEquals(
            PredicateCanonicalizer.canonicalize(left),
            PredicateCanonicalizer.canonicalize(right));
    }

    public void testNullIsEmptyFingerprint() {
        assertEquals("", PredicateCanonicalizer.canonicalize(null));
    }

    public void testDifferentRangesDifferentFingerprint() {
        final RolapStar.Column col = mockColumn(4, "c_range");
        final RangeColumnPredicate lowToTen =
            new RangeColumnPredicate(
                col,
                true,
                new ValueColumnPredicate(col, 1),
                true,
                new ValueColumnPredicate(col, 10));
        final RangeColumnPredicate lowToNine =
            new RangeColumnPredicate(
                col,
                true,
                new ValueColumnPredicate(col, 1),
                true,
                new ValueColumnPredicate(col, 9));

        assertFalse(
            PredicateCanonicalizer.canonicalize(lowToTen).equals(
                PredicateCanonicalizer.canonicalize(lowToNine)));
    }

    public void testEquivalentListsSameFingerprint() {
        final RolapStar.Column col = mockColumn(5, "c_list");
        final ListColumnPredicate left =
            new ListColumnPredicate(
                col,
                Arrays.<StarColumnPredicate>asList(
                    new ValueColumnPredicate(col, "A"),
                    new ValueColumnPredicate(col, "B")));
        final ListColumnPredicate right =
            new ListColumnPredicate(
                col,
                Arrays.<StarColumnPredicate>asList(
                    new ValueColumnPredicate(col, "B"),
                    new ValueColumnPredicate(col, "A")));

        assertEquals(
            PredicateCanonicalizer.canonicalize(left),
            PredicateCanonicalizer.canonicalize(right));
    }

    private RolapStar.Column mockColumn(int bitPosition, String genericExpr) {
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final RolapStar.Table table = mock(RolapStar.Table.class);
        final MondrianDef.Expression expression = mock(MondrianDef.Expression.class);

        when(column.getBitPosition()).thenReturn(bitPosition);
        when(column.getTable()).thenReturn(table);
        when(column.getExpression()).thenReturn(expression);
        when(column.getName()).thenReturn(genericExpr);
        when(table.getAlias()).thenReturn("fact_alias");
        when(expression.getGenericExpression()).thenReturn(genericExpr);
        return column;
    }
}

