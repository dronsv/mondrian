package mondrian.rolap.agg;

import junit.framework.TestCase;
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarPredicate;
import mondrian.rolap.sql.SqlQuery;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NotPredicateTest extends TestCase {

    public void testToSqlWrapsInnerInNot() {
        StarPredicate inner = mockPredicate("brand = 'Mars'");
        NotPredicate not = new NotPredicate(inner);

        SqlQuery sqlQuery = mock(SqlQuery.class);
        StringBuilder buf = new StringBuilder();
        not.toSql(sqlQuery, buf);

        assertEquals("NOT (brand = 'Mars')", buf.toString());
    }

    public void testToSqlWrapsOrPredicateInNot() {
        RolapStar.Column column = mockColumn(3);

        StarPredicate predA = mockPredicate("col = 'A'", column, 3);
        StarPredicate predB = mockPredicate("col = 'B'", column, 3);
        OrPredicate or = new OrPredicate(
            Arrays.<StarPredicate>asList(predA, predB));

        NotPredicate not = new NotPredicate(or);
        SqlQuery sqlQuery = mock(SqlQuery.class);
        StringBuilder buf = new StringBuilder();
        not.toSql(sqlQuery, buf);

        assertEquals("NOT ((col = 'A' or col = 'B'))", buf.toString());
    }

    public void testEvaluateInvertsInner() {
        StarPredicate inner = mock(StarPredicate.class);
        when(inner.evaluate(any())).thenReturn(true);
        NotPredicate not = new NotPredicate(inner);

        assertFalse(not.evaluate(Collections.<Object>singletonList("x")));

        when(inner.evaluate(any())).thenReturn(false);
        assertTrue(not.evaluate(Collections.<Object>singletonList("x")));
    }

    public void testDescribe() {
        StarPredicate inner = mock(StarPredicate.class);
        doAnswer(inv -> {
            ((StringBuilder) inv.getArguments()[0]).append("col=X");
            return null;
        }).when(inner).describe(any(StringBuilder.class));

        NotPredicate not = new NotPredicate(inner);
        StringBuilder buf = new StringBuilder();
        not.describe(buf);

        assertEquals("not(col=X)", buf.toString());
    }

    public void testConstrainedColumnsDelegateToInner() {
        RolapStar.Column column = mockColumn(5);
        BitKey bitKey = BitKey.Factory.makeBitKey(8);
        bitKey.set(5);

        StarPredicate inner = mock(StarPredicate.class);
        when(inner.getConstrainedColumnList())
            .thenReturn(Collections.singletonList(column));
        when(inner.getConstrainedColumnBitKey()).thenReturn(bitKey);

        NotPredicate not = new NotPredicate(inner);

        assertEquals(1, not.getConstrainedColumnList().size());
        assertSame(column, not.getConstrainedColumnList().get(0));
        assertTrue(not.getConstrainedColumnBitKey().get(5));
    }

    public void testEqualsAndHashCode() {
        StarPredicate innerA = mockPredicate("a");
        StarPredicate innerB = mockPredicate("b");

        NotPredicate notA1 = new NotPredicate(innerA);
        NotPredicate notA2 = new NotPredicate(innerA);
        NotPredicate notB = new NotPredicate(innerB);

        assertEquals(notA1, notA2);
        assertEquals(notA1.hashCode(), notA2.hashCode());
        assertFalse(notA1.equals(notB));
    }

    public void testGetInner() {
        StarPredicate inner = mockPredicate("x");
        NotPredicate not = new NotPredicate(inner);
        assertSame(inner, not.getInner());
    }

    private static RolapStar.Column mockColumn(int bitPosition) {
        RolapStar.Column column = mock(RolapStar.Column.class);
        when(column.getBitPosition()).thenReturn(bitPosition);
        return column;
    }

    private static StarPredicate mockPredicate(String sql) {
        StarPredicate predicate = mock(StarPredicate.class);
        doAnswer(inv -> {
            ((StringBuilder) inv.getArguments()[1]).append(sql);
            return null;
        }).when(predicate).toSql(
            any(SqlQuery.class), any(StringBuilder.class));
        return predicate;
    }

    private static StarPredicate mockPredicate(
        String sql,
        RolapStar.Column column,
        int bitPosition)
    {
        BitKey bitKey = BitKey.Factory.makeBitKey(8);
        bitKey.set(bitPosition);

        StarPredicate predicate = mock(StarPredicate.class);
        when(predicate.getConstrainedColumnList())
            .thenReturn(Collections.singletonList(column));
        when(predicate.getConstrainedColumnBitKey()).thenReturn(bitKey);
        doAnswer(inv -> {
            ((StringBuilder) inv.getArguments()[1]).append(sql);
            return null;
        }).when(predicate).toSql(
            any(SqlQuery.class), any(StringBuilder.class));
        return predicate;
    }
}

// End NotPredicateTest.java
