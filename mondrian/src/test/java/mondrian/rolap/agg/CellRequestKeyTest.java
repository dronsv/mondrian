package mondrian.rolap.agg;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import mondrian.rolap.RolapStar;

import org.junit.jupiter.api.Test;

/**
 * emondrian-clickhouse#84 — immutable structural key for a pending
 * {@link CellRequest}. Two requests recorded by re-evaluating the same
 * expression in different phases must map to the same key; requests
 * differing in measure, coordinates, compound predicates or subcube
 * predicate must not. Constrained-column insertion order is not
 * semantic and must not affect the key.
 */
class CellRequestKeyTest {

    private final RolapStar star = mockStar(8);
    private final RolapStar.Measure measure = mockMeasure(star);
    private final RolapStar.Column col1 = mockColumn(star, 1);
    private final RolapStar.Column col2 = mockColumn(star, 2);

    @Test
    void sameStructure_differentRequestInstances_equalKeys() {
        CellRequest a = request(measure, col1, "X", col2, "Y");
        CellRequest b = request(measure, col1, "X", col2, "Y");

        CellRequestKey keyA = CellRequestKey.of(a);
        CellRequestKey keyB = CellRequestKey.of(b);

        assertNotNull(keyA);
        assertEquals(keyA, keyB);
        assertEquals(keyA.hashCode(), keyB.hashCode());
    }

    @Test
    void insertionOrder_notSemantic_equalKeys() {
        CellRequest a = request(measure, col1, "X", col2, "Y");
        CellRequest b = request(measure, col2, "Y", col1, "X");

        assertEquals(CellRequestKey.of(a), CellRequestKey.of(b));
    }

    @Test
    void differentCoordinateValue_differentKeys() {
        CellRequest a = request(measure, col1, "X", col2, "Y");
        CellRequest b = request(measure, col1, "X", col2, "Z");

        assertNotEquals(CellRequestKey.of(a), CellRequestKey.of(b));
    }

    @Test
    void differentMeasure_differentKeys() {
        RolapStar.Measure other = mockMeasure(star);
        CellRequest a = request(measure, col1, "X");
        CellRequest b = request(other, col1, "X");

        assertNotEquals(CellRequestKey.of(a), CellRequestKey.of(b));
    }

    @Test
    void compoundPredicateString_participatesInIdentity() {
        CellRequest a = request(measure, col1, "X");
        CellRequest b = request(measure, col1, "X");
        b.addPredicateString("(brand in ('A','B'))");

        assertNotEquals(CellRequestKey.of(a), CellRequestKey.of(b));
    }

    @Test
    void subcubePredicate_participatesInIdentity() {
        CellRequest a = request(measure, col1, "X");
        CellRequest b = request(measure, col1, "X");
        b.setSubcubePredicate(new LiteralStarPredicate(col2, false));

        assertNotEquals(CellRequestKey.of(a), CellRequestKey.of(b));
    }

    @Test
    void unconstrainedOutputColumn_toleratedAndDistinct() {
        CellRequest constrained = request(measure, col1, "X");
        CellRequest unconstrained = new CellRequest(measure, false, false);
        unconstrained.addConstrainedColumn(col1, null);

        CellRequestKey key = CellRequestKey.of(unconstrained);
        assertNotNull(key);
        assertNotEquals(CellRequestKey.of(constrained), key);
    }

    @Test
    void unsatisfiableRequest_returnsNull() {
        CellRequest request = new CellRequest(measure, false, false);
        request.addConstrainedColumn(
            col1, new ValueColumnPredicate(col1, "X"));
        request.addConstrainedColumn(
            col1, new ValueColumnPredicate(col1, "Y"));

        assertTrue(request.isUnsatisfiable());
        assertNull(CellRequestKey.of(request));
    }

    @Test
    void describe_namesMeasureForDiagnostics() {
        CellRequestKey key = CellRequestKey.of(request(measure, col1, "X"));
        assertNotNull(key);
        assertTrue(
            key.describe().contains("Продажи руб"),
            "describe() must render the measure for log diagnostics: "
                + key.describe());
    }

    // ----- fixtures --------------------------------------------------------

    private static CellRequest request(
        RolapStar.Measure measure, Object... colValuePairs)
    {
        CellRequest request = new CellRequest(measure, false, false);
        for (int i = 0; i < colValuePairs.length; i += 2) {
            RolapStar.Column column = (RolapStar.Column) colValuePairs[i];
            request.addConstrainedColumn(
                column,
                new ValueColumnPredicate(column, colValuePairs[i + 1]));
        }
        return request;
    }

    private static RolapStar mockStar(int columnCount) {
        RolapStar star = mock(RolapStar.class);
        when(star.getColumnCount()).thenReturn(columnCount);
        return star;
    }

    private static RolapStar.Measure mockMeasure(RolapStar star) {
        RolapStar.Measure measure = mock(RolapStar.Measure.class);
        when(measure.getStar()).thenReturn(star);
        when(measure.getName()).thenReturn("Продажи руб");
        when(measure.getCubeName()).thenReturn("LemanaPro");
        return measure;
    }

    private static RolapStar.Column mockColumn(RolapStar star, int bitPos) {
        RolapStar.Column column = mock(RolapStar.Column.class);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(bitPos);
        // AbstractColumnPredicate derives its bit key only when the
        // column has a table — needed by equalConstraint in the
        // contradictory-constraint (unsatisfiable) path.
        when(column.getTable()).thenReturn(mock(RolapStar.Table.class));
        return column;
    }
}
