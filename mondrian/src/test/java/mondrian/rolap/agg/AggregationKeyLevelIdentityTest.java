/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.agg;

import junit.framework.TestCase;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapStar;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationKeyLevelIdentityTest extends TestCase {

    public void testAggregationKeyDiffersForSameColumnDifferentLevels() {
        final RolapStar star = createStar();
        final AggregationKey left = aggregationKey(star, "[Flat].[Manufacturer]");
        final AggregationKey right = aggregationKey(star, "[Hier].[Manufacturer]");

        assertFalse(left.equals(right));
        assertFalse(right.equals(left));
        assertFalse(left.hashCode() == right.hashCode());
    }

    public void testAggregationKeyMatchesForSameColumnSameLevel() {
        final RolapStar star = createStar();
        final AggregationKey left = aggregationKey(star, "[Flat].[Manufacturer]");
        final AggregationKey right = aggregationKey(star, "[Flat].[Manufacturer]");

        assertTrue(left.equals(right));
        assertTrue(right.equals(left));
        assertEquals(left.hashCode(), right.hashCode());
    }

    private static RolapStar createStar() {
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);

        when(star.getColumnCount()).thenReturn(32);
        when(star.getFactTable()).thenReturn(factTable);
        when(factTable.getTableName()).thenReturn("mock_fact");
        return star;
    }

    private static AggregationKey aggregationKey(
        RolapStar star,
        String levelUniqueName)
    {
        final RolapStar.Measure measure = mock(RolapStar.Measure.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final RolapLevel level = mock(RolapLevel.class);

        when(measure.getStar()).thenReturn(star);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(7);
        when(level.getUniqueName()).thenReturn(levelUniqueName);

        final CellRequest request = new CellRequest(measure, false, false);
        request.addConstrainedColumn(column, null, level);
        return new AggregationKey(request);
    }
}
