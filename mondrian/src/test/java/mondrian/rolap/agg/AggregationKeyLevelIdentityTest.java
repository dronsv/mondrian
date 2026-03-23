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
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;

import java.util.Collections;
import java.util.SortedMap;
import java.util.SortedSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationKeyLevelIdentityTest extends TestCase {

    public void testSegmentMatchHashIgnoresConstrainedLevelNames() {
        final RolapStar star = createStar();
        final RolapStar.Measure measure = mock(RolapStar.Measure.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);

        when(measure.getStar()).thenReturn(star);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(7);

        final AggregationKey flatKey =
            aggregationKey(star, measure, column, "[Flat].[Manufacturer]");
        final AggregationKey hierKey =
            aggregationKey(star, measure, column, "[Hier].[Manufacturer]");

        assertEquals(
            flatKey.segmentMatchHashCode(),
            hierKey.segmentMatchHashCode());
    }

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

    public void testCellRequestRetainsAllLevelNamesForDuplicateConstraint() {
        final RolapStar star = createStar();
        final RolapStar.Measure measure = mock(RolapStar.Measure.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final RolapLevel leftLevel = mock(RolapLevel.class);
        final RolapLevel rightLevel = mock(RolapLevel.class);
        final StarColumnPredicate firstPredicate = mock(StarColumnPredicate.class);
        final StarColumnPredicate secondPredicate = mock(StarColumnPredicate.class);

        when(measure.getStar()).thenReturn(star);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(7);
        when(leftLevel.getUniqueName()).thenReturn("[TimeExtra].[Year]");
        when(rightLevel.getUniqueName()).thenReturn("[TimeAlias].[Year]");
        when(secondPredicate.equalConstraint(firstPredicate)).thenReturn(true);

        final CellRequest request = new CellRequest(measure, false, false);
        request.addConstrainedColumn(column, firstPredicate, leftLevel);
        request.addConstrainedColumn(column, secondPredicate, rightLevel);

        final SortedMap<Integer, SortedSet<String>> constrainedLevels =
            request.getConstrainedLevelNamesByBitPosition();
        assertEquals(1, constrainedLevels.size());
        assertEquals(
            2,
            constrainedLevels.get(7).size());
        assertTrue(constrainedLevels.get(7).contains("[TimeExtra].[Year]"));
        assertTrue(constrainedLevels.get(7).contains("[TimeAlias].[Year]"));
    }

    public void testAggregationRetainsConstrainedLevelNames() {
        final RolapStar star = createStar();
        final AggregationKey key =
            aggregationKeyWithDuplicateLevels(star);

        final Aggregation aggregation = new Aggregation(key);

        assertEquals(
            key.getConstrainedLevelNamesByBitPosition(),
            aggregation.getConstrainedLevelNamesByBitPosition());
    }

    public void testGroupingSetsListRetainsConstrainedLevelNames() {
        final Segment segment = mock(Segment.class);
        final BitKey levelBitKey = BitKey.Factory.makeBitKey(8);
        final BitKey measureBitKey = BitKey.Factory.makeBitKey(8);
        final SortedMap<Integer, SortedSet<String>> constrainedLevels =
            aggregationKeyWithDuplicateLevels(createStar())
                .getConstrainedLevelNamesByBitPosition();

        final GroupingSet groupingSet =
            new GroupingSet(
                Collections.singletonList(segment),
                levelBitKey,
                measureBitKey,
                new StarColumnPredicate[0],
                new RolapStar.Column[0],
                constrainedLevels);
        final GroupingSetsList groupingSetsList =
            new GroupingSetsList(Collections.singletonList(groupingSet));

        assertEquals(
            constrainedLevels,
            groupingSetsList.getDefaultConstrainedLevelNamesByBitPosition());
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
        when(measure.getStar()).thenReturn(star);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(7);
        return aggregationKey(star, measure, column, levelUniqueName);
    }

    private static AggregationKey aggregationKey(
        RolapStar star,
        RolapStar.Measure measure,
        RolapStar.Column column,
        String levelUniqueName)
    {
        final RolapLevel level = mock(RolapLevel.class);

        when(measure.getStar()).thenReturn(star);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(7);
        when(level.getUniqueName()).thenReturn(levelUniqueName);

        final CellRequest request = new CellRequest(measure, false, false);
        request.addConstrainedColumn(column, null, level);
        return new AggregationKey(request);
    }

    private static AggregationKey aggregationKeyWithDuplicateLevels(
        RolapStar star)
    {
        final RolapStar.Measure measure = mock(RolapStar.Measure.class);
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final RolapLevel leftLevel = mock(RolapLevel.class);
        final RolapLevel rightLevel = mock(RolapLevel.class);
        final StarColumnPredicate firstPredicate = mock(StarColumnPredicate.class);
        final StarColumnPredicate secondPredicate = mock(StarColumnPredicate.class);

        when(measure.getStar()).thenReturn(star);
        when(column.getStar()).thenReturn(star);
        when(column.getBitPosition()).thenReturn(7);
        when(leftLevel.getUniqueName()).thenReturn("[TimeExtra].[Year]");
        when(rightLevel.getUniqueName()).thenReturn("[TimeAlias].[Year]");
        when(secondPredicate.equalConstraint(firstPredicate)).thenReturn(true);

        final CellRequest request = new CellRequest(measure, false, false);
        request.addConstrainedColumn(column, firstPredicate, leftLevel);
        request.addConstrainedColumn(column, secondPredicate, rightLevel);
        return new AggregationKey(request);
    }
}
