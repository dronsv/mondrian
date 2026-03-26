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
import mondrian.olap.MondrianDef;
import mondrian.rolap.BitKey;
import mondrian.rolap.RolapSchema;
import mondrian.rolap.RolapStar;
import mondrian.rolap.StarColumnPredicate;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.sql.SqlQuery;
import mondrian.spi.Dialect;
import mondrian.util.ByteString;
import mondrian.util.Pair;

import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggQuerySpecTest extends TestCase {
    public void testGenerateSqlKeepsExtraPredicateForProjectedColumn() {
        final RolapStar star = mock(RolapStar.class);
        final RolapSchema schema = mock(RolapSchema.class);
        final RolapStar.Table factTable = mock(RolapStar.Table.class);
        when(schema.getName()).thenReturn("FoodMart");
        when(schema.getChecksum())
            .thenReturn(new ByteString("checksum".getBytes()));
        when(star.getSchema()).thenReturn(schema);
        when(star.getFactTable()).thenReturn(factTable);
        when(factTable.getAlias()).thenReturn("agg_fact");

        final RolapStar.Column projectedStarColumn = mock(RolapStar.Column.class);
        final MondrianDef.Expression projectedExpression =
            mock(MondrianDef.Expression.class);
        when(projectedExpression.getGenericExpression())
            .thenReturn("manufacturer_group");
        when(projectedStarColumn.getBitPosition()).thenReturn(1);
        when(projectedStarColumn.getExpression()).thenReturn(projectedExpression);
        when(projectedStarColumn.getCardinality()).thenReturn(100L);
        when(projectedStarColumn.getDatatype()).thenReturn(Dialect.Datatype.String);
        when(projectedStarColumn.generateExprString(any(SqlQuery.class)))
            .thenReturn("manufacturer_group");

        final RolapStar.Measure segmentMeasure = mock(RolapStar.Measure.class);
        when(segmentMeasure.getBitPosition()).thenReturn(2);
        when(segmentMeasure.getCubeName()).thenReturn("Sales");
        when(segmentMeasure.getName()).thenReturn("CustomerDistinctState");

        final Segment segment = new Segment(
            star,
            bitKey(4, 1, 2),
            new RolapStar.Column[] {projectedStarColumn},
            segmentMeasure,
            new StarColumnPredicate[] {
                new LiteralStarPredicate(projectedStarColumn, true)
            },
            Collections.<Segment.ExcludedRegion>emptyList(),
            Collections.emptyList(),
            null);

        final GroupingSet groupingSet = new GroupingSet(
            Collections.singletonList(segment),
            bitKey(4, 1),
            bitKey(4, 2),
            segment.predicates,
            segment.getColumns(),
            new TreeMap<Integer, SortedSet<String>>());
        final GroupingSetsList groupingSetsList =
            new GroupingSetsList(Collections.singletonList(groupingSet));

        final AggStar aggStar = mock(AggStar.class);
        final AggStar.Table.Column projectedAggColumn =
            mock(AggStar.Table.Column.class);
        final AggStar.Table projectedAggTable = mock(AggStar.Table.class);
        when(projectedAggColumn.getTable()).thenReturn(projectedAggTable);
        when(projectedAggColumn.generateExprString(any(SqlQuery.class)))
            .thenReturn("manufacturer_group");
        when(projectedAggColumn.getDatatype()).thenReturn(Dialect.Datatype.String);

        final AggStar.FactTable.Measure aggMeasure =
            mock(AggStar.FactTable.Measure.class);
        final AggStar.FactTable aggMeasureTable = mock(AggStar.FactTable.class);
        when(aggMeasure.getTable()).thenReturn(aggMeasureTable);
        when(aggMeasure.generateExprString(any(SqlQuery.class)))
            .thenReturn("distinct_state");

        when(aggStar.lookupColumn(1)).thenReturn(projectedAggColumn);
        when(aggStar.lookupColumn(2)).thenReturn(aggMeasure);

        final SortedMap<Integer, StarColumnPredicate> extraPredicatesByBitPos =
            new TreeMap<Integer, StarColumnPredicate>();
        extraPredicatesByBitPos.put(
            1,
            new ValueColumnPredicate(projectedStarColumn, "Acme"));

        final AggQuerySpec querySpec =
            new AggQuerySpec(
                aggStar,
                false,
                groupingSetsList,
                extraPredicatesByBitPos)
            {
                @Override
                protected SqlQuery newSqlQuery() {
                    return new SqlQuery(clickHouseDialect(), false);
                }
            };

        final Pair<String, List<mondrian.rolap.SqlStatement.Type>> sqlAndTypes =
            querySpec.generateSqlQuery();
        final String sql = sqlAndTypes.left;

        assertTrue(sql.contains("manufacturer_group"));
        assertTrue(sql.contains("manufacturer_group = 'Acme'"));
        assertTrue(sql.contains("distinct_state"));
    }

    private BitKey bitKey(int size, int... bits) {
        final BitKey key = BitKey.Factory.makeBitKey(size);
        for (int bit : bits) {
            key.set(bit);
        }
        return key;
    }

    private Dialect clickHouseDialect() {
        final Dialect dialect = mock(Dialect.class);
        when(dialect.getDatabaseProduct())
            .thenReturn(Dialect.DatabaseProduct.CLICKHOUSE);
        when(dialect.allowsAs()).thenReturn(true);
        when(dialect.requiresDrillthroughMaxRowsInLimit()).thenReturn(true);
        when(dialect.quoteIdentifier(anyString()))
            .thenAnswer(invocation -> {
                final String identifier = invocation.getArgument(0, String.class);
                return '`' + identifier + '`';
            });
        doAnswer(invocation -> {
            final String identifier = invocation.getArgument(0, String.class);
            final StringBuilder buf =
                invocation.getArgument(1, StringBuilder.class);
            buf.append('`').append(identifier).append('`');
            return null;
        }).when(dialect).quoteIdentifier(anyString(), any(StringBuilder.class));
        doAnswer(invocation -> {
            final StringBuilder buf =
                invocation.getArgument(0, StringBuilder.class);
            final Object value = invocation.getArgument(1);
            if (value instanceof Number) {
                buf.append(value);
            } else {
                buf.append('\'')
                    .append(String.valueOf(value).replace("'", "''"))
                    .append('\'');
            }
            return null;
        }).when(dialect).quote(any(StringBuilder.class), any(), any(Dialect.Datatype.class));
        return dialect;
    }
}
