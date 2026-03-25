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
import mondrian.rolap.RolapMember;
import mondrian.rolap.RolapStar;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationManagerSubcubePredicateNormalizationTest
    extends TestCase
{
    public void testNormalizeSingleColumnMemberSet() {
        final RolapStar.Column manufacturerColumn = mockColumn(5);
        final MemberColumnPredicate mfrA =
            memberPredicate(manufacturerColumn, "A", "[Mfr].[A]");
        final MemberColumnPredicate mfrB =
            memberPredicate(manufacturerColumn, "B", "[Mfr].[B]");

        final AggregationManager.NormalizedSubcubePredicate normalized =
            AggregationManager.normalizeSubcubePredicate(
                new OrPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                    mfrA,
                    mfrB)));

        assertNotNull(normalized);
        assertEquals(1, normalized.getPredicatesByBitPos().size());
        assertTrue(
            normalized.getPredicatesByBitPos().get(5)
                instanceof ListColumnPredicate);
        assertEquals(
            "[Mfr].[Level]",
            normalized.getConstrainedLevelNames().get(5).first());
    }

    public void testNormalizeCommonPrefixMemberChains() {
        final RolapStar.Column manufacturerColumn = mockColumn(5);
        final RolapStar.Column brandColumn = mockColumn(7);
        final MemberColumnPredicate mfrA =
            memberPredicate(manufacturerColumn, "A", "[Mfr].[A]");
        final MemberColumnPredicate brandX =
            memberPredicate(brandColumn, "X", "[Brand].[X]");
        final MemberColumnPredicate brandY =
            memberPredicate(brandColumn, "Y", "[Brand].[Y]");

        final AggregationManager.NormalizedSubcubePredicate normalized =
            AggregationManager.normalizeSubcubePredicate(
                new OrPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                    new AndPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                        mfrA,
                        brandX)),
                    new AndPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                        mfrA,
                        brandY)))));

        assertNotNull(normalized);
        assertEquals(2, normalized.getPredicatesByBitPos().size());
        assertTrue(
            normalized.getPredicatesByBitPos().get(7)
                instanceof ListColumnPredicate);
        assertTrue(
            normalized.getPredicatesByBitPos().get(5)
                instanceof MemberColumnPredicate);
    }

    public void testDoNotNormalizeMixedMultiColumnOr() {
        final RolapStar.Column manufacturerColumn = mockColumn(5);
        final RolapStar.Column brandColumn = mockColumn(7);
        final MemberColumnPredicate mfrA =
            memberPredicate(manufacturerColumn, "A", "[Mfr].[A]");
        final MemberColumnPredicate mfrB =
            memberPredicate(manufacturerColumn, "B", "[Mfr].[B]");
        final MemberColumnPredicate brandX =
            memberPredicate(brandColumn, "X", "[Brand].[X]");
        final MemberColumnPredicate brandY =
            memberPredicate(brandColumn, "Y", "[Brand].[Y]");

        assertNull(
            AggregationManager.normalizeSubcubePredicate(
                new OrPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                    new AndPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                        mfrA,
                        brandX)),
                    new AndPredicate(Arrays.<mondrian.rolap.StarPredicate>asList(
                        mfrB,
                        brandY))))));
    }

    private RolapStar.Column mockColumn(int bitPosition) {
        final RolapStar.Column column = mock(RolapStar.Column.class);
        final RolapStar star = mock(RolapStar.class);
        final RolapStar.Table table = mock(RolapStar.Table.class);
        when(column.getBitPosition()).thenReturn(bitPosition);
        when(column.getStar()).thenReturn(star);
        when(column.getTable()).thenReturn(table);
        when(star.getColumnCount()).thenReturn(32);
        return column;
    }

    private MemberColumnPredicate memberPredicate(
        RolapStar.Column column,
        Object key,
        String uniqueName)
    {
        final RolapMember member = mock(RolapMember.class);
        final RolapLevel level = mock(RolapLevel.class);
        when(member.getKey()).thenReturn(key);
        when(member.getUniqueName()).thenReturn(uniqueName);
        when(member.getLevel()).thenReturn(level);
        when(level.getUniqueName()).thenReturn(
            uniqueName.startsWith("[Mfr]")
                ? "[Mfr].[Level]"
                : "[Brand].[Level]");
        return new MemberColumnPredicate(column, member);
    }
}
