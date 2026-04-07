/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import junit.framework.TestCase;
import mondrian.calc.TupleCollections;
import mondrian.calc.TupleList;
import mondrian.olap.Member;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DrilldownLevelCrossJoinArgTest extends TestCase {

    public void testExpandTupleListAddsParentTuplesForSimpleDrilldown() {
        final RolapLevel level = mock(RolapLevel.class);
        final RolapMember allBrand = member("[Brand].[All]");
        final RolapMember allCategory = member("[Category].[All]");
        final RolapMember brand1 = member("[Brand].[B1]");
        final RolapMember brand2 = member("[Brand].[B2]");
        final RolapMember category1 = member("[Category].[C1]");
        final RolapMember category2 = member("[Category].[C2]");

        final TupleList input = TupleCollections.createList(2);
        input.addTuple(brand1, category1);
        input.addTuple(brand1, category2);
        input.addTuple(brand2, category1);

        final CrossJoinArg[] args = new CrossJoinArg[] {
            new DrilldownLevelCrossJoinArg(
                new DescendantsCrossJoinArg(level, null),
                allBrand),
            new DrilldownLevelCrossJoinArg(
                new DescendantsCrossJoinArg(level, null),
                allCategory)
        };

        final TupleList expanded =
            DrilldownLevelCrossJoinArg.expandTupleList(input, args);

        assertEquals(8, expanded.size());
        assertEquals(expectedKeys(), tupleKeys(expanded));
    }

    private RolapMember member(String uniqueName) {
        final RolapMember member = mock(RolapMember.class);
        when(member.getUniqueName()).thenReturn(uniqueName);
        return member;
    }

    private Set<String> tupleKeys(TupleList tupleList) {
        final Set<String> keys = new HashSet<String>();
        for (List<Member> tuple : tupleList) {
            keys.add(
                tuple.get(0).getUniqueName() + "|" + tuple.get(1).getUniqueName());
        }
        return keys;
    }

    private Set<String> expectedKeys() {
        final Set<String> keys = new HashSet<String>();
        Collections.addAll(
            keys,
            "[Brand].[All]|[Category].[All]",
            "[Brand].[All]|[Category].[C1]",
            "[Brand].[All]|[Category].[C2]",
            "[Brand].[B1]|[Category].[All]",
            "[Brand].[B1]|[Category].[C1]",
            "[Brand].[B1]|[Category].[C2]",
            "[Brand].[B2]|[Category].[All]",
            "[Brand].[B2]|[Category].[C1]");
        return keys;
    }
}
