/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import junit.framework.TestCase;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;

import java.util.Arrays;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RolapNativeCrossJoinGuardEstimateTest extends TestCase {

    public void testEstimateUpperBoundUsesApproxRowCountForLevelMembers() {
        final CrossJoinArg[] args = new CrossJoinArg[] {
            new DescendantsCrossJoinArg(level(120), null),
            new DescendantsCrossJoinArg(level(80), null)
        };

        assertEquals(9600L, RolapNativeCrossJoin.estimateUpperBoundCardinality(args));
    }

    public void testEstimateUpperBoundSupportsMixedMemberListAndLevelMembers() {
        final CrossJoinArg[] args = new CrossJoinArg[] {
            memberListArg(3),
            new DescendantsCrossJoinArg(level(40), null)
        };

        assertEquals(120L, RolapNativeCrossJoin.estimateUpperBoundCardinality(args));
    }

    public void testEstimateUpperBoundReturnsUnknownWhenApproxRowCountMissing() {
        final CrossJoinArg[] args = new CrossJoinArg[] {
            new DescendantsCrossJoinArg(level(Integer.MIN_VALUE), null),
            new DescendantsCrossJoinArg(level(80), null)
        };

        assertEquals(-1L, RolapNativeCrossJoin.estimateUpperBoundCardinality(args));
    }

    private static RolapLevel level(int approxRowCount) {
        final RolapLevel level = mock(RolapLevel.class);
        when(level.getApproxRowCount()).thenReturn(approxRowCount);
        return level;
    }

    private static MemberListCrossJoinArg memberListArg(int size) {
        final MemberListCrossJoinArg arg = mock(MemberListCrossJoinArg.class);
        final java.util.List<RolapMember> members = Arrays.asList(
            member(false),
            member(false),
            member(false)
        ).subList(0, size);
        when(arg.isEmptyCrossJoinArg()).thenReturn(false);
        when(arg.hasCalcMembers()).thenReturn(false);
        when(arg.hasAllMember()).thenReturn(false);
        when(arg.getMembers()).thenReturn(members);
        return arg;
    }

    private static RolapMember member(boolean all) {
        final RolapMember member = mock(RolapMember.class);
        when(member.isAll()).thenReturn(all);
        return member;
    }
}
