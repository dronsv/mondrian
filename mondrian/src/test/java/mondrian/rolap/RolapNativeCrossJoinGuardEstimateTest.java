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
import mondrian.calc.IterCalc;
import mondrian.calc.TupleCursor;
import mondrian.calc.TupleIterable;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.DescendantsCrossJoinArg;
import mondrian.rolap.sql.MemberListCrossJoinArg;

import java.util.Arrays;
import java.util.HashMap;
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

    public void testEstimateRestrictedHierarchyCardinalityUsesSubcubeMembers() {
        final RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        final RolapLevel brandLevel = level(834, hierarchy);
        final DescendantsCrossJoinArg arg =
            new DescendantsCrossJoinArg(brandLevel, null);
        final HashMap<Member, Member> restrictedMembers =
            new HashMap<Member, Member>();
        restrictedMembers.put(member(brandLevel, false), member(brandLevel, false));
        restrictedMembers.put(member(brandLevel, false), member(brandLevel, false));
        restrictedMembers.put(member(level(10, hierarchy), false), member(level(10, hierarchy), false));

        final HashMap<Hierarchy, HashMap<Member, Member>> subcubeHierarchies =
            new HashMap<Hierarchy, HashMap<Member, Member>>();
        subcubeHierarchies.put(hierarchy, restrictedMembers);

        assertEquals(
            2L,
            RolapNativeCrossJoin.estimateRestrictedHierarchyCardinality(
                arg,
                subcubeHierarchies));
    }

    public void testEstimateUpperBoundUsesRestrictedHierarchyCardinalityWhenAvailable() {
        final RolapHierarchy brandHierarchy = mock(RolapHierarchy.class);
        final RolapHierarchy addressHierarchy = mock(RolapHierarchy.class);
        final RolapLevel brandLevel = level(834, brandHierarchy);
        final RolapLevel addressLevel = level(840, addressHierarchy);
        final CrossJoinArg[] args = new CrossJoinArg[] {
            new DescendantsCrossJoinArg(brandLevel, null),
            new DescendantsCrossJoinArg(addressLevel, null)
        };
        final HashMap<Hierarchy, HashMap<Member, Member>> subcubeHierarchies =
            new HashMap<Hierarchy, HashMap<Member, Member>>();
        subcubeHierarchies.put(
            brandHierarchy,
            restrictedMembers(brandLevel, 5));
        subcubeHierarchies.put(
            addressHierarchy,
            restrictedMembers(addressLevel, 1));

        assertEquals(
            5L,
            RolapNativeCrossJoin.estimateUpperBoundCardinalityWithSubcube(
                args,
                subcubeHierarchies));
    }

    public void testEstimateRestrictedHierarchyCardinalityUsesSubcubeCalc() {
        final RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        final RolapLevel brandLevel = level(834, hierarchy);
        final DescendantsCrossJoinArg arg =
            new DescendantsCrossJoinArg(brandLevel, null);
        final RolapEvaluator evaluator = mock(RolapEvaluator.class);
        final IterCalc calc = mock(IterCalc.class);
        final TupleIterable iterable = mock(TupleIterable.class);
        final TupleCursor cursor = mock(TupleCursor.class);
        final HashMap<Hierarchy, mondrian.calc.Calc> calcs =
            new HashMap<Hierarchy, mondrian.calc.Calc>();
        calcs.put(hierarchy, calc);
        when(calc.evaluateIterable(evaluator)).thenReturn(iterable);
        when(iterable.tupleCursor()).thenReturn(cursor);
        final Member member1 = member(brandLevel, false);
        final Member member2 = member(brandLevel, false);
        when(cursor.forward()).thenReturn(true, true, false);
        when(cursor.member(0)).thenReturn(member1, member2);

        assertEquals(
            2L,
            RolapNativeCrossJoin.estimateRestrictedHierarchyCardinality(
                arg,
                calcs,
                evaluator));
    }

    public void testEstimateUpperBoundUsesSubcubeCalcWhenAvailable() {
        final RolapHierarchy brandHierarchy = mock(RolapHierarchy.class);
        final RolapHierarchy addressHierarchy = mock(RolapHierarchy.class);
        final RolapLevel brandLevel = level(834, brandHierarchy);
        final RolapLevel addressLevel = level(840, addressHierarchy);
        final CrossJoinArg[] args = new CrossJoinArg[] {
            new DescendantsCrossJoinArg(brandLevel, null),
            new DescendantsCrossJoinArg(addressLevel, null)
        };
        final RolapEvaluator evaluator = mock(RolapEvaluator.class);
        final HashMap<Hierarchy, mondrian.calc.Calc> calcs =
            new HashMap<Hierarchy, mondrian.calc.Calc>();
        calcs.put(
            brandHierarchy,
            iterCalcForMembers(member(brandLevel, false)));
        calcs.put(
            addressHierarchy,
            iterCalcForMembers(member(addressLevel, false)));

        assertEquals(
            1L,
            RolapNativeCrossJoin.estimateRestrictedHierarchyCardinality(
                (DescendantsCrossJoinArg) args[0],
                calcs,
                evaluator)
                * RolapNativeCrossJoin.estimateRestrictedHierarchyCardinality(
                    (DescendantsCrossJoinArg) args[1],
                    calcs,
                    evaluator));
    }

    public void testEstimateRestrictedHierarchyCardinalityIgnoresCalcFailures() {
        final RolapHierarchy hierarchy = mock(RolapHierarchy.class);
        final RolapLevel brandLevel = level(834, hierarchy);
        final DescendantsCrossJoinArg arg =
            new DescendantsCrossJoinArg(brandLevel, null);
        final RolapEvaluator evaluator = mock(RolapEvaluator.class);
        final IterCalc calc = mock(IterCalc.class);
        final HashMap<Hierarchy, mondrian.calc.Calc> calcs =
            new HashMap<Hierarchy, mondrian.calc.Calc>();
        calcs.put(hierarchy, calc);
        when(calc.evaluateIterable(evaluator))
            .thenThrow(new RuntimeException("subcube calc failed"));

        assertEquals(
            -1L,
            RolapNativeCrossJoin.estimateRestrictedHierarchyCardinality(
                arg,
                calcs,
                evaluator));
    }

    private static RolapLevel level(int approxRowCount) {
        return level(approxRowCount, mock(RolapHierarchy.class));
    }

    private static RolapLevel level(int approxRowCount, RolapHierarchy hierarchy) {
        final RolapLevel level = mock(RolapLevel.class);
        when(level.getApproxRowCount()).thenReturn(approxRowCount);
        when(level.getHierarchy()).thenReturn(hierarchy);
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
        return member(mock(RolapLevel.class), all);
    }

    private static RolapMember member(RolapLevel level, boolean all) {
        final RolapMember member = mock(RolapMember.class);
        when(member.isAll()).thenReturn(all);
        when(member.getLevel()).thenReturn(level);
        return member;
    }

    private static HashMap<Member, Member> restrictedMembers(
        RolapLevel level,
        int count)
    {
        final HashMap<Member, Member> members = new HashMap<Member, Member>();
        for (int i = 0; i < count; i++) {
            members.put(member(level, false), member(level, false));
        }
        return members;
    }

    private static IterCalc iterCalcForMembers(final Member... members) {
        final IterCalc calc = mock(IterCalc.class);
        final TupleIterable iterable = mock(TupleIterable.class);
        final TupleCursor cursor = mock(TupleCursor.class);
        when(calc.evaluateIterable(org.mockito.ArgumentMatchers.any(RolapEvaluator.class)))
            .thenReturn(iterable);
        when(iterable.tupleCursor()).thenReturn(cursor);
        final Boolean[] forwards = new Boolean[members.length + 1];
        Arrays.fill(forwards, 0, members.length, Boolean.TRUE);
        forwards[members.length] = Boolean.FALSE;
        when(cursor.forward()).thenReturn(forwards[0], Arrays.copyOfRange(forwards, 1, forwards.length));
        when(cursor.member(0)).thenReturn(
            members[0],
            Arrays.copyOfRange(members, 1, members.length));
        return calc;
    }
}
