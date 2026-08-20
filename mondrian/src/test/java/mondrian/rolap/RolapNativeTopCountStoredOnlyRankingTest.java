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

import mondrian.mdx.MemberExpr;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Literal;
import mondrian.olap.Member;
import mondrian.olap.type.NumericType;
import mondrian.mdx.ResolvedFunCall;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * #86: native TopCount's measure-member-conflict veto is skipped when
 * the ranking expression references stored measures only — other
 * calculated measures on the query never enter the TopN SQL (same
 * carve-out as native Filter's NOT-IsEmpty path). These tests pin the
 * classifier that makes that decision.
 */
public class RolapNativeTopCountStoredOnlyRankingTest {

    private static MemberExpr storedMeasureExpr() {
        return new MemberExpr(
            mock(RolapBaseCubeMeasure.class));
    }

    private static MemberExpr calcMemberExpr() {
        return new MemberExpr(mock(RolapCalculatedMember.class));
    }

    private static MemberExpr dimensionMemberExpr() {
        return new MemberExpr(mock(RolapMember.class));
    }

    private static ResolvedFunCall fun(Exp... args) {
        return new ResolvedFunCall(
            mock(FunDef.class), args, new NumericType());
    }

    @Test public void testStoredMeasureIsStoredOnly() {
        assertTrue(
            RolapNativeTopCount.isStoredOnlyRanking(storedMeasureExpr()));
    }

    @Test public void testCalculatedMemberIsNotStoredOnly() {
        assertFalse(
            RolapNativeTopCount.isStoredOnlyRanking(calcMemberExpr()));
    }

    @Test public void testArithmeticOverStoredMeasuresIsStoredOnly() {
        assertTrue(
            RolapNativeTopCount.isStoredOnlyRanking(
                fun(
                    storedMeasureExpr(),
                    Literal.create(BigDecimal.TEN),
                    storedMeasureExpr())));
    }

    @Test public void testFunCallContainingCalcMemberIsNotStoredOnly() {
        assertFalse(
            RolapNativeTopCount.isStoredOnlyRanking(
                fun(storedMeasureExpr(), calcMemberExpr())));
    }

    @Test public void testTuplePinningDimensionMemberIsNotStoredOnly() {
        // Ranking by ([Sales], [Time].[2025]) pins a dimension member —
        // that pin CAN conflict with the slicer, so the veto must stay.
        assertFalse(
            RolapNativeTopCount.isStoredOnlyRanking(
                fun(storedMeasureExpr(), dimensionMemberExpr())));
    }

    @Test public void testNullRankingIsNotStoredOnly() {
        // TopCount without a ranking expression keeps the full veto.
        assertFalse(RolapNativeTopCount.isStoredOnlyRanking(null));
    }
}

// End RolapNativeTopCountStoredOnlyRankingTest.java
