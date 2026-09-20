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

import mondrian.calc.DummyExp;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Literal;
import mondrian.olap.type.EmptyType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * #88: {@code Head(Order(set, storedExpr, BDESC), N)} is the
 * hand-written spelling of {@code TopCount(set, N, storedExpr)} — the
 * rewrite lets it reach the native TopCount evaluator instead of
 * materializing the full set in Java (the 2026-08-19 &gt;300s runaway).
 * Any non-conforming piece fails closed (null = keep the Java path).
 */
public class RolapNativeTopCountHeadOrderRewriteTest {

    private final Exp set = new DummyExp(new EmptyType());
    private final Exp rankExpr = new DummyExp(new EmptyType());
    private final Literal count = Literal.create(BigDecimal.valueOf(200));

    private ResolvedFunCall order(Exp... args) {
        final FunDef orderFun = mock(FunDef.class);
        when(orderFun.getName()).thenReturn("Order");
        return new ResolvedFunCall(orderFun, args, new EmptyType());
    }

    @Test public void testBdescRewritesToTopCount() {
        RolapNativeTopCount.HeadOrderRewrite r =
            RolapNativeTopCount.rewriteHeadOrderToTopCount(
                new Exp[] {
                    order(set, rankExpr, Literal.createSymbol("BDESC")),
                    count});

        assertNotNull(r);
        assertEquals("TopCount", r.funName());
        assertSame(set, r.args()[0]);
        assertSame(count, r.args()[1]);
        assertSame(rankExpr, r.args()[2]);
    }

    /**
     * Java {@code Order(..., BASC)} sorts empty values first; native
     * BottomCount ranks non-empty values and pads empties last, so the
     * rewrite would return different members (see
     * RolapNativeTopCountHeadOrderParityTest).
     */
    @Test public void testBascFailsClosed() {
        assertNull(
            RolapNativeTopCount.rewriteHeadOrderToTopCount(
                new Exp[] {
                    order(set, rankExpr, Literal.createSymbol("BASC")),
                    count}));
    }

    @Test public void testHierarchicalDescFailsClosed() {
        // DESC sorts within parent groups — TopCount is break-hierarchy,
        // so the semantics differ and the rewrite must not fire.
        assertNull(
            RolapNativeTopCount.rewriteHeadOrderToTopCount(
                new Exp[] {
                    order(set, rankExpr, Literal.createSymbol("DESC")),
                    count}));
    }

    @Test public void testOrderWithoutDirectionFailsClosed() {
        // 2-arg Order defaults to ASC (hierarchical).
        assertNull(
            RolapNativeTopCount.rewriteHeadOrderToTopCount(
                new Exp[] {order(set, rankExpr), count}));
    }

    @Test public void testMultiKeyOrderFailsClosed() {
        assertNull(
            RolapNativeTopCount.rewriteHeadOrderToTopCount(
                new Exp[] {
                    order(
                        set, rankExpr, Literal.createSymbol("BDESC"),
                        rankExpr, Literal.createSymbol("BASC")),
                    count}));
    }

    @Test public void testNonLiteralCountFailsClosed() {
        assertNull(
            RolapNativeTopCount.rewriteHeadOrderToTopCount(
                new Exp[] {
                    order(set, rankExpr, Literal.createSymbol("BDESC")),
                    new DummyExp(new EmptyType())}));
    }

    @Test public void testNonOrderSetFailsClosed() {
        assertNull(
            RolapNativeTopCount.rewriteHeadOrderToTopCount(
                new Exp[] {set, count}));
    }

    @Test public void testSingleArgHeadFailsClosed() {
        assertNull(
            RolapNativeTopCount.rewriteHeadOrderToTopCount(
                new Exp[] {order(set, rankExpr,
                    Literal.createSymbol("BDESC"))}));
    }

    @Test public void testNonConformingHeadSkipsContextValidation() {
        final int[] validations = {0};
        final RolapNativeTopCount nativeTopCount = new RolapNativeTopCount() {
            @Override
            boolean isValidContext(
                RolapEvaluator evaluator, boolean checkMeasureConflicts)
            {
                validations[0]++;
                return true;
            }
        };
        nativeTopCount.setEnabled(true);
        final FunDef headFun = mock(FunDef.class);
        when(headFun.getName()).thenReturn("Head");

        assertNull(
            nativeTopCount.createEvaluator(
                null, headFun, new Exp[] {new DummyExp(new EmptyType()), count}));
        assertEquals(0, validations[0],
            "every Head(...) consults the registry; a non-conforming shape"
                + " must fail before the context walk");
    }

    @Test public void testCreateEvaluatorFailsClosedForNonConformingHead() {
        final RolapNativeTopCount nativeTopCount = new RolapNativeTopCount();
        nativeTopCount.setEnabled(true);
        final FunDef headFun = mock(FunDef.class);
        when(headFun.getName()).thenReturn("Head");

        assertNull(
            nativeTopCount.createEvaluator(
                null,
                headFun,
                new Exp[] {new DummyExp(new EmptyType()), count}));
    }
}

// End RolapNativeTopCountHeadOrderRewriteTest.java
