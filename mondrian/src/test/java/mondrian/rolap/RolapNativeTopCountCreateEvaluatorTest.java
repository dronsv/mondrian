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
import mondrian.calc.DummyExp;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Literal;
import mondrian.olap.MondrianProperties;
import mondrian.olap.type.EmptyType;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.CrossJoinArgFactory;

import java.math.BigDecimal;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RolapNativeTopCountCreateEvaluatorTest extends TestCase {

    private boolean previousEnableNativeTopCount;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        previousEnableNativeTopCount =
            MondrianProperties.instance().EnableNativeTopCount.get();
        MondrianProperties.instance().EnableNativeTopCount.set(true);
    }

    @Override
    protected void tearDown() throws Exception {
        MondrianProperties.instance().EnableNativeTopCount.set(
            previousEnableNativeTopCount);
        super.tearDown();
    }

    public void testCreateEvaluatorReturnsNullWhenCountIsZero() {
        final RolapNativeTopCount nativeTopCount =
            createTopCountWithSimpleSetArg();
        final FunDef topCountFun = mock(FunDef.class);
        when(topCountFun.getName()).thenReturn("TopCount");

        final Exp[] args = new Exp[] {
            new DummyExp(new EmptyType()),
            Literal.create(BigDecimal.ZERO)
        };

        assertNull(nativeTopCount.createEvaluator(null, topCountFun, args));
    }

    public void testCreateEvaluatorReturnsNullWhenCountIsNegative() {
        final RolapNativeTopCount nativeTopCount =
            createTopCountWithSimpleSetArg();
        final FunDef topCountFun = mock(FunDef.class);
        when(topCountFun.getName()).thenReturn("TopCount");

        final Exp[] args = new Exp[] {
            new DummyExp(new EmptyType()),
            Literal.create(BigDecimal.valueOf(-1))
        };

        assertNull(nativeTopCount.createEvaluator(null, topCountFun, args));
    }

    private RolapNativeTopCount createTopCountWithSimpleSetArg() {
        final CrossJoinArgFactory factory = mock(CrossJoinArgFactory.class);
        final CrossJoinArg cjArg = mock(CrossJoinArg.class);
        when(cjArg.isPreferInterpreter(false)).thenReturn(false);
        when(factory.checkCrossJoinArg(any(RolapEvaluator.class), any(Exp.class)))
            .thenReturn(Collections.singletonList(new CrossJoinArg[] {cjArg}));
        return new TestableRolapNativeTopCount(factory);
    }

    private static class TestableRolapNativeTopCount
        extends RolapNativeTopCount {
        private final CrossJoinArgFactory crossJoinArgFactory;

        TestableRolapNativeTopCount(CrossJoinArgFactory crossJoinArgFactory) {
            this.crossJoinArgFactory = crossJoinArgFactory;
        }

        @Override
        protected CrossJoinArgFactory crossJoinArgFactory() {
            return crossJoinArgFactory;
        }

        @Override
        boolean isValidContext(RolapEvaluator evaluator) {
            return true;
        }
    }
}
