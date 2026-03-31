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
import mondrian.mdx.MdxVisitor;
import mondrian.mdx.MemberExpr;
import mondrian.olap.Evaluator;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.NativeEvaluator;
import mondrian.olap.Query;
import mondrian.olap.type.EmptyType;
import mondrian.rolap.sql.CrossJoinArg;
import mondrian.rolap.sql.CrossJoinArgFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RolapNativeFilterVirtualCubeTest extends TestCase {

    public void testCollectLevelsSkipsNullCrossJoinEntries() {
        final CrossJoinArg withLevel = mock(CrossJoinArg.class);
        final RolapLevel level = mock(RolapLevel.class);
        final CrossJoinArg withoutLevel = mock(CrossJoinArg.class);

        when(withLevel.getLevel()).thenReturn(level);
        when(withoutLevel.getLevel()).thenReturn(null);

        final mondrian.olap.Level[] levels =
            RolapNativeFilter.collectLevels(
                new CrossJoinArg[] {withLevel, null, withoutLevel});

        assertEquals(1, levels.length);
        assertSame(level, levels[0]);
    }

    public void testCreateEvaluatorExtractsCrossJoinArgsBeforeContextValidation() {
        final CrossJoinArg cjArg = mock(CrossJoinArg.class);
        when(cjArg.isPreferInterpreter(false)).thenReturn(false);
        final CrossJoinArgFactory factory = mock(CrossJoinArgFactory.class);
        when(factory.checkCrossJoinArg(any(RolapEvaluator.class), any(Exp.class)))
            .thenReturn(Collections.singletonList(new CrossJoinArg[] {cjArg}));

        final TestableRolapNativeFilter nativeFilter =
            new TestableRolapNativeFilter(factory);
        final FunDef filterFun = mock(FunDef.class);
        when(filterFun.getName()).thenReturn("Filter");

        final Exp[] args = new Exp[] {
            new DummyExp(new EmptyType()),
            new DummyExp(new EmptyType())
        };

        final NativeEvaluator evaluatorResult =
            nativeFilter.createEvaluator(mock(RolapEvaluator.class), filterFun, args);

        assertNull(evaluatorResult);
        assertNotNull(nativeFilter.capturedArgs);
        assertEquals(1, nativeFilter.capturedArgs.length);
        assertSame(cjArg, nativeFilter.capturedArgs[0]);
    }

    public void testResolveConstraintBaseCubeUsesStoredMeasureFromFilterExpr() {
        final Evaluator evaluator = mock(Evaluator.class);
        final RolapCube virtualCube = mock(RolapCube.class);
        final RolapCube filterCube = mock(RolapCube.class);
        final RolapCube otherCube = mock(RolapCube.class);
        final Query query = mock(Query.class);
        final RolapStoredMeasure measure = mock(RolapStoredMeasure.class);

        when(evaluator.getCube()).thenReturn(virtualCube);
        when(evaluator.getQuery()).thenReturn(query);
        when(virtualCube.isVirtual()).thenReturn(true);
        when(measure.getCube()).thenReturn(filterCube);
        when(query.getBaseCubes()).thenReturn(Arrays.asList(otherCube, filterCube));

        assertSame(
            filterCube,
            RolapNativeFilter.FilterConstraint.resolveConstraintBaseCube(
                evaluator,
                new MemberExpr(measure)));
    }

    public void testResolveConstraintBaseCubeFallsBackToQueryBaseCube() {
        final Evaluator evaluator = mock(Evaluator.class);
        final RolapCube virtualCube = mock(RolapCube.class);
        final RolapCube baseCube = mock(RolapCube.class);
        final Query query = mock(Query.class);
        final Exp filterExpr = mock(Exp.class);

        when(evaluator.getCube()).thenReturn(virtualCube);
        when(evaluator.getQuery()).thenReturn(query);
        when(virtualCube.isVirtual()).thenReturn(true);
        when(query.getBaseCubes()).thenReturn(Collections.singletonList(baseCube));
        when(filterExpr.accept(any(MdxVisitor.class))).thenReturn(null);

        assertSame(
            baseCube,
            RolapNativeFilter.FilterConstraint.resolveConstraintBaseCube(
                evaluator,
                filterExpr));
    }

    public void testResolveStoredMeasureFromExpressionUsesStoredMeasureFromPredicate() {
        final RolapStoredMeasure sales = mock(RolapStoredMeasure.class);

        assertSame(
            sales,
            RolapNativeFilter.FilterConstraint.resolveStoredMeasureFromExpression(
                new MemberExpr(sales)));
    }

    public void testResolveStoredMeasureFromExpressionReturnsNullForMixedCubes() {
        final RolapStoredMeasure sales = mock(RolapStoredMeasure.class);
        final RolapStoredMeasure wd = mock(RolapStoredMeasure.class);
        final RolapCube salesCube = mock(RolapCube.class);
        final RolapCube wdCube = mock(RolapCube.class);

        when(sales.getCube()).thenReturn(salesCube);
        when(wd.getCube()).thenReturn(wdCube);

        final Exp filterExpr = new DummyExp(new EmptyType()) {
            public Object accept(MdxVisitor visitor) {
                new MemberExpr(sales).accept(visitor);
                new MemberExpr(wd).accept(visitor);
                return null;
            }
        };

        assertNull(
            RolapNativeFilter.FilterConstraint.resolveStoredMeasureFromExpression(
                filterExpr));
    }

    private static class TestableRolapNativeFilter extends RolapNativeFilter {
        private final CrossJoinArgFactory crossJoinArgFactory;
        private CrossJoinArg[] capturedArgs;

        TestableRolapNativeFilter(CrossJoinArgFactory crossJoinArgFactory) {
            this.crossJoinArgFactory = crossJoinArgFactory;
        }

        @Override
        protected CrossJoinArgFactory crossJoinArgFactory() {
            return crossJoinArgFactory;
        }

        @Override
        boolean isValidContext(RolapEvaluator evaluator, CrossJoinArg[] cjArgs) {
            capturedArgs = cjArgs;
            return false;
        }
    }
}
