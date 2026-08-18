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

import mondrian.calc.Calc;
import mondrian.olap.Annotation;
import mondrian.olap.Exp;
import mondrian.olap.MondrianProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The per-query {@code ResolvedQueryCache} inside {@link NativeSqlCalc}
 * only works if the same calc instance is reused for every cell of a
 * statement. These tests pin the statement-scoped memoization on
 * {@link RolapEvaluatorRoot} and the routing of both compiled-expression
 * call sites through it.
 */
public class NativeSqlCalcMemoizationTest {

    private boolean previousEnable;

    @BeforeEach public void enableNativeSql() {
        previousEnable = MondrianProperties.instance().NativeSqlEnable.get();
        MondrianProperties.instance().NativeSqlEnable.set(true);
    }

    @AfterEach public void restoreNativeSql() {
        MondrianProperties.instance().NativeSqlEnable.set(previousEnable);
    }

    @Test public void testSameCalcInstanceForAllCellsOfOneStatement() {
        final RolapCalculatedMember member = nativeSqlMember();
        final RolapEvaluatorRoot root = memoizingRoot();

        final Calc first = root.getNativeSqlCalc(member);
        final Calc second = root.getNativeSqlCalc(member);

        assertInstanceOf(NativeSqlCalc.class, first);
        assertSame(first, second);
    }

    @Test public void testDifferentStatementsGetDifferentCalcInstances() {
        final RolapCalculatedMember member = nativeSqlMember();
        final RolapEvaluatorRoot firstRoot = memoizingRoot();
        final RolapEvaluatorRoot secondRoot = memoizingRoot();

        assertNotSame(
            firstRoot.getNativeSqlCalc(member),
            secondRoot.getNativeSqlCalc(member));
    }

    @Test public void testNegativeResultMemoizedWithoutAnnotationReparse() {
        final RolapCalculatedMember member =
            mock(RolapCalculatedMember.class);
        when(member.getAnnotationMap())
            .thenReturn(new LinkedHashMap<String, Annotation>());
        final RolapEvaluatorRoot root = memoizingRoot();

        assertNull(root.getNativeSqlCalc(member));
        assertNull(root.getNativeSqlCalc(member));

        verify(member, times(1)).getAnnotationMap();
    }

    @Test public void testRolapMemberCalculationReturnsMemoizedCalc() {
        final RolapCalculatedMember member = nativeSqlMember();
        when(member.isEvaluated()).thenReturn(true);
        final RolapEvaluatorRoot root = mock(RolapEvaluatorRoot.class);
        final Calc memoized = mock(Calc.class);
        when(root.getNativeSqlCalc(member)).thenReturn(memoized);

        final RolapMemberCalculation calculation =
            new RolapMemberCalculation(member);

        assertSame(memoized, calculation.getCompiledExpression(root));
    }

    @Test public void testRolapCalculatedMemberReturnsMemoizedCalc() {
        final RolapCalculatedMember member =
            mock(RolapCalculatedMember.class);
        final RolapEvaluatorRoot root = mock(RolapEvaluatorRoot.class);
        final Calc memoized = mock(Calc.class);
        when(member.getCompiledExpression(root)).thenCallRealMethod();
        when(root.getNativeSqlCalc(member)).thenReturn(memoized);

        assertSame(memoized, member.getCompiledExpression(root));
    }

    /**
     * A mocked root whose {@code getNativeSqlCalc} runs the real
     * memoizing implementation. The real {@link RolapEvaluatorRoot}
     * constructor needs a full statement/schema graph, so tests reuse
     * the codebase's mock-based idiom instead.
     */
    private static RolapEvaluatorRoot memoizingRoot() {
        final RolapEvaluatorRoot root = mock(RolapEvaluatorRoot.class);
        when(root.getNativeSqlCalc(any(RolapMember.class)))
            .thenCallRealMethod();
        return root;
    }

    private static RolapCalculatedMember nativeSqlMember() {
        final RolapCalculatedMember member =
            mock(RolapCalculatedMember.class);
        when(member.getName()).thenReturn("WD %");
        when(member.getAnnotationMap()).thenReturn(nativeSqlAnnotations());
        when(member.getExpression()).thenReturn(mock(Exp.class));
        return member;
    }

    private static Map<String, Annotation> nativeSqlAnnotations() {
        final Map<String, Annotation> anns =
            new LinkedHashMap<String, Annotation>();
        anns.put("nativeSql.enabled", ann("true"));
        anns.put("nativeSql.template", ann("SELECT 1 AS val"));
        return anns;
    }

    private static Annotation ann(final String value) {
        return new Annotation() {
            @Override public String getName() {
                return null;
            }

            @Override public Object getValue() {
                return value;
            }
        };
    }
}
